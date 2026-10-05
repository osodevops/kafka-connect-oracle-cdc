/*
 * Copyright 2026 OSO DevOps Ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sh.oso.connect.oracle.e2e.connector;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * P1-14 (CORE-TX-4, CORE-TX-5, ADR-0003): a transaction open across a worker kill is journaled, the
 * committed position moves past its start, the archived log holding that start is deleted, and
 * after the restart the transaction's commit still delivers every event exactly once. The journal
 * topic ends with tombstones for the transaction.
 */
@Tag("connector")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class JournaledTransactionConnectorIT {

  static final String PREFIX = "jt";

  private final OracleTestDatabase db = OracleTestDatabase.get();
  private final String schema = SchemaFixtures.nameFor(getClass());
  private ConnectCluster cluster;

  @BeforeAll
  void up() throws Exception {
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Statement s = w.createStatement()) {
      s.execute("CREATE TABLE jt (id NUMBER(9) PRIMARY KEY, v VARCHAR2(100))");
      s.execute("ALTER TABLE jt ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
    }
    cluster = new ConnectCluster().start();
  }

  @AfterAll
  void down() throws Exception {
    OracleSql.restoreHiddenLogs(db);
    if (cluster != null) {
      cluster.close();
    }
    SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
  }

  @Test
  void aJournaledTransactionCommitsOnceAfterItsStartWasPurgedAndTheWorkerKilled() throws Exception {
    Map<String, String> c = new HashMap<>(ConnectCluster.oracleDatabaseProps("FREEPDB1"));
    c.put("connector.class", FirstRecordConnectorIT.CONNECTOR_CLASS);
    c.put("tasks.max", "1");
    c.put("cdc.topic.prefix", PREFIX);
    c.put("cdc.tables.include", "FREEPDB1\\." + schema + "\\.JT");
    c.put("cdc.poll.linger.ms", "100");
    c.put("cdc.heartbeat.interval.ms", "500");
    c.put("cdc.kafka.bootstrap.servers", "kafka:19092");
    c.put("cdc.txjournal.threshold.events", "5");
    c.put("cdc.txjournal.threshold.ms", "3600000");
    cluster.register("journal", c);
    cluster.awaitRunning("journal", Duration.ofMinutes(2));
    cluster.awaitOffsets("journal", Duration.ofSeconds(60));

    try (Connection root = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection a = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Connection b = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      a.setAutoCommit(false);
      long before = OracleSql.currentScn(root);
      try (Statement s = a.createStatement()) {
        for (int i = 1; i <= 4; i++) {
          s.execute("INSERT INTO jt VALUES (" + i + ", 'open-" + i + "')");
        }
      }
      long afterFirstChanges = OracleSql.currentScn(root);
      String xidA = localTransactionId(a);
      assertThat(xidA).matches("\\d+\\.\\d+\\.\\d+");
      // other work commits and the log holding the start of A is archived
      try (Statement s = b.createStatement()) {
        s.execute("INSERT INTO jt VALUES (101, 'other-1')");
      }
      OracleSql.archiveLogCurrent(db);
      try (Statement s = a.createStatement()) {
        for (int i = 5; i <= 8; i++) {
          s.execute("INSERT INTO jt VALUES (" + i + ", 'open-" + i + "')");
        }
      }
      try (Statement s = b.createStatement()) {
        s.execute("INSERT INTO jt VALUES (102, 'other-2')");
      }
      OracleSql.archiveLogCurrent(db);

      // the committed position passes the start of A while A is still open
      long resume = awaitResumePast("journal", afterFirstChanges, Duration.ofMinutes(2));
      int chunks = chunksFor(xidA);
      if (chunks == 0) {
        dump(PREFIX + ".cdc.txjournal", 20);
        dump(PREFIX + ".cdc.heartbeat", 6);
        dump(PREFIX + ".cdc.ops", 10);
        dump(PREFIX + ".FREEPDB1." + schema + ".JT", 20);
        System.out.println(
            "diagnose: xidA="
                + xidA
                + " resume="
                + resume
                + " afterFirstChanges="
                + afterFirstChanges
                + " before="
                + before);
      }
      assertThat(chunks).as("A is in the journal topic").isPositive();

      // the archived logs that end before the resume SCN are no longer needed: delete them
      List<String> deleted = new ArrayList<>();
      try (Statement s = root.createStatement();
          ResultSet rs =
              s.executeQuery(
                  "SELECT name FROM v$archived_log WHERE dest_id = 1 AND name IS NOT NULL AND"
                      + " deleted = 'NO' AND next_change# > "
                      + before
                      + " AND next_change# <= "
                      + resume)) {
        while (rs.next()) {
          deleted.add(rs.getString(1));
        }
      }
      assertThat(deleted)
          .as("a log holding the start of A ended before the resume SCN")
          .isNotEmpty();
      for (String f : deleted) {
        OracleSql.hideArchivedLog(db, f);
      }

      cluster.killAndRestartWorker();
      cluster.awaitRunning("journal", Duration.ofMinutes(3));
      try (Statement s = a.createStatement()) {
        s.execute("INSERT INTO jt VALUES (9, 'open-9')");
        s.execute("INSERT INTO jt VALUES (10, 'open-10')");
      }
      a.commit();
      try (Statement s = b.createStatement()) {
        s.execute("INSERT INTO jt VALUES (103, 'other-3')");
      }

      String topic = PREFIX + ".FREEPDB1." + schema + ".JT";
      try (KafkaConsumer<String, String> consumer = cluster.consumer("journal-data", topic)) {
        List<ConsumerRecord<String, String>> records =
            ConnectCluster.consume(consumer, 13, Duration.ofMinutes(3), Duration.ofSeconds(8));
        List<Integer> aIds = new ArrayList<>();
        List<Integer> aIndex = new ArrayList<>();
        int others = 0;
        for (ConsumerRecord<String, String> r : records) {
          JsonNode v = ConnectCluster.json(r.value());
          if (xidA.equals(v.path("source").path("txId").asText())) {
            aIds.add(v.path("after").path("ID").asInt());
            aIndex.add(
                Integer.parseInt(
                    new String(
                        r.headers().lastHeader("cdc.event_index").value(),
                        StandardCharsets.UTF_8)));
            assertThat(v.path("transaction").path("total_order").asInt()).isEqualTo(aIds.size());
          } else {
            others++;
          }
        }
        assertThat(aIds)
            .as("every event of A once, in order")
            .containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        assertThat(aIndex)
            .as("cdc.event_index is zero-based")
            .containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        assertThat(others).as("the other session's three commits").isEqualTo(3);
        assertThat(records).hasSize(13);
      }
      // the journal forgets A once its commit is delivered
      long deadline = System.currentTimeMillis() + Duration.ofMinutes(1).toMillis();
      while (chunksFor(xidA) > 0 && System.currentTimeMillis() < deadline) {
        Thread.sleep(1000);
      }
      assertThat(chunksFor(xidA)).as("tombstones replaced every chunk of A").isZero();
      JsonNode offsets = cluster.awaitOffsets("journal", Duration.ofSeconds(30));
      assertThat(offsets.path("offsets").get(0).path("offset").path("journal_generation").asLong())
          .as("the second task start carries generation 2")
          .isEqualTo(2);
    }
  }

  private long awaitResumePast(String name, long scn, Duration timeout) throws Exception {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    long last = -1;
    while (System.currentTimeMillis() < deadline) {
      JsonNode offsets = cluster.awaitOffsets(name, Duration.ofSeconds(30));
      last = offsets.path("offsets").get(0).path("offset").path("resume_scn").asLong();
      if (last > scn) {
        return last;
      }
      Thread.sleep(500);
    }
    throw new AssertionError(
        "resume_scn stayed at " + last + ", not past " + scn + " within " + timeout);
  }

  /** Live chunk records for a transaction: the last record per key, ignoring tombstoned keys. */
  private int chunksFor(String xid) {
    Map<String, Boolean> live = new LinkedHashMap<>();
    try (KafkaConsumer<String, String> consumer =
        cluster.consumer("journal-check-" + System.nanoTime(), PREFIX + ".cdc.txjournal")) {
      for (ConsumerRecord<String, String> r :
          ConnectCluster.consume(consumer, 1, Duration.ofSeconds(15), Duration.ofSeconds(2))) {
        JsonNode key = ConnectCluster.json(r.key());
        JsonNode payload = key.has("payload") ? key.path("payload") : key;
        if (!xid.equals(payload.path("xid").asText())) {
          continue;
        }
        String id = payload.path("chunk").asText() + "@" + payload.path("generation").asText();
        live.put(id, r.value() != null);
      }
    } catch (java.io.IOException e) {
      throw new IllegalStateException(e);
    }
    return (int) live.values().stream().filter(Boolean::booleanValue).count();
  }

  /** The session's XID as usn.slot.sqn, the form the envelope's txId uses. */
  private static String localTransactionId(Connection c) throws Exception {
    try (Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("SELECT DBMS_TRANSACTION.LOCAL_TRANSACTION_ID FROM dual")) {
      rs.next();
      return rs.getString(1);
    }
  }

  private void dump(String topic, int last) {
    try (KafkaConsumer<String, String> consumer =
        cluster.consumer("dump-" + System.nanoTime(), topic)) {
      List<ConsumerRecord<String, String>> all =
          ConnectCluster.consume(consumer, 1, Duration.ofSeconds(15), Duration.ofSeconds(2));
      System.out.println("diagnose topic " + topic + ": " + all.size() + " records");
      all.stream()
          .skip(Math.max(0, all.size() - last))
          .forEach(
              r ->
                  System.out.println(
                      "  key="
                          + r.key()
                          + " value="
                          + (r.value() == null
                              ? "null"
                              : r.value().length() > 300
                                  ? r.value().substring(0, 300) + "..."
                                  : r.value())));
    }
  }
}
