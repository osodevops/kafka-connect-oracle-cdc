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
package sh.oso.connect.oracle.e2e.regression;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * Invariant: the committed offset never counts a change Kafka has not acknowledged, so stopping the
 * connector while a large transaction is being delivered and resuming it delivers the rest of the
 * transaction: every insert, every delete and every delete's tombstone (a change that becomes two
 * records is counted as delivered only with its last record). In Debezium the saved event count ran
 * ahead of what reached Kafka and the restart skipped the tail of the transaction. The T0 cases are
 * in {@code OracleCdcSourceTaskTest}, tagged with the same issue.
 *
 * @see <a href="https://github.com/debezium/dbz/issues/2544">dbz#2544</a>
 */
@Tag("connector")
@Tag("dbz-2544")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NeverSkipsUnacknowledgedEventsOnRestartConnectorIT {

  static final String CONNECTOR_CLASS = "sh.oso.connect.oracle.OracleCdcSourceConnector";
  static final String PREFIX = "tail";
  static final int ROWS = 20000;
  static final int DELETED = 2000;

  private final OracleTestDatabase db = OracleTestDatabase.get();
  private final String schema = SchemaFixtures.nameFor(getClass());
  private ConnectCluster cluster;

  @BeforeAll
  void up() throws Exception {
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Statement s = w.createStatement()) {
      s.execute("CREATE TABLE big (id NUMBER PRIMARY KEY, v VARCHAR2(20))");
      s.execute("ALTER TABLE big ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
    }
    cluster = new ConnectCluster().start();
  }

  @AfterAll
  void down() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
    SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
  }

  @Test
  void aStopPartWayThroughALargeTransactionLosesNothingOfIt() throws Exception {
    Map<String, String> c = new HashMap<>(ConnectCluster.oracleDatabaseProps("FREEPDB1"));
    c.put("connector.class", CONNECTOR_CLASS);
    c.put("tasks.max", "1");
    c.put("cdc.topic.prefix", PREFIX);
    c.put("cdc.tables.include", "FREEPDB1\\." + schema + "\\.BIG");
    c.put("cdc.snapshot.mode", "none");
    c.put("cdc.decimal.mode", "string");
    c.put("cdc.poll.max.records", "50");
    c.put("cdc.poll.linger.ms", "100");
    c.put("cdc.heartbeat.interval.ms", "1000");
    cluster.register("tail", c);
    cluster.awaitRunning("tail", Duration.ofMinutes(2));
    cluster.awaitOffsets("tail", Duration.ofSeconds(60)); // the start position is durable

    try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Statement s = w.createStatement()) {
      w.setAutoCommit(false);
      s.execute("INSERT INTO big SELECT LEVEL, 'v' || LEVEL FROM dual CONNECT BY LEVEL <= " + ROWS);
      s.execute("DELETE FROM big WHERE id <= " + DELETED);
      w.commit();
    }

    String topic = PREFIX + ".FREEPDB1." + schema + ".BIG";
    Set<Integer> created = new TreeSet<>();
    Set<Integer> deleted = new TreeSet<>();
    Set<Integer> tombstones = new TreeSet<>();
    long records = 0;
    String stoppedAt;
    try (KafkaConsumer<String, String> consumer = cluster.consumer("tail-reader", topic)) {
      // stop as soon as the transaction starts to arrive, while most of it is still queued
      long deadline = System.currentTimeMillis() + Duration.ofMinutes(3).toMillis();
      while (records == 0 && System.currentTimeMillis() < deadline) {
        for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(200))) {
          records++;
          record(r, created, deleted, tombstones);
        }
      }
      assertThat(records).as("the transaction started to arrive").isPositive();
      cluster.lifecycle("tail", "stop");
      cluster.awaitTaskState("tail", "STOPPED", Duration.ofMinutes(2));
      JsonNode offset =
          cluster
              .awaitOffsets("tail", Duration.ofSeconds(30))
              .path("offsets")
              .get(0)
              .path("offset");
      stoppedAt = "event_index " + offset.path("event_index").asText() + " of " + (ROWS + DELETED);
      cluster.lifecycle("tail", "resume");
      cluster.awaitRunning("tail", Duration.ofMinutes(2));
      deadline = System.currentTimeMillis() + Duration.ofMinutes(6).toMillis();
      long lastNew = System.currentTimeMillis();
      while (System.currentTimeMillis() < deadline
          && !(created.size() == ROWS
              && tombstones.size() == DELETED
              && System.currentTimeMillis() - lastNew > 10_000)) {
        for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
          records++;
          lastNew = System.currentTimeMillis();
          record(r, created, deleted, tombstones);
        }
      }
    }
    assertThat(created).as("inserts").hasSize(ROWS).contains(1, ROWS);
    assertThat(deleted).as("deletes").hasSize(DELETED).contains(1, DELETED);
    assertThat(tombstones).as("tombstones").isEqualTo(deleted);
    System.out.println(
        "dbz-2544: stopped at "
            + stoppedAt
            + ", "
            + records
            + " records for "
            + (ROWS + 2 * DELETED)
            + " expected (duplicates after the restart allowed)");
  }

  private static void record(
      ConsumerRecord<String, String> r,
      Set<Integer> created,
      Set<Integer> deleted,
      Set<Integer> tombstones)
      throws Exception {
    int key = Integer.parseInt(ConnectCluster.json(r.key()).path("ID").asText());
    if (r.value() == null) {
      tombstones.add(key);
      return;
    }
    String op = ConnectCluster.json(r.value()).path("op").asText();
    if ("c".equals(op)) {
      created.add(key);
    } else if ("d".equals(op)) {
      deleted.add(key);
    }
  }
}
