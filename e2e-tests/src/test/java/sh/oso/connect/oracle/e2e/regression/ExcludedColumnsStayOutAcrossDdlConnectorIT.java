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
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
 * Invariant: {@code cdc.columns.exclude} (SRC-SEL-2) keeps its columns out of snapshot and change
 * records across ALTER TABLE: a column the DDL adds that matches a pattern is excluded from its
 * first row, columns added or dropped around it flow as usual, no change is lost, and validation
 * refuses a pattern that matches a key column. Debezium's column filter broke on rows whose names a
 * DDL had made unresolvable; the replayed-row case is {@code
 * ExcludesColumnsOfRowsReplayedAfterDdlEngineIT}.
 *
 * @see <a href="https://github.com/debezium/dbz/issues/1599">dbz#1599</a>
 */
@Tag("connector")
@Tag("dbz-1599")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ExcludedColumnsStayOutAcrossDdlConnectorIT {

  static final String CONNECTOR_CLASS = "sh.oso.connect.oracle.OracleCdcSourceConnector";
  static final String PREFIX = "colx";

  private final OracleTestDatabase db = OracleTestDatabase.get();
  private final String schema = SchemaFixtures.nameFor(getClass());
  private ConnectCluster cluster;

  @BeforeAll
  void up() throws Exception {
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Statement s = w.createStatement()) {
      s.execute("CREATE TABLE cust (id NUMBER PRIMARY KEY, name VARCHAR2(30), ssn VARCHAR2(20))");
      s.execute("ALTER TABLE cust ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      for (int i = 1; i <= 50; i++) {
        s.execute("INSERT INTO cust VALUES (" + i + ", 'n" + i + "', 'SECRET-" + i + "')");
      }
    }
    Thread.sleep(3500); // flashback reads need the SCN-to-time mapping past the CREATE TABLE
    cluster = new ConnectCluster().start();
  }

  @AfterAll
  void down() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
    SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
  }

  private Map<String, String> connector(String exclude) {
    Map<String, String> c = new HashMap<>(ConnectCluster.oracleDatabaseProps("FREEPDB1"));
    c.put("connector.class", CONNECTOR_CLASS);
    c.put("tasks.max", "1");
    c.put("cdc.topic.prefix", PREFIX);
    c.put("cdc.tables.include", "FREEPDB1\\." + schema + "\\.CUST");
    c.put("cdc.columns.exclude", exclude);
    c.put("cdc.decimal.mode", "string");
    c.put("cdc.poll.linger.ms", "100");
    c.put("cdc.heartbeat.interval.ms", "1000");
    c.put("cdc.kafka.bootstrap.servers", "kafka:19092");
    c.put("cdc.snapshot.chunk.rows", "1000");
    return c;
  }

  @Test
  void excludedColumnsNeverReachKafkaAcrossAlterTable() throws Exception {
    // validation names a key column the patterns would exclude
    JsonNode validation =
        cluster.validate(CONNECTOR_CLASS, connector("FREEPDB1\\." + schema + "\\.CUST\\.(ID|SSN)"));
    List<String> errors = new ArrayList<>();
    for (JsonNode cfg : validation.path("configs")) {
      if ("cdc.columns.exclude".equals(cfg.path("value").path("name").asText())) {
        cfg.path("value").path("errors").forEach(e -> errors.add(e.asText()));
      }
    }
    assertThat(errors).as(validation.toString()).anyMatch(e -> e.contains("ID"));

    cluster.register("colx", connector("FREEPDB1\\." + schema + "\\.CUST\\.SSN.*"));
    cluster.awaitRunning("colx", Duration.ofMinutes(2));
    String topic = PREFIX + ".FREEPDB1." + schema + ".CUST";
    List<ConsumerRecord<String, String>> all = new ArrayList<>();
    try (KafkaConsumer<String, String> consumer = cluster.consumer("colx-reader", topic)) {
      all.addAll(
          ConnectCluster.consume(consumer, 50, Duration.ofMinutes(3), Duration.ofSeconds(5)));
      assertThat(all).as("the snapshot").hasSizeGreaterThanOrEqualTo(50);
      try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
        for (int i = 51; i <= 60; i++) {
          exec(w, "INSERT INTO cust VALUES (" + i + ", 'n" + i + "', 'SECRET-" + i + "')");
        }
        exec(w, "ALTER TABLE cust ADD (ssn_hash VARCHAR2(64))");
        for (int i = 61; i <= 70; i++) {
          exec(
              w,
              "INSERT INTO cust VALUES ("
                  + i
                  + ", 'n"
                  + i
                  + "', 'SECRET-"
                  + i
                  + "', 'HASH-"
                  + i
                  + "')");
        }
        exec(w, "ALTER TABLE cust ADD (note VARCHAR2(20))");
        for (int i = 71; i <= 80; i++) {
          exec(
              w,
              "INSERT INTO cust VALUES ("
                  + i
                  + ", 'n"
                  + i
                  + "', 'SECRET-"
                  + i
                  + "', 'HASH-"
                  + i
                  + "', 'note"
                  + i
                  + "')");
        }
        exec(w, "UPDATE cust SET ssn = 'SECRET-NEW', name = 'renamed' WHERE id <= 10");
        exec(w, "ALTER TABLE cust DROP COLUMN note");
        for (int i = 81; i <= 90; i++) {
          exec(w, "INSERT INTO cust (id, name, ssn) VALUES (" + i + ", 'n" + i + "', 'SECRET')");
        }
        exec(w, "DELETE FROM cust WHERE id = 90");
      }
      // 50 read, 40 created, 10 updated, one delete and its tombstone
      all.addAll(
          ConnectCluster.consume(consumer, 52, Duration.ofMinutes(4), Duration.ofSeconds(10)));
    }
    Set<Integer> created = new TreeSet<>();
    Set<Integer> updated = new TreeSet<>();
    Set<Integer> deleted = new TreeSet<>();
    Map<Integer, String> notes = new HashMap<>();
    for (ConsumerRecord<String, String> r : all) {
      if (r.value() == null) {
        continue; // tombstone
      }
      assertThat(r.value()).doesNotContain("SECRET", "HASH-");
      JsonNode v = ConnectCluster.json(r.value());
      for (String image : List.of("before", "after")) {
        assertThat(v.path(image).has("SSN") || v.path(image).has("SSN_HASH"))
            .as("an excluded field in %s of %s", image, r.value())
            .isFalse();
      }
      String op = v.path("op").asText();
      JsonNode img = "d".equals(op) ? v.path("before") : v.path("after");
      int id = Integer.parseInt(img.path("ID").asText());
      switch (op) {
        case "r", "c" -> created.add(id);
        case "u" -> {
          updated.add(id);
          assertThat(img.path("NAME").asText()).isEqualTo("renamed");
        }
        case "d" -> deleted.add(id);
        default -> throw new AssertionError("unexpected op " + op);
      }
      if (img.hasNonNull("NOTE")) {
        notes.put(id, img.path("NOTE").asText());
      }
    }
    Set<Integer> expected = new TreeSet<>();
    for (int i = 1; i <= 90; i++) {
      expected.add(i);
    }
    assertThat(created).isEqualTo(expected);
    assertThat(updated).containsExactly(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    assertThat(deleted).containsExactly(90);
    assertThat(notes).hasSize(10).containsEntry(71, "note71").containsEntry(80, "note80");
    System.out.println("dbz-1599: " + all.size() + " records, none with an excluded column");
  }

  private static void exec(Connection c, String sql) throws SQLException {
    try (Statement s = c.createStatement()) {
      s.execute(sql);
    }
  }
}
