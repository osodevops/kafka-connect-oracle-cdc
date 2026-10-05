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
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
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
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * ADR-0002 and SRC-SEL-4 through a real worker: one connector mines once at the root for two PDBs
 * holding tables of the same name, snapshots both, routes every change to its PDB's topic, and
 * picks up a table created with rows in one PDB without a restart.
 */
@Tag("connector")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MultiPdbConnectorIT {

  static final String PREFIX = "mpdb";
  static final String NAME = "multi-pdb";

  private final OracleTestDatabase db = OracleTestDatabase.get();
  private final String schema = SchemaFixtures.nameFor(getClass());
  private ConnectCluster cluster;

  @BeforeAll
  void up() throws Exception {
    for (String pdb : List.of(OracleTestDatabase.PDB1, OracleTestDatabase.PDB2)) {
      SchemaFixtures.recreate(db, pdb, schema);
      sql(
          pdb,
          "CREATE TABLE items (id NUMBER(9) PRIMARY KEY, name VARCHAR2(30))",
          "ALTER TABLE items ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
          "INSERT INTO items SELECT LEVEL, '"
              + pdb
              + "-' || LEVEL FROM dual CONNECT BY LEVEL <= 3");
    }
    Thread.sleep(3500); // flashback reads need the SCN-to-time mapping past the CREATE TABLE
    cluster = new ConnectCluster().start();
  }

  @AfterAll
  void down() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
    for (String pdb : List.of(OracleTestDatabase.PDB1, OracleTestDatabase.PDB2)) {
      SchemaFixtures.drop(db, pdb, schema);
    }
  }

  @Test
  void twoPdbsAreSnapshottedRoutedApartAndANewTableJoinsWithoutARestart() throws Exception {
    Map<String, String> c = new HashMap<>(ConnectCluster.oracleDatabaseProps("FREEPDB1,FREEPDB2"));
    c.put("connector.class", FirstRecordConnectorIT.CONNECTOR_CLASS);
    c.put("tasks.max", "1");
    c.put("cdc.topic.prefix", PREFIX);
    c.put("cdc.tables.include", "FREEPDB[12]\\." + schema + "\\..*");
    c.put("cdc.poll.linger.ms", "100");
    c.put("cdc.kafka.bootstrap.servers", "kafka:19092");
    cluster.register(NAME, c);
    cluster.awaitRunning(NAME, Duration.ofMinutes(2));
    cluster.awaitOffsets(NAME, Duration.ofSeconds(60));

    String t1 = PREFIX + ".FREEPDB1." + schema + ".ITEMS";
    String t2 = PREFIX + ".FREEPDB2." + schema + ".ITEMS";
    String fresh = PREFIX + ".FREEPDB2." + schema + ".FRESH";
    // changes in both PDBs, interleaved, after the snapshot SCN
    for (int i = 10; i < 15; i++) {
      sql(OracleTestDatabase.PDB1, "INSERT INTO items VALUES (" + i + ", 'FREEPDB1-" + i + "')");
      sql(OracleTestDatabase.PDB2, "INSERT INTO items VALUES (" + i + ", 'FREEPDB2-" + i + "')");
    }
    // SRC-SEL-4: a table created with rows in one PDB, then changed
    sql(
        OracleTestDatabase.PDB2,
        "CREATE TABLE fresh (id NUMBER(9) PRIMARY KEY, name VARCHAR2(30))",
        "ALTER TABLE fresh ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO fresh SELECT id, 'copied-' || id FROM items WHERE id <= 3");
    Thread.sleep(4000);
    sql(OracleTestDatabase.PDB2, "INSERT INTO fresh VALUES (99, 'streamed')");

    Map<String, List<JsonNode>> byTopic = new HashMap<>();
    try (KafkaConsumer<String, String> consumer = cluster.consumer("mpdb", t1, t2, fresh)) {
      for (ConsumerRecord<String, String> r :
          ConnectCluster.consume(consumer, 20, Duration.ofMinutes(3), Duration.ofSeconds(10))) {
        if (r.value() != null) {
          byTopic
              .computeIfAbsent(r.topic(), k -> new ArrayList<>())
              .add(ConnectCluster.json(r.value()));
        }
      }
    }
    // a consumer keeping the latest record per key sees each PDB's own rows, whatever the
    // snapshot and streaming both delivered
    for (String pdb : List.of("FREEPDB1", "FREEPDB2")) {
      List<JsonNode> rows = byTopic.get(PREFIX + "." + pdb + "." + schema + ".ITEMS");
      assertThat(rows)
          .as("every record on %s's topic comes from %s", pdb, pdb)
          .allMatch(v -> v.path("after").path("NAME").asText().startsWith(pdb + "-"))
          .allMatch(v -> v.path("source").path("pdb").asText().equals(pdb));
      assertThat(materialise(rows).keySet())
          .containsExactlyInAnyOrder("1", "2", "3", "10", "11", "12", "13", "14");
      assertThat(rows).anyMatch(v -> v.path("op").asText().equals("r"));
    }
    List<JsonNode> freshRows = byTopic.getOrDefault(fresh, List.of());
    assertThat(materialise(freshRows).values())
        .containsExactlyInAnyOrder("copied-1", "copied-2", "copied-3", "streamed");
    assertThat(freshRows.get(freshRows.size() - 1).path("after").path("NAME").asText())
        .isEqualTo("streamed");

    List<String> events = new ArrayList<>();
    try (KafkaConsumer<String, String> ops = cluster.consumer("mpdb-ops", PREFIX + ".cdc.ops")) {
      for (ConsumerRecord<String, String> r :
          ConnectCluster.consume(ops, 5, Duration.ofMinutes(1), Duration.ofSeconds(5))) {
        JsonNode v = ConnectCluster.json(r.value());
        events.add(v.path("type").asText() + ":" + v.path("details").path("table").asText());
      }
    }
    assertThat(events).contains("table-added:FREEPDB2." + schema + ".FRESH");
    System.out.println(
        "multi-pdb: " + byTopic.keySet() + ", fresh table records " + freshRows.size());
  }

  /** The latest NAME per ID, as a compacting consumer would keep it. */
  private static Map<String, String> materialise(List<JsonNode> records) {
    Map<String, String> out = new java.util.LinkedHashMap<>();
    for (JsonNode v : records) {
      JsonNode after = v.path("after");
      if (after.isObject()) {
        out.put(after.path("ID").asText(), after.path("NAME").asText());
      } else {
        out.remove(v.path("before").path("ID").asText());
      }
    }
    return out;
  }

  private void sql(String pdb, String... statements) throws Exception {
    try (Connection w = db.connect(pdb, schema, schema);
        Statement s = w.createStatement()) {
      for (String q : statements) {
        s.execute(q);
      }
    }
  }
}
