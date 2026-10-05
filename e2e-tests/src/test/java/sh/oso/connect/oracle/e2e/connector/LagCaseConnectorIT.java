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
import java.sql.ResultSet;
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
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * PRD-03 acceptance through a real worker: the connector writes a dictionary build when the
 * archived logs hold none; stopped, 50 DML, a column drop and add, 50 DML; resumed, all 100 records
 * carry the layout of their moment. The first 50 are mined again with the redo dictionary and
 * render with the version from the schema topic, not today's.
 */
@Tag("connector")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LagCaseConnectorIT {

  static final String CONNECTOR_CLASS = "sh.oso.connect.oracle.OracleCdcSourceConnector";
  static final String PREFIX = "lagcase";
  static final String NAME = "lag-case";

  private final OracleTestDatabase db = OracleTestDatabase.get();
  private final String schema = SchemaFixtures.nameFor(getClass());
  private ConnectCluster cluster;

  @BeforeAll
  void up() throws Exception {
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    sql(
        "CREATE TABLE lagc (id NUMBER(9) PRIMARY KEY, name VARCHAR2(20), amount NUMBER(9))",
        "ALTER TABLE lagc ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
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
  void rowsWrittenBeforeDdlWhileStoppedArriveWithTheirLayout() throws Exception {
    boolean buildsBefore = builds() > 0;
    Map<String, String> c = new HashMap<>(ConnectCluster.oracleDatabaseProps("FREEPDB1"));
    c.put("connector.class", CONNECTOR_CLASS);
    c.put("tasks.max", "1");
    c.put("cdc.topic.prefix", PREFIX);
    c.put("cdc.tables.include", "FREEPDB1\\." + schema + "\\.LAGC");
    c.put("cdc.poll.linger.ms", "100");
    c.put("cdc.heartbeat.interval.ms", "1000");
    c.put("cdc.kafka.bootstrap.servers", "kafka:19092");
    cluster.register(NAME, c);
    cluster.awaitRunning(NAME, Duration.ofMinutes(2));
    cluster.awaitOffsets(NAME, Duration.ofSeconds(60));
    String data = PREFIX + ".FREEPDB1." + schema + ".LAGC";

    try (KafkaConsumer<String, String> rows = cluster.consumer("lag-rows", data);
        KafkaConsumer<String, String> ops = cluster.consumer("lag-ops", PREFIX + ".cdc.ops")) {
      if (!buildsBefore) {
        // with no build in the archived logs, the task writes one at start
        JsonNode built = awaitOps(ops, "dictionary-build");
        assertThat(built.path("details").path("status").asText()).isEqualTo("built");
      }
      OracleSql.archiveLogCurrent(db); // the build is usable once its log is archived
      assertThat(builds()).isPositive();

      sql("INSERT INTO lagc VALUES (0, 'seen', 0)");
      assertThat(after(rows, 1).get(0).path("NAME").asText()).isEqualTo("seen");
      Thread.sleep(3000); // heartbeats commit a position past the row
      cluster.lifecycle(NAME, "stop");
      cluster.awaitTaskState(NAME, "STOPPED", Duration.ofMinutes(1));

      // one connection for the lot: a connection per statement churns server processes
      List<String> statements = new ArrayList<>();
      for (int i = 1; i <= 50; i++) {
        statements.add("INSERT INTO lagc VALUES (" + i + ", 'n" + i + "', " + i + ")");
      }
      statements.add("ALTER TABLE lagc DROP COLUMN name");
      statements.add("ALTER TABLE lagc ADD (extra VARCHAR2(10))");
      for (int i = 51; i <= 100; i++) {
        statements.add("INSERT INTO lagc VALUES (" + i + ", " + i + ", 'e" + i + "')");
      }
      sql(statements.toArray(String[]::new));

      cluster.lifecycle(NAME, "resume");
      cluster.awaitRunning(NAME, Duration.ofMinutes(2));
      List<JsonNode> got = after(rows, 100);
      for (int i = 0; i < 50; i++) {
        JsonNode r = got.get(i);
        assertThat(r.path("ID").asInt()).isEqualTo(i + 1);
        assertThat(r.path("NAME").asText())
            .as("row %d keeps its NAME", i + 1)
            .isEqualTo("n" + (i + 1));
        assertThat(r.has("EXTRA")).isFalse();
      }
      for (int i = 50; i < 100; i++) {
        JsonNode r = got.get(i);
        assertThat(r.path("ID").asInt()).isEqualTo(i + 1);
        assertThat(r.path("EXTRA").asText()).isEqualTo("e" + (i + 1));
        assertThat(r.has("NAME")).isFalse();
      }
      JsonNode replay = awaitOps(ops, "dictionary-replay");
      assertThat(replay.path("details").path("tables").asText()).contains(schema + ".LAGC");
      System.out.println("lag-case connector: 100 records with the layout of their moment");
    }
  }

  private long builds() throws Exception {
    try (Connection meta = db.capture(OracleTestDatabase.CDB_SERVICE);
        Statement s = meta.createStatement();
        ResultSet rs =
            s.executeQuery(
                "SELECT COUNT(*) FROM v$archived_log WHERE dest_id = 1 AND dictionary_end = 'YES'"
                    + " AND deleted = 'NO'")) {
      rs.next();
      return rs.getLong(1);
    }
  }

  private JsonNode awaitOps(KafkaConsumer<String, String> ops, String type) throws Exception {
    long deadline = System.currentTimeMillis() + Duration.ofMinutes(2).toMillis();
    while (System.currentTimeMillis() < deadline) {
      for (ConsumerRecord<String, String> r : ops.poll(Duration.ofMillis(500))) {
        JsonNode j = ConnectCluster.json(r.value());
        if (type.equals(j.path("type").asText())) {
          return j;
        }
      }
    }
    throw new AssertionError("no " + type + " event on the ops topic");
  }

  private void sql(String... statements) throws Exception {
    try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Statement s = w.createStatement()) {
      for (String q : statements) {
        s.execute(q);
      }
    }
  }

  private static List<JsonNode> after(KafkaConsumer<String, String> c, int n) throws Exception {
    List<JsonNode> out = new ArrayList<>();
    for (ConsumerRecord<String, String> r :
        ConnectCluster.consume(c, n, Duration.ofMinutes(3), Duration.ofSeconds(2))) {
      if (r.value() != null) {
        out.add(ConnectCluster.json(r.value()).path("after"));
      }
    }
    assertThat(out).hasSize(n);
    return out;
  }
}
