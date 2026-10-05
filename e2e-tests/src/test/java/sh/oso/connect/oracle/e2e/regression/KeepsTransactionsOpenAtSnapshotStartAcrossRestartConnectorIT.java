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
import sh.oso.connect.oracle.bench.check.CheckReport;
import sh.oso.connect.oracle.bench.check.CorrectnessCheck;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * Invariant: a transaction that is already open when the connector first starts (with an initial
 * snapshot) and commits after the connector has been restarted part way through that snapshot is
 * delivered whole: its changes made before the start are neither lost nor taken for part of the
 * snapshot, and the topic materialises to the table (correctness oracle, ADR-0012). Debezium kept
 * the snapshot SCN in its offsets but dropped the transactions pending at it, so after a restart
 * the transaction's earlier update was skipped as "already included by the initial snapshot".
 *
 * @see <a href="https://github.com/debezium/dbz/issues/2779">dbz#2779</a>
 */
@Tag("connector")
@Tag("dbz-2779")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KeepsTransactionsOpenAtSnapshotStartAcrossRestartConnectorIT {

  static final String CONNECTOR_CLASS = "sh.oso.connect.oracle.OracleCdcSourceConnector";
  static final String PREFIX = "pend";

  private final OracleTestDatabase db = OracleTestDatabase.get();
  private final String schema = SchemaFixtures.nameFor(getClass());
  private ConnectCluster cluster;

  @BeforeAll
  void up() throws Exception {
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Statement s = w.createStatement()) {
      s.execute("CREATE TABLE snapt (id NUMBER(10) PRIMARY KEY, v VARCHAR2(30))");
      s.execute("ALTER TABLE snapt ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      s.execute("INSERT INTO snapt SELECT LEVEL, 'n' || LEVEL FROM dual CONNECT BY LEVEL <= 6000");
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

  @Test
  void aTransactionOpenAtTheStartSurvivesARestartDuringTheSnapshot() throws Exception {
    String topic = PREFIX + ".FREEPDB1." + schema + ".SNAPT";
    Connection open = db.connect(OracleTestDatabase.PDB1, schema, schema);
    boolean committed = false;
    try {
      // T changes rows and stays open while the connector starts
      open.setAutoCommit(false);
      exec(
          open,
          "UPDATE snapt SET v = 't-early' WHERE id = 10",
          "INSERT INTO snapt VALUES (100001, 't-early')");

      Map<String, String> c = new HashMap<>(ConnectCluster.oracleDatabaseProps("FREEPDB1"));
      c.put("connector.class", CONNECTOR_CLASS);
      c.put("tasks.max", "1");
      c.put("cdc.topic.prefix", PREFIX);
      c.put("cdc.tables.include", "FREEPDB1\\." + schema + "\\.SNAPT");
      c.put("cdc.poll.linger.ms", "100");
      c.put("cdc.heartbeat.interval.ms", "1000");
      c.put("cdc.kafka.bootstrap.servers", "kafka:19092");
      c.put("cdc.snapshot.chunk.rows", "1000");
      c.put("cdc.snapshot.threads", "1");
      c.put("cdc.snapshot.max.pending.chunks", "1");
      cluster.register("pend", c);
      cluster.awaitRunning("pend", Duration.ofMinutes(2));

      // another transaction commits while the snapshot runs
      try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
        exec(w, "UPDATE snapt SET v = 'other' WHERE id = 20");
      }

      // restart part way through the snapshot, with T still open
      long seen = 0;
      try (KafkaConsumer<String, String> consumer = cluster.consumer("pend-probe", topic)) {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(3).toMillis();
        while (seen < 1000 && System.currentTimeMillis() < deadline) {
          seen += consumer.poll(Duration.ofMillis(200)).count();
        }
      }
      assertThat(seen).as("the snapshot started").isGreaterThanOrEqualTo(1000);
      cluster.lifecycle("pend", "stop");
      cluster.awaitTaskState("pend", "STOPPED", Duration.ofMinutes(2));
      JsonNode offset =
          cluster
              .awaitOffsets("pend", Duration.ofSeconds(30))
              .path("offsets")
              .get(0)
              .path("offset");
      String block = offset.path("snapshot").asText();
      cluster.lifecycle("pend", "resume");
      cluster.awaitRunning("pend", Duration.ofMinutes(2));

      // T commits after the restart, then a last change marks the end
      exec(open, "UPDATE snapt SET v = 't-late' WHERE id = 30");
      open.commit();
      committed = true;
      try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
        exec(w, "UPDATE snapt SET v = 'final' WHERE id = 40");
      }

      try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
        CheckReport report =
            new CorrectnessCheck(
                    cluster.bootstrapServers(),
                    List.of(topic),
                    w,
                    schema,
                    List.of("SNAPT"),
                    null,
                    Duration.ofMinutes(4),
                    Duration.ofSeconds(15))
                .run();
        System.out.println(
            "dbz-2779: restarted with snapshot block "
                + block
                + ", verdict="
                + report.verdict()
                + " records="
                + report.recordsConsumed());
        assertThat(report.verdict()).as(report.toJson()).isEqualTo(CheckReport.Verdict.PASS);
      }
      // the transaction's changes from before the start, named explicitly
      Map<Integer, String> latest = new HashMap<>();
      try (KafkaConsumer<String, String> consumer = cluster.consumer("pend-check", topic)) {
        for (ConsumerRecord<String, String> r :
            ConnectCluster.consume(consumer, 6003, Duration.ofMinutes(3), Duration.ofSeconds(10))) {
          if (r.value() == null) {
            continue;
          }
          JsonNode after = ConnectCluster.json(r.value()).path("after");
          if (!after.isMissingNode() && !after.isNull()) {
            latest.put(ConnectCluster.json(r.key()).path("ID").asInt(), after.path("V").asText());
          }
        }
      }
      assertThat(latest).containsEntry(10, "t-early").containsEntry(100001, "t-early");
      assertThat(latest).containsEntry(30, "t-late").containsEntry(20, "other");
    } finally {
      if (!committed) {
        open.rollback();
      }
      open.close();
    }
  }

  private static void exec(Connection c, String... sql) throws SQLException {
    try (Statement s = c.createStatement()) {
      for (String q : sql) {
        s.execute(q);
      }
    }
  }
}
