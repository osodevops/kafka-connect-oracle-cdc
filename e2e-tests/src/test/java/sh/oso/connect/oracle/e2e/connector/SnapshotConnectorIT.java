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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import sh.oso.connect.oracle.bench.check.CheckReport;
import sh.oso.connect.oracle.bench.check.CorrectnessCheck;
import sh.oso.connect.oracle.bench.workload.WorkloadGenerator;
import sh.oso.connect.oracle.bench.workload.WorkloadSpec;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * PRD-02 acceptance through a real worker: an initial snapshot taken while the table changes
 * materialises to the database state (correctness oracle), and a snapshot stopped part way resumes
 * at its frontier instead of starting again.
 */
@Tag("connector")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SnapshotConnectorIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();
  private final String schema = SchemaFixtures.nameFor(getClass());
  private ConnectCluster cluster;

  @BeforeAll
  void up() throws Exception {
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    cluster = new ConnectCluster().start();
  }

  @AfterAll
  void down() throws Exception {
    if (cluster != null) {
      cluster.close();
    }
    SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
  }

  private Map<String, String> connector(String prefix, String table) {
    Map<String, String> c = new HashMap<>(ConnectCluster.oracleDatabaseProps("FREEPDB1"));
    c.put("connector.class", FirstRecordConnectorIT.CONNECTOR_CLASS);
    c.put("tasks.max", "1");
    c.put("cdc.topic.prefix", prefix);
    c.put("cdc.tables.include", "FREEPDB1\\." + schema + "\\." + table);
    c.put("cdc.poll.linger.ms", "100");
    c.put("cdc.heartbeat.interval.ms", "1000");
    c.put("cdc.kafka.bootstrap.servers", "kafka:19092");
    c.put("cdc.snapshot.chunk.rows", "1000");
    return c;
  }

  @Test
  void aSnapshotTakenWhileTheTableChangesMaterialisesToTheDatabase() throws Exception {
    WorkloadSpec spec = WorkloadSpec.defaults();
    spec.tables = 1;
    WorkloadGenerator g =
        new WorkloadGenerator(spec, db.jdbcUrl(OracleTestDatabase.PDB1), schema, schema);
    g.reset(); // WL_T1, the table shape the correctness oracle compares
    sql(
        "INSERT INTO wl_t1 (id, grp, name, amount, flag, note, updated_at) SELECT LEVEL,"
            + " MOD(LEVEL, 7), 'n' || LEVEL, LEVEL / 100, 'Y', CASE WHEN MOD(LEVEL, 500) = 0 THEN"
            + " TO_CLOB('lob ' || LEVEL) END, TIMESTAMP '2026-10-05 10:00:00' FROM dual CONNECT BY"
            + " LEVEL <= 20000");
    Thread.sleep(3500); // flashback reads need the SCN-to-time mapping past the CREATE TABLE

    Map<String, String> c = connector("snap", "WL_T1");
    c.put("cdc.snapshot.threads", "2");
    c.put("cdc.snapshot.max.pending.chunks", "2");
    c.put("cdc.lob.mode", "reselect");
    cluster.register("snapshot", c);
    cluster.awaitRunning("snapshot", Duration.ofMinutes(2));

    // changes to rows the snapshot has and has not read yet, while it runs
    AtomicBoolean stop = new AtomicBoolean();
    AtomicLong changes = new AtomicLong();
    Thread writer =
        new Thread(
            () -> {
              SplittableRandom rnd = new SplittableRandom(7);
              long next = 100_000;
              try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
                  Statement s = w.createStatement()) {
                w.setAutoCommit(false);
                long until = System.currentTimeMillis() + 15_000;
                while (!stop.get() && System.currentTimeMillis() < until) {
                  int id = 1 + rnd.nextInt(20000);
                  switch (rnd.nextInt(3)) {
                    case 0 ->
                        s.executeUpdate(
                            "UPDATE wl_t1 SET name = 'u"
                                + changes.get()
                                + "', amount = amount + 1"
                                + " WHERE id = "
                                + id);
                    case 1 -> s.executeUpdate("DELETE FROM wl_t1 WHERE id = " + id);
                    default ->
                        s.executeUpdate(
                            "INSERT INTO wl_t1 (id, grp, name, amount, flag) VALUES ("
                                + next++
                                + ", 1, 'new', 1, 'N')");
                  }
                  w.commit();
                  changes.incrementAndGet();
                }
              } catch (Exception e) {
                throw new IllegalStateException(e);
              }
            });
    writer.start();
    writer.join();
    sql("UPDATE wl_t1 SET flag = 'Z' WHERE id = 100000"); // a final commit after everything

    String topic = "snap.FREEPDB1." + schema + ".WL_T1";
    try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      CheckReport report =
          new CorrectnessCheck(
                  cluster.bootstrapServers(),
                  List.of(topic),
                  w,
                  schema,
                  List.of("WL_T1"),
                  null, // rows written before the connector started are in no transaction
                  Duration.ofMinutes(4),
                  Duration.ofSeconds(15))
              .run();
      System.out.println(
          "snapshot-correctness: "
              + changes.get()
              + " concurrent changes, verdict="
              + report.verdict()
              + " records="
              + report.recordsConsumed());
      assertThat(report.verdict()).as(report.toJson()).isEqualTo(CheckReport.Verdict.PASS);
    }
  }

  @Test
  void aSnapshotStoppedPartWayResumesAtItsFrontier() throws Exception {
    sql(
        "CREATE TABLE snapres (id NUMBER PRIMARY KEY, name VARCHAR2(50))",
        "ALTER TABLE snapres ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO snapres SELECT LEVEL, 'n' || LEVEL FROM dual CONNECT BY LEVEL <= 60000");
    Thread.sleep(3500);
    Map<String, String> c = connector("snapres", "SNAPRES");
    c.put("cdc.snapshot.threads", "1");
    c.put("cdc.snapshot.max.pending.chunks", "1");
    cluster.register("snapshot-resume", c);
    cluster.awaitRunning("snapshot-resume", Duration.ofMinutes(2));
    String topic = "snapres.FREEPDB1." + schema + ".SNAPRES";
    Set<String> ids = new HashSet<>();
    long reads = 0;
    long firsts = 0;
    try (KafkaConsumer<String, String> consumer = cluster.consumer("snapres-reader", topic)) {
      long deadline = System.currentTimeMillis() + Duration.ofMinutes(2).toMillis();
      while (reads < 5000 && System.currentTimeMillis() < deadline) {
        for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(200))) {
          JsonNode v = ConnectCluster.json(r.value());
          reads++;
          firsts += "first".equals(v.path("source").path("snapshot").asText()) ? 1 : 0;
          ids.add(v.path("after").path("NAME").asText());
        }
      }
      long beforeStop = reads;
      cluster.lifecycle("snapshot-resume", "stop");
      cluster.awaitTaskState("snapshot-resume", "STOPPED", Duration.ofMinutes(1));
      JsonNode offset =
          cluster.awaitOffsets("snapshot-resume", Duration.ofSeconds(30)).path("offsets").get(0);
      JsonNode block = ConnectCluster.json(offset.path("offset").path("snapshot").asText());
      assertThat(block.path("complete").asBoolean()).as("stopped part way: %s", block).isFalse();
      JsonNode table = block.path("tables").path("FREEPDB1." + schema + ".SNAPRES");
      assertThat(table.path("frontier").isArray()).as(block.toString()).isTrue();

      cluster.lifecycle("snapshot-resume", "resume");
      cluster.awaitRunning("snapshot-resume", Duration.ofMinutes(2));
      deadline = System.currentTimeMillis() + Duration.ofMinutes(4).toMillis();
      while (ids.size() < 60000 && System.currentTimeMillis() < deadline) {
        for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
          JsonNode v = ConnectCluster.json(r.value());
          reads++;
          firsts += "first".equals(v.path("source").path("snapshot").asText()) ? 1 : 0;
          ids.add(v.path("after").path("NAME").asText());
        }
      }
      assertThat(ids).hasSize(60000);
      assertThat(firsts).as("one snapshot, not a second from the start").isEqualTo(1);
      // only what was in flight at the stop is read again: far below a second full read
      assertThat(reads).isLessThanOrEqualTo(60000 + beforeStop + 2000);
      System.out.println(
          "snapshot-resume: stopped after "
              + beforeStop
              + " records at frontier "
              + table.path("frontier")
              + ", "
              + reads
              + " records for 60000 rows");
    }
  }

  private void sql(String... statements) throws Exception {
    try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Statement s = w.createStatement()) {
      for (String q : statements) {
        s.execute(q);
      }
    }
  }
}
