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
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import sh.oso.connect.oracle.e2e.support.Cli;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * PRD-05 {@code oracle-cdc-admin resnapshot} (PRD-02 SNAP-7) against a running connector whose redo
 * is all present: the command writes one snapshot signal to the signals topic, keyed by the
 * connector name, and leaves the offset and the connector alone; the connector acknowledges the
 * signal on the ops topic and publishes every row of the table as an incremental snapshot record
 * while streaming goes on. A table the connector does not capture is a usage error.
 */
@Tag("connector")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ResnapshotConnectorIT {

  static final String PREFIX = "resnap";
  static final String NAME = "resnapshot";
  static final int ROWS = 1500;

  private final OracleTestDatabase db = OracleTestDatabase.get();
  private final String schema = SchemaFixtures.nameFor(getClass());
  private ConnectCluster cluster;
  @TempDir Path dir;

  @BeforeAll
  void up() throws Exception {
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    sql(
        "CREATE TABLE items (id NUMBER(9) PRIMARY KEY, name VARCHAR2(40))",
        "ALTER TABLE items ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
        "INSERT INTO items SELECT LEVEL, 'n' || LEVEL FROM dual CONNECT BY LEVEL <= " + ROWS,
        "CREATE TABLE tick (id NUMBER(9) PRIMARY KEY)");
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
  void resnapshotWritesASignalTheRunningConnectorAcknowledgesAndPublishes() throws Exception {
    Map<String, String> c = new HashMap<>(ConnectCluster.oracleDatabaseProps("FREEPDB1"));
    c.put("connector.class", FirstRecordConnectorIT.CONNECTOR_CLASS);
    c.put("tasks.max", "1");
    c.put("cdc.topic.prefix", PREFIX);
    c.put("cdc.tables.include", "FREEPDB1\\." + schema + "\\.ITEMS");
    c.put("cdc.poll.linger.ms", "100");
    c.put("cdc.heartbeat.interval.ms", "1000");
    c.put("cdc.kafka.bootstrap.servers", "kafka:19092");
    c.put("cdc.decimal.mode", "string");
    c.put("cdc.snapshot.mode", "none"); // the rows already there are not published at start
    c.put("cdc.snapshot.chunk.rows", "1000"); // the smallest chunk: the rows take two
    cluster.register(NAME, c);
    cluster.awaitRunning(NAME, Duration.ofMinutes(2));
    cluster.awaitOffsets(NAME, Duration.ofSeconds(60));
    String table = "FREEPDB1." + schema + ".ITEMS";
    String data = PREFIX + ".FREEPDB1." + schema + ".ITEMS";
    Path cfg = Cli.hostConfig(dir.resolve("resnapshot.json"), NAME, c, db);

    try (KafkaConsumer<String, String> rows = cluster.consumer("resnap-rows", data);
        KafkaConsumer<String, String> ops = cluster.consumer("resnap-ops", PREFIX + ".cdc.ops");
        KafkaConsumer<String, String> signals =
            cluster.consumer("resnap-signals", PREFIX + ".cdc.signals")) {
      // streaming first: one new row, and nothing of the rows that were there before
      sql("INSERT INTO items VALUES (" + (ROWS + 1) + ", 'streamed')");
      List<ConsumerRecord<String, String>> first =
          ConnectCluster.consume(rows, 1, Duration.ofMinutes(2), Duration.ofSeconds(3));
      assertThat(first).hasSize(1);
      assertThat(ConnectCluster.json(first.get(0).value()).path("op").asText()).isEqualTo("c");
      long offsetBefore = resumeScn();

      // a table the connector does not capture is refused before anything is written
      Cli.Result wrong =
          admin(cfg, "--tables", "FREEPDB1." + schema + ".TICK", "--reason", "not captured");
      assertThat(wrong.exit()).as(wrong.toString()).isEqualTo(64);
      assertThat(wrong.err()).contains("is not one of the 1 tables the connector captures");

      Cli.Result r = admin(cfg, "--tables", table, "--reason", "rebuild the downstream copy");
      assertThat(r.exit()).as(r.toString()).isZero();
      Matcher m =
          Pattern.compile("Signal (oracle-cdc-admin-[0-9a-f-]+) written to ").matcher(r.out());
      assertThat(m.find()).as(r.out()).isTrue();
      String id = m.group(1);
      assertThat(r.out())
          .contains("written to " + PREFIX + ".cdc.signals: snapshot of " + table)
          .contains("the offset is unchanged");

      // exactly one signal: keyed by the connector name, naming the table
      List<ConsumerRecord<String, String>> sent =
          ConnectCluster.consume(signals, 1, Duration.ofMinutes(1), Duration.ofSeconds(3));
      assertThat(sent).hasSize(1);
      assertThat(sent.get(0).key()).isEqualTo(NAME);
      JsonNode signal = ConnectCluster.json(sent.get(0).value());
      assertThat(signal.path("id").asText()).isEqualTo(id);
      assertThat(signal.path("type").asText()).isEqualTo("snapshot");
      assertThat(signal.path("data").path("tables").get(0).asText()).isEqualTo(table);

      // every row, as an incremental snapshot record, while streaming goes on
      Set<Integer> snapshot = new HashSet<>();
      List<String> markers = new ArrayList<>();
      long deadline = System.currentTimeMillis() + Duration.ofMinutes(4).toMillis();
      // one connection for the loop: a connection per statement exhausts the server processes
      try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
          Statement s = w.createStatement()) {
        int tick = 0;
        while (snapshot.size() < ROWS + 1 && System.currentTimeMillis() < deadline) {
          s.execute("INSERT INTO tick VALUES (" + (++tick) + ")"); // the SCN moves, steps run
          for (ConsumerRecord<String, String> rec : rows.poll(Duration.ofSeconds(1))) {
            JsonNode v = ConnectCluster.json(rec.value());
            if (v != null && "r".equals(v.path("op").asText())) {
              snapshot.add(v.path("after").path("ID").asInt());
              markers.add(v.path("source").path("snapshot").asText());
            }
          }
        }
      }
      assertThat(snapshot).hasSize(ROWS + 1);
      assertThat(markers).containsOnly("incremental");

      // acknowledged
      JsonNode ack = null;
      long until = System.currentTimeMillis() + Duration.ofMinutes(1).toMillis();
      while (ack == null && System.currentTimeMillis() < until) {
        for (ConsumerRecord<String, String> rec : ops.poll(Duration.ofMillis(500))) {
          JsonNode v = ConnectCluster.json(rec.value());
          assertThat(v.path("type").asText())
              .as("the admin never moved this offset")
              .isNotEqualTo("offsets-set");
          if ("signal-ack".equals(v.path("type").asText())
              && id.equals(v.path("details").path("id").asText())) {
            ack = v;
          }
        }
      }
      assertThat(ack).as("signal-ack for %s", id).isNotNull();
      assertThat(ack.path("details").path("type").asText()).isEqualTo("snapshot");
      assertThat(ack.path("details").path("outcome").asText()).isEqualTo("ok");

      // the connector ran throughout and its offset only moved forward
      assertThat(cluster.connectorState(NAME)).isEqualTo("RUNNING");
      assertThat(resumeScn()).isGreaterThanOrEqualTo(offsetBefore);
      System.out.println("resnapshot: " + snapshot.size() + " rows by signal " + id);
    }
  }

  private long resumeScn() throws Exception {
    return cluster
        .awaitOffsets(NAME, Duration.ofSeconds(30))
        .path("offsets")
        .get(0)
        .path("offset")
        .path("resume_scn")
        .asLong();
  }

  private Cli.Result admin(Path cfg, String... more) {
    List<String> args = new ArrayList<>();
    args.add("resnapshot");
    args.add("--connect-url");
    args.add(cluster.restUrl());
    args.add("--name");
    args.add(NAME);
    args.add("--config");
    args.add(cfg.toString());
    args.add("--bootstrap-servers");
    args.add(cluster.bootstrapServers());
    args.addAll(List.of(more));
    return Cli.admin(args.toArray(String[]::new));
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
