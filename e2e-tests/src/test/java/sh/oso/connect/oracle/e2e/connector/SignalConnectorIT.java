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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
 * SRC-SIG-1 to SRC-SIG-3 through a real worker, in archive-only mode and with dictionary builds
 * off, so the connector writes nothing to the source: a snapshot by signal publishes the named
 * table's rows as incremental snapshot records, and every signal is acknowledged on the ops topic.
 */
@Tag("connector")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SignalConnectorIT {

  static final String PREFIX = "sig";
  static final String NAME = "signals";

  private final OracleTestDatabase db = OracleTestDatabase.get();
  private final String schema = SchemaFixtures.nameFor(getClass());
  private ConnectCluster cluster;

  @BeforeAll
  void up() throws Exception {
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Statement s = w.createStatement()) {
      s.execute("CREATE TABLE sigt (id NUMBER(9) PRIMARY KEY, name VARCHAR2(20))");
      s.execute("ALTER TABLE sigt ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      s.execute("INSERT INTO sigt SELECT LEVEL, 'n' || LEVEL FROM dual CONNECT BY LEVEL <= 2500");
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
  void aSnapshotBySignalInArchiveOnlyModeIsPublishedAndAcknowledged() throws Exception {
    Map<String, String> c = new HashMap<>(ConnectCluster.oracleDatabaseProps("FREEPDB1"));
    c.put("connector.class", FirstRecordConnectorIT.CONNECTOR_CLASS);
    c.put("tasks.max", "1");
    c.put("cdc.topic.prefix", PREFIX);
    c.put("cdc.tables.include", "FREEPDB1\\." + schema + "\\.SIGT");
    c.put("cdc.poll.linger.ms", "100");
    c.put("cdc.kafka.bootstrap.servers", "kafka:19092");
    c.put("cdc.capture.mode", "archive_only");
    c.put("cdc.snapshot.mode", "none");
    c.put("cdc.snapshot.chunk.rows", "1000");
    c.put("cdc.dictionary.build.interval.ms", "0");
    cluster.register(NAME, c);
    cluster.awaitRunning(NAME, Duration.ofMinutes(2));

    String table = "FREEPDB1." + schema + ".SIGT";
    cluster.produce(
        PREFIX + ".cdc.signals",
        NAME,
        "{\"id\": \"snap-1\", \"type\": \"snapshot\", \"data\": {\"tables\": [\""
            + table
            + "\"], \"predicate\": \"ID <= 2000\"}}");
    cluster.produce(
        PREFIX + ".cdc.signals", NAME, "{\"id\": \"state-1\", \"type\": \"log-state\"}");
    cluster.produce(
        PREFIX + ".cdc.signals", "someone-else", "{\"id\": \"x\", \"type\": \"log-state\"}");

    // archive-only: streaming, and so the snapshot's chunks, advance as logs are archived
    Set<String> ids = new HashSet<>();
    List<String> markers = new ArrayList<>();
    try (KafkaConsumer<String, String> rows =
        cluster.consumer("sig-rows", PREFIX + ".FREEPDB1." + schema + ".SIGT")) {
      long deadline = System.currentTimeMillis() + Duration.ofMinutes(3).toMillis();
      while (ids.size() < 2000 && System.currentTimeMillis() < deadline) {
        OracleSql.archiveLogCurrent(db);
        for (ConsumerRecord<String, String> r : rows.poll(Duration.ofSeconds(2))) {
          JsonNode v = ConnectCluster.json(r.value());
          ids.add(v.path("after").path("NAME").asText());
          markers.add(v.path("source").path("snapshot").asText());
        }
      }
    }
    assertThat(ids).hasSize(2000).doesNotContain("n2001");
    assertThat(markers).containsOnly("incremental");

    Map<String, JsonNode> acks = new HashMap<>();
    try (KafkaConsumer<String, String> ops = cluster.consumer("sig-ops", PREFIX + ".cdc.ops")) {
      for (ConsumerRecord<String, String> r :
          ConnectCluster.consume(ops, 3, Duration.ofMinutes(1), Duration.ofSeconds(5))) {
        JsonNode v = ConnectCluster.json(r.value());
        if ("signal-ack".equals(v.path("type").asText())) {
          acks.put(v.path("details").path("id").asText(), v.path("details"));
        }
      }
    }
    assertThat(acks).containsOnlyKeys("snap-1", "state-1");
    assertThat(acks.get("snap-1").path("outcome").asText()).isEqualTo("ok");
    assertThat(acks.get("state-1").path("outcome").asText()).isEqualTo("ok");
    assertThat(acks.get("state-1").path("mined_to_scn").asLong()).isPositive();
    System.out.println("signals: 2000 rows by signal in archive-only mode, acks " + acks.keySet());
  }
}
