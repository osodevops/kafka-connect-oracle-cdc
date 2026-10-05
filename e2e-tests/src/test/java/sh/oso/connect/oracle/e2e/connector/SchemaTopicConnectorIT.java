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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
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
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * PRD-03 SCH-1 and SCH-6 against a real worker: each schema version change is one record per table
 * on the compacted schema topic, a stopped connector applies a DDL made while it was down, and an
 * offset moved past a DDL the connector never mined stops the task with CDC-6003.
 */
@Tag("connector")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SchemaTopicConnectorIT {

  static final String CONNECTOR_CLASS = "sh.oso.connect.oracle.OracleCdcSourceConnector";
  static final String PREFIX = "schemas";
  static final String NAME = "schema-topic";
  static final ObjectMapper JSON = new ObjectMapper();

  private final OracleTestDatabase db = OracleTestDatabase.get();
  private final String schema = SchemaFixtures.nameFor(getClass());
  private ConnectCluster cluster;

  @BeforeAll
  void up() throws Exception {
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    sql(
        "CREATE TABLE orders (id NUMBER(9) PRIMARY KEY, name VARCHAR2(50))",
        "ALTER TABLE orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
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
  void versionsAreKeptOnTheSchemaTopicAndAMissedDdlStopsTheTask() throws Exception {
    Map<String, String> c = new HashMap<>(ConnectCluster.oracleDatabaseProps("FREEPDB1"));
    c.put("connector.class", CONNECTOR_CLASS);
    c.put("tasks.max", "1");
    c.put("cdc.topic.prefix", PREFIX);
    c.put("cdc.tables.include", "FREEPDB1\\." + schema + "\\.ORDERS");
    c.put("cdc.poll.linger.ms", "100");
    c.put("cdc.heartbeat.interval.ms", "1000");
    c.put("cdc.kafka.bootstrap.servers", "kafka:19092");
    cluster.register(NAME, c);
    cluster.awaitRunning(NAME, Duration.ofMinutes(2));
    cluster.awaitOffsets(NAME, Duration.ofSeconds(60));
    String data = PREFIX + ".FREEPDB1." + schema + ".ORDERS";
    String topic = PREFIX + ".cdc.schema";

    try (KafkaConsumer<String, String> rows = cluster.consumer("schema-rows", data);
        KafkaConsumer<String, String> versions = cluster.consumer("schema-versions", topic)) {
      // the first row reads the dictionary: version 1 goes to the topic
      sql("INSERT INTO orders VALUES (1, 'one')");
      assertThat(after(rows, 1).get(0).has("NOTE")).isFalse();
      Versioned first = latest(versions);
      assertThat(first.key().path("server").asText()).isEqualTo(PREFIX);
      assertThat(first.key().path("owner").asText()).isEqualTo(schema);
      assertThat(first.key().path("table").asText()).isEqualTo("ORDERS");
      assertThat(numbers(first.value())).containsExactly(1);

      // a column added while running: version 2, with version 1 still needed by a restart
      sql(
          "ALTER TABLE orders ADD (note VARCHAR2(20))",
          "INSERT INTO orders VALUES (2, 'two', 'n')");
      assertThat(after(rows, 1).get(0).path("NOTE").asText()).isEqualTo("n");
      Versioned second = latest(versions);
      assertThat(numbers(second.value())).containsExactly(1, 2);
      JsonNode v2 = second.value().path("versions").get(1);
      assertThat(v2.path("valid_from_scn").asLong()).isPositive();
      assertThat(v2.path("columns").size()).isEqualTo(3);

      // stopped, altered, resumed: the DDL is after the resume point, so the stored version is
      // still trusted, and the DDL is applied when the redo reaches it
      awaitCommittedPast(v2.path("valid_from_scn").asLong());
      cluster.lifecycle(NAME, "stop");
      cluster.awaitTaskState(NAME, "STOPPED", Duration.ofMinutes(1));
      sql(
          "ALTER TABLE orders ADD (extra NUMBER(5))",
          "INSERT INTO orders VALUES (3, 'three', 'n', 7)");
      cluster.lifecycle(NAME, "resume");
      cluster.awaitRunning(NAME, Duration.ofMinutes(2));
      assertThat(after(rows, 1).get(0).path("EXTRA").asInt()).isEqualTo(7);
      assertThat(numbers(latest(versions).value()))
          .as("pruned to the version valid at the resume point and later ones")
          .containsExactly(2, 3);

      // an operator moves the offset past a DDL the connector never saw
      cluster.lifecycle(NAME, "stop");
      cluster.awaitTaskState(NAME, "STOPPED", Duration.ofMinutes(1));
      sql("ALTER TABLE orders ADD (missed VARCHAR2(5))");
      Thread.sleep(20_000); // well past SCN_TO_TIMESTAMP's precision and the check's slack
      JsonNode current = cluster.awaitOffsets(NAME, Duration.ofSeconds(10)).path("offsets").get(0);
      Map<String, Object> offset = JSON.convertValue(current.path("offset"), LinkedHashMap.class);
      try (Connection meta = db.capture(OracleTestDatabase.CDB_SERVICE)) {
        offset.put("resume_scn", LogMinerHelper.currentScn(meta));
      }
      offset.remove("resume_rs_id");
      offset.remove("resume_ssn");
      @SuppressWarnings("unchecked")
      Map<String, Object> partition = JSON.convertValue(current.path("partition"), Map.class);
      cluster.patchOffset(NAME, partition, offset);
      cluster.lifecycle(NAME, "resume");
      JsonNode failed = cluster.awaitTaskState(NAME, "FAILED", Duration.ofMinutes(2));
      String trace = failed.path("tasks").get(0).path("trace").asText();
      assertThat(trace).contains("CDC-6003").contains(schema + ".ORDERS");
      System.out.println("schema-topic: missed DDL stopped the task with CDC-6003");
    }
  }

  record Versioned(JsonNode key, JsonNode value) {}

  /** Waits for the committed offset to resume after {@code scn}, so a stop keeps the DDL behind. */
  private void awaitCommittedPast(long scn) throws Exception {
    long deadline = System.currentTimeMillis() + 60_000;
    long resume = 0;
    while (System.currentTimeMillis() < deadline) {
      resume =
          cluster
              .awaitOffsets(NAME, Duration.ofSeconds(10))
              .path("offsets")
              .get(0)
              .path("offset")
              .path("resume_scn")
              .asLong();
      if (resume > scn) {
        return;
      }
      Thread.sleep(500);
    }
    throw new AssertionError("committed resume SCN " + resume + " never passed " + scn);
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
        ConnectCluster.consume(c, n, Duration.ofMinutes(2), Duration.ofSeconds(1))) {
      if (r.value() != null) {
        out.add(ConnectCluster.json(r.value()).path("after"));
      }
    }
    assertThat(out).hasSize(n);
    return out;
  }

  /** The newest record on the schema topic once it settles. */
  private static Versioned latest(KafkaConsumer<String, String> c) throws Exception {
    List<ConsumerRecord<String, String>> got =
        ConnectCluster.consume(c, 1, Duration.ofMinutes(2), Duration.ofSeconds(3));
    assertThat(got).as("a schema topic record").isNotEmpty();
    ConsumerRecord<String, String> last = got.get(got.size() - 1);
    return new Versioned(ConnectCluster.json(last.key()), ConnectCluster.json(last.value()));
  }

  private static List<Integer> numbers(JsonNode value) {
    List<Integer> out = new ArrayList<>();
    value.path("versions").forEach(v -> out.add(v.path("version").asInt()));
    return out;
  }
}
