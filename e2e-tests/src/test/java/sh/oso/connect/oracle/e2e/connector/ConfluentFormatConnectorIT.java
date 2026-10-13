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
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.e2e.support.Cli;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/** A real worker checked against committed external fixtures; no Confluent software is used. */
@Tag("connector")
class ConfluentFormatConnectorIT {
  private static final ObjectMapper JSON = new ObjectMapper();
  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void defaultTopicsAndCompositeKeyChangesMatchTheCommittedConfluentFixture() throws Exception {
    String schema = "CF_FIXTURE";
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    String topic = "FREEPDB1." + schema + ".COMPOSITE";
    try (ConnectCluster cluster = new ConnectCluster().start()) {
      try (Connection work = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
        exec(
            work,
            "CREATE TABLE composite (a NUMBER(5), b VARCHAR2(10), v VARCHAR2(20), PRIMARY KEY"
                + " (a,b))",
            "ALTER TABLE composite ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
            "INSERT INTO composite VALUES (0, 's', 'seed')");
      }
      Map<String, String> config =
          new HashMap<>(ConnectCluster.oracleDatabaseProps(OracleTestDatabase.PDB1));
      config.put("connector.class", FirstRecordConnectorIT.CONNECTOR_CLASS);
      config.put("tasks.max", "1");
      config.put("cdc.topic.prefix", "fixture-internal");
      config.put("cdc.tables.include", "FREEPDB1\\." + schema + "\\.COMPOSITE");
      config.put("cdc.output.format", "confluent");
      config.put("cdc.snapshot.mode", "initial");
      config.put("cdc.poll.linger.ms", "100");
      for (String side : List.of("key", "value")) {
        config.put(side + ".converter", "org.apache.kafka.connect.json.JsonConverter");
        config.put(side + ".converter.schemas.enable", "true");
      }
      cluster.register("confluent-fixture", config);
      cluster.awaitRunning("confluent-fixture", Duration.ofMinutes(2));
      try (KafkaConsumer<String, String> consumer = cluster.consumer("fixture-reader", topic)) {
        List<ConsumerRecord<String, String>> actual =
            new ArrayList<>(
                ConnectCluster.consume(consumer, 1, Duration.ofMinutes(2), Duration.ofSeconds(2)));
        try (Connection work = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
          exec(
              work,
              "INSERT INTO composite VALUES (1, 'a', 'x')",
              "UPDATE composite SET v = 'y' WHERE a = 1",
              "UPDATE composite SET b = 'b' WHERE a = 1");
        }
        actual.addAll(
            ConnectCluster.consume(consumer, 5, Duration.ofMinutes(2), Duration.ofSeconds(2)));
        List<String> expected =
            Files.readAllLines(
                Cli.repoRoot()
                    .resolve(
                        "kafka-connect-oracle-cdc/src/test/resources/confluent-reference/default/FREEPDB1.SCHEMA.COMPOSITE.jsonl"));
        assertThat(actual).hasSize(expected.size());
        for (int i = 0; i < actual.size(); i++) {
          ObjectNode record = JSON.createObjectNode();
          record.set(
              "key", actual.get(i).key() == null ? null : JSON.readTree(actual.get(i).key()));
          record.set(
              "value", actual.get(i).value() == null ? null : JSON.readTree(actual.get(i).value()));
          ObjectNode fixture = (ObjectNode) JSON.readTree(expected.get(i));
          fixture.remove(List.of("timestamp", "timestampType", "headers"));
          assertThat(normalise(record, schema))
              .as("fixture record " + i)
              .isEqualTo(normalise(fixture, "SCHEMA"));
        }
      }
      assertThat(cluster.topics()).contains(topic);
      assertThat(cluster.topics()).doesNotContain("fixture-internal." + topic);
      cluster.awaitRunning("confluent-fixture", Duration.ofSeconds(10));
      cluster.delete("confluent-fixture");
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  private static JsonNode normalise(ObjectNode record, String schema) throws Exception {
    JsonNode payload = record.path("value").path("payload");
    if (payload instanceof ObjectNode row) {
      for (String field : List.of("scn", "op_ts", "current_ts", "row_id"))
        if (row.hasNonNull(field)) row.put(field, "<volatile>");
    }
    return JSON.readTree(record.toString().replace(schema, "SCHEMA"));
  }

  private static void exec(Connection connection, String... sql) throws Exception {
    try (Statement statement = connection.createStatement()) {
      for (String text : sql) statement.execute(text);
    }
  }
}
