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
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/** Real worker delivery of the implicit commit, TRUNCATE commit and following commit. */
@Tag("connector")
class TruncateConnectorIT {
  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void committedTruncateKeepsItsPlaceAndNullKeyInBothFormats() throws Exception {
    try (ConnectCluster cluster = new ConnectCluster().start()) {
      for (String format : List.of("debezium", "confluent")) {
        String schema =
            SchemaFixtures.nameFor(getClass())
                + "_"
                + format.substring(0, 3).toUpperCase(java.util.Locale.ROOT);
        String connector = "truncate-" + format;
        String prefix = "trunc-" + format;
        SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
        try {
          try (Connection work = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
            exec(
                work,
                "CREATE TABLE events (id NUMBER(10) PRIMARY KEY, v VARCHAR2(20))",
                "ALTER TABLE events ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
          }
          Map<String, String> config =
              new HashMap<>(ConnectCluster.oracleDatabaseProps(OracleTestDatabase.PDB1));
          config.put("connector.class", FirstRecordConnectorIT.CONNECTOR_CLASS);
          config.put("tasks.max", "1");
          config.put("cdc.topic.prefix", prefix);
          config.put("cdc.topic.template", "${prefix}.${pdb}.${schema}.${table}");
          config.put("cdc.tables.include", "FREEPDB1\\." + schema + "\\.EVENTS");
          config.put("cdc.snapshot.mode", "none");
          config.put("cdc.output.format", format);
          config.put("cdc.truncate.emit", "true");
          config.put("cdc.poll.linger.ms", "100");
          cluster.register(connector, config);
          cluster.awaitRunning(connector, Duration.ofMinutes(2));
          cluster.awaitOffsets(connector, Duration.ofMinutes(1));
          try (Connection work = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
            work.setAutoCommit(false);
            exec(
                work,
                "INSERT INTO events VALUES (1, 'before')",
                "TRUNCATE TABLE events",
                "INSERT INTO events VALUES (2, 'after')");
            work.commit();
          }
          OracleSql.archiveLogCurrent(db);
          try (KafkaConsumer<String, String> consumer =
              cluster.consumer(
                  "truncate-check-" + format, prefix + ".FREEPDB1." + schema + ".EVENTS")) {
            List<ConsumerRecord<String, String>> records =
                ConnectCluster.consume(consumer, 3, Duration.ofMinutes(2), Duration.ofSeconds(3));
            assertThat(records).hasSize(3);
            String op = format.equals("confluent") ? "op_type" : "op";
            List<String> operations = new java.util.ArrayList<>();
            for (ConsumerRecord<String, String> record : records) {
              operations.add(ConnectCluster.json(record.value()).path(op).asText());
            }
            assertThat(operations)
                .containsExactly(
                    format.equals("confluent") ? "I" : "c",
                    format.equals("confluent") ? "T" : "t",
                    format.equals("confluent") ? "I" : "c");
            assertThat(records.get(1).key()).isNull();
            JsonNode truncate = ConnectCluster.json(records.get(1).value());
            if (format.equals("confluent")) assertThat(truncate.has("ID")).isFalse();
            else {
              assertThat(truncate.path("before").isNull()).isTrue();
              assertThat(truncate.path("after").isNull()).isTrue();
            }
            long previous = 0;
            for (ConsumerRecord<String, String> record : records) {
              long commit =
                  Long.parseLong(
                      new String(
                          record.headers().lastHeader("cdc.commit_scn").value(),
                          StandardCharsets.UTF_8));
              assertThat(commit).isGreaterThan(previous);
              previous = commit;
            }
          }
          cluster.awaitRunning(connector, Duration.ofSeconds(10));
        } finally {
          cluster.delete(connector);
          SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
        }
      }
    }
  }

  private static void exec(Connection connection, String... sql) throws Exception {
    try (Statement statement = connection.createStatement()) {
      for (String text : sql) statement.execute(text);
    }
  }
}
