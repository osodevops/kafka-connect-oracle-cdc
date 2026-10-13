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
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
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

/**
 * Complete LOB bytes, selective updates and key moves through a real worker, without Confluent
 * software.
 */
@Tag("connector")
class ConfluentLobTopicsConnectorIT {
  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void snapshotsChangesAndKeyMovesKeepEveryLobColumnConsistent() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    String rowTopic = "lob-it.FREEPDB1." + schema + ".DOCS";
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (ConnectCluster cluster = new ConnectCluster().start()) {
      try (Connection work = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
        exec(
            work,
            "CREATE TABLE docs (id NUMBER(10) PRIMARY KEY, v VARCHAR2(20), c CLOB, n NCLOB, b"
                + " BLOB)",
            "ALTER TABLE docs ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
            "INSERT INTO docs VALUES (0, 'seed', 'seed clob', N'né seed', HEXTORAW('DEADBEEF'))");
      }
      Map<String, String> config =
          new HashMap<>(ConnectCluster.oracleDatabaseProps(OracleTestDatabase.PDB1));
      config.put("connector.class", FirstRecordConnectorIT.CONNECTOR_CLASS);
      config.put("tasks.max", "1");
      config.put("cdc.topic.prefix", "lob-it");
      config.put("cdc.tables.include", "FREEPDB1\\." + schema + "\\.DOCS");
      config.put("cdc.topic.template", "${prefix}.${pdb}.${schema}.${table}");
      config.put("cdc.output.format", "confluent");
      config.put("cdc.snapshot.mode", "initial");
      config.put("cdc.lob.mode", "topic");
      config.put("cdc.lob.topic.template", "${prefix}.lob.${pdb}.${schema}.${table}.${column}");
      config.put("cdc.output.confluent.numeric.mapping", "best_fit_or_decimal");
      config.put("cdc.poll.linger.ms", "100");
      cluster.register("lob-it", config);
      cluster.awaitRunning("lob-it", Duration.ofMinutes(2));
      try (KafkaConsumer<String, String> rowConsumer = cluster.consumer("lob-rows", rowTopic)) {
        List<ConsumerRecord<String, String>> rows =
            new ArrayList<>(
                ConnectCluster.consume(
                    rowConsumer, 1, Duration.ofMinutes(2), Duration.ofSeconds(2)));
        assertThat(rows).hasSize(1);
        assertThat(ConnectCluster.json(rows.get(0).value()).path("op_type").asText())
            .isEqualTo("R");
        try (Connection work = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
          exec(work, "INSERT INTO docs VALUES (1, 'v1', 'clob one', N'né one', HEXTORAW('0102'))");
          try (PreparedStatement statement =
              work.prepareStatement("UPDATE docs SET c = ? WHERE id = 1")) {
            statement.setString(1, "large clob " + "x".repeat(6000));
            statement.executeUpdate();
          }
          exec(
              work,
              "UPDATE docs SET v = 'v2' WHERE id = 1",
              "UPDATE docs SET id = 2 WHERE id = 1",
              "DELETE FROM docs WHERE id = 2");
          work.setAutoCommit(false);
          exec(work, append(" kept"));
          java.sql.Savepoint savepoint = work.setSavepoint("lob_before_undone");
          exec(work, append(" undone"));
          work.rollback(savepoint);
          work.commit();
          work.setAutoCommit(true);
        }
        OracleSql.archiveLogCurrent(db);
        rows.addAll(
            ConnectCluster.consume(rowConsumer, 6, Duration.ofMinutes(2), Duration.ofSeconds(3)));
        assertThat(rows).hasSize(7);
        List<String> operations = new ArrayList<>();
        for (ConsumerRecord<String, String> row : rows)
          operations.add(
              row.value() == null
                  ? "tombstone"
                  : ConnectCluster.json(row.value()).path("op_type").asText());
        assertThat(operations).containsExactly("R", "I", "U", "D", "tombstone", "I", "D");
      }
      for (String column : List.of("C", "N", "B")) {
        String topic = "lob-it.lob.FREEPDB1." + schema + ".DOCS." + column;
        try (KafkaConsumer<String, String> consumer =
            cluster.consumer("lob-check-" + column, topic)) {
          int count = column.equals("C") ? 7 : 5;
          List<ConsumerRecord<String, String>> records =
              ConnectCluster.consume(consumer, count, Duration.ofMinutes(2), Duration.ofSeconds(3));
          assertThat(records).hasSize(count);
          List<Integer> keys = new ArrayList<>();
          for (ConsumerRecord<String, String> record : records) {
            JsonNode key = ConnectCluster.json(record.key());
            assertThat(key.path("table").asText()).isEqualTo("FREEPDB1." + schema + ".DOCS");
            assertThat(key.path("column").asText()).isEqualTo(column);
            keys.add(key.path("primary_key").asInt());
          }
          assertThat(keys)
              .containsExactlyElementsOf(
                  column.equals("C") ? List.of(0, 1, 1, 1, 2, 2, 0) : List.of(0, 1, 1, 2, 2));
          int movedIndex = column.equals("C") ? 4 : 3;
          assertThat(records.get(movedIndex - 1).value()).isNull();
          assertThat(records.get(movedIndex + 1).value()).isNull();
          if (column.equals("C"))
            assertThat(
                    Base64.getDecoder()
                        .decode(ConnectCluster.json(records.get(6).value()).asText()))
                .isEqualTo("seed clob kept".getBytes(StandardCharsets.UTF_8));
          byte[] moved =
              Base64.getDecoder()
                  .decode(ConnectCluster.json(records.get(movedIndex).value()).asText());
          byte[] expected =
              column.equals("C")
                  ? ("large clob " + "x".repeat(6000)).getBytes(StandardCharsets.UTF_8)
                  : column.equals("N")
                      ? "né one".getBytes(StandardCharsets.UTF_16BE)
                      : new byte[] {1, 2};
          assertThat(moved).containsExactly(expected);
        }
      }
      cluster.awaitRunning("lob-it", Duration.ofSeconds(10));
      cluster.delete("lob-it");
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  private static String append(String text) {
    return "DECLARE content CLOB; BEGIN SELECT c INTO content FROM docs WHERE id = 0 FOR UPDATE; "
        + "DBMS_LOB.WRITEAPPEND(content, "
        + text.length()
        + ", '"
        + text
        + "'); END;";
  }

  private static void exec(Connection connection, String... sql) throws Exception {
    try (Statement statement = connection.createStatement()) {
      for (String text : sql) statement.execute(text);
    }
  }
}
