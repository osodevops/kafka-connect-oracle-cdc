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
 * P1-21 (SRC-ERR-2, CORE-MINE-10): with cdc.on.decode.error=dlq a row LogMiner cannot reconstruct
 * (a BOOLEAN column on 23ai Free, DOC-5) goes to the DLQ topic with its raw redo and an ops event,
 * the task keeps running, and rows of ordinary tables keep flowing. The table is created after the
 * connector started so that validation (which would block the column type) is not in the way.
 */
@Tag("connector")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DecodeDlqConnectorIT {

  static final String PREFIX = "dq";

  private final OracleTestDatabase db = OracleTestDatabase.get();
  private final String schema = SchemaFixtures.nameFor(getClass());
  private ConnectCluster cluster;

  @BeforeAll
  void up() throws Exception {
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Statement s = w.createStatement()) {
      s.execute("CREATE TABLE plain (id NUMBER PRIMARY KEY, v VARCHAR2(20))");
      s.execute("ALTER TABLE plain ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
    }
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
  void unsupportedRowsGoToTheDlqAndTheTaskKeepsRunning() throws Exception {
    Map<String, String> c = new HashMap<>(ConnectCluster.oracleDatabaseProps("FREEPDB1"));
    c.put("connector.class", FirstRecordConnectorIT.CONNECTOR_CLASS);
    c.put("tasks.max", "1");
    c.put("cdc.topic.prefix", PREFIX);
    c.put("cdc.tables.include", "FREEPDB1\\." + schema + "\\..*");
    c.put("cdc.poll.linger.ms", "100");
    c.put("cdc.on.decode.error", "dlq");
    c.put("cdc.kafka.bootstrap.servers", "kafka:19092");
    cluster.register("dlq", c);
    cluster.awaitRunning("dlq", Duration.ofMinutes(2));
    cluster.awaitOffsets("dlq", Duration.ofSeconds(60));

    try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Statement s = w.createStatement()) {
      w.setAutoCommit(false);
      s.execute("CREATE TABLE flagged (id NUMBER PRIMARY KEY, ok BOOLEAN)");
      s.execute("ALTER TABLE flagged ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      w.commit();
      s.execute("INSERT INTO flagged VALUES (1, TRUE)");
      w.commit();
      s.execute("INSERT INTO plain VALUES (1, 'after')");
      w.commit();
    }
    try (KafkaConsumer<String, String> consumer =
        cluster.consumer("dlq-reader", PREFIX + ".cdc.dlq")) {
      List<ConsumerRecord<String, String>> dlq =
          ConnectCluster.consume(consumer, 1, Duration.ofMinutes(2), Duration.ofSeconds(3));
      assertThat(dlq).isNotEmpty();
      JsonNode v = ConnectCluster.json(dlq.get(0).value());
      assertThat(v.path("kind").asText()).isEqualTo("unsupported-row");
      assertThat(v.path("table").asText()).isEqualTo("FLAGGED");
      assertThat(v.path("schema").asText()).isEqualTo(schema);
      assertThat(v.path("xid").asText()).matches("\\d+\\.\\d+\\.\\d+");
      assertThat(v.path("scn").asLong()).isPositive();
      assertThat(ConnectCluster.json(dlq.get(0).key()).path("server").asText()).isEqualTo(PREFIX);
    }
    try (KafkaConsumer<String, String> consumer =
        cluster.consumer("plain-reader", PREFIX + ".FREEPDB1." + schema + ".PLAIN")) {
      List<ConsumerRecord<String, String>> rows =
          ConnectCluster.consume(consumer, 1, Duration.ofMinutes(2), Duration.ofSeconds(2));
      assertThat(rows).as("the task kept running past the unsupported row").hasSize(1);
    }
    try (KafkaConsumer<String, String> consumer =
        cluster.consumer("ops-reader", PREFIX + ".cdc.ops")) {
      List<ConsumerRecord<String, String>> ops =
          ConnectCluster.consume(consumer, 2, Duration.ofMinutes(1), Duration.ofSeconds(3));
      assertThat(ops.stream().map(r -> type(r.value())).toList()).contains("unsupported-row");
    }
    JsonNode status = cluster.status("dlq");
    assertThat(status.path("tasks").get(0).path("state").asText()).isEqualTo("RUNNING");
  }

  private static String type(String value) {
    try {
      return ConnectCluster.json(value).path("type").asText();
    } catch (java.io.IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
