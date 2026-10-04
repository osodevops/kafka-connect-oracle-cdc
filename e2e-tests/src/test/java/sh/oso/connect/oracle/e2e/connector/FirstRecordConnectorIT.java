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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * P1-10: a record from Oracle reaches Kafka through a real Connect worker running the packaged
 * plugin: insert, update and delete arrive as Debezium-shaped envelopes with cdc.* headers and a
 * tombstone; validate() reports the doctor's findings through the REST API.
 */
@Tag("connector")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FirstRecordConnectorIT {

  static final String CONNECTOR_CLASS = "sh.oso.connect.oracle.OracleCdcSourceConnector";

  private final OracleTestDatabase db = OracleTestDatabase.get();
  private final String schema = SchemaFixtures.nameFor(getClass());
  private ConnectCluster cluster;

  @BeforeAll
  void up() throws Exception {
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Statement s = w.createStatement()) {
      s.execute(
          "CREATE TABLE orders (id NUMBER(9) PRIMARY KEY, name VARCHAR2(50), amount NUMBER(10,2))");
      s.execute("ALTER TABLE orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      s.execute("CREATE TABLE nolog (id NUMBER PRIMARY KEY, v NUMBER)");
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

  private Map<String, String> config(String include) {
    Map<String, String> c = new HashMap<>(ConnectCluster.oracleDatabaseProps("FREEPDB1"));
    c.put("connector.class", CONNECTOR_CLASS);
    c.put("tasks.max", "1");
    c.put("cdc.topic.prefix", "cdc");
    c.put("cdc.tables.include", include);
    c.put("cdc.poll.linger.ms", "100");
    return c;
  }

  @Test
  void validateReportsDoctorFindingsOnTheRightKeys() throws Exception {
    JsonNode v = cluster.validate(CONNECTOR_CLASS, config("FREEPDB1\\." + schema + "\\.NOLOG"));
    assertThat(v.path("error_count").asInt()).isPositive();
    boolean onInclude = false;
    for (JsonNode cfg : v.path("configs")) {
      if ("cdc.tables.include".equals(cfg.path("definition").path("name").asText())) {
        for (JsonNode err : cfg.path("value").path("errors")) {
          onInclude |= err.asText().startsWith("DOC-3");
        }
      }
    }
    assertThat(onInclude).as("DOC-3 attached to cdc.tables.include: %s", v).isTrue();
    JsonNode ok = cluster.validate(CONNECTOR_CLASS, config("FREEPDB1\\." + schema + "\\.ORDERS"));
    assertThat(ok.path("error_count").asInt()).as(ok.toString()).isZero();
  }

  @Test
  void insertUpdateDeleteReachKafkaAsDebeziumEnvelopes() throws Exception {
    cluster.register("oracle-cdc", config("FREEPDB1\\." + schema + "\\.ORDERS"));
    cluster.awaitRunning("oracle-cdc", Duration.ofMinutes(2));
    // the start heartbeat must make the start SCN durable before any change (SRC-HB-1)
    JsonNode offsets = cluster.awaitOffsets("oracle-cdc", Duration.ofSeconds(60));
    assertThat(offsets.path("offsets").get(0).path("offset").path("resume_scn").asLong())
        .isPositive();
    try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      w.setAutoCommit(false);
      try (Statement s = w.createStatement()) {
        s.execute("INSERT INTO orders VALUES (1, 'first', 10.5)");
        w.commit();
        s.execute("UPDATE orders SET name = 'renamed', amount = 11 WHERE id = 1");
        w.commit();
        s.execute("DELETE FROM orders WHERE id = 1");
        w.commit();
      }
    }
    String topic = "cdc.FREEPDB1." + schema + ".ORDERS";
    try (KafkaConsumer<String, String> consumer = cluster.consumer("first-record", topic)) {
      List<ConsumerRecord<String, String>> records =
          ConnectCluster.consume(consumer, 4, Duration.ofMinutes(2), Duration.ofSeconds(3));
      assertThat(records).hasSize(4);
      JsonNode c = ConnectCluster.json(records.get(0).value());
      assertThat(c.path("op").asText()).isEqualTo("c");
      assertThat(c.path("before").isNull()).isTrue();
      assertThat(c.path("after").path("ID").asInt()).isEqualTo(1);
      assertThat(c.path("after").path("NAME").asText()).isEqualTo("first");
      assertThat(c.path("after").path("AMOUNT").decimalValue()).isEqualByComparingTo("10.5");
      assertThat(c.path("source").path("connector").asText()).isEqualTo("oracle-cdc");
      assertThat(c.path("source").path("db").asText()).isEqualTo("FREEPDB1");
      assertThat(c.path("source").path("schema").asText()).isEqualTo(schema);
      assertThat(c.path("source").path("table").asText()).isEqualTo("ORDERS");
      assertThat(c.path("source").path("txId").asText()).matches("\\d+\\.\\d+\\.\\d+");
      assertThat(c.path("source").path("scn").asLong()).isPositive();
      assertThat(c.path("transaction").path("total_order").asInt()).isEqualTo(1);
      assertThat(ConnectCluster.json(records.get(0).key()).path("ID").asInt()).isEqualTo(1);
      assertThat(
              new String(
                  records.get(0).headers().lastHeader("cdc.xid").value(), StandardCharsets.UTF_8))
          .isEqualTo(c.path("source").path("txId").asText());
      assertThat(
              new String(
                  records.get(0).headers().lastHeader("cdc.event_count").value(),
                  StandardCharsets.UTF_8))
          .isEqualTo("1");
      assertThat(records.get(0).headers().lastHeader("cdc.commit_scn")).isNotNull();

      JsonNode u = ConnectCluster.json(records.get(1).value());
      assertThat(u.path("op").asText()).isEqualTo("u");
      assertThat(u.path("before").path("NAME").asText()).isEqualTo("first");
      assertThat(u.path("after").path("NAME").asText()).isEqualTo("renamed");
      assertThat(u.path("after").path("AMOUNT").decimalValue()).isEqualByComparingTo("11");

      JsonNode d = ConnectCluster.json(records.get(2).value());
      assertThat(d.path("op").asText()).isEqualTo("d");
      assertThat(d.path("after").isNull()).isTrue();
      assertThat(d.path("before").path("ID").asInt()).isEqualTo(1);
      assertThat(records.get(3).value()).as("tombstone").isNull();
      assertThat(records.get(3).key()).isEqualTo(records.get(2).key());
    }
  }
}
