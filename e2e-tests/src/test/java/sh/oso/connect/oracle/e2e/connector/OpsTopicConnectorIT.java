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
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * P1-11 (SRC-TOP-6, SRC-OPS): with broker access configured the task creates the internal topics
 * with the right cleanup policy before its first record, and the ops topic carries a startup event,
 * then ddl-seen and ids-refreshed when a table is created in a captured schema.
 */
@Tag("connector")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OpsTopicConnectorIT {

  static final String CONNECTOR_CLASS = "sh.oso.connect.oracle.OracleCdcSourceConnector";
  static final String PREFIX = "ops";

  private final OracleTestDatabase db = OracleTestDatabase.get();
  private final String schema = SchemaFixtures.nameFor(getClass());
  private ConnectCluster cluster;

  @BeforeAll
  void up() throws Exception {
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Statement s = w.createStatement()) {
      s.execute("CREATE TABLE orders (id NUMBER(9) PRIMARY KEY, name VARCHAR2(50))");
      s.execute("ALTER TABLE orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
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
  void internalTopicsAreCreatedAndTheOpsTopicReportsStartupAndDdl() throws Exception {
    Map<String, String> c = new HashMap<>(ConnectCluster.oracleDatabaseProps("FREEPDB1"));
    c.put("connector.class", CONNECTOR_CLASS);
    c.put("tasks.max", "1");
    c.put("cdc.topic.prefix", PREFIX);
    c.put("cdc.tables.include", "FREEPDB1\\." + schema + "\\..*");
    c.put("cdc.poll.linger.ms", "100");
    // the worker's own in-network listener; the task's admin client runs inside the worker
    c.put("cdc.kafka.bootstrap.servers", "kafka:19092");
    cluster.register("ops-topic", c);
    cluster.awaitRunning("ops-topic", Duration.ofMinutes(2));
    JsonNode offsets = cluster.awaitOffsets("ops-topic", Duration.ofSeconds(60));
    long startScn = offsets.path("offsets").get(0).path("offset").path("resume_scn").asLong();
    assertThat(startScn).isPositive();

    Properties p = new Properties();
    p.put("bootstrap.servers", cluster.bootstrapServers());
    try (Admin admin = Admin.create(p)) {
      Set<String> names = admin.listTopics().names().get();
      assertThat(names)
          .contains(
              PREFIX + ".cdc.ops",
              PREFIX + ".cdc.heartbeat",
              PREFIX + ".cdc.signals",
              PREFIX + ".cdc.schema",
              PREFIX + ".cdc.txjournal");
      assertThat(cleanupPolicy(admin, PREFIX + ".cdc.schema")).isEqualTo("compact");
      assertThat(cleanupPolicy(admin, PREFIX + ".cdc.txjournal")).isEqualTo("compact");
      assertThat(cleanupPolicy(admin, PREFIX + ".cdc.ops")).isEqualTo("delete");
    }

    try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Statement s = w.createStatement()) {
      w.setAutoCommit(false);
      s.execute("CREATE TABLE late (id NUMBER PRIMARY KEY, v VARCHAR2(10))");
      s.execute("ALTER TABLE late ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      s.execute("INSERT INTO late VALUES (1, 'x')");
      w.commit();
    }
    try (KafkaConsumer<String, String> consumer =
        cluster.consumer("ops-reader", PREFIX + ".cdc.ops")) {
      List<ConsumerRecord<String, String>> events =
          ConnectCluster.consume(consumer, 3, Duration.ofMinutes(2), Duration.ofSeconds(5));
      List<String> types = events.stream().map(r -> type(r.value())).toList();
      assertThat(types)
          .as(types.toString())
          .startsWith("startup")
          .contains("ddl-seen", "ids-refreshed");
      JsonNode startup = ConnectCluster.json(events.get(0).value());
      assertThat(startup.path("v").asInt()).isEqualTo(1);
      assertThat(startup.path("server").asText()).isEqualTo(PREFIX);
      assertThat(startup.path("details").path("resume_scn").asLong()).isEqualTo(startScn);
      assertThat(startup.path("details").path("database").asText()).isNotEmpty();
      assertThat(ConnectCluster.json(events.get(0).key()).path("server").asText())
          .isEqualTo(PREFIX);
      JsonNode ddl =
          events.stream()
              .map(r -> uncheckedJson(r.value()))
              .filter(j -> "ddl-seen".equals(j.path("type").asText()))
              .filter(j -> "LATE".equals(j.path("details").path("object").asText()))
              .findFirst()
              .orElseThrow();
      assertThat(ddl.path("details").path("owner").asText()).isEqualTo(schema);
      assertThat(ddl.path("details").path("sql").asText()).startsWith("CREATE TABLE");
      assertThat(ddl.path("resume_scn").asLong()).isGreaterThanOrEqualTo(startScn);
    }
    // the table created after start is captured (SRC-SEL-4 in effect for new tables)
    try (KafkaConsumer<String, String> consumer =
        cluster.consumer("late-reader", PREFIX + ".FREEPDB1." + schema + ".LATE")) {
      List<ConsumerRecord<String, String>> rows =
          ConnectCluster.consume(consumer, 1, Duration.ofMinutes(2), Duration.ofSeconds(2));
      assertThat(rows).hasSize(1);
      assertThat(ConnectCluster.json(rows.get(0).value()).path("op").asText()).isEqualTo("c");
    }
  }

  private static String cleanupPolicy(Admin admin, String topic) throws Exception {
    ConfigResource r = new ConfigResource(ConfigResource.Type.TOPIC, topic);
    Config cfg = admin.describeConfigs(List.of(r)).all().get().get(r);
    return cfg.get("cleanup.policy").value();
  }

  private static String type(String value) {
    return uncheckedJson(value).path("type").asText();
  }

  private static JsonNode uncheckedJson(String value) {
    try {
      return ConnectCluster.json(value);
    } catch (java.io.IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
