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
package sh.oso.connect.oracle.e2e.regression;

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
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * Invariant: on a database with no changes to captured tables the committed offset still advances
 * (CORE-POS-5, SRC-HB-1), so archived logs older than the position can be purged and a restart does
 * not re-mine hours of redo. Debezium users hit the opposite with dbz#2781 (no periodic heartbeats
 * for quiet tables) and dbz#2475 (the {@code scn} in the offset stuck while the commit SCN moved
 * on).
 *
 * @see <a href="https://github.com/debezium/dbz/issues/2781">dbz#2781</a>
 * @see <a href="https://github.com/debezium/dbz/issues/2475">dbz#2475</a>
 */
@Tag("connector")
@Tag("dbz-2781")
@Tag("dbz-2475")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class AdvancesOffsetsOnQuietDatabaseConnectorIT {

  static final String CONNECTOR_CLASS = "sh.oso.connect.oracle.OracleCdcSourceConnector";
  static final String PREFIX = "quiet";

  private final OracleTestDatabase db = OracleTestDatabase.get();
  private final String schema = SchemaFixtures.nameFor(getClass());
  private ConnectCluster cluster;

  @BeforeAll
  void up() throws Exception {
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Statement s = w.createStatement()) {
      s.execute("CREATE TABLE quiet_t (id NUMBER PRIMARY KEY, v VARCHAR2(10))");
      s.execute("ALTER TABLE quiet_t ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
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
  void committedResumeScnAdvancesWithoutCapturedChanges() throws Exception {
    Map<String, String> c = new HashMap<>(ConnectCluster.oracleDatabaseProps("FREEPDB1"));
    c.put("connector.class", CONNECTOR_CLASS);
    c.put("tasks.max", "1");
    c.put("cdc.topic.prefix", PREFIX);
    c.put("cdc.tables.include", "FREEPDB1\\." + schema + "\\.QUIET_T");
    c.put("cdc.poll.linger.ms", "100");
    c.put("cdc.heartbeat.interval.ms", "1000");
    cluster.register("quiet", c);
    cluster.awaitRunning("quiet", Duration.ofMinutes(2));
    long start = resumeScn(cluster.awaitOffsets("quiet", Duration.ofSeconds(60)));
    // other activity moves the SCN: uncaptured DML in another schema and a log switch, no captured
    // change at all
    try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Statement s = w.createStatement()) {
      w.setAutoCommit(false);
      s.execute("CREATE TABLE other_t (id NUMBER PRIMARY KEY)");
      for (int i = 0; i < 20; i++) {
        s.execute("INSERT INTO other_t VALUES (" + i + ")");
      }
      w.commit();
    }
    OracleSql.archiveLogCurrent(db);
    long deadline = System.currentTimeMillis() + Duration.ofMinutes(2).toMillis();
    long last = start;
    int advances = 0;
    while (System.currentTimeMillis() < deadline && advances < 2) {
      Thread.sleep(2000);
      long now = resumeScn(cluster.awaitOffsets("quiet", Duration.ofSeconds(30)));
      if (now > last) {
        advances++;
        last = now;
      }
    }
    assertThat(advances)
        .as("resume_scn advanced at least twice from %s (last %s)", start, last)
        .isEqualTo(2);
    try (KafkaConsumer<String, String> consumer =
        cluster.consumer("quiet-hb", PREFIX + ".cdc.heartbeat")) {
      List<ConsumerRecord<String, String>> hbs =
          ConnectCluster.consume(consumer, 2, Duration.ofMinutes(1), Duration.ofSeconds(2));
      assertThat(hbs.size()).isGreaterThanOrEqualTo(2);
      assertThat(ConnectCluster.json(hbs.get(0).value()).path("reason").asText())
          .isEqualTo("start");
      assertThat(hbs.stream().anyMatch(r -> uncheckedReason(r.value()).equals("quiet"))).isTrue();
    }
    // no change record was produced for the captured table
    try (KafkaConsumer<String, String> consumer =
        cluster.consumer("quiet-data", PREFIX + ".FREEPDB1." + schema + ".QUIET_T")) {
      assertThat(ConnectCluster.consume(consumer, 1, Duration.ofSeconds(5), Duration.ofSeconds(1)))
          .isEmpty();
    }
  }

  private static long resumeScn(JsonNode offsets) {
    return offsets.path("offsets").get(0).path("offset").path("resume_scn").asLong();
  }

  private static String uncheckedReason(String value) {
    try {
      return ConnectCluster.json(value).path("reason").asText();
    } catch (java.io.IOException e) {
      throw new IllegalStateException(e);
    }
  }
}
