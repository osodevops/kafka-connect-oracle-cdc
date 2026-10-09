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
package sh.oso.connect.oracle.e2e.qual;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.Evidence;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;
import sh.oso.connect.oracle.e2e.support.TestDatabase;

/**
 * A real Kafka Connect worker captures the target and loses nothing when it is killed mid-stream:
 * every committed row arrives in Kafka at least once after the restart. On an external database the
 * worker, in Docker on the workstation, reaches it through the host (the SSM port forward); the
 * suite skips when the worker has no route.
 */
@Tag("qual")
class ConnectorQualIT {

  static final String CONNECTOR_CLASS = "sh.oso.connect.oracle.OracleCdcSourceConnector";
  static final int ROWS_PER_PHASE = 50;

  private final TestDatabase db = TestDatabase.get();

  @Test
  void aConnectWorkerCapturesEveryCommittedRowAcrossAWorkerKill() throws Exception {
    Evidence ev =
        Evidence.of(getClass(), "connect worker, kill and restart").param("target", db.describe());
    String schema = SchemaFixtures.nameFor(getClass());
    try (ConnectCluster cluster = new ConnectCluster().start()) {
      assumeTrue(
          cluster.workerReaches(db.workerHost(), db.workerPort()),
          "the Connect worker has no route to " + db.workerHost() + ":" + db.workerPort());
      db.recreateSchema(schema);
      try (Connection w = db.workload(schema)) {
        exec(
            w,
            "CREATE TABLE q (id NUMBER(10) PRIMARY KEY, name VARCHAR2(40))",
            "ALTER TABLE q ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        Map<String, String> c = new HashMap<>(db.connectorDatabaseProps());
        c.put("connector.class", CONNECTOR_CLASS);
        c.put("tasks.max", "1");
        c.put("cdc.topic.prefix", "ql");
        c.put("cdc.tables.include", db.include(schema, "Q"));
        c.put("cdc.snapshot.mode", "none");
        c.put("cdc.poll.linger.ms", "100");
        cluster.register("oracle-cdc", c);
        cluster.awaitRunning("oracle-cdc", Duration.ofMinutes(3));
        Thread.sleep(5000); // the task's first step starts after registration

        int id = 0;
        id = insert(w, id, ROWS_PER_PHASE); // before the kill
        Thread.sleep(3000);
        cluster.killAndRestartWorker();
        id = insert(w, id, ROWS_PER_PHASE); // while the worker is down or restarting
        cluster.awaitRunning("oracle-cdc", Duration.ofMinutes(5));
        id = insert(w, id, ROWS_PER_PHASE); // after the restart
        int expected = id;

        String topic = awaitTopic(cluster, schema, Duration.ofMinutes(5));
        ev.param("topic", topic);
        Set<Integer> seen = new TreeSet<>();
        int records = 0;
        try (KafkaConsumer<String, String> consumer = cluster.consumer("qual-connector", topic)) {
          long deadline = System.currentTimeMillis() + Duration.ofMinutes(10).toMillis();
          while (seen.size() < expected && System.currentTimeMillis() < deadline) {
            List<ConsumerRecord<String, String>> batch =
                ConnectCluster.consume(consumer, 1, Duration.ofSeconds(30), Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> r : batch) {
              records++;
              if (r.value() == null) {
                continue;
              }
              int rowId = ConnectCluster.json(r.value()).path("after").path("ID").asInt(-1);
              if (rowId > 0) {
                seen.add(rowId);
              }
            }
          }
        }
        ev.count("committedRows", expected)
            .count("distinctRowsSeen", seen.size())
            .count("records", records);
        Set<Integer> missing = new TreeSet<>();
        for (int i = 1; i <= expected; i++) {
          if (!seen.contains(i)) {
            missing.add(i);
          }
        }
        assertThat(missing).as("committed rows never delivered").isEmpty();
        cluster.delete("oracle-cdc");
      }
    } catch (Throwable t) {
      ev.failed(t);
      throw t;
    } finally {
      ev.write();
      db.dropSchema(schema);
    }
  }

  /** Inserts {@code n} rows after {@code from}, one committed transaction each. */
  private static int insert(Connection w, int from, int n) throws SQLException {
    try (Statement s = w.createStatement()) {
      for (int i = from + 1; i <= from + n; i++) {
        s.execute("INSERT INTO q VALUES (" + i + ", 'row " + i + "')");
      }
    }
    return from + n;
  }

  private static String awaitTopic(ConnectCluster cluster, String schema, Duration timeout)
      throws InterruptedException {
    String suffix = ("." + schema + ".Q").toUpperCase(java.util.Locale.ROOT);
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    while (System.currentTimeMillis() < deadline) {
      for (String t : cluster.topics()) {
        if (t.toUpperCase(java.util.Locale.ROOT).endsWith(suffix)) {
          return t;
        }
      }
      Thread.sleep(2000);
    }
    throw new AssertionError("no topic for " + schema + ".Q within " + timeout);
  }

  private static void exec(Connection c, String... sql) throws SQLException {
    try (Statement s = c.createStatement()) {
      for (String q : sql) {
        s.execute(q);
      }
    }
  }
}
