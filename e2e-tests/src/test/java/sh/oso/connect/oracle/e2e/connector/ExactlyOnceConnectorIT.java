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

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * SRC-EOS-1 to SRC-EOS-4 (ADR-0007) through a real worker with exactly.once.support=required and
 * transaction.boundary=connector: while the worker is killed and restarted twice, a read_committed
 * consumer sees every row exactly once and every Oracle transaction whole, and a transaction above
 * cdc.eos.split.max.records arrives in several Kafka transactions marked with cdc.split.
 */
@Tag("connector")
class ExactlyOnceConnectorIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void workerKillsNeitherDuplicateNorSplitOracleTransactions() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (ConnectCluster cluster = new ConnectCluster().start();
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      try (Statement s = w.createStatement()) {
        s.execute("CREATE TABLE eo (id NUMBER PRIMARY KEY, v VARCHAR2(20))");
        s.execute("ALTER TABLE eo ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      }
      Map<String, String> c = new HashMap<>(ConnectCluster.oracleDatabaseProps("FREEPDB1"));
      c.put("connector.class", FirstRecordConnectorIT.CONNECTOR_CLASS);
      c.put("tasks.max", "1");
      c.put("exactly.once.support", "required");
      c.put("transaction.boundary", "connector");
      c.put("cdc.topic.prefix", "eo");
      c.put("cdc.tables.include", "FREEPDB1\\." + schema + "\\.EO");
      c.put("cdc.poll.linger.ms", "100");
      c.put("cdc.decimal.mode", "string");
      c.put("cdc.eos.batch.max.records", "40");
      c.put("cdc.eos.split.max.records", "50");
      // the table is empty at the start; without this a kill before the empty snapshot's end is
      // committed makes the next run snapshot the rows written since, as op=r records that carry
      // no transaction headers
      c.put("cdc.snapshot.mode", "none");
      cluster.register("oracle-cdc", c);
      cluster.awaitRunning("oracle-cdc", Duration.ofMinutes(2));
      Thread.sleep(2000);

      int transactions = 400;
      int big = 180; // one transaction this size, split into four Kafka transactions
      CompletableFuture<Integer> workload =
          CompletableFuture.supplyAsync(
              () -> {
                int rows = 0;
                try (PreparedStatement ps = w.prepareStatement("INSERT INTO eo VALUES (?, ?)")) {
                  w.setAutoCommit(false);
                  for (int t = 0; t < transactions; t++) {
                    int n = t == 150 ? big : 1 + t % 7;
                    for (int j = 0; j < n; j++) {
                      ps.setInt(1, t * 1000 + j);
                      ps.setString(2, "t" + t);
                      ps.executeUpdate();
                      rows++;
                    }
                    w.commit();
                    Thread.sleep(40);
                  }
                } catch (Exception e) {
                  throw new IllegalStateException(e);
                }
                return rows;
              });
      Thread.sleep(5000);
      cluster.killAndRestartWorker();
      cluster.awaitRunning("oracle-cdc", Duration.ofMinutes(3));
      Thread.sleep(4000);
      cluster.killAndRestartWorker();
      cluster.awaitRunning("oracle-cdc", Duration.ofMinutes(3));
      int expected = workload.get();

      Map<Integer, Integer> copies = new HashMap<>();
      Map<String, Set<Integer>> indexesByXid = new HashMap<>();
      Map<String, Integer> countByXid = new HashMap<>();
      Set<String> splitXids = new HashSet<>();
      String topic = "eo.FREEPDB1." + schema + ".EO";
      try (KafkaConsumer<String, String> consumer = cluster.consumer("eos-check", topic)) {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(4).toMillis();
        long lastNew = System.currentTimeMillis();
        while (System.currentTimeMillis() < deadline && copies.size() < expected) {
          var batch = consumer.poll(Duration.ofMillis(500));
          if (batch.isEmpty()) {
            if (System.currentTimeMillis() - lastNew > Duration.ofSeconds(60).toMillis()) {
              break;
            }
            continue;
          }
          lastNew = System.currentTimeMillis();
          for (ConsumerRecord<String, String> r : batch) {
            if (r.value() == null) {
              continue;
            }
            int id =
                Integer.parseInt(ConnectCluster.json(r.value()).path("after").path("ID").asText());
            copies.merge(id, 1, Integer::sum);
            String xid = header(r, "cdc.xid");
            indexesByXid
                .computeIfAbsent(xid, k -> new HashSet<>())
                .add(Integer.parseInt(header(r, "cdc.event_index")));
            countByXid.put(xid, Integer.parseInt(header(r, "cdc.event_count")));
            if (r.headers().lastHeader("cdc.split") != null) {
              splitXids.add(xid);
            }
          }
        }
      }
      assertThat(copies).as("every row delivered").hasSize(expected);
      assertThat(copies.values()).as("no row delivered twice").allMatch(n -> n == 1);
      for (Map.Entry<String, Set<Integer>> e : indexesByXid.entrySet()) {
        assertThat(e.getValue())
            .as("transaction %s delivered whole", e.getKey())
            .hasSize(countByXid.get(e.getKey()));
      }
      assertThat(indexesByXid).hasSize(transactions);
      assertThat(splitXids).hasSize(1);
      assertThat(indexesByXid.get(splitXids.iterator().next())).hasSize(big);
      System.out.println(
          "exactly-once: "
              + expected
              + " rows in "
              + transactions
              + " transactions, no duplicates");
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  private static String header(ConsumerRecord<String, String> r, String name) {
    Header h = r.headers().lastHeader(name);
    return h == null ? null : new String(h.value(), StandardCharsets.UTF_8);
  }
}
