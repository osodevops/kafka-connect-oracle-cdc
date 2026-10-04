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
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.bench.workload.Ledger;
import sh.oso.connect.oracle.bench.workload.WorkloadGenerator;
import sh.oso.connect.oracle.bench.workload.WorkloadResult;
import sh.oso.connect.oracle.bench.workload.WorkloadSpec;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * P1-10, at-least-once: the Connect worker is killed with SIGKILL while a workload runs and
 * restarted; afterwards every committed transaction in the ledger is in Kafka and duplicates are
 * confined to a handful of in-flight transactions (SRC-EOS-5).
 */
@Tag("connector")
class RestartNoLossConnectorIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void killingTheWorkerMidWorkloadLosesNoCommittedTransaction() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (ConnectCluster cluster = new ConnectCluster().start();
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      WorkloadSpec spec = WorkloadSpec.defaults();
      spec.seed = 21;
      spec.sessions = 2;
      spec.tables = 2;
      spec.transactionsPerSession = 0;
      spec.durationSeconds = 25;
      spec.maxRowsPerTransaction = 4;
      spec.savepointRollbackProbability = 0.1;
      spec.fullRollbackProbability = 0.1;
      spec.lobWeight = 0;
      spec.keyChangeWeight = 0;
      WorkloadGenerator g =
          new WorkloadGenerator(spec, db.jdbcUrl(OracleTestDatabase.PDB1), schema, schema);
      g.reset();

      Map<String, String> c = new HashMap<>(ConnectCluster.oracleDatabaseProps("FREEPDB1"));
      c.put("connector.class", FirstRecordConnectorIT.CONNECTOR_CLASS);
      c.put("tasks.max", "1");
      c.put("cdc.topic.prefix", "rl");
      c.put("cdc.tables.include", "FREEPDB1\\." + schema + "\\.WL_.*");
      c.put("cdc.tables.exclude", "FREEPDB1\\." + schema + "\\.WL_LEDGER");
      c.put("cdc.poll.linger.ms", "100");
      cluster.register("oracle-cdc", c);
      cluster.awaitRunning("oracle-cdc", Duration.ofMinutes(2));
      Thread.sleep(2000);

      CompletableFuture<WorkloadResult> workload =
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  return g.run();
                } catch (Exception e) {
                  throw new IllegalStateException(e);
                }
              });
      Thread.sleep(8000);
      String killedAt = java.time.LocalTime.now(java.time.ZoneOffset.UTC).toString();
      cluster.killAndRestartWorker();
      cluster.awaitRunning("oracle-cdc", Duration.ofMinutes(3));
      WorkloadResult r = workload.get();
      assertThat(r.committed.sum()).isPositive();

      Set<String> ledger = Ledger.committedXids(w, schema, spec.ledgerTable);
      // a transaction whose only data operations hit zero rows has nothing to publish
      Set<String> expected = new HashSet<>();
      try (var ps =
              w.prepareStatement(
                  "SELECT xid FROM " + schema + "." + spec.ledgerTable + " WHERE ops > 0");
          var rs = ps.executeQuery()) {
        while (rs.next()) {
          expected.add(rs.getString(1));
        }
      }
      assertThat(ledger).containsAll(expected);

      String[] topics = new String[spec.tables];
      for (int i = 1; i <= spec.tables; i++) {
        topics[i - 1] = "rl.FREEPDB1." + schema + "." + spec.tableName(i);
      }
      Set<String> seen = new HashSet<>();
      Map<String, Integer> copies = new HashMap<>();
      // consume until every expected transaction has been seen, or nothing new has arrived for a
      // generous idle period: the restarted task may still be catching up on the backlog
      try (KafkaConsumer<String, String> consumer = cluster.consumer("restart-no-loss", topics)) {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(5).toMillis();
        long lastNew = System.currentTimeMillis();
        while (System.currentTimeMillis() < deadline && !seen.containsAll(expected)) {
          var batch = consumer.poll(Duration.ofMillis(500));
          if (batch.isEmpty()) {
            if (System.currentTimeMillis() - lastNew > Duration.ofSeconds(45).toMillis()) {
              break;
            }
            continue;
          }
          lastNew = System.currentTimeMillis();
          for (ConsumerRecord<String, String> rec : batch) {
            if (rec.value() == null) {
              continue; // tombstone
            }
            JsonNode v = ConnectCluster.json(rec.value());
            String xid = v.path("source").path("txId").asText();
            String idx =
                new String(
                    rec.headers().lastHeader("cdc.event_index").value(), StandardCharsets.UTF_8);
            seen.add(xid);
            copies.merge(xid + "#" + idx, 1, Integer::sum);
          }
        }
      }
      Set<String> missing = new HashSet<>(expected);
      missing.removeAll(seen);
      if (!missing.isEmpty()) {
        StringBuilder when = new StringBuilder();
        try (var ps =
            w.prepareStatement(
                "SELECT xid, seq, session_id, TO_CHAR(SYS_EXTRACT_UTC(committed_at),"
                    + " 'HH24:MI:SS.FF3') FROM "
                    + schema
                    + "."
                    + spec.ledgerTable
                    + " WHERE xid = ?")) {
          for (String xid : missing.stream().limit(5).toList()) {
            ps.setString(1, xid);
            try (var rs = ps.executeQuery()) {
              while (rs.next()) {
                when.append(rs.getString(1))
                    .append(" seq=")
                    .append(rs.getInt(2))
                    .append(" session=")
                    .append(rs.getInt(3))
                    .append(" committed=")
                    .append(rs.getString(4))
                    .append("; ");
              }
            }
          }
        }
        assertThat(missing)
            .as("committed transactions missing from Kafka (kill at %s UTC): %s", killedAt, when)
            .isEmpty();
      }
      assertThat(seen).as("only committed transactions reach Kafka").allMatch(ledger::contains);
      long duplicatedTransactions =
          copies.entrySet().stream()
              .filter(e -> e.getValue() > 1)
              .map(e -> e.getKey().split("#")[0])
              .distinct()
              .count();
      assertThat(duplicatedTransactions)
          .as("duplicates confined to transactions in flight at the kill")
          .isLessThanOrEqualTo(4);
      System.out.println(
          "restart-no-loss: "
              + r.toJson()
              + " expected="
              + expected.size()
              + " seen="
              + seen.size()
              + " duplicatedTransactions="
              + duplicatedTransactions);
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }
}
