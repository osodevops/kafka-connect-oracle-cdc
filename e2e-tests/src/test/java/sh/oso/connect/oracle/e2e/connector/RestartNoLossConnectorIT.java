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
import java.util.List;
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
        Connection root = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      long workloadStartScn = sh.oso.connect.oracle.e2e.support.LogMinerHelper.currentScn(root);
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
        diagnose(root, cluster, missing, workloadStartScn);
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

  /**
   * On a miss, print what LogMiner holds for the missing transactions and what the connector
   * published about itself, so the failure can be explained from the log alone.
   */
  private static void diagnose(
      Connection root, ConnectCluster cluster, Set<String> missing, long fromScn) {
    try {
      long end = sh.oso.connect.oracle.e2e.support.LogMinerHelper.currentScn(root);
      sh.oso.connect.oracle.e2e.support.LogMinerHelper.start(root, fromScn, end);
      for (String xid : missing.stream().limit(3).toList()) {
        String[] p = xid.split("\\.");
        String raw =
            String.format(
                "%04X%04X%08X", Long.parseLong(p[0]), Long.parseLong(p[1]), Long.parseLong(p[2]));
        var rows =
            sh.oso.connect.oracle.e2e.support.LogMinerHelper.rows(
                root, "XID = HEXTORAW('" + raw + "') ORDER BY SCN, RS_ID, SSN");
        System.out.println("diagnose " + xid + ": " + rows.size() + " LogMiner rows");
        for (var r : rows) {
          System.out.println(
              "  scn="
                  + r.get("SCN")
                  + " op="
                  + r.get("OPERATION")
                  + " table="
                  + r.get("TABLE_NAME")
                  + " rs_id="
                  + r.get("RS_ID")
                  + " ssn="
                  + r.get("SSN")
                  + " rollback="
                  + r.get("ROLLBACK")
                  + " status="
                  + r.get("STATUS"));
        }
      }
      sh.oso.connect.oracle.e2e.support.LogMinerHelper.end(root);
    } catch (Exception e) {
      System.out.println("diagnose: LogMiner lookup failed: " + e);
    }
    try (KafkaConsumer<String, String> c = cluster.consumer("diag-ops", "rl.cdc.ops")) {
      for (ConsumerRecord<String, String> r :
          ConnectCluster.consume(c, 1, Duration.ofSeconds(20), Duration.ofSeconds(2))) {
        System.out.println("diagnose ops: " + r.value());
      }
    }
    try (KafkaConsumer<String, String> c = cluster.consumer("diag-hb", "rl.cdc.heartbeat")) {
      List<ConsumerRecord<String, String>> hb =
          ConnectCluster.consume(c, 1, Duration.ofSeconds(20), Duration.ofSeconds(2));
      hb.stream()
          .skip(Math.max(0, hb.size() - 6))
          .forEach(r -> System.out.println("diagnose heartbeat: " + r.value()));
    }
  }
}
