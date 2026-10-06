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
package sh.oso.connect.oracle.e2e.nightly;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.Connection;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.bench.check.CheckReport;
import sh.oso.connect.oracle.bench.workload.WorkloadGenerator;
import sh.oso.connect.oracle.bench.workload.WorkloadResult;
import sh.oso.connect.oracle.bench.workload.WorkloadSpec;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.Evidence;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * T2 database restart (testing strategy section 4): SHUTDOWN ABORT and STARTUP of the instance
 * while the connector streams a running workload. Workload A loses its sessions with the instance
 * (its open transactions are rolled back by crash recovery; the ledger holds only what committed);
 * workload B runs on new sessions after the restart. The connector must reconnect without a task
 * failure (CORE-CONN-6) and the correctness oracle must pass for both workloads with no duplicate.
 * Transactions left open by the crash exercise orphan detection with a short check interval; any
 * release is recorded in the evidence.
 *
 * <p>The container is shared by every suite in the JVM: the finally block makes sure the instance
 * is open again with its three PDBs READ WRITE before the next suite runs.
 */
@Tag("nightly")
class DatabaseRestartNightlyIT {

  static final String NAME = "db-restart";
  static final String PREFIX = "dr";

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void shutdownAbortDuringStreamingEndsInAReconnectAndTheOraclePasses() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    long seed = NightlyRun.seed(404);
    WorkloadSpec a = NightlyRun.openEnded(seed);
    a.tablePrefix = "WL_A";
    a.ledgerTable = "WL_LEDGER_A";
    WorkloadSpec b = NightlyRun.openEnded(seed + 1);
    b.tablePrefix = "WL_B";
    b.ledgerTable = "WL_LEDGER_B";
    b.transactionsPerSession = 120;
    b.durationSeconds = 0;
    Evidence ev =
        Evidence.of(
            getClass(),
            "SHUTDOWN ABORT and STARTUP during streaming: workload A cut off by the crash,"
                + " workload B after it; reconnect and correctness oracle for both");
    ev.param("workloadA", NightlyRun.describe(a))
        .param("workloadB", NightlyRun.describe(b))
        .param("orphanCheckIntervalMs", 10_000);
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (ConnectCluster cluster = new ConnectCluster().start()) {
      WorkloadGenerator ga =
          new WorkloadGenerator(a, db.jdbcUrl(OracleTestDatabase.PDB1), schema, schema);
      WorkloadGenerator gb =
          new WorkloadGenerator(b, db.jdbcUrl(OracleTestDatabase.PDB1), schema, schema);
      ga.reset();
      gb.reset();
      Thread.sleep(3500);
      var config =
          NightlyRun.connector(
              PREFIX, schema, "WL_[AB][0-9]+", ConnectCluster.oracleDatabaseProps("FREEPDB1"));
      config.put("cdc.transaction.orphan.check.interval.ms", "10000");
      cluster.register(NAME, config);
      cluster.awaitRunning(NAME, Duration.ofMinutes(3));
      cluster.awaitOffsets(NAME, Duration.ofSeconds(90));

      CompletableFuture<WorkloadResult> runA = NightlyRun.start(ga);
      Thread.sleep(15_000);
      ev.fault("shutdown-abort-and-startup");
      String sqlplus = OracleSql.abortAndRestartInstance(db, Duration.ofMinutes(6));
      ev.fault("instance-open");
      ev.note("sqlplus: " + sqlplus.replaceAll("\\s+", " ").trim());
      String endA;
      try {
        WorkloadResult r = runA.get(3, TimeUnit.MINUTES);
        endA = "completed " + r.toJson();
      } catch (ExecutionException e) {
        endA = "cut off by the restart: " + e.getCause();
      }
      ev.note("workload A " + (endA.length() > 300 ? endA.substring(0, 300) : endA));

      // the task stays RUNNING through the outage: a FAILED task fails here with its trace
      cluster.awaitRunning(NAME, Duration.ofMinutes(5));
      WorkloadResult rb = gb.run();
      ev.param("workloadBResult", ConnectCluster.json(rb.toJson()));

      // connections opened before the restart are gone: the checks use new ones
      try (Connection root = db.capture(OracleTestDatabase.CDB_SERVICE);
          Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
        long end = NightlyRun.scn(root);
        cluster.awaitRunning(NAME, Duration.ofMinutes(2));
        boolean caughtUp = NightlyRun.awaitResumePast(cluster, NAME, end, Duration.ofMinutes(8));
        // both workloads have ended: the tables' state at the end SCN is their state at their last
        // commit. Workload A's last commit is just before SHUTDOWN ABORT, which a flashback read
        // cannot reach afterwards (ORA-01466), so the state is compared at the end SCN
        CheckReport ra =
            NightlyRun.check(cluster, w, schema, a, PREFIX, Duration.ofMinutes(6), end);
        CheckReport rbReport =
            NightlyRun.check(cluster, w, schema, b, PREFIX, Duration.ofMinutes(6), end);
        List<JsonNode> ops = NightlyRun.ops(cluster, PREFIX);
        List<JsonNode> reconnects = NightlyRun.ofType(ops, "reconnected");
        List<JsonNode> released = NightlyRun.ofType(ops, "transaction-orphan-released");
        ev.check("oracleWorkloadA", ra)
            .check("oracleWorkloadB", rbReport)
            .count("caughtUp", caughtUp)
            .count("reconnectedEvents", reconnects.size())
            .count("orphanReleases", released.size());
        released.forEach(
            j ->
                ev.note(
                    "orphan released: xid="
                        + j.path("details").path("xid").asText()
                        + " reason="
                        + j.path("details").path("reason").asText()));
        System.out.println(
            "db-restart: reconnects="
                + reconnects.size()
                + " orphanReleases="
                + released.size()
                + " A="
                + ra.verdict()
                + " B="
                + rbReport.verdict());

        assertThat(ra.verdict()).as(ra.toJson()).isEqualTo(CheckReport.Verdict.PASS);
        assertThat(rbReport.verdict()).as(rbReport.toJson()).isEqualTo(CheckReport.Verdict.PASS);
        assertThat(ra.invariants().get("transactionsWithDuplicateEvents")).isEqualTo(0L);
        assertThat(rbReport.invariants().get("transactionsWithDuplicateEvents")).isEqualTo(0L);
        assertThat(reconnects).as("CORE-CONN-6: the outage ended in a reconnect").isNotEmpty();
      }
    } catch (Throwable t) {
      ev.failed(t);
      throw t;
    } finally {
      ev.write();
      OracleSql.ensureOpen(db, Duration.ofMinutes(6));
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }
}
