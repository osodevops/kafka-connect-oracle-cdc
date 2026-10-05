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

import java.sql.Connection;
import java.time.Duration;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.bench.check.CheckReport;
import sh.oso.connect.oracle.bench.workload.WorkloadGenerator;
import sh.oso.connect.oracle.bench.workload.WorkloadResult;
import sh.oso.connect.oracle.bench.workload.WorkloadSpec;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.Evidence;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * T2 repeated worker kills under exactly-once (testing strategy section 4, "1,000 iterations in
 * nightly"; P1-12 follow-up): the Connect worker is killed with SIGKILL N times at seeded random
 * moments while a workload with large, split transactions runs, and restarted each time. A
 * read_committed consumer must then see every committed transaction of the ledger, the table state
 * must equal the database at the check SCN, and no (xid, event_index) may arrive twice.
 *
 * <p>{@code -Dnightly.kills} sets N (default 25). Each kill waits for the restarted task to run
 * again, so a kill costs a worker start (tens of seconds); 1,000 kills take many hours and need a
 * job timeout to match.
 */
@Tag("nightly")
class RepeatedWorkerKillsNightlyIT {

  static final String NAME = "worker-kills";
  static final String PREFIX = "wk";
  static final int FAULT_ENTRIES = 100;

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void repeatedSigkillsUnderExactlyOnceNeitherLoseNorDuplicate() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    int kills = Integer.getInteger("nightly.kills", 25);
    long seed = NightlyRun.seed(808);
    WorkloadSpec spec = NightlyRun.openEnded(seed);
    spec.durationSeconds = (int) Math.max(4L * 3600, kills * 120L);
    spec.largeTransactionEvery = 30;
    spec.largeTransactionRows = 120;
    Evidence ev =
        Evidence.of(
            getClass(),
            "Connect worker killed with SIGKILL N times under exactly-once while a workload with"
                + " large transactions runs; read_committed correctness oracle, no duplicates");
    ev.param("workload", NightlyRun.describe(spec))
        .param("kills", kills)
        .param("killDelayMs", "1000 to 8000 after the task runs again, seeded")
        .param("eosBatchMaxRecords", 40)
        .param("eosSplitMaxRecords", 50);
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    int done = 0;
    try (ConnectCluster cluster = new ConnectCluster().start();
        Connection root = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      WorkloadGenerator g =
          new WorkloadGenerator(spec, db.jdbcUrl(OracleTestDatabase.PDB1), schema, schema);
      g.reset();
      Thread.sleep(3500);
      Map<String, String> config =
          NightlyRun.connector(
              PREFIX, schema, spec, ConnectCluster.oracleDatabaseProps("FREEPDB1"));
      config.put("exactly.once.support", "required");
      config.put("transaction.boundary", "connector");
      config.put("cdc.eos.batch.max.records", "40");
      config.put("cdc.eos.split.max.records", "50");
      cluster.register(NAME, config);
      cluster.awaitRunning(NAME, Duration.ofMinutes(3));
      cluster.awaitOffsets(NAME, Duration.ofSeconds(90));

      CompletableFuture<WorkloadResult> run = NightlyRun.start(g);
      Random rnd = new Random(seed);
      for (int i = 1; i <= kills; i++) {
        Thread.sleep(1000 + rnd.nextInt(7000));
        if (run.isDone()) {
          run.get(); // surfaces a workload failure
          throw new AssertionError("the workload ended before kill " + i);
        }
        if (i <= FAULT_ENTRIES) {
          ev.fault("sigkill-worker", "kill", i);
        }
        cluster.killAndRestartWorker();
        cluster.awaitRunning(NAME, Duration.ofMinutes(4));
        done = i;
        if (i % 25 == 0) {
          System.out.println("worker-kills: " + i + " of " + kills);
        }
      }
      ev.count("killsDone", done);

      WorkloadResult r = NightlyRun.stop(g, run);
      long end = NightlyRun.scn(root);
      ev.param("workloadResult", ConnectCluster.json(r.toJson()));
      cluster.awaitRunning(NAME, Duration.ofMinutes(3));
      boolean caughtUp = NightlyRun.awaitResumePast(cluster, NAME, end, Duration.ofMinutes(10));
      CheckReport report =
          NightlyRun.check(cluster, w, schema, spec, PREFIX, Duration.ofMinutes(15));
      ev.check("oracle", report)
          .count("caughtUp", caughtUp)
          .count("committedTransactions", r.committed.sum())
          .count("recordsConsumed", report.recordsConsumed())
          .count(
              "transactionsWithDuplicateEvents",
              report.invariants().get("transactionsWithDuplicateEvents"));
      System.out.println(
          "worker-kills: kills="
              + done
              + " "
              + r.toJson()
              + " duplicates="
              + report.invariants().get("transactionsWithDuplicateEvents")
              + " verdict="
              + report.verdict());
      assertThat(report.verdict()).as(report.toJson()).isEqualTo(CheckReport.Verdict.PASS);
      assertThat(report.transactions().get("seen")).isEqualTo(report.transactions().get("ledger"));
      assertThat(report.invariants().get("transactionsWithDuplicateEvents"))
          .as("exactly-once: no (xid, event_index) delivered twice to read_committed")
          .isEqualTo(0L);
    } catch (Throwable t) {
      ev.count("killsDone", done);
      ev.failed(t);
      throw t;
    } finally {
      ev.write();
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }
}
