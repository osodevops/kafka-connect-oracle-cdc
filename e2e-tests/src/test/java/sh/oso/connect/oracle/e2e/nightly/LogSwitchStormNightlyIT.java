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
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * T2 log switch storm (testing strategy section 4, "online log overwritten during mining"): while a
 * workload with large transactions runs, one SYSDBA session switches the small online logs every
 * few hundred milliseconds, every fourth time with ARCHIVE LOG CURRENT. Online logs the connector
 * is reading are recycled under it (ORA-00310 and ORA-00334 are step retries, CORE-MINE), and
 * transactions span many logs. The correctness oracle must pass with no duplicate.
 *
 * <p>{@code -Dnightly.storm.seconds} sets the storm's length (default 60).
 */
@Tag("nightly")
class LogSwitchStormNightlyIT {

  static final String NAME = "switch-storm";
  static final String PREFIX = "ss";

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void rapidLogSwitchesDuringAWorkloadLoseAndDuplicateNothing() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    long stormSeconds = Long.getLong("nightly.storm.seconds", 60);
    long seed = NightlyRun.seed(202);
    WorkloadSpec spec = NightlyRun.openEnded(seed);
    spec.sessions = 3;
    spec.largeTransactionEvery = 25;
    spec.largeTransactionRows = 400;
    Evidence ev =
        Evidence.of(
            getClass(),
            "log switch storm: SWITCH LOGFILE and ARCHIVE LOG CURRENT every few hundred"
                + " milliseconds while a workload with large transactions runs; correctness"
                + " oracle");
    ev.param("workload", NightlyRun.describe(spec))
        .param("stormSeconds", stormSeconds)
        .param("pauseBetweenSwitchesMs", "100 to 500, seeded")
        .param("archiveLogCurrentEvery", 4);
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (ConnectCluster cluster = new ConnectCluster().start();
        Connection root = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      WorkloadGenerator g =
          new WorkloadGenerator(spec, db.jdbcUrl(OracleTestDatabase.PDB1), schema, schema);
      g.reset();
      Thread.sleep(3500);
      cluster.register(
          NAME,
          NightlyRun.connector(
              PREFIX, schema, spec, ConnectCluster.oracleDatabaseProps("FREEPDB1")));
      cluster.awaitRunning(NAME, Duration.ofMinutes(3));
      cluster.awaitOffsets(NAME, Duration.ofSeconds(90));

      CompletableFuture<WorkloadResult> run = NightlyRun.start(g);
      Thread.sleep(5_000);
      int switches = 0;
      int archives = 0;
      long firstSequence;
      long lastSequence;
      // one SYSDBA session for the whole storm: a connection per statement exhausts processes
      try (Connection sys = db.sysdba(OracleTestDatabase.CDB_SERVICE)) {
        firstSequence = OracleSql.maxArchivedSequence(sys);
        Random rnd = new Random(seed);
        ev.fault("storm-start", "archivedSequence", firstSequence);
        long until = System.currentTimeMillis() + stormSeconds * 1000;
        while (System.currentTimeMillis() < until) {
          if (switches % 4 == 3) {
            OracleSql.archiveLogCurrent(sys);
            archives++;
          } else {
            OracleSql.switchLogfile(sys);
          }
          switches++;
          Thread.sleep(100 + rnd.nextInt(400));
        }
        lastSequence = OracleSql.maxArchivedSequence(sys);
        ev.fault("storm-end", "archivedSequence", lastSequence, "switches", switches);
      }
      Thread.sleep(5_000);
      WorkloadResult r = NightlyRun.stop(g, run);
      long end = NightlyRun.scn(root);
      ev.param("workloadResult", ConnectCluster.json(r.toJson()));
      cluster.awaitRunning(NAME, Duration.ofMinutes(3));
      boolean caughtUp = NightlyRun.awaitResumePast(cluster, NAME, end, Duration.ofMinutes(8));

      CheckReport report =
          NightlyRun.check(cluster, w, schema, spec, PREFIX, Duration.ofMinutes(8));
      ev.check("oracle", report)
          .count("caughtUp", caughtUp)
          .count("logSwitches", switches)
          .count("archiveLogCurrent", archives)
          .count("archivedLogsDuringStorm", lastSequence - firstSequence)
          .count("committedTransactions", r.committed.sum())
          .count("recordsConsumed", report.recordsConsumed());
      System.out.println(
          "switch-storm: switches="
              + switches
              + " archived="
              + (lastSequence - firstSequence)
              + " "
              + r.toJson()
              + " verdict="
              + report.verdict());

      assertThat(lastSequence - firstSequence)
          .as("the storm archived logs while the workload ran")
          .isGreaterThanOrEqualTo(archives);
      assertThat(report.verdict()).as(report.toJson()).isEqualTo(CheckReport.Verdict.PASS);
      assertThat(report.invariants().get("transactionsWithDuplicateEvents"))
          .as("no worker restart, so no transaction is delivered twice")
          .isEqualTo(0L);
    } catch (Throwable t) {
      ev.failed(t);
      throw t;
    } finally {
      ev.write();
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }
}
