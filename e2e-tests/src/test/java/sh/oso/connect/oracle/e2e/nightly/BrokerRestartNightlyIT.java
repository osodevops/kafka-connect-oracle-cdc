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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.bench.check.CheckReport;
import sh.oso.connect.oracle.bench.workload.WorkloadGenerator;
import sh.oso.connect.oracle.bench.workload.WorkloadResult;
import sh.oso.connect.oracle.bench.workload.WorkloadSpec;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.Evidence;
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * T2 Kafka broker restart (testing strategy section 4, "Kafka faults"): while a workload streams,
 * the single broker is restarted in place twice, once with a graceful stop and once with SIGKILL.
 * Once at-least-once (a worker without exactly-once source support) and once exactly-once ({@code
 * exactly.once.support=required}, {@code transaction.boundary=connector}). The correctness oracle
 * must pass in both; under exactly-once a read_committed consumer must see no event twice.
 *
 * <p>A task the framework fails on a Kafka-side error (no CDC code in its trace) is restarted, as
 * an operator or Strimzi's auto-restart would, and the restart is recorded in the evidence; a task
 * that stops with one of our CDC codes fails the suite.
 */
@Tag("nightly")
class BrokerRestartNightlyIT {

  static final String NAME = "broker-restart";

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void brokerRestartsUnderAtLeastOnceLoseNothing() throws Exception {
    scenario("at-least-once", "bra", false);
  }

  @Test
  void brokerRestartsUnderExactlyOnceNeitherLoseNorDuplicate() throws Exception {
    scenario("exactly-once", "bre", true);
  }

  private void scenario(String mode, String prefix, boolean eos) throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    WorkloadSpec spec = NightlyRun.openEnded(NightlyRun.seed(eos ? 707 : 701));
    Evidence ev =
        Evidence.of(
            getClass(),
            mode,
            "Kafka broker restarted in place twice (graceful, then SIGKILL) while a workload"
                + " streams, "
                + mode
                + "; correctness oracle");
    ev.param("workload", NightlyRun.describe(spec)).param("exactlyOnce", eos);
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    Map<String, String> worker = eos ? Map.of() : Map.of("exactly.once.source.support", "disabled");
    int[] restarts = {0};
    try (ConnectCluster cluster = new ConnectCluster(worker).withPinnedKafkaHostPort().start();
        Connection root = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      WorkloadGenerator g =
          new WorkloadGenerator(spec, db.jdbcUrl(OracleTestDatabase.PDB1), schema, schema);
      g.reset();
      Thread.sleep(3500);
      Map<String, String> config =
          NightlyRun.connector(
              prefix, schema, spec, ConnectCluster.oracleDatabaseProps("FREEPDB1"));
      if (eos) {
        config.put("exactly.once.support", "required");
        config.put("transaction.boundary", "connector");
        config.put("cdc.eos.batch.max.records", "40");
      }
      cluster.register(NAME, config);
      cluster.awaitRunning(NAME, Duration.ofMinutes(3));
      cluster.awaitOffsets(NAME, Duration.ofSeconds(90));

      long workloadStart = NightlyRun.scn(root);
      CompletableFuture<WorkloadResult> run = NightlyRun.start(g);
      Thread.sleep(10_000);
      ev.fault("broker-restart", "stop", "graceful");
      cluster.restartBroker(false, Duration.ofMinutes(3));
      ev.fault("broker-back");
      NightlyRun.supervise(cluster, NAME, Duration.ofSeconds(20), ev, restarts, 3);
      ev.fault("broker-restart", "stop", "SIGKILL");
      cluster.restartBroker(true, Duration.ofMinutes(3));
      ev.fault("broker-back");
      NightlyRun.supervise(cluster, NAME, Duration.ofSeconds(20), ev, restarts, 3);

      WorkloadResult r = NightlyRun.stop(g, run);
      long end = NightlyRun.scn(root);
      ev.param("workloadResult", ConnectCluster.json(r.toJson()));
      NightlyRun.supervise(cluster, NAME, Duration.ofSeconds(10), ev, restarts, 3);
      // after the broker returns, Connect can take many minutes to restart the task on a loaded
      // host (18 to 20 minutes seen locally on 8 October 2026); the oracle only means something
      // once the task has caught up
      boolean caughtUp = NightlyRun.awaitResumePast(cluster, NAME, end, Duration.ofMinutes(25));
      NightlyRun.supervise(cluster, NAME, Duration.ofSeconds(2), ev, restarts, 3);

      CheckReport report =
          NightlyRun.check(cluster, w, schema, spec, prefix, Duration.ofMinutes(8));
      ev.check("oracle", report)
          .count("caughtUp", caughtUp)
          .count("taskRestarts", restarts[0])
          .count("committedTransactions", r.committed.sum())
          .count("recordsConsumed", report.recordsConsumed())
          .count(
              "transactionsWithDuplicateEvents",
              report.invariants().get("transactionsWithDuplicateEvents"));
      System.out.println(
          "broker-restart "
              + mode
              + ": restarts="
              + restarts[0]
              + " duplicates="
              + report.invariants().get("transactionsWithDuplicateEvents")
              + " verdict="
              + report.verdict());
      assertThat(caughtUp)
          .as("the restarted task caught up with the workload within 25 minutes")
          .isTrue();
      if (report.verdict() != CheckReport.Verdict.PASS) {
        explainMissing(root, report, workloadStart, end);
      }
      assertThat(report.verdict()).as(report.toJson()).isEqualTo(CheckReport.Verdict.PASS);
      if (eos) {
        assertThat(report.invariants().get("transactionsWithDuplicateEvents"))
            .as("exactly-once: no (xid, event_index) delivered twice to read_committed")
            .isEqualTo(0L);
      }
    } catch (Throwable t) {
      ev.failed(t);
      throw t;
    } finally {
      ev.write();
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  /**
   * Prints the LogMiner rows of each transaction the oracle reports missing, with every row of the
   * same undo slot that carries the partial XID sequence, so a failed run says how the connector
   * could have lost it.
   */
  private void explainMissing(Connection root, CheckReport report, long fromScn, long toScn) {
    Object examples = report.transactions().get("missingExamples");
    if (!(examples instanceof List<?> xids) || xids.isEmpty()) {
      return;
    }
    try {
      OracleSql.archiveLogCurrent(db);
      LogMinerHelper.start(root, fromScn, toScn);
      try {
        for (Object o : xids) {
          sh.oso.connect.oracle.core.model.Xid x =
              sh.oso.connect.oracle.core.model.Xid.parse(o.toString());
          List<Map<String, String>> rows =
              LogMinerHelper.rows(
                  root,
                  "XIDUSN = "
                      + x.usn()
                      + " AND XIDSLT = "
                      + x.slot()
                      + " AND XIDSQN IN ("
                      + x.sqn()
                      + ", "
                      + sh.oso.connect.oracle.core.model.Xid.PARTIAL_SQN
                      + ") AND SCN BETWEEN "
                      + fromScn
                      + " AND "
                      + toScn);
          System.out.println("missing " + x + ": " + rows.size() + " LogMiner rows in its slot");
          rows.stream()
              .filter(r -> "COMMIT".equals(r.get("OPERATION")))
              .findFirst()
              .ifPresent(
                  commit -> {
                    long at = Long.parseLong(commit.get("SCN"));
                    try {
                      for (Map<String, String> c :
                          LogMinerHelper.rows(
                              root,
                              "OPERATION_CODE = 7 AND SCN BETWEEN "
                                  + (at - 30)
                                  + " AND "
                                  + (at + 30))) {
                        System.out.println(
                            "  commit near "
                                + x
                                + " | scn="
                                + c.get("SCN")
                                + " rs_id="
                                + c.get("RS_ID")
                                + " ssn="
                                + c.get("SSN")
                                + " xid="
                                + c.get("XIDUSN")
                                + "."
                                + c.get("XIDSLT")
                                + "."
                                + c.get("XIDSQN")
                                + " thread="
                                + c.get("THREAD#"));
                      }
                    } catch (java.sql.SQLException e) {
                      System.out.println("  commits near " + x + " unavailable: " + e);
                    }
                  });
          for (Map<String, String> r : rows.subList(0, Math.min(rows.size(), 200))) {
            System.out.println(
                "  missing "
                    + x
                    + " | scn="
                    + r.get("SCN")
                    + " rs_id="
                    + r.get("RS_ID")
                    + " ssn="
                    + r.get("SSN")
                    + " op="
                    + r.get("OPERATION")
                    + " xidsqn="
                    + r.get("XIDSQN")
                    + " rollback="
                    + r.get("ROLLBACK")
                    + " seg="
                    + r.get("SEG_NAME")
                    + " row_id="
                    + r.get("ROW_ID")
                    + " status="
                    + r.get("STATUS"));
          }
        }
      } finally {
        LogMinerHelper.end(root);
      }
    } catch (Exception e) {
      System.out.println("missing transactions could not be explained: " + e);
    }
  }
}
