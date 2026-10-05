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
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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
 * T2 archive purge during lag (testing strategy section 4, PRD-00 CORE-LOG-4): the connector is
 * stopped after it delivered a first phase of the workload, a second phase runs while it is down,
 * every online group is recycled and the archived logs holding the lag are removed the way an rm
 * outside RMAN does (the catalog still lists them). On resume the task must stop with CDC-2002 and
 * publish nothing of the second phase; it must never skip the gap. Once the logs are back, a
 * restart delivers the second phase and the correctness oracle passes: the stop lost nothing.
 *
 * <p>The logs are moved aside, not deleted, and put back in a finally block: the Oracle container
 * is shared by every suite in the JVM.
 */
@Tag("nightly")
class ArchivePurgeDuringLagNightlyIT {

  static final String NAME = "archive-purge";
  static final String PREFIX = "ap";

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void aPurgedLogDuringLagStopsWithCdc2002AndNothingPastTheGapIsPublished() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    WorkloadSpec spec = NightlyRun.openEnded(NightlyRun.seed(303));
    Evidence ev =
        Evidence.of(
            getClass(),
            "archive purge during lag: connector stopped, workload continues, the archived logs"
                + " holding the lag are removed; expect CDC-2002 and nothing past the gap, then"
                + " full delivery once the logs are back");
    ev.param("workload", NightlyRun.describe(spec))
        .param("phase1Seconds", 15)
        .param("phase2Seconds", 15);
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

      // phase 1: delivered and acknowledged before the stop
      CompletableFuture<WorkloadResult> run = NightlyRun.start(g);
      Thread.sleep(15_000);
      g.pause();
      assertThat(g.awaitPaused(120_000)).as("workload sessions parked").isTrue();
      Set<String> phase1 = NightlyRun.ledgerXids(w, schema, spec);
      long phase1End = NightlyRun.scn(root);
      assertThat(NightlyRun.awaitResumePast(cluster, NAME, phase1End, Duration.ofMinutes(4)))
          .as("phase 1 acknowledged before the stop")
          .isTrue();
      cluster.lifecycle(NAME, "stop");
      cluster.awaitTaskState(NAME, "STOPPED", Duration.ofMinutes(2));
      long resume = NightlyRun.offset(cluster, NAME).path("resume_scn").asLong();
      ev.fault("connector-stopped", "resumeScn", resume);

      // phase 2: the lag
      g.resume();
      Thread.sleep(15_000);
      WorkloadResult r = NightlyRun.stop(g, run);
      long lagEnd = NightlyRun.scn(root);
      Set<String> phase2 = new LinkedHashSet<>(NightlyRun.ledgerXids(w, schema, spec));
      phase2.removeAll(phase1);
      assertThat(phase2).as("transactions committed while the connector was stopped").isNotEmpty();

      // the lag only in archived logs, then those logs gone
      List<String> victims = new ArrayList<>();
      try (Connection sys = db.sysdba(OracleTestDatabase.CDB_SERVICE)) {
        OracleSql.recycleOnlineLogs(sys);
        try (Statement s = sys.createStatement();
            ResultSet rs =
                s.executeQuery(
                    "SELECT name FROM v$archived_log WHERE dest_id = 1 AND name IS NOT NULL AND"
                        + " deleted = 'NO' AND next_change# > "
                        + resume
                        + " AND first_change# <= "
                        + lagEnd
                        + " ORDER BY sequence#")) {
          while (rs.next()) {
            victims.add(rs.getString(1));
          }
        }
      }
      assertThat(victims).as("archived logs holding the lag").isNotEmpty();
      for (String f : victims) {
        OracleSql.hideArchivedLog(db, f);
      }
      ev.fault("archived-logs-removed", "logs", victims.size(), "fromScn", resume, "toScn", lagEnd);

      cluster.lifecycle(NAME, "resume");
      JsonNode failed = cluster.awaitTaskState(NAME, "FAILED", Duration.ofMinutes(4));
      String trace = failed.path("tasks").get(0).path("trace").asText();
      ev.count("stopCode", NightlyRun.cdcCode(trace)).note("stop: " + NightlyRun.firstLine(trace));

      Set<String> seen = xidsOnTopics(cluster, schema, spec);
      Set<String> pastGap = new HashSet<>(seen);
      pastGap.retainAll(phase2);
      ev.count("phase1Transactions", phase1.size())
          .count("phase2Transactions", phase2.size())
          .count("hiddenArchivedLogs", victims.size())
          .count("phase2PublishedBeforeRestore", pastGap.size());
      assertThat(trace).as("the purge is a typed stop").contains("CDC-2002");
      assertThat(pastGap).as("nothing past the gap is published").isEmpty();
      assertThat(seen).as("phase 1 was delivered whole before the stop").containsAll(phase1);

      // the stop lost nothing: with the logs back, a restart delivers phase 2
      OracleSql.restoreHiddenLogs(db);
      ev.fault("archived-logs-restored");
      NightlyRun.restartUntilRunning(cluster, NAME, Duration.ofMinutes(4));
      boolean caughtUp = NightlyRun.awaitResumePast(cluster, NAME, lagEnd, Duration.ofMinutes(6));
      CheckReport report =
          NightlyRun.check(cluster, w, schema, spec, PREFIX, Duration.ofMinutes(6));
      ev.check("oracleAfterRestore", report)
          .count("caughtUpAfterRestore", caughtUp)
          .count("committedTransactions", r.committed.sum());
      System.out.println(
          "archive-purge: stop="
              + NightlyRun.cdcCode(trace)
              + " phase2="
              + phase2.size()
              + " pastGap="
              + pastGap.size()
              + " verdictAfterRestore="
              + report.verdict());
      assertThat(report.verdict()).as(report.toJson()).isEqualTo(CheckReport.Verdict.PASS);
    } catch (Throwable t) {
      ev.failed(t);
      throw t;
    } finally {
      OracleSql.restoreHiddenLogs(db);
      ev.write();
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  /** Every transaction id on the workload's table topics, read from the start. */
  private static Set<String> xidsOnTopics(ConnectCluster cluster, String schema, WorkloadSpec spec)
      throws Exception {
    Set<String> out = new HashSet<>();
    String[] topics =
        NightlyRun.tables(spec).stream()
            .map(t -> NightlyRun.topic(PREFIX, schema, t))
            .toArray(String[]::new);
    try (KafkaConsumer<String, String> c =
        cluster.consumer("purge-check-" + System.nanoTime(), topics)) {
      for (ConsumerRecord<String, String> rec :
          ConnectCluster.consume(c, 1, Duration.ofMinutes(2), Duration.ofSeconds(10))) {
        if (rec.value() != null) {
          String xid = ConnectCluster.json(rec.value()).path("source").path("txId").asText(null);
          if (xid != null) {
            out.add(xid);
          }
        }
      }
    }
    return out;
  }
}
