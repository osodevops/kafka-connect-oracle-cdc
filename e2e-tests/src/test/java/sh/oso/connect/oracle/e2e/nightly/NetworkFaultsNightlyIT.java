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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.bench.check.CheckReport;
import sh.oso.connect.oracle.bench.workload.WorkloadGenerator;
import sh.oso.connect.oracle.bench.workload.WorkloadResult;
import sh.oso.connect.oracle.bench.workload.WorkloadSpec;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.Evidence;
import sh.oso.connect.oracle.e2e.support.OracleProxy;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * T2 network faults (testing strategy section 4) with Toxiproxy between the Connect worker and the
 * Oracle listener, while a seeded workload runs on a direct connection: latency with jitter, three
 * rounds of connection resets, then a quiet database followed by an idle drop (every flow silenced
 * and closed, as a load balancer does when its idle timer expires). The connector must reconnect
 * (PRD-00 CORE-CONN-6, a {@code reconnected} ops event) without a task failure, and the correctness
 * oracle must pass with no duplicate: no worker restarts here, so nothing may be delivered twice.
 *
 * <p>{@code -Dnightly.idle.seconds} sets the quiet period before the drop (default 30; the AWS
 * Network Load Balancer case is 350). The drop closes the connections with a FIN after one second
 * without traffic: the application-level idle probe of CORE-CONN-3 is not built yet
 * (cdc.database.idle.timeout.ms is reserved), so a flow that is silently blackholed for good is not
 * tested here.
 */
@Tag("nightly")
class NetworkFaultsNightlyIT {

  static final String NAME = "network-faults";
  static final String PREFIX = "nf";

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void latencyResetsAndAnIdleDropEndInReconnectsAndTheOraclePasses() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    long idleSeconds = Long.getLong("nightly.idle.seconds", 30);
    int resetRounds = 3;
    WorkloadSpec spec = NightlyRun.openEnded(NightlyRun.seed(101));
    Evidence ev =
        Evidence.of(
            getClass(),
            "Toxiproxy between the Connect worker and Oracle: latency, connection resets and an"
                + " idle drop while a seeded workload runs; reconnect and correctness oracle");
    ev.param("workload", NightlyRun.describe(spec))
        .param("latency", java.util.Map.of("downstreamMs", 150, "jitterMs", 50, "upstreamMs", 50))
        .param("latencySeconds", 30)
        .param("resetRounds", resetRounds)
        .param("resetSeconds", 5)
        .param("idleSeconds", idleSeconds)
        .param("idleDropCloseAfterMs", 1000)
        .param("idleDropHeldSeconds", 10)
        .param("toxiproxyImage", OracleProxy.IMAGE);
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (OracleProxy proxy = OracleProxy.start();
        ConnectCluster cluster = new ConnectCluster().start();
        Connection root = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      WorkloadGenerator g =
          new WorkloadGenerator(spec, db.jdbcUrl(OracleTestDatabase.PDB1), schema, schema);
      g.reset();
      Thread.sleep(3500); // the oracle's flashback reads need the SCN-to-time map past the DDL
      cluster.register(
          NAME,
          NightlyRun.connector(
              PREFIX, schema, spec, OracleProxy.connectorDatabaseProps("FREEPDB1")));
      cluster.awaitRunning(NAME, Duration.ofMinutes(3));
      cluster.awaitOffsets(NAME, Duration.ofSeconds(90));

      CompletableFuture<WorkloadResult> run = NightlyRun.start(g);
      Thread.sleep(10_000);

      ev.fault("latency", "downstreamMs", 150, "jitterMs", 50, "upstreamMs", 50);
      proxy.latency(150, 50, 50);
      Thread.sleep(30_000);
      proxy.clear();
      ev.fault("latency-removed");

      for (int i = 1; i <= resetRounds; i++) {
        Thread.sleep(15_000);
        ev.fault("reset-peer", "round", i);
        proxy.resetPeer();
        Thread.sleep(5_000);
        proxy.clear();
        ev.fault("reset-peer-removed", "round", i);
      }
      Thread.sleep(15_000);

      g.pause();
      assertThat(g.awaitPaused(120_000)).as("workload sessions parked").isTrue();
      ev.fault("quiet-database", "seconds", idleSeconds);
      Thread.sleep(idleSeconds * 1000);
      ev.fault("idle-drop", "closeAfterMs", 1000);
      proxy.dropAfter(1000);
      Thread.sleep(10_000);
      proxy.clear();
      ev.fault("idle-drop-removed");
      g.resume();
      Thread.sleep(20_000);

      WorkloadResult r = NightlyRun.stop(g, run);
      long end = NightlyRun.scn(root);
      ev.param("workloadResult", ConnectCluster.json(r.toJson()));
      // a task that failed would not reconnect: fail with its trace
      cluster.awaitRunning(NAME, Duration.ofMinutes(3));
      boolean caughtUp = NightlyRun.awaitResumePast(cluster, NAME, end, Duration.ofMinutes(6));
      ev.count("caughtUp", caughtUp);

      CheckReport report =
          NightlyRun.check(cluster, w, schema, spec, PREFIX, Duration.ofMinutes(8));
      ev.check("oracle", report);
      List<JsonNode> ops = NightlyRun.ops(cluster, PREFIX);
      List<JsonNode> reconnects = NightlyRun.ofType(ops, "reconnected");
      ev.count("committedTransactions", r.committed.sum())
          .count("reconnectedEvents", reconnects.size())
          .count("recordsConsumed", report.recordsConsumed());
      reconnects.stream()
          .limit(10)
          .forEach(j -> ev.note("reconnected: " + j.path("details").path("cause").asText()));
      System.out.println(
          "network-faults: "
              + r.toJson()
              + " reconnects="
              + reconnects.size()
              + " verdict="
              + report.verdict());

      assertThat(report.verdict()).as(report.toJson()).isEqualTo(CheckReport.Verdict.PASS);
      assertThat(report.invariants().get("transactionsWithDuplicateEvents"))
          .as("a reconnect re-mines the failed step and never delivers a transaction twice")
          .isEqualTo(0L);
      assertThat(reconnects)
          .as("CORE-CONN-6: the resets and the idle drop end in reconnects, not a task failure")
          .isNotEmpty();
    } catch (Throwable t) {
      ev.failed(t);
      throw t;
    } finally {
      ev.write();
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }
}
