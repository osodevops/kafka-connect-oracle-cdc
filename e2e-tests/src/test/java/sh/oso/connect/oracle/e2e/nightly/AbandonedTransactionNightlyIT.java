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
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container.ExecResult;
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
 * T2 abandoned transactions (testing strategy section 4, PRD-00 CORE-TX-7, ADR-0006): two sessions
 * open transactions on a captured table, the connector mines their changes and holds them (its
 * committed resume point stays at or before their first change while later commits flow), then one
 * session is killed with ALTER SYSTEM KILL SESSION and the other loses its server process to
 * SIGKILL. Both XIDs leave GV$TRANSACTION. The connector must let go of both, through the mined
 * rollback or through orphan release (a short check interval is set; the evidence records which),
 * publish nothing of either, keep running, and deliver a later commit on the same table; the oracle
 * over the background workload must pass.
 *
 * <p>The SIGKILL variant needs dedicated server processes; with threaded execution the second
 * session is killed with KILL SESSION too, and the evidence says so.
 */
@Tag("nightly")
class AbandonedTransactionNightlyIT {

  static final String NAME = "abandoned";
  static final String PREFIX = "ab";
  static final int CONTROL_ID = 100;

  private final OracleTestDatabase db = OracleTestDatabase.get();

  record Victim(String xid, long sid, long serial, String spid, long before, long after) {}

  @Test
  void sessionsKilledMidTransactionAreDroppedAndNothingOfThemIsPublished() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    WorkloadSpec spec = NightlyRun.openEnded(NightlyRun.seed(909));
    spec.sessions = 1;
    Evidence ev =
        Evidence.of(
            getClass(),
            "sessions killed mid-transaction (KILL SESSION and SIGKILL of the server process):"
                + " XIDs leave GV$TRANSACTION, the connector drops them, publishes nothing of"
                + " them and keeps streaming");
    ev.param("workload", NightlyRun.describe(spec)).param("orphanCheckIntervalMs", 5000);
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    Connection va = null;
    Connection vb = null;
    try (ConnectCluster cluster = new ConnectCluster().start();
        Connection root = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection sys = db.sysdba(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      try (Statement s = w.createStatement()) {
        s.execute("CREATE TABLE ab (id NUMBER(9) PRIMARY KEY, v VARCHAR2(50))");
        s.execute("ALTER TABLE ab ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      }
      WorkloadGenerator g =
          new WorkloadGenerator(spec, db.jdbcUrl(OracleTestDatabase.PDB1), schema, schema);
      g.reset();
      Thread.sleep(3500);
      Map<String, String> config =
          NightlyRun.connector(
              PREFIX,
              schema,
              "(AB|" + spec.tablePrefix + "[0-9]+)",
              ConnectCluster.oracleDatabaseProps("FREEPDB1"));
      config.put("cdc.transaction.orphan.check.interval.ms", "5000");
      config.put("cdc.decimal.mode", "string");
      cluster.register(NAME, config);
      cluster.awaitRunning(NAME, Duration.ofMinutes(3));
      cluster.awaitOffsets(NAME, Duration.ofSeconds(90));
      CompletableFuture<WorkloadResult> run = NightlyRun.start(g);
      Thread.sleep(5_000);

      boolean threaded = threadedExecution(sys);
      ev.param("threadedExecution", threaded);
      va = db.connect(OracleTestDatabase.PDB1, schema, schema);
      vb = db.connect(OracleTestDatabase.PDB1, schema, schema);
      Victim a = open(va, sys, root, 1);
      Victim b = open(vb, sys, root, 11);
      // binds the private strands: the open changes are in the redo from here on
      OracleSql.archiveLogCurrent(sys);
      long flushed = NightlyRun.scn(root);

      // a commit after the flush is delivered only once the connector has read the open changes
      assertThat(awaitLastCommitPast(cluster, flushed, Duration.ofMinutes(4)))
          .as("the connector delivered commits made after the open changes")
          .isTrue();
      long pinned = NightlyRun.offset(cluster, NAME).path("resume_scn").asLong();
      ev.count("resumeScnWhileOpen", pinned).count("firstOpenChangeAtOrBefore", a.after());
      assertThat(pinned)
          .as("the open transactions hold the committed resume point at their first change")
          .isLessThanOrEqualTo(a.after());

      ev.fault("kill-session", "xid", a.xid());
      try (Statement s = sys.createStatement()) {
        s.execute("ALTER SYSTEM KILL SESSION '" + a.sid() + "," + a.serial() + "' IMMEDIATE");
      }
      if (threaded) {
        ev.fault("kill-session", "xid", b.xid(), "why", "threaded execution: no process to kill");
        try (Statement s = sys.createStatement()) {
          s.execute("ALTER SYSTEM KILL SESSION '" + b.sid() + "," + b.serial() + "' IMMEDIATE");
        }
      } else {
        ev.fault("sigkill-server-process", "xid", b.xid(), "spid", b.spid());
        ExecResult k = db.container().execInContainer("bash", "-c", "kill -9 " + b.spid());
        assertThat(k.getExitCode()).as("kill -9 " + b.spid() + ": " + k.getStderr()).isZero();
      }
      assertThat(awaitGone(sys, a, Duration.ofMinutes(2)))
          .as("A gone from GV$TRANSACTION")
          .isTrue();
      assertThat(awaitGone(sys, b, Duration.ofMinutes(2)))
          .as("B gone from GV$TRANSACTION")
          .isTrue();
      ev.fault("xids-gone-from-gv$transaction");

      long last = Math.max(a.after(), b.after());
      boolean let = NightlyRun.awaitResumePast(cluster, NAME, last + 1, Duration.ofMinutes(5));
      ev.count("resumePassedAbandoned", let);
      try (Statement s = w.createStatement()) {
        s.execute("INSERT INTO ab VALUES (" + CONTROL_ID + ", 'control')");
      }
      // w autocommits: the control row is committed

      WorkloadResult r = NightlyRun.stop(g, run);
      long end = NightlyRun.scn(root);
      cluster.awaitRunning(NAME, Duration.ofMinutes(2));
      boolean caughtUp = NightlyRun.awaitResumePast(cluster, NAME, end, Duration.ofMinutes(5));

      Set<Integer> ids = new TreeSet<>();
      Set<String> xids = new HashSet<>();
      try (KafkaConsumer<String, String> c =
          cluster.consumer(
              "abandoned-" + System.nanoTime(), NightlyRun.topic(PREFIX, schema, "AB"))) {
        for (ConsumerRecord<String, String> rec :
            ConnectCluster.consume(c, 1, Duration.ofMinutes(2), Duration.ofSeconds(10))) {
          if (rec.value() == null) {
            continue;
          }
          JsonNode v = ConnectCluster.json(rec.value());
          BigDecimal id = NightlyRun.number(v.path("after").path("ID"));
          if (id == null) {
            id = NightlyRun.number(v.path("before").path("ID"));
          }
          if (id != null) {
            ids.add(id.intValue());
          }
          xids.add(v.path("source").path("txId").asText());
        }
      }
      List<JsonNode> ops = NightlyRun.ops(cluster, PREFIX);
      Set<String> released = new HashSet<>();
      NightlyRun.ofType(ops, "transaction-orphan-released")
          .forEach(j -> released.add(j.path("details").path("xid").asText()));
      CheckReport report =
          NightlyRun.check(cluster, w, schema, spec, PREFIX, Duration.ofMinutes(6));
      ev.check("oracle", report)
          .count("caughtUp", caughtUp)
          .count("idsPublishedOnAb", ids.toString())
          .count(
              "victimA",
              a.xid() + (released.contains(a.xid()) ? " orphan-released" : " rollback-mined"))
          .count(
              "victimB",
              b.xid() + (released.contains(b.xid()) ? " orphan-released" : " rollback-mined"))
          .count("orphanReleases", released.size())
          .count("reconnectedEvents", NightlyRun.ofType(ops, "reconnected").size());
      System.out.println(
          "abandoned: released=" + released + " ids=" + ids + " verdict=" + report.verdict());

      assertThat(let).as("the connector let go of both abandoned transactions").isTrue();
      assertThat(xids).as("nothing of the abandoned transactions").doesNotContain(a.xid(), b.xid());
      assertThat(ids).as("only the control commit on AB").containsExactly(CONTROL_ID);
      assertThat(released)
          .as("orphan release names only abandoned XIDs")
          .isSubsetOf(a.xid(), b.xid());
      assertThat(report.verdict()).as(report.toJson()).isEqualTo(CheckReport.Verdict.PASS);
    } catch (Throwable t) {
      ev.failed(t);
      throw t;
    } finally {
      closeKilled(va);
      closeKilled(vb);
      ev.write();
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  /** Opens a transaction with three inserts and an update, and reads its identity. */
  private static Victim open(Connection v, Connection sys, Connection root, int firstId)
      throws Exception {
    long before = NightlyRun.scn(root);
    v.setAutoCommit(false);
    String xid;
    long sid;
    try (Statement s = v.createStatement()) {
      for (int i = firstId; i < firstId + 3; i++) {
        s.execute("INSERT INTO ab VALUES (" + i + ", 'abandoned')");
      }
      s.execute("UPDATE ab SET v = 'abandoned-updated' WHERE id = " + firstId);
      try (ResultSet rs =
          s.executeQuery(
              "SELECT DBMS_TRANSACTION.LOCAL_TRANSACTION_ID, SYS_CONTEXT('USERENV', 'SID') FROM"
                  + " dual")) {
        rs.next();
        xid = rs.getString(1);
        sid = rs.getLong(2);
      }
    }
    long after = NightlyRun.scn(root);
    try (PreparedStatement ps =
        sys.prepareStatement(
            "SELECT s.serial#, p.spid FROM v$session s JOIN v$process p ON p.addr = s.paddr"
                + " WHERE s.sid = ?")) {
      ps.setLong(1, sid);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return new Victim(xid, sid, rs.getLong(1), rs.getString(2), before, after);
      }
    }
  }

  /** Until the XID has left GV$TRANSACTION and its session is gone. */
  private static boolean awaitGone(Connection sys, Victim v, Duration timeout) throws Exception {
    String[] p = v.xid().split("\\.");
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    while (System.currentTimeMillis() < deadline) {
      int open;
      try (PreparedStatement ps =
          sys.prepareStatement(
              "SELECT (SELECT COUNT(*) FROM gv$transaction WHERE xidusn = ? AND xidslot = ? AND"
                  + " xidsqn = ?) + (SELECT COUNT(*) FROM v$session WHERE sid = ? AND serial# = ?)"
                  + " FROM dual")) {
        ps.setLong(1, Long.parseLong(p[0]));
        ps.setLong(2, Long.parseLong(p[1]));
        ps.setLong(3, Long.parseLong(p[2]));
        ps.setLong(4, v.sid());
        ps.setLong(5, v.serial());
        try (ResultSet rs = ps.executeQuery()) {
          rs.next();
          open = rs.getInt(1);
        }
      }
      if (open == 0) {
        return true;
      }
      Thread.sleep(500);
    }
    return false;
  }

  private boolean awaitLastCommitPast(ConnectCluster cluster, long scn, Duration timeout)
      throws Exception {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    while (System.currentTimeMillis() < deadline) {
      if (NightlyRun.offset(cluster, NAME).path("last_commit_scn").asLong() > scn) {
        return true;
      }
      Thread.sleep(1000);
    }
    return false;
  }

  private static boolean threadedExecution(Connection sys) throws SQLException {
    try (Statement s = sys.createStatement();
        ResultSet rs =
            s.executeQuery("SELECT value FROM v$parameter WHERE name = 'threaded_execution'")) {
      return rs.next() && "TRUE".equalsIgnoreCase(rs.getString(1));
    }
  }

  /** Killed sessions throw on close; they are not try-with-resources for that reason. */
  private static void closeKilled(Connection c) {
    if (c != null) {
      try {
        c.close();
      } catch (SQLException ignore) {
        // killed by the test
      }
    }
  }
}
