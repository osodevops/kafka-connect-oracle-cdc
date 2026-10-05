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
package sh.oso.connect.oracle.e2e.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.buffer.HeapTransactionBuffer;
import sh.oso.connect.oracle.core.config.CoreConfig.CaptureMode;
import sh.oso.connect.oracle.core.engine.CaptureEngine;
import sh.oso.connect.oracle.core.engine.ChangeDecoder;
import sh.oso.connect.oracle.core.engine.EngineSettings;
import sh.oso.connect.oracle.core.engine.EventSink;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.jdbc.ConnectionRole;
import sh.oso.connect.oracle.core.jdbc.SessionInitializer;
import sh.oso.connect.oracle.core.logs.LogInventory;
import sh.oso.connect.oracle.core.mining.DictionaryMode;
import sh.oso.connect.oracle.core.mining.JdbcLogMinerSession;
import sh.oso.connect.oracle.core.mining.JdbcObjectCatalog;
import sh.oso.connect.oracle.core.mining.ObjectIdResolver;
import sh.oso.connect.oracle.core.mining.ResolvedObjects;
import sh.oso.connect.oracle.core.mining.event.LogMinerEventSource;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;
import sh.oso.connect.oracle.core.orphan.JdbcTransactionProbe;
import sh.oso.connect.oracle.core.orphan.OrphanDetector;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.schema.JdbcDictionaryReader;
import sh.oso.connect.oracle.core.schema.KeySelector;
import sh.oso.connect.oracle.core.schema.SchemaRegistry;
import sh.oso.connect.oracle.core.testkit.InMemorySchemaStore;
import sh.oso.connect.oracle.core.topology.JdbcCatalogSource;
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * CORE-TX-7 against a real database: the probe sees an open transaction and its session; a
 * transaction that stays open through several checks is never released; a session killed
 * mid-transaction is rolled back by Oracle, the ROLLBACK is mined and the buffer drops the entry
 * without the detector ever releasing it (no false positive). The positive release path is proven
 * at T0 with a fake probe, because a real orphan needs redo that never shows its end.
 */
@Tag("engine")
class OrphanReleaseEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void probeSeesOpenTransactionsAndSessionsAndKilledSessionsRollBackWithoutARelease()
      throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection meta = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection mining = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection sys = db.sysdba(OracleTestDatabase.CDB_SERVICE);
        Connection setup = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      // the victim is killed by the test, so its close is best effort rather than a resource
      Connection victim = db.connect(OracleTestDatabase.PDB1, schema, schema);
      SessionInitializer.apply(mining, ConnectionRole.MINING);
      SessionInitializer.apply(meta, ConnectionRole.METADATA);
      try (Statement s = setup.createStatement()) {
        s.execute("CREATE TABLE orp (id NUMBER PRIMARY KEY, v VARCHAR2(20))");
        s.execute("ALTER TABLE orp ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      }
      OracleSql.archiveLogCurrent(db);
      long startScn = LogMinerHelper.currentScn(meta);

      // the probe against the real views
      JdbcTransactionProbe probe = new JdbcTransactionProbe(() -> meta);
      victim.setAutoCommit(false);
      try (Statement s = victim.createStatement()) {
        s.execute("INSERT INTO orp VALUES (1, 'open')");
        s.execute("INSERT INTO orp VALUES (2, 'open')");
      }
      TxKey victimKey = txKey(victim);
      long[] session = sidSerial(victim, sys);
      assertThat(probe.activeTransactions()).as("open transaction listed").contains(victimKey);
      assertThat(probe.sessionExists(session[0], session[1])).isTrue();
      assertThat(probe.currentScn()).isGreaterThan(startScn);

      // an engine with a very short orphan interval watches the open transaction
      JdbcCatalogSource catalog = new JdbcCatalogSource(() -> meta);
      LogInventory inventory = new LogInventory(catalog, CaptureMode.ONLINE, 1);
      ObjectIdResolver resolver =
          new ObjectIdResolver(
              new JdbcObjectCatalog(() -> meta),
              List.of("FREEPDB1\\." + schema + "\\.ORP"),
              List.of(),
              List.of("FREEPDB1"),
              false);
      ResolvedObjects objects = resolver.resolve();
      LogMinerEventSource source =
          new LogMinerEventSource(
              inventory,
              new JdbcLogMinerSession(mining, 2000, Duration.ofMinutes(5)),
              objects,
              objects.filter(Set.of(), 1000),
              DictionaryMode.ONLINE_CATALOG);
      SchemaRegistry registry =
          new SchemaRegistry(
              new InMemorySchemaStore(),
              new JdbcDictionaryReader(() -> meta),
              new KeySelector(Map.of(), KeySelector.MissingKeyPolicy.ROWID));
      List<CommittedTransaction> committed = new ArrayList<>();
      EventSink sink =
          new EventSink() {
            public void committed(
                CommittedTransaction tx,
                int skip,
                sh.oso.connect.oracle.core.model.RedoRecordId resume) {
              committed.add(tx);
            }

            public void stepApplied(
                long minedTo, sh.oso.connect.oracle.core.model.RedoRecordId resume) {}
          };
      var info = catalog.database();
      java.util.function.Supplier<Long> safeEnd =
          () -> {
            try {
              return catalog.currentScn();
            } catch (SQLException e) {
              throw new OraErrorClassifier().toException(e, "safe end");
            }
          };
      HeapTransactionBuffer buffer = new HeapTransactionBuffer();
      OrphanDetector detector =
          new OrphanDetector(
              probe, Duration.ofSeconds(2), OrphanDetector.Action.RELEASE, List.of());
      CaptureEngine engine =
          new CaptureEngine(
                  Position.initial(
                      startScn, new DatabaseIdentity(info.dbid(), info.resetlogsChangeScn())),
                  source,
                  inventory,
                  safeEnd,
                  buffer,
                  registry,
                  ChangeDecoder.rowDecoder(),
                  sink,
                  new EngineSettings(
                      Duration.ofSeconds(2),
                      8,
                      Duration.ofHours(1),
                      Duration.ofMillis(50),
                      20,
                      sh.oso.connect.oracle.core.config.CoreConfig.DecodeErrorAction.FAIL),
                  new OraErrorClassifier(),
                  objects.owners(),
                  objects::owners,
                  cause -> new CaptureEngine.Sources(source, inventory, safeEnd),
                  Instant::now)
              .withOrphanDetector(detector);

      // the open transaction's redo may still sit in its private strand: a log switch binds it
      OracleSql.archiveLogCurrent(db);
      runFor(engine, Duration.ofSeconds(8));
      assertThat(buffer.openTransactions()).as("the open transaction is buffered").isEqualTo(1);
      assertThat(engine.metrics().orphansReleased.get())
          .as("open in GV$TRANSACTION: never released")
          .isZero();

      // kill the session: Oracle rolls the transaction back and LogMiner shows the ROLLBACK
      try (Statement s = sys.createStatement()) {
        s.execute("ALTER SYSTEM KILL SESSION '" + session[0] + "," + session[1] + "' IMMEDIATE");
      }
      long deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
      while (probe.sessionExists(session[0], session[1]) && System.currentTimeMillis() < deadline) {
        Thread.sleep(200);
      }
      assertThat(probe.sessionExists(session[0], session[1])).as("session gone").isFalse();
      deadline = System.currentTimeMillis() + Duration.ofSeconds(30).toMillis();
      while (probe.activeTransactions().contains(victimKey)
          && System.currentTimeMillis() < deadline) {
        Thread.sleep(200);
      }
      assertThat(probe.activeTransactions())
          .as("rolled back by the database")
          .doesNotContain(victimKey);
      OracleSql.archiveLogCurrent(db);
      runFor(engine, Duration.ofSeconds(8));
      assertThat(buffer.openTransactions()).as("the mined ROLLBACK dropped the entry").isZero();
      assertThat(buffer.metrics().rolledBackTransactions()).isEqualTo(1);
      assertThat(engine.metrics().orphansReleased.get())
          .as("a rollback that LogMiner shows is never an orphan release")
          .isZero();
      assertThat(committed).isEmpty();
      System.out.println(
          "orphan-release: open transaction tracked, session kill mined as ROLLBACK, releases="
              + engine.metrics().orphansReleased.get());
      source.close();
      try {
        victim.close();
      } catch (SQLException ignore) {
        // killed by the test
      }
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  private static void runFor(CaptureEngine engine, Duration d) throws Exception {
    long until = System.currentTimeMillis() + d.toMillis();
    while (System.currentTimeMillis() < until) {
      if (engine.runOnce() == CaptureEngine.Progress.IDLE) {
        Thread.sleep(100);
      }
    }
  }

  private static TxKey txKey(Connection c) throws SQLException {
    try (Statement s = c.createStatement();
        ResultSet rs =
            s.executeQuery(
                "SELECT DBMS_TRANSACTION.LOCAL_TRANSACTION_ID, SYS_CONTEXT('USERENV', 'CON_ID')"
                    + " FROM dual")) {
      rs.next();
      String[] p = rs.getString(1).split("\\.");
      return new TxKey(
          rs.getInt(2), new Xid(Long.parseLong(p[0]), Long.parseLong(p[1]), Long.parseLong(p[2])));
    }
  }

  /** SID and SERIAL# of {@code c}: the SID from its own context, the serial from a SYSDBA view. */
  private static long[] sidSerial(Connection c, Connection sys) throws SQLException {
    long sid;
    try (Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("SELECT SYS_CONTEXT('USERENV', 'SID') FROM dual")) {
      rs.next();
      sid = rs.getLong(1);
    }
    try (java.sql.PreparedStatement ps =
        sys.prepareStatement("SELECT serial# FROM v$session WHERE sid = ?")) {
      ps.setLong(1, sid);
      try (ResultSet rs = ps.executeQuery()) {
        rs.next();
        return new long[] {sid, rs.getLong(1)};
      }
    }
  }
}
