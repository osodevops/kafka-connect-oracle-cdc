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
package sh.oso.connect.oracle.e2e.regression;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
import sh.oso.connect.oracle.core.errors.OracleCdcPurgedException;
import sh.oso.connect.oracle.core.jdbc.ConnectionRole;
import sh.oso.connect.oracle.core.jdbc.SessionInitializer;
import sh.oso.connect.oracle.core.logs.LogInventory;
import sh.oso.connect.oracle.core.mining.DictionaryMode;
import sh.oso.connect.oracle.core.mining.JdbcLogMinerSession;
import sh.oso.connect.oracle.core.mining.JdbcObjectCatalog;
import sh.oso.connect.oracle.core.mining.ObjectIdResolver;
import sh.oso.connect.oracle.core.mining.ResolvedObjects;
import sh.oso.connect.oracle.core.mining.event.LogMinerEventSource;
import sh.oso.connect.oracle.core.model.RedoRecordId;
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
 * Invariant: when the redo holding the start of an open transaction has been purged, a restart
 * stops with the purge named (CORE-LOG-4) instead of resuming later and publishing the transaction
 * without its early changes. Debezium users hit the opposite with DBZ-2713.
 *
 * @see <a href="https://issues.redhat.com/browse/DBZ-2713">DBZ-2713</a>
 */
@Tag("engine")
@Tag("dbz-2713")
class StopsWhenRedoForOpenTransactionIsMissingEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void restartWithThePurgedStartOfAnOpenTransactionIsATypedStop() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection meta = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Connection open = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      SessionInitializer.apply(meta, ConnectionRole.METADATA);
      try (Statement s = w.createStatement()) {
        s.execute("CREATE TABLE pg (id NUMBER PRIMARY KEY, v VARCHAR2(20))");
        s.execute("ALTER TABLE pg ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      }
      OracleSql.archiveLogCurrent(db);
      long startScn = LogMinerHelper.currentScn(meta);
      long before = startScn;

      // an open transaction whose first changes land in a log that is then archived
      open.setAutoCommit(false);
      try (Statement s = open.createStatement()) {
        s.execute("INSERT INTO pg VALUES (1, 'open')");
        s.execute("INSERT INTO pg VALUES (2, 'open')");
      }
      OracleSql.archiveLogCurrent(db); // binds the private strand and archives the log
      try (Statement s = w.createStatement()) {
        s.execute("INSERT INTO pg VALUES (100, 'other')");
      }
      OracleSql.archiveLogCurrent(db);

      JdbcCatalogSource catalog = new JdbcCatalogSource(() -> meta);
      ObjectIdResolver resolver =
          new ObjectIdResolver(
              new JdbcObjectCatalog(() -> meta),
              List.of("FREEPDB1\\." + schema + "\\.PG"),
              List.of(),
              List.of("FREEPDB1"),
              false);
      ResolvedObjects objects = resolver.resolve();
      var info = catalog.database();
      DatabaseIdentity identity = new DatabaseIdentity(info.dbid(), info.resetlogsChangeScn());

      // run 1: the open transaction is buffered; the resume point is its first change
      Run first = new Run(meta, catalog, objects, Position.initial(startScn, identity));
      long end = LogMinerHelper.currentScn(meta);
      first.runUntil(end);
      assertThat(first.committed).hasSize(1);
      assertThat(first.buffer.openTransactions()).isEqualTo(1);
      RedoRecordId resume = first.lastResume;
      assertThat(resume).isNotNull();
      assertThat(resume.scn()).isLessThan(first.engine.cursor().scn());
      first.close();

      // recycle every online redo group so the only copy of the start is the archived file, then
      // delete that file (the lab image has six 50 MB groups: switch one more time than that)
      int groups;
      try (Statement s = meta.createStatement();
          ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM v$log")) {
        rs.next();
        groups = rs.getInt(1);
      }
      for (int i = 0; i <= groups; i++) {
        try (Statement s = w.createStatement()) {
          s.execute("INSERT INTO pg VALUES (" + (200 + i) + ", 'filler')");
        }
        OracleSql.archiveLogCurrent(db);
      }
      List<String> victims = new ArrayList<>();
      try (Statement s = meta.createStatement();
          ResultSet rs =
              s.executeQuery(
                  "SELECT name FROM v$archived_log WHERE dest_id = 1 AND name IS NOT NULL AND"
                      + " deleted = 'NO' AND next_change# > "
                      + before
                      + " AND first_change# <= "
                      + resume.scn()
                      + " AND next_change# > "
                      + resume.scn())) {
        while (rs.next()) {
          victims.add(rs.getString(1));
        }
      }
      assertThat(victims).as("the log holding the resume point").isNotEmpty();
      for (String f : victims) {
        OracleSql.hideArchivedLog(db, f);
      }

      // run 2 from the acknowledged position: a typed stop, nothing emitted
      Run second =
          new Run(
              meta, catalog, objects, Position.initial(resume.scn(), identity).withResume(resume));
      assertThatThrownBy(() -> second.runUntil(LogMinerHelper.currentScn(meta)))
          .isInstanceOf(OracleCdcPurgedException.class)
          .hasMessageContaining("CDC-2002");
      assertThat(second.committed).isEmpty();
      second.close();
      open.rollback();
      System.out.println("dbz-2713: restart over a purged start stopped with CDC-2002");
    } finally {
      OracleSql.restoreHiddenLogs(db);
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  /**
   * One engine over the shared metadata connection and a mining connection of its own (closing the
   * LogMiner session closes its connection), collecting commits and the latest resume point.
   */
  final class Run {
    final CaptureEngine engine;
    final LogMinerEventSource source;
    final HeapTransactionBuffer buffer = new HeapTransactionBuffer();
    final List<CommittedTransaction> committed = new ArrayList<>();
    RedoRecordId lastResume;

    Run(Connection meta, JdbcCatalogSource catalog, ResolvedObjects objects, Position start)
        throws SQLException {
      Connection mining = db.capture(OracleTestDatabase.CDB_SERVICE);
      SessionInitializer.apply(mining, ConnectionRole.MINING);
      LogInventory inventory = new LogInventory(catalog, CaptureMode.ONLINE, 1);
      source =
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
      EventSink sink =
          new EventSink() {
            public void committed(CommittedTransaction tx, int skip, RedoRecordId resume) {
              committed.add(tx);
              lastResume = resume;
            }

            public void stepApplied(long minedTo, RedoRecordId resume) {
              lastResume = resume;
            }
          };
      java.util.function.Supplier<Long> safeEnd =
          () -> {
            try {
              return catalog.currentScn();
            } catch (SQLException e) {
              throw new OraErrorClassifier().toException(e, "safe end");
            }
          };
      engine =
          new CaptureEngine(
              start,
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
                  3,
                  sh.oso.connect.oracle.core.config.CoreConfig.DecodeErrorAction.FAIL),
              new OraErrorClassifier(),
              objects.owners(),
              objects::owners,
              // a reconnect is not part of this scenario: fail with its cause instead of looping
              cause -> {
                throw new SQLException("unexpected reconnect", cause);
              },
              Instant::now);
    }

    void runUntil(long scn) throws Exception {
      long deadline = System.currentTimeMillis() + Duration.ofMinutes(2).toMillis();
      while (engine.cursor().scn() < scn && System.currentTimeMillis() < deadline) {
        if (engine.runOnce() == CaptureEngine.Progress.IDLE) {
          Thread.sleep(100);
        }
      }
    }

    void close() throws SQLException {
      source.close();
    }
  }
}
