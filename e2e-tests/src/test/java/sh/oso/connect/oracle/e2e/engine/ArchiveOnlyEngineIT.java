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
import sh.oso.connect.oracle.core.logs.LogSet;
import sh.oso.connect.oracle.core.logs.RedoLog;
import sh.oso.connect.oracle.core.mining.DictionaryMode;
import sh.oso.connect.oracle.core.mining.JdbcLogMinerSession;
import sh.oso.connect.oracle.core.mining.JdbcObjectCatalog;
import sh.oso.connect.oracle.core.mining.ObjectIdResolver;
import sh.oso.connect.oracle.core.mining.ResolvedObjects;
import sh.oso.connect.oracle.core.mining.event.LogMinerEventSource;
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
 * CORE-LOG-6: in archive-only mode no online log is ever added, the safe end is the archived
 * frontier, and a committed transaction becomes visible only after the log holding it is archived.
 */
@Tag("engine")
class ArchiveOnlyEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void committedChangesArriveOnlyOnceTheirLogIsArchivedAndNoOnlineLogIsAdded() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection meta = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection mining = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      SessionInitializer.apply(mining, ConnectionRole.MINING);
      SessionInitializer.apply(meta, ConnectionRole.METADATA);
      try (Statement s = w.createStatement()) {
        s.execute("CREATE TABLE ao (id NUMBER PRIMARY KEY, v VARCHAR2(20))");
        s.execute("ALTER TABLE ao ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      }
      OracleSql.archiveLogCurrent(db);
      long startScn = LogMinerHelper.currentScn(meta);

      JdbcCatalogSource catalog = new JdbcCatalogSource(() -> meta);
      LogInventory inventory = new LogInventory(catalog, CaptureMode.ARCHIVE_ONLY, 1);
      ObjectIdResolver resolver =
          new ObjectIdResolver(
              new JdbcObjectCatalog(() -> meta),
              List.of("FREEPDB1\\." + schema + "\\.AO"),
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
              return inventory.archiveOnlySafeEnd(startScn);
            } catch (SQLException e) {
              throw new OraErrorClassifier().toException(e, "safe end");
            }
          };
      CaptureEngine engine =
          new CaptureEngine(
              Position.initial(
                  startScn, new DatabaseIdentity(info.dbid(), info.resetlogsChangeScn())),
              source,
              inventory,
              safeEnd,
              new HeapTransactionBuffer(),
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
              Instant::now);

      w.setAutoCommit(false);
      try (Statement s = w.createStatement()) {
        s.execute("INSERT INTO ao VALUES (1, 'committed')");
      }
      w.commit();
      // the commit sits in the current online log: archive-only mining must not see it yet
      runFor(engine, Duration.ofSeconds(6));
      assertThat(committed).as("online redo is never read in archive-only mode").isEmpty();
      long frontier = safeEnd.get();
      OracleSql.archiveLogCurrent(db);
      runFor(engine, Duration.ofSeconds(10));
      assertThat(committed).hasSize(1);
      assertThat(committed.get(0).events()).hasSize(1);
      assertThat(safeEnd.get())
          .as("the frontier moved with the archived log")
          .isGreaterThan(frontier);
      LogSet set = inventory.forRange(startScn, safeEnd.get());
      assertThat(set.logs()).isNotEmpty();
      for (RedoLog l : set.logs()) {
        assertThat(l.archived()).as("no online log in %s", set).isTrue();
      }
      System.out.println(
          "archive-only: "
              + set.logs().size()
              + " archived logs in the set; commit seen after switch");
      source.close();
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
}
