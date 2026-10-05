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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.bench.workload.Ledger;
import sh.oso.connect.oracle.bench.workload.WorkloadGenerator;
import sh.oso.connect.oracle.bench.workload.WorkloadResult;
import sh.oso.connect.oracle.bench.workload.WorkloadSpec;
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
 * CORE-CONN-6: the mining and metadata sessions are killed while a workload runs; the engine
 * reconnects, re-mines the failed step from the same cursor and ends with exactly the ledger. This
 * is the engine-level form of the Strimzi oracle_restart edge case that found the bare
 * NullPointerException from an unclassified ORA code.
 */
@Tag("engine")
class ReconnectEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  /** Owns the two connections and rebuilds them on request. */
  final class Connections {
    volatile Connection meta;
    volatile Connection mining;
    int reopened;

    Connections() throws SQLException {
      open();
    }

    void open() throws SQLException {
      meta = db.capture(OracleTestDatabase.CDB_SERVICE);
      mining = db.capture(OracleTestDatabase.CDB_SERVICE);
      SessionInitializer.apply(mining, ConnectionRole.MINING);
      SessionInitializer.apply(meta, ConnectionRole.METADATA);
    }

    void reopen() throws SQLException {
      for (Connection c : List.of(meta, mining)) {
        try {
          c.close();
        } catch (SQLException ignore) {
          // killed
        }
      }
      open();
      reopened++;
    }
  }

  @Test
  void killedSessionsAreReconnectedAndNothingIsLost() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    Connections conns = new Connections();
    try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Connection sys = db.sysdba(OracleTestDatabase.CDB_SERVICE)) {
      WorkloadSpec spec = WorkloadSpec.defaults();
      spec.seed = 17;
      spec.sessions = 2;
      spec.tables = 2;
      spec.transactionsPerSession = 0;
      spec.durationSeconds = 10;
      spec.maxRowsPerTransaction = 4;
      spec.lobWeight = 0;
      spec.ddlProbability = 0;
      spec.truncateProbability = 0;
      WorkloadGenerator g =
          new WorkloadGenerator(spec, db.jdbcUrl(OracleTestDatabase.PDB1), schema, schema);
      g.reset();
      OracleSql.archiveLogCurrent(db);
      long startScn = LogMinerHelper.currentScn(conns.meta);

      JdbcCatalogSource catalog = new JdbcCatalogSource(() -> conns.meta);
      LogInventory inventory = new LogInventory(catalog, CaptureMode.ONLINE, 1);
      ObjectIdResolver resolver =
          new ObjectIdResolver(
              new JdbcObjectCatalog(() -> conns.meta),
              List.of("FREEPDB1\\." + schema + "\\.WL_.*"),
              List.of(),
              List.of("FREEPDB1"),
              false);
      ResolvedObjects[] objects = {resolver.resolve()};
      LogMinerEventSource[] source = {source(conns, inventory, objects[0])};
      SchemaRegistry registry =
          new SchemaRegistry(
              new InMemorySchemaStore(),
              new JdbcDictionaryReader(() -> conns.meta),
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
      CaptureEngine engine =
          new CaptureEngine(
              Position.initial(
                  startScn, new DatabaseIdentity(info.dbid(), info.resetlogsChangeScn())),
              source[0],
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
              objects[0].owners(),
              () -> {
                objects[0] = resolver.resolve();
                source[0].update(objects[0], objects[0].filter(Set.of(), 1000));
                return objects[0].owners();
              },
              cause -> {
                conns.reopen();
                source[0] = source(conns, inventory, objects[0]);
                return new CaptureEngine.Sources(source[0], inventory, safeEnd);
              },
              Instant::now);

      CompletableFuture<WorkloadResult> workload =
          CompletableFuture.supplyAsync(
              () -> {
                try {
                  return g.run();
                } catch (Exception e) {
                  throw new IllegalStateException(e);
                }
              });
      long killAt = System.currentTimeMillis() + 3000;
      int kills = 0;
      while (!workload.isDone()) {
        if (kills < 2 && System.currentTimeMillis() > killAt) {
          killCaptureSessions(sys);
          kills++;
          killAt = System.currentTimeMillis() + 3000;
        }
        if (engine.runOnce() == CaptureEngine.Progress.IDLE) {
          Thread.sleep(50);
        }
      }
      WorkloadResult r = workload.get();
      // mine to a fixed SCN taken after the generator finished: the live safe end keeps moving
      // on an idle database, so a loop against it would never end
      OracleSql.archiveLogCurrent(db);
      long endScn = LogMinerHelper.currentScn(conns.meta);
      long deadline = System.currentTimeMillis() + Duration.ofMinutes(3).toMillis();
      while (engine.cursor().scn() < endScn && System.currentTimeMillis() < deadline) {
        if (engine.runOnce() == CaptureEngine.Progress.IDLE) {
          Thread.sleep(50);
        }
      }
      assertThat(engine.cursor().scn())
          .as("mined through the workload")
          .isGreaterThanOrEqualTo(endScn);

      assertThat(engine.metrics().reconnects.get())
          .as("at least one reconnect happened")
          .isPositive();
      assertThat(conns.reopened).isEqualTo((int) engine.metrics().reconnects.get());
      Set<String> ledger = Ledger.committedXids(w, schema, spec.ledgerTable);
      Set<String> seen = new LinkedHashSet<>();
      for (CommittedTransaction t : committed) {
        assertThat(seen.add(t.key().xid().toString())).as("duplicate %s", t.key()).isTrue();
      }
      assertThat(seen).containsExactlyInAnyOrderElementsOf(ledger);
      System.out.println(
          "reconnect: "
              + r.toJson()
              + " ledger="
              + ledger.size()
              + " reconnects="
              + engine.metrics().reconnects.get()
              + " kills="
              + kills);
      source[0].close();
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  private LogMinerEventSource source(
      Connections conns, LogInventory inventory, ResolvedObjects objects) {
    return new LogMinerEventSource(
        inventory,
        new JdbcLogMinerSession(conns.mining, 2000, Duration.ofMinutes(5)),
        objects,
        objects.filter(Set.of(), 1000),
        DictionaryMode.ONLINE_CATALOG);
  }

  /** Kills every capture-user session except the one doing the killing. */
  private static void killCaptureSessions(Connection sys) throws SQLException {
    List<String> victims = new ArrayList<>();
    try (Statement s = sys.createStatement();
        ResultSet rs =
            s.executeQuery(
                "SELECT sid, serial# FROM v$session WHERE username = '"
                    + OracleTestDatabase.CAPTURE_USER.toUpperCase(java.util.Locale.ROOT)
                    + "'")) {
      while (rs.next()) {
        victims.add(rs.getLong(1) + "," + rs.getLong(2));
      }
    }
    for (String v : victims) {
      try (Statement s = sys.createStatement()) {
        s.execute("ALTER SYSTEM KILL SESSION '" + v + "' IMMEDIATE");
      } catch (SQLException ignore) {
        // already gone
      }
    }
  }
}
