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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import sh.oso.connect.oracle.core.model.RowChange;
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
 * P1-09, the first correctness milestone without Kafka: the engine mines a seeded workload and the
 * committed transactions it emits equal the ledger exactly, per transaction, with the ledger's
 * operation count; then the same window is mined again in two runs split by a simulated restart and
 * the union is identical with no duplicates.
 */
@Tag("engine")
class EngineCorrectnessEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  /** Collects committed transactions and the latest resume candidate. */
  static final class Collector implements EventSink {
    final List<CommittedTransaction> committed = new ArrayList<>();
    final List<Integer> skipped = new ArrayList<>();
    final List<Long> resumes = new ArrayList<>();
    long minedTo;
    long resume;

    public void committed(CommittedTransaction tx, int skip, long resumeCandidate) {
      resumes.add(resumeCandidate);
      committed.add(tx);
      skipped.add(skip);
    }

    public void stepApplied(long minedToScn, long resumeCandidate) {
      minedTo = minedToScn;
      resume = resumeCandidate;
    }
  }

  @Test
  void committedTransactionsEqualTheLedgerAcrossARestart() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection meta = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      WorkloadSpec spec = WorkloadSpec.defaults();
      spec.seed = 11;
      spec.sessions = 2;
      spec.tables = 3;
      spec.transactionsPerSession = 40;
      spec.maxRowsPerTransaction = 6;
      spec.savepointRollbackProbability = 0.25;
      spec.fullRollbackProbability = 0.15;
      spec.keyChangeWeight = 1;
      spec.lobWeight = 0; // LOB assembly is P1-18
      spec.ddlProbability = 0; // dictionary replay across DDL is P1-17
      spec.truncateProbability = 0.05;
      WorkloadGenerator g =
          new WorkloadGenerator(spec, db.jdbcUrl(OracleTestDatabase.PDB1), schema, schema);
      g.reset();
      OracleSql.archiveLogCurrent(db);
      long startScn = LogMinerHelper.currentScn(meta);
      WorkloadResult r = g.run();
      OracleSql.archiveLogCurrent(db);
      long endScn = LogMinerHelper.currentScn(meta);
      assertThat(r.committed.sum()).isPositive();
      assertThat(r.rolledBack.sum()).isPositive();

      Map<String, int[]> ledger = ledgerOps(w, schema, spec.ledgerTable);
      assertThat(ledger).hasSize((int) r.committed.sum());

      // one run over the whole window
      Collector full = new Collector();
      runEngine(schema, spec, Position.initial(startScn, identity(meta)), endScn, full, meta);
      assertThat(full.minedTo).isEqualTo(endScn);
      assertThat(full.resume).isEqualTo(endScn);
      verify(full, ledger, spec);

      // the same window in two runs: the first is "acknowledged" up to its k-th commit, the
      // second starts from the position that acknowledgement would have produced
      Collector first = new Collector();
      // split just after the median commit, so both runs have work to do
      long mid = full.committed.get(full.committed.size() / 2).commitScn() + 1;
      runEngine(schema, spec, Position.initial(startScn, identity(meta)), mid, first, meta);
      assertThat(first.committed).as("commits before the split").isNotEmpty();
      int k = Math.max(1, first.committed.size() * 2 / 3);
      CommittedTransaction acked = first.committed.get(k - 1);
      // the resume SCN the engine attached to the acknowledged commit is exactly what a restart
      // may mine from: lower than every transaction still open when that commit was emitted
      long resume = first.resumes.get(k - 1);
      Position restart =
          Position.initial(resume, identity(meta))
              .withCommit(acked.commitScn(), acked.thread(), acked.key(), acked.size());
      Collector second = new Collector();
      runEngine(schema, spec, restart, endScn, second, meta);
      Collector union = new Collector();
      for (int i = 0; i < k; i++) {
        union.committed(first.committed.get(i), first.skipped.get(i), 0);
      }
      for (int i = 0; i < second.committed.size(); i++) {
        union.committed(second.committed.get(i), second.skipped.get(i), 0);
      }
      Set<String> keys = new LinkedHashSet<>();
      for (CommittedTransaction t : union.committed) {
        assertThat(keys.add(t.key().xid().toString())).as("duplicate commit %s", t.key()).isTrue();
      }
      verify(union, ledger, spec);
      System.out.println(
          "engine-correctness: "
              + r.toJson()
              + " ledger="
              + ledger.size()
              + " firstRun="
              + first.committed.size()
              + " acked="
              + k
              + " secondRun="
              + second.committed.size());
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  private static void verify(Collector c, Map<String, int[]> ledger, WorkloadSpec spec) {
    Set<String> emitted = new LinkedHashSet<>();
    for (int i = 0; i < c.committed.size(); i++) {
      CommittedTransaction t = c.committed.get(i);
      String xid = t.key().xid().toString();
      emitted.add(xid);
      int[] expected = ledger.get(xid);
      assertThat(expected).as("transaction %s is not in the ledger", xid).isNotNull();
      int dataOps = 0;
      int ledgerRows = 0;
      RowChange previous = null;
      for (RowChange ch : t.events()) {
        if (previous != null) {
          assertThat(ch.id()).as("redo order inside %s", xid).isGreaterThan(previous.id());
        }
        previous = ch;
        if (ch.table().table().equals(spec.ledgerTable)) {
          ledgerRows++;
        } else {
          dataOps++;
        }
      }
      assertThat(ledgerRows).as("exactly one ledger row in %s", xid).isEqualTo(1);
      assertThat(dataOps)
          .as(
              "data operations in %s: %s",
              xid,
              t.events().stream()
                  .map(
                      ch ->
                          ch.op()
                              + " "
                              + ch.table().table()
                              + " rowid="
                              + ch.rowId()
                              + " id="
                              + ch.id()
                              + " after="
                              + ch.after())
                  .toList())
          .isEqualTo(expected[0]);
      assertThat(c.skipped.get(i)).isLessThan(t.size());
    }
    assertThat(emitted).containsExactlyInAnyOrderElementsOf(ledger.keySet());
  }

  private static Map<String, int[]> ledgerOps(Connection w, String schema, String table)
      throws SQLException {
    Map<String, int[]> out = new HashMap<>();
    try (PreparedStatement ps = w.prepareStatement("SELECT xid, ops FROM " + schema + "." + table);
        ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        out.put(rs.getString(1), new int[] {rs.getInt(2)});
      }
    }
    assertThat(Ledger.committedXids(w, schema, table))
        .containsExactlyInAnyOrderElementsOf(out.keySet());
    return out;
  }

  private static DatabaseIdentity identity(Connection meta) throws SQLException {
    var info = new JdbcCatalogSource(meta).database();
    return new DatabaseIdentity(info.dbid(), info.resetlogsChangeScn());
  }

  private void runEngine(
      String schema,
      WorkloadSpec spec,
      Position start,
      long safeEnd,
      Collector sink,
      Connection meta)
      throws Exception {
    try (Connection mining = db.capture(OracleTestDatabase.CDB_SERVICE)) {
      SessionInitializer.apply(mining, ConnectionRole.MINING);
      ObjectIdResolver resolver =
          new ObjectIdResolver(
              new JdbcObjectCatalog(meta),
              List.of("FREEPDB1\\." + schema + "\\.WL_.*"),
              List.of(),
              List.of("FREEPDB1"),
              false);
      ResolvedObjects objects = resolver.resolve();
      assertThat(objects.tables()).hasSize(spec.tables + 1);
      LogMinerEventSource source =
          new LogMinerEventSource(
              new LogInventory(new JdbcCatalogSource(meta), CaptureMode.ONLINE, 1),
              new JdbcLogMinerSession(mining, 2000, Duration.ofMinutes(5)),
              objects,
              objects.filter(Set.of(), 1000),
              DictionaryMode.ONLINE_CATALOG);
      SchemaRegistry registry =
          new SchemaRegistry(
              new InMemorySchemaStore(),
              new JdbcDictionaryReader(meta),
              new KeySelector(Map.of(), KeySelector.MissingKeyPolicy.ROWID));
      EngineSettings settings =
          new EngineSettings(
              Duration.ofSeconds(2),
              8,
              Duration.ofHours(1),
              Duration.ofMillis(50),
              20,
              sh.oso.connect.oracle.core.config.CoreConfig.DecodeErrorAction.FAIL);
      CaptureEngine engine =
          new CaptureEngine(
              start,
              source,
              new LogInventory(new JdbcCatalogSource(meta), CaptureMode.ONLINE, 1),
              () -> safeEnd,
              new HeapTransactionBuffer(),
              registry,
              ChangeDecoder.rowDecoder(),
              sink,
              settings,
              new OraErrorClassifier(),
              objects.owners(),
              () -> {
                ResolvedObjects refreshed = resolver.resolve();
                source.update(refreshed, refreshed.filter(Set.of(), 1000));
                return refreshed.owners();
              },
              cause -> {
                throw new java.sql.SQLException("this suite expects no reconnect", cause);
              },
              Instant::now);
      for (int i = 0; i < 1000; i++) {
        if (engine.runOnce() == CaptureEngine.Progress.IDLE) {
          break;
        }
      }
      assertThat(engine.cursor().scn()).isEqualTo(safeEnd);
      source.close();
    }
  }
}
