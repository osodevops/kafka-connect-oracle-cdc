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
package sh.oso.connect.oracle.core.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.buffer.HeapTransactionBuffer;
import sh.oso.connect.oracle.core.config.CoreConfig.CaptureMode;
import sh.oso.connect.oracle.core.config.CoreConfig.DecodeErrorAction;
import sh.oso.connect.oracle.core.errors.DecodeException;
import sh.oso.connect.oracle.core.errors.MiningStalledException;
import sh.oso.connect.oracle.core.errors.MiningStepRetryException;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.logs.LogInventory;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.DictionaryReader;
import sh.oso.connect.oracle.core.schema.KeySelector;
import sh.oso.connect.oracle.core.schema.KeySource;
import sh.oso.connect.oracle.core.schema.OracleType;
import sh.oso.connect.oracle.core.schema.SchemaRegistry;
import sh.oso.connect.oracle.core.schema.TableSchema;
import sh.oso.connect.oracle.core.testkit.FakeCatalog;
import sh.oso.connect.oracle.core.testkit.FakeLogMiner;
import sh.oso.connect.oracle.core.testkit.FixedClock;
import sh.oso.connect.oracle.core.testkit.InMemorySchemaStore;

class CaptureEngineTest {

  static final TableId T = FakeLogMiner.DEFAULT_TABLE;

  /** Collects what the engine emits. */
  static final class Sink implements EventSink {
    final List<CommittedTransaction> committed = new ArrayList<>();
    final List<Integer> skipped = new ArrayList<>();
    final List<Long> resumes = new ArrayList<>();
    final List<long[]> steps = new ArrayList<>();
    final List<MiningEvent.Ddl> ddls = new ArrayList<>();
    final List<MiningEvent.Dml> failed = new ArrayList<>();

    public void committed(CommittedTransaction tx, int skip, long resumeCandidate) {
      resumes.add(resumeCandidate);
      committed.add(tx);
      skipped.add(skip);
    }

    public void stepApplied(long minedTo, long resume) {
      steps.add(new long[] {minedTo, resume});
    }

    public void ddl(MiningEvent.Ddl d) {
      ddls.add(d);
    }

    public void decodeFailed(MiningEvent.Dml d, DecodeException e) {
      failed.add(d);
    }

    List<String> sqls() {
      List<String> out = new ArrayList<>();
      for (int i = 0; i < committed.size(); i++) {
        List<RowChange> ev = committed.get(i).events();
        for (int j = skipped.get(i); j < ev.size(); j++) {
          out.add((String) ev.get(j).after().get("SQL"));
        }
      }
      return out;
    }
  }

  /** Test decoder: the fake's SQL text becomes the single column SQL; "bad" fails. */
  static final ChangeDecoder DECODER =
      (d, schema) -> {
        if ("bad".equals(d.sqlRedo())) {
          throw new DecodeException("bad row", "none");
        }
        return new RowChange(
            d.table(),
            d.op(),
            null,
            Map.of("SQL", d.sqlRedo()),
            false,
            d.rowId(),
            d.id(),
            d.tx(),
            d.timestamp());
      };

  static SchemaRegistry registry() {
    InMemorySchemaStore store = new InMemorySchemaStore();
    store.save(
        new TableSchema(
            T,
            List.of(ColumnSpec.of("SQL", 1, OracleType.VARCHAR2)),
            List.of(),
            KeySource.NONE,
            true,
            false));
    DictionaryReader none =
        new DictionaryReader() {
          public Optional<TableSchema> read(TableId t) {
            throw new IllegalStateException("dictionary must not be read: " + t);
          }

          public KeySelector.Candidates keyCandidates(TableId t) {
            throw new IllegalStateException();
          }
        };
    return new SchemaRegistry(
        store, none, new KeySelector(Map.of(), KeySelector.MissingKeyPolicy.NONE));
  }

  /** One 100-SCN log per hundred, from 1000; current SCN is the safe end. */
  static final class Harness {
    final FakeCatalog catalog = new FakeCatalog().archivedRun(1, 1, 20, 1000, 100);
    final FakeLogMiner fake = new FakeLogMiner().startAt(1000);
    final Sink sink = new Sink();
    final HeapTransactionBuffer buffer = new HeapTransactionBuffer();
    final FixedClock clock = new FixedClock(0);
    int refreshes;
    long safeEnd = 1000;

    CaptureEngine engine(Position start, DecodeErrorAction onError) {
      EngineSettings s =
          new EngineSettings(
              Duration.ofSeconds(2), 8, Duration.ofHours(1), Duration.ofMillis(10), 3, onError);
      return new CaptureEngine(
          start,
          fake,
          new LogInventory(catalog, CaptureMode.ONLINE, 1),
          () -> safeEnd,
          buffer,
          registry(),
          DECODER,
          sink,
          s,
          new OraErrorClassifier(),
          Set.of("APP"),
          () -> {
            refreshes++;
            return Set.of("APP");
          },
          () -> Instant.ofEpochMilli(clock.getAsLong()));
    }

    Position initial() {
      return Position.initial(1000, new DatabaseIdentity(1, 1));
    }

    void runUntilIdle(CaptureEngine e) throws SQLException {
      for (int i = 0; i < 100; i++) {
        if (e.runOnce() == CaptureEngine.Progress.IDLE) {
          return;
        }
      }
      throw new AssertionError("did not go idle");
    }
  }

  @Test
  void multiRowTransactionRollbackAndSavepointThroughTheWholeLoop() throws Exception {
    Harness h = new Harness();
    TxKey a = h.fake.tx(1, 1, 1);
    TxKey b = h.fake.tx(1, 1, 2);
    TxKey c = h.fake.tx(1, 1, 3);
    h.fake
        .start(a, "APP")
        .insert(a, T, "a1")
        .insert(a, T, "a2")
        .start(b, "APP")
        .insert(b, T, "b1")
        .update(a, T, "a3")
        .commit(a) // scn 1005
        .dmlWithRowId(b, Operation.INSERT, T, "b2", "RB2")
        .undo(b, Operation.DELETE, T, "undo b2", "RB2")
        .rollback(b)
        .start(c, "APP")
        .dmlWithRowId(c, Operation.INSERT, T, "c1", "RC1")
        .dmlWithRowId(c, Operation.UPDATE, T, "c2", "RC1")
        .undo(c, Operation.UPDATE, T, "undo c2", "RC1")
        .commit(c);
    h.safeEnd = h.fake.nextScn();
    CaptureEngine e = h.engine(h.initial(), DecodeErrorAction.FAIL);
    h.runUntilIdle(e);
    assertThat(h.sink.committed).hasSize(2);
    assertThat(h.sink.sqls()).containsExactly("a1", "a2", "a3", "c1");
    // when a committed, b (started at 1003) was still open: a's resume is b's first capture
    assertThat(h.sink.resumes.get(0)).isEqualTo(1004);
    // when c committed nothing else was open: its resume is its own commit scn
    assertThat(h.sink.resumes.get(1)).isEqualTo(h.sink.committed.get(1).commitScn());
    assertThat(h.sink.committed.get(0).username()).isEqualTo("APP");
    assertThat(h.buffer.openTransactions()).isZero();
    assertThat(e.metrics().transactionsCommitted.get()).isEqualTo(2);
    assertThat(e.metrics().steps.get()).isPositive();
    assertThat(h.sink.steps).isNotEmpty();
    long[] last = h.sink.steps.get(h.sink.steps.size() - 1);
    assertThat(last[0]).isEqualTo(h.safeEnd);
    assertThat(last[1]).as("no open transaction: resume is the mined end").isEqualTo(h.safeEnd);
    assertThat(e.runOnce()).isEqualTo(CaptureEngine.Progress.IDLE);
  }

  @Test
  void resumeCandidateStaysAtTheOldestOpenTransaction() throws Exception {
    Harness h = new Harness();
    TxKey open = h.fake.tx(1, 1, 7);
    TxKey done = h.fake.tx(1, 1, 8);
    h.fake
        .start(open, "APP")
        .insert(open, T, "o1") // first captured at 1001
        .start(done, "APP")
        .insert(done, T, "d1")
        .commit(done); // 1004
    h.safeEnd = 1500;
    CaptureEngine e = h.engine(h.initial(), DecodeErrorAction.FAIL);
    h.runUntilIdle(e);
    assertThat(h.sink.sqls()).containsExactly("d1");
    long[] last = h.sink.steps.get(h.sink.steps.size() - 1);
    assertThat(last[0]).isEqualTo(1500);
    assertThat(last[1]).isEqualTo(1001);
    assertThat(h.sink.resumes)
        .as("the committed one must not outrun the open one")
        .containsExactly(1001L);
    assertThat(h.buffer.openTransactions()).isEqualTo(1);
  }

  @Test
  void restartAfterCommitAndMidTransactionReplaysOnlyTheUnacknowledgedSuffix() throws Exception {
    Harness h = new Harness();
    TxKey a = h.fake.tx(1, 1, 1);
    TxKey b = h.fake.tx(1, 1, 2);
    h.fake
        .start(a, "APP")
        .insert(a, T, "a1")
        .insert(a, T, "a2")
        .commit(a) // commit scn 1003
        .start(b, "APP")
        .insert(b, T, "b1")
        .insert(b, T, "b2")
        .insert(b, T, "b3")
        .commit(b); // 1008
    h.safeEnd = h.fake.nextScn();
    // first run acknowledged all of a and the first event of b
    Position afterCrash = h.initial().withCommit(1008, 1, b, 1);
    CaptureEngine e = h.engine(afterCrash, DecodeErrorAction.FAIL);
    h.runUntilIdle(e);
    assertThat(h.sink.committed).hasSize(1);
    assertThat(h.sink.skipped).containsExactly(1);
    assertThat(h.sink.sqls()).containsExactly("b2", "b3");
    assertThat(e.metrics().transactionsSkipped.get()).isEqualTo(1);

    // everything acknowledged: a duplicate replay emits nothing
    Harness h2 = new Harness();
    h2.fake.add(h.fake.events().get(0));
    for (MiningEvent ev : h.fake.events()) {
      if (ev != h.fake.events().get(0)) {
        h2.fake.add(ev);
      }
    }
    h2.safeEnd = h.safeEnd;
    CaptureEngine e2 = h2.engine(h2.initial().withCommit(1008, 1, b, 3), DecodeErrorAction.FAIL);
    h2.runUntilIdle(e2);
    assertThat(h2.sink.committed).isEmpty();
    assertThat(e2.metrics().transactionsSkipped.get()).isEqualTo(2);
  }

  @Test
  void stepRetryReMinesTheSameRangeAndPersistentRetryStops() throws Exception {
    Harness h = new Harness();
    TxKey a = h.fake.tx(1, 1, 1);
    h.fake.start(a, "APP").insert(a, T, "a1").commit(a);
    h.safeEnd = h.fake.nextScn();
    h.fake.faultAt(1, new SQLException("ORA-00310", "72000", 310), false);
    CaptureEngine e = h.engine(h.initial(), DecodeErrorAction.FAIL);
    assertThat(e.runOnce()).isEqualTo(CaptureEngine.Progress.STEP_RETRIED);
    assertThat(h.sink.committed).isEmpty();
    assertThat(h.buffer.openTransactions()).as("nothing applied from the failed step").isZero();
    h.runUntilIdle(e);
    assertThat(h.sink.sqls()).containsExactly("a1");
    assertThat(e.metrics().stepRetries.get()).isEqualTo(1);

    Harness p = new Harness();
    p.fake.start(a, "APP").insert(a, T, "a1").commit(a);
    p.safeEnd = p.fake.nextScn();
    p.fake.faultAt(0, new SQLException("ORA-00334", "72000", 334), true);
    CaptureEngine pe = p.engine(p.initial(), DecodeErrorAction.FAIL);
    for (int i = 0; i < 3; i++) {
      assertThat(pe.runOnce()).isEqualTo(CaptureEngine.Progress.STEP_RETRIED);
    }
    assertThatThrownBy(pe::runOnce)
        .isInstanceOf(MiningStepRetryException.class)
        .hasMessageContaining("4 times");
  }

  @Test
  void timeoutsHalveTheWindowAndStallAfterThreeAtOneLog() throws Exception {
    Harness h = new Harness();
    TxKey a = h.fake.tx(1, 1, 1);
    h.fake.start(a, "APP").insert(a, T, "a1").commit(a);
    h.safeEnd = 1050;
    h.fake.faultAt(0, new java.sql.SQLTimeoutException("slow"), true);
    CaptureEngine e = h.engine(h.initial(), DecodeErrorAction.FAIL);
    assertThat(e.runOnce()).isEqualTo(CaptureEngine.Progress.STEP_TIMED_OUT);
    assertThat(e.runOnce()).isEqualTo(CaptureEngine.Progress.STEP_TIMED_OUT);
    assertThatThrownBy(e::runOnce).isInstanceOf(MiningStalledException.class);
    assertThat(e.metrics().stepTimeouts.get()).isEqualTo(3);
  }

  @Test
  void ddlStepCutAppliesThePrefixRefreshesIdsAndContinues() throws Exception {
    Harness h = new Harness();
    TxKey a = h.fake.tx(1, 1, 1);
    TxKey d = h.fake.tx(1, 1, 2);
    h.fake
        .start(a, "APP")
        .insert(a, T, "a1")
        .ddl(
            d,
            new TableId("FREEPDB1", "APP", "EVENTS"),
            77,
            "alter table events add partition p2 values less than (2)")
        .insert(a, T, "a2")
        .commit(a);
    h.safeEnd = h.fake.nextScn();
    CaptureEngine e = h.engine(h.initial(), DecodeErrorAction.FAIL);
    assertThat(e.runOnce()).isEqualTo(CaptureEngine.Progress.STEP_APPLIED);
    assertThat(h.refreshes).isEqualTo(1);
    assertThat(h.sink.ddls).hasSize(1);
    assertThat(h.sink.committed).isEmpty();
    assertThat(e.cursor().lastApplied()).isNotNull();
    h.runUntilIdle(e);
    assertThat(h.sink.sqls()).containsExactly("a1", "a2");
    assertThat(e.metrics().stepCuts.get()).isEqualTo(1);
    assertThat(e.metrics().rowsMined.get())
        .as("the DDL row is re-mined once and skipped")
        .isEqualTo(6);
  }

  @Test
  void decodeErrorsStopOrGoToTheDlqPerPolicy() throws Exception {
    Harness h = new Harness();
    TxKey a = h.fake.tx(1, 1, 1);
    h.fake.start(a, "APP").insert(a, T, "a1").insert(a, T, "bad").insert(a, T, "a3").commit(a);
    h.safeEnd = h.fake.nextScn();
    assertThatThrownBy(() -> h.runUntilIdle(h.engine(h.initial(), DecodeErrorAction.FAIL)))
        .isInstanceOf(DecodeException.class);

    Harness dlq = new Harness();
    dlq.fake
        .start(a, "APP")
        .insert(a, T, "a1")
        .insert(a, T, "bad")
        .insert(a, T, "a3")
        .commit(a)
        .add(
            new MiningEvent.Unsupported(
                a,
                new sh.oso.connect.oracle.core.model.RedoRecordId(1010, "0x1", 0),
                T,
                100,
                2,
                null,
                null));
    dlq.safeEnd = dlq.fake.nextScn() + 20;
    CaptureEngine e = dlq.engine(dlq.initial(), DecodeErrorAction.DLQ);
    dlq.runUntilIdle(e);
    assertThat(dlq.sink.sqls()).containsExactly("a1", "a3");
    assertThat(dlq.sink.failed).hasSize(1);
    assertThat(e.metrics().decodeFailures.get()).isEqualTo(2);
  }

  @Test
  void lifecycleRunsOnAThreadStopsCleanlyAndSurfacesFailures() throws Exception {
    Harness h = new Harness();
    TxKey a = h.fake.tx(1, 1, 1);
    h.fake.start(a, "APP").insert(a, T, "a1").commit(a);
    h.safeEnd = h.fake.nextScn();
    CaptureEngine e = h.engine(h.initial(), DecodeErrorAction.FAIL);
    List<Throwable> failures = new ArrayList<>();
    try (EngineLifecycle life = new EngineLifecycle(e, Duration.ofMillis(5), failures::add)) {
      life.start("engine-test");
      long deadline = System.currentTimeMillis() + 5000;
      while (h.sink.committed.isEmpty() && System.currentTimeMillis() < deadline) {
        Thread.sleep(5);
      }
      assertThat(h.sink.sqls()).containsExactly("a1");
      assertThat(life.isRunning()).isTrue();
      assertThat(life.stop(Duration.ofSeconds(5))).isTrue();
      assertThat(life.isRunning()).isFalse();
      assertThat(life.failure()).isNull();
      assertThatThrownBy(() -> life.start("again")).isInstanceOf(IllegalStateException.class);
    }
    assertThat(failures).isEmpty();

    Harness bad = new Harness();
    bad.fake.start(a, "APP").insert(a, T, "bad").commit(a);
    bad.safeEnd = bad.fake.nextScn();
    EngineLifecycle life =
        new EngineLifecycle(
            bad.engine(bad.initial(), DecodeErrorAction.FAIL), Duration.ofMillis(5), failures::add);
    life.start("engine-bad");
    long deadline = System.currentTimeMillis() + 5000;
    while (life.failure() == null && System.currentTimeMillis() < deadline) {
      Thread.sleep(5);
    }
    assertThat(life.failure()).isInstanceOf(DecodeException.class);
    assertThat(failures).hasSize(1);
    assertThat(life.stop(Duration.ofSeconds(1))).isTrue();
  }

  @Test
  void sessionIsRecycledAfterMaxAge() throws Exception {
    Harness h = new Harness();
    TxKey a = h.fake.tx(1, 1, 1);
    h.fake.start(a, "APP").insert(a, T, "a1").commit(a);
    h.safeEnd = h.fake.nextScn();
    CaptureEngine e = h.engine(h.initial(), DecodeErrorAction.FAIL);
    h.clock.advance(Duration.ofHours(2));
    e.runOnce();
    assertThat(e.metrics().sessionRecycles.get()).isEqualTo(1);
    assertThat(EngineSettings.defaults().maxConsecutiveRetries()).isEqualTo(20);
  }
}
