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

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Supplier;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.buffer.TransactionBuffer;
import sh.oso.connect.oracle.core.config.CoreConfig.DecodeErrorAction;
import sh.oso.connect.oracle.core.errors.DecodeException;
import sh.oso.connect.oracle.core.errors.MiningStepRetryException;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.errors.OracleCdcException;
import sh.oso.connect.oracle.core.errors.TransientDatabaseException;
import sh.oso.connect.oracle.core.logs.LogInventory;
import sh.oso.connect.oracle.core.logs.LogSet;
import sh.oso.connect.oracle.core.mining.event.EventSource;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.mining.step.MiningScheduler;
import sh.oso.connect.oracle.core.mining.step.SessionRecycler;
import sh.oso.connect.oracle.core.mining.step.StepCursor;
import sh.oso.connect.oracle.core.mining.step.StepOutcome;
import sh.oso.connect.oracle.core.mining.step.StepPlan;
import sh.oso.connect.oracle.core.mining.step.StepRunner;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.position.ResumeCalculator;
import sh.oso.connect.oracle.core.position.SkipRule;
import sh.oso.connect.oracle.core.schema.SchemaRegistry;
import sh.oso.connect.oracle.core.schema.TableSchema;

/**
 * The capture loop (PRD-00 section 3): plan a step, run it staged, apply its events to the buffer
 * in redo order, hand committed transactions to the sink in commit order, report where the position
 * may advance to. One call to {@link #runOnce()} is one step or one idle poll; {@link
 * EngineLifecycle} loops it on a thread. Everything here is synchronous and deterministic so the
 * fake can drive it.
 */
public final class CaptureEngine {

  /** The database-bound collaborators a reconnect replaces. */
  public record Sources(EventSource source, LogInventory inventory, Supplier<Long> safeEndScn) {}

  /**
   * CORE-CONN-6: on a transient database error the engine asks its owner for fresh connections and
   * continues from the same cursor; nothing of the failed step was applied. The owner's connection
   * factory retries with backoff inside the retry budget and throws when it is exhausted.
   */
  public interface Reconnector {
    Sources reconnect(Throwable cause) throws SQLException, InterruptedException;
  }

  /** Called after a DDL step cut so the owner refreshes the pushed-down object ids (ADR-0001). */
  public interface IdRefresher {
    /** Re-resolves the pushed-down ids; returns the captured owners afterwards. */
    java.util.Set<String> refresh() throws SQLException;
  }

  private EventSource source;
  private LogInventory inventory;
  private Supplier<Long> safeEnd;
  private final Reconnector reconnector;
  private boolean pendingRefresh;
  private final MiningScheduler scheduler;
  private StepRunner runner;
  private final OraErrorClassifier classifier;
  private final TransactionBuffer buffer;
  private final SchemaRegistry schemas;
  private final ChangeDecoder decoder;
  private final EventSink sink;
  private final EngineSettings settings;
  private final SessionRecycler recycler;
  private final IdRefresher idRefresher;
  private final Position start;
  private final EngineMetrics metrics = new EngineMetrics();
  private final LobAssembler lobs;
  private LobReselector reselector;
  private int decodeThreads = 1;
  private java.util.concurrent.ForkJoinPool decodePool;
  private java.util.Map<MiningEvent.Dml, Object> decoded = java.util.Map.of();

  /** Below this many rows a step is decoded on the engine thread: the hand-off costs more. */
  static final int PARALLEL_DECODE_MIN_ROWS = 256;

  private final Supplier<Instant> clock;
  private sh.oso.connect.oracle.core.orphan.OrphanDetector orphans =
      sh.oso.connect.oracle.core.orphan.OrphanDetector.disabled();
  private StepCursor cursor;
  private int consecutiveRetries;

  public CaptureEngine(
      Position start,
      EventSource source,
      LogInventory inventory,
      Supplier<Long> safeEndScn,
      TransactionBuffer buffer,
      SchemaRegistry schemas,
      ChangeDecoder decoder,
      EventSink sink,
      EngineSettings settings,
      OraErrorClassifier classifier,
      java.util.Set<String> capturedOwners,
      IdRefresher idRefresher,
      Reconnector reconnector,
      Supplier<Instant> clock) {
    this.start = start;
    this.source = source;
    this.inventory = inventory;
    this.safeEnd = safeEndScn;
    this.buffer = buffer;
    this.schemas = schemas;
    this.decoder = decoder;
    this.sink = sink;
    this.settings = settings;
    this.scheduler = new MiningScheduler(settings.targetLatency(), settings.maxLogsPerStep());
    this.classifier = classifier;
    this.runner =
        new StepRunner(
            classifier, new sh.oso.connect.oracle.core.mining.step.DdlStepCut(capturedOwners));
    this.recycler = new SessionRecycler(settings.sessionMaxAge(), clock);
    this.idRefresher = idRefresher;
    this.reconnector = reconnector;
    this.clock = clock;
    this.cursor = StepCursor.resume(start.resumePoint());
    this.lobs = new LobAssembler(settings.lobMode(), settings.lobMaxBytes());
  }

  /** CORE-MINE-6: decode a step's rows on this many threads before applying them in order. */
  public CaptureEngine withDecodeThreads(int threads) {
    this.decodeThreads = Math.max(1, threads);
    return this;
  }

  /** CORE-DEC-7: how unavailable LOB values are fetched at commit in reselect mode. */
  public CaptureEngine withReselector(LobReselector r) {
    this.reselector = r;
    return this;
  }

  /** CORE-TX-7: the orphan detector to run between steps; disabled by default. */
  public CaptureEngine withOrphanDetector(sh.oso.connect.oracle.core.orphan.OrphanDetector d) {
    this.orphans = d;
    return this;
  }

  public enum Progress {
    STEP_APPLIED,
    STEP_RETRIED,
    STEP_TIMED_OUT,
    IDLE,
    RECONNECTED
  }

  public EngineMetrics metrics() {
    return metrics;
  }

  public StepCursor cursor() {
    return cursor;
  }

  public MiningScheduler scheduler() {
    return scheduler;
  }

  /**
   * One step, one idle poll or one reconnect. Throws only {@link OracleCdcException}s, which stop
   * the task; a transient database error leads to a reconnect instead (CORE-CONN-6).
   */
  public Progress runOnce() throws SQLException, InterruptedException {
    try {
      return step();
    } catch (TransientDatabaseException e) {
      return reconnect(e);
    } catch (SQLException e) {
      OracleCdcException typed = classifier.toException(e, "mining from " + cursor.scn());
      if (typed instanceof TransientDatabaseException) {
        return reconnect(typed);
      }
      throw typed;
    }
  }

  private Progress reconnect(OracleCdcException cause) throws SQLException, InterruptedException {
    metrics.reconnects.incrementAndGet();
    Sources fresh = reconnector.reconnect(cause); // retries with backoff; throws when exhausted
    this.source = fresh.source();
    this.inventory = fresh.inventory();
    this.safeEnd = fresh.safeEndScn();
    recycler.recycled();
    sink.reconnected(cause.getMessage());
    return Progress.RECONNECTED;
  }

  private Progress step() throws SQLException {
    if (pendingRefresh) {
      refreshIds();
    }
    long end = safeEnd.get();
    metrics.safeEndScn.set(end);
    if (end <= cursor.scn()) {
      metrics.idlePolls.incrementAndGet();
      buffer.flushJournal(clock.get()); // age-based journaling must not wait for new redo
      enforceTransactionAge();
      checkOrphans();
      sink.idle(cursor.scn(), ResumeCalculator.resume(cursor, oldestOpen()));
      publishBuffer();
      return Progress.IDLE;
    }
    if (recycler.due()) {
      source.recycle();
      recycler.recycled();
      metrics.sessionRecycles.incrementAndGet();
    }
    LogSet logs = inventory.forRange(cursor.scn(), end);
    StepPlan plan = scheduler.plan(cursor.scn(), end, logs.logs());
    metrics.windowLogs.set(plan.windowLogs());
    Instant t0 = clock.get();
    StepOutcome outcome = runner.run(source, cursor, plan.endScn());
    metrics.rowsMined.addAndGet(outcome.rowsSeen());
    switch (outcome.kind()) {
      case RETRY:
        metrics.stepRetries.incrementAndGet();
        if (++consecutiveRetries > settings.maxConsecutiveRetries()) {
          throw new MiningStepRetryException(
              "Mining "
                  + plan.startScn()
                  + ".."
                  + plan.endScn()
                  + " failed "
                  + consecutiveRetries
                  + " times in a row with a retriable LogMiner error: "
                  + outcome.cause().getMessage(),
              "The redo for this range keeps failing to mine; check the alert log and the archived"
                  + " logs covering it.",
              outcome.cause());
        }
        return Progress.STEP_RETRIED;
      case TIMEOUT:
        metrics.stepTimeouts.incrementAndGet();
        scheduler.stepTimedOut(plan);
        return Progress.STEP_TIMED_OUT;
      default:
        break;
    }
    consecutiveRetries = 0;
    warmSchemas(outcome); // every database read happens before the buffer changes
    decoded = preDecode(outcome);
    try {
      apply(outcome);
    } finally {
      decoded = java.util.Map.of();
    }
    cursor = outcome.next();
    Duration elapsed = Duration.between(t0, clock.get());
    metrics.lastStepMillis.set(elapsed.toMillis());
    metrics.steps.incrementAndGet();
    metrics.minedToScn.set(cursor.scn());
    if (outcome.kind() == StepOutcome.Kind.CUT) {
      metrics.stepCuts.incrementAndGet();
      refreshIds();
    } else {
      scheduler.stepCompleted(elapsed);
    }
    buffer.flushJournal(clock.get()); // CORE-TX-4: chunks for due and journaled transactions
    enforceTransactionAge();
    checkOrphans();
    sink.stepApplied(cursor.scn(), ResumeCalculator.resume(cursor, oldestOpen()));
    publishBuffer();
    return Progress.STEP_APPLIED;
  }

  /** The buffer belongs to this thread; JMX readers see the snapshot taken here. */
  private void publishBuffer() {
    metrics.buffer = buffer.metrics();
    metrics.largest = java.util.List.copyOf(buffer.largest(20));
  }

  /**
   * CORE-TX-6: a transaction open longer than the limit either stops the task or is discarded with
   * an ops event and a DLQ record; a discarded key joins the released ledger so its COMMIT, if it
   * ever comes, is a stop rather than a partial emission.
   */
  private void enforceTransactionAge() {
    if (settings.transactionMaxAge() == null) {
      return;
    }
    Instant now = clock.get();
    for (sh.oso.connect.oracle.core.buffer.TransactionBuffer.OpenTransaction t : buffer.open()) {
      if (t.firstSeenAt() == null) {
        continue;
      }
      Duration age = Duration.between(t.firstSeenAt(), now);
      if (age.compareTo(settings.transactionMaxAge()) < 0) {
        continue;
      }
      if (settings.maxAgeAction() == EngineSettings.MaxAgeAction.FAIL) {
        throw new sh.oso.connect.oracle.core.errors.TransactionTooOldException(
            "Transaction "
                + t.key()
                + " (user "
                + t.username()
                + ", "
                + t.events()
                + " events from SCN "
                + t.firstScn()
                + ") has been open for "
                + age.toSeconds()
                + " seconds, longer than cdc.transaction.max.age.ms allows.",
            "Commit or roll back the transaction in the database, raise"
                + " cdc.transaction.max.age.ms, or set cdc.transaction.max.age.action=discard to"
                + " drop such transactions with an ops event and a DLQ record; then restart the"
                + " task.");
      }
      lobs.discard(t.key());
      buffer.discard(t.key());
      orphans.releasedByPolicy(t.key());
      metrics.transactionsDiscarded.incrementAndGet();
      sink.transactionDiscarded(t, age, orphans.released());
    }
  }

  /** CORE-TX-7: release orphaned transactions the detector has confirmed, with an ops event. */
  private void checkOrphans() throws SQLException {
    java.util.List<sh.oso.connect.oracle.core.orphan.OrphanDetector.Release> releases =
        orphans.check(clock.get(), cursor.scn(), buffer.open());
    for (sh.oso.connect.oracle.core.orphan.OrphanDetector.Release r : releases) {
      lobs.discard(r.tx().key());
      buffer.discard(r.tx().key());
      metrics.orphansReleased.incrementAndGet();
      sink.orphanReleased(r, orphans.released());
    }
  }

  private void refreshIds() throws SQLException {
    java.util.Set<String> owners = idRefresher.refresh();
    runner =
        new StepRunner(classifier, new sh.oso.connect.oracle.core.mining.step.DdlStepCut(owners));
    pendingRefresh = false;
    sink.idsRefreshed(owners);
  }

  /** Loads the schema of every table in the step so apply() touches no connection. */
  private void warmSchemas(StepOutcome outcome) throws SQLException {
    for (MiningEvent e : outcome.events()) {
      if (e instanceof MiningEvent.Dml d && !d.undo()) {
        schemas.current(d.table());
      }
    }
  }

  private void apply(StepOutcome outcome) throws SQLException {
    for (MiningEvent e : outcome.events()) {
      metrics.eventsApplied.incrementAndGet();
      if (e instanceof MiningEvent.TxStart s) {
        buffer.start(s);
      } else if (e instanceof MiningEvent.Dml d) {
        if (d.op() == sh.oso.connect.oracle.core.model.Operation.SELECT_LOB_LOCATOR) {
          // 23ai: no SQL_REDO, and every LOB_WRITE row selects its own locator (ADR-0015)
          continue;
        } else if (d.undo() && !d.op().isLobOp()) {
          lobs.flush(d.tx()).ifPresent(held -> buffer.add(d.tx(), held));
          buffer.undo(d.tx(), d.id(), d.rowId(), d.table(), d.op());
        } else if (d.op().isLobOp()) {
          decodeLobAndBuffer(d);
        } else {
          decodeAndBuffer(d);
        }
      } else if (e instanceof MiningEvent.Commit c) {
        if (orphans.wasReleased(c.tx())) {
          throw new sh.oso.connect.oracle.core.errors.OrphanReleaseViolationException(
              "Transaction "
                  + c.tx()
                  + " committed at SCN "
                  + c.scn()
                  + " after the orphan detector had released it as rolled back; its earlier"
                  + " changes were discarded.",
              "Reset the offsets to a position before SCN "
                  + c.scn()
                  + " so the transaction is re-mined whole, or resnapshot the affected tables;"
                  + " then raise cdc.transaction.orphan.check.interval.ms.");
        }
        lobs.flush(c.tx()).ifPresent(held -> buffer.add(c.tx(), held));
        Optional<LobAssembler.Oversize> over = lobs.takeOversize(c.tx());
        if (over.isPresent() && settings.lobOversizeFail()) {
          throw new sh.oso.connect.oracle.core.errors.LobTooLargeException(
              "Transaction "
                  + c.tx()
                  + " committed at SCN "
                  + c.scn()
                  + " wrote "
                  + over.get().bytes()
                  + " bytes or more into "
                  + over.get().table()
                  + "."
                  + over.get().column()
                  + ", above cdc.lob.max.bytes="
                  + settings.lobMaxBytes()
                  + ".",
              "Raise cdc.lob.max.bytes, or set cdc.lob.oversize.action=placeholder to publish the"
                  + " placeholder for such values.");
        }
        Optional<CommittedTransaction> tx = buffer.commit(c);
        if (tx.isPresent()) {
          emit(tx.get());
          buffer.release(tx.get().key()); // the sink has consumed the events; a spilled copy can go
        }
      } else if (e instanceof MiningEvent.Rollback r) {
        lobs.discard(r.tx());
        buffer.rollback(r);
      } else if (e instanceof MiningEvent.Ddl d) {
        if (d.objectName() != null && d.owner() != null) {
          schemas.invalidate(
              new sh.oso.connect.oracle.core.model.TableId(d.pdb(), d.owner(), d.objectName()));
        }
        sink.ddl(d);
      } else if (e instanceof MiningEvent.Unsupported u) {
        if (settings.onDecodeError() == DecodeErrorAction.FAIL) {
          throw new DecodeException(
              "LogMiner marked a row of "
                  + u.table().fqn()
                  + " UNSUPPORTED at "
                  + u.id()
                  + (u.info() == null ? "" : " (" + u.info() + ")"),
              "The table has a column type LogMiner cannot reconstruct (DOC-5); exclude the table"
                  + " or set cdc.on.decode.error=dlq.");
        }
        metrics.decodeFailures.incrementAndGet();
        sink.unsupported(u);
      }
      // MissingScn never reaches here (the runner stops); Other and LogBoundary are ignored
    }
  }

  /**
   * CORE-MINE-6: decodes the step's rows up to its first DDL (which may change the schema of what
   * follows) on {@code decodeThreads} threads. Schemas are looked up here, on the engine thread;
   * results and decode errors are consumed in redo order by {@link #apply}.
   */
  private java.util.Map<MiningEvent.Dml, Object> preDecode(StepOutcome outcome)
      throws SQLException {
    if (decodeThreads <= 1) {
      return java.util.Map.of();
    }
    java.util.List<MiningEvent.Dml> rows = new java.util.ArrayList<>();
    for (MiningEvent e : outcome.events()) {
      if (e instanceof MiningEvent.Ddl) {
        break;
      }
      if (e instanceof MiningEvent.Dml d
          && !d.undo()
          && d.op() != sh.oso.connect.oracle.core.model.Operation.SELECT_LOB_LOCATOR) {
        rows.add(d);
      }
    }
    if (rows.size() < PARALLEL_DECODE_MIN_ROWS) {
      return java.util.Map.of();
    }
    TableSchema[] tables = new TableSchema[rows.size()];
    for (int i = 0; i < tables.length; i++) {
      tables[i] = schemas.current(rows.get(i).table());
    }
    Object[] results = new Object[rows.size()];
    if (decodePool == null) {
      decodePool = new java.util.concurrent.ForkJoinPool(decodeThreads);
    }
    decodePool
        .submit(
            () ->
                java.util.stream.IntStream.range(0, results.length)
                    .parallel()
                    .forEach(i -> results[i] = decodeOne(rows.get(i), tables[i])))
        .join();
    java.util.Map<MiningEvent.Dml, Object> out = new java.util.IdentityHashMap<>();
    for (int i = 0; i < results.length; i++) {
      out.put(rows.get(i), results[i]);
    }
    return out;
  }

  private Object decodeOne(MiningEvent.Dml d, TableSchema schema) {
    try {
      return d.op().isLobOp()
          ? sh.oso.connect.oracle.core.decode.RowDecoder.decodeLob(d, schema)
          : decoder.decode(d, schema);
    } catch (RuntimeException ex) {
      return ex; // raised in order when the row is applied
    }
  }

  /** The pre-decoded result for a row, rethrowing its exception, or null when not pre-decoded. */
  private Object predecoded(MiningEvent.Dml d) {
    Object r = decoded.get(d);
    if (r instanceof RuntimeException ex) {
      throw ex;
    }
    return r;
  }

  private void decodeAndBuffer(MiningEvent.Dml d) throws SQLException {
    TableSchema schema = schemas.current(d.table());
    RowChange change;
    try {
      Object pre = predecoded(d);
      change = pre != null ? (RowChange) pre : decoder.decode(d, schema);
    } catch (DecodeException ex) {
      if (settings.onDecodeError() == DecodeErrorAction.FAIL) {
        throw ex;
      }
      metrics.decodeFailures.incrementAndGet();
      sink.decodeFailed(d, ex);
      return;
    }
    for (RowChange ready : lobs.accept(d.tx(), change, schema)) {
      buffer.add(d.tx(), ready);
    }
    metrics.lobInsertsMerged.set(lobs.merged());
  }

  /** CORE-DEC-6: a LOB_WRITE, LOB_TRIM or LOB_ERASE row joins the change of its statement. */
  private void decodeLobAndBuffer(MiningEvent.Dml d) throws SQLException {
    TableSchema schema = schemas.current(d.table());
    sh.oso.connect.oracle.core.decode.LobFragment f;
    try {
      if (d.undo()) {
        throw new DecodeException(
            "LogMiner returned a ROLLBACK=1 "
                + d.op()
                + " row for "
                + d.table().fqn()
                + " at "
                + d.id(),
            "Report the row shape with the Oracle version (reference/lob-redo-shapes.md).");
      }
      Object pre = predecoded(d);
      f =
          pre != null
              ? (sh.oso.connect.oracle.core.decode.LobFragment) pre
              : sh.oso.connect.oracle.core.decode.RowDecoder.decodeLob(d, schema);
    } catch (DecodeException ex) {
      if (settings.onDecodeError() == DecodeErrorAction.FAIL) {
        throw ex;
      }
      metrics.decodeFailures.incrementAndGet();
      sink.decodeFailed(d, ex);
      return;
    }
    for (RowChange ready : lobs.acceptLob(d.tx(), f, schema)) {
      buffer.add(d.tx(), ready);
    }
    metrics.lobRowsApplied.set(lobs.fragments());
  }

  /** Oldest unfinished work: open buffer entries and changes held by the LOB assembler. */
  private Optional<sh.oso.connect.oracle.core.model.RedoRecordId> oldestOpen() {
    Optional<sh.oso.connect.oracle.core.model.RedoRecordId> oldest = buffer.oldestFirstCaptured();
    Optional<sh.oso.connect.oracle.core.model.RedoRecordId> pending = lobs.oldestPending();
    if (pending.isPresent() && (oldest.isEmpty() || pending.get().compareTo(oldest.get()) < 0)) {
      return pending;
    }
    return oldest;
  }

  private void emit(CommittedTransaction tx) {
    if (settings.lobMode() == LobAssembler.Mode.RESELECT && reselector != null) {
      tx =
          ReselectingEvents.wrap(
              tx,
              schemas,
              reselector,
              classifier,
              settings.lobMaxBytes(),
              settings.lobOversizeFail());
    }
    int skip = SkipRule.eventsToSkip(start, tx);
    if (skip >= tx.size()) {
      metrics.transactionsSkipped.incrementAndGet();
      return;
    }
    metrics.transactionsCommitted.incrementAndGet();
    // the candidate at this commit: the commit row itself bounds the cursor side of the rule
    sh.oso.connect.oracle.core.model.RedoRecordId resume =
        ResumeCalculator.resume(new StepCursor(tx.commitScn(), tx.commitId(), false), oldestOpen());
    sink.committed(tx, skip, resume);
  }
}
