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
import sh.oso.connect.oracle.core.schema.ColumnFilter;
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
  private volatile boolean pendingRefresh;
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
  private final PartialXidResolver partialXids = new PartialXidResolver();
  private FirstStartScope firstStart;
  private final EngineMetrics metrics = new EngineMetrics();
  private final LobAssembler lobs;
  private LobReselector reselector;
  private ColumnFilter excluded = ColumnFilter.none();
  private java.util.function.Predicate<sh.oso.connect.oracle.core.model.TableId> captured =
      t -> true;
  private volatile int decodeThreads = 1; // set before start, read on the engine thread
  private java.util.concurrent.ForkJoinPool decodePool;
  private java.util.Map<MiningEvent.Dml, Object> decoded = java.util.Map.of();

  /** P1-17: tables whose rows in the current step were mined with a redo dictionary. */
  private java.util.Set<sh.oso.connect.oracle.core.model.TableId> replayed = java.util.Set.of();

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
    this.firstStart = FirstStartScope.of(start);
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

  /** SCH-5: which tables are captured; DDL on the others is ignored without classification. */
  public CaptureEngine withCapturedTables(
      java.util.function.Predicate<sh.oso.connect.oracle.core.model.TableId> captured) {
    this.captured = captured;
    return this;
  }

  /**
   * SRC-SEL-2: columns kept out of every change. They are dropped while decoding, before their
   * values are converted, so the buffer, the spill files and the journal never hold them; a table
   * whose key would lose a column stops the task (CDC-3001).
   */
  public CaptureEngine withColumnFilter(ColumnFilter excluded) {
    this.excluded = java.util.Objects.requireNonNull(excluded, "excluded");
    lobs.excluding(excluded);
    return this;
  }

  /**
   * ADR-0016 amendment, before the first step: stores version 1 of every table in {@code tables}
   * that has no stored version, so a DDL soon after the start, or during an initial snapshot that
   * holds streaming back for hours, cannot leave the rows written before it without a version. A
   * version is valid from the start position's resume SCN when no DDL on its table can lie after
   * that SCN ({@link SchemaRegistry#readLayouts}).
   *
   * <p>On a first start that mines from before its start SCN (ADR-0019) the resume SCN is the
   * mining start, and the check is made at the SCN read before the open transactions were listed.
   * Every row decoded below it belongs to a transaction that holds its table's lock across it, and
   * no DDL on a table completes while a transaction holds that lock; transactions that ended
   * earlier are dropped before they are decoded. So those rows were written with the layout read
   * here, and the version can be valid from the mining start: valid from the start SCN, it would
   * not cover them.
   */
  public java.util.List<TableSchema> readStartLayouts(
      java.util.Collection<sh.oso.connect.oracle.core.model.TableId> tables) throws SQLException {
    long from = start.resumeScn();
    long quiet = start.beforeFirstStart(from) ? Math.max(from, start.startOpenScn()) : from;
    return schemas.readLayouts(tables, from, quiet);
  }

  /**
   * SRC-SEL-4, on the engine thread while the object ids are refreshed: stores version 1 of tables
   * that joined the captured set, valid from the cursor when no DDL on them can lie after it. A
   * table that joined through its own CREATE or RENAME has that DDL as its last, too close to the
   * cursor to tell apart, so it keeps the first-read rule: valid from ten seconds after the DDL.
   */
  public java.util.List<TableSchema> readJoinedLayouts(
      java.util.Collection<sh.oso.connect.oracle.core.model.TableId> tables) throws SQLException {
    return schemas.readLayouts(tables, cursor.scn(), cursor.scn());
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

  private final java.util.concurrent.ConcurrentLinkedQueue<Runnable> actions =
      new java.util.concurrent.ConcurrentLinkedQueue<>();

  /**
   * Runs {@code action} on the engine thread before the next step, so it may use the engine's
   * metadata connection and buffer (SRC-SIG-1 signals). An action handles its own errors.
   */
  public void submit(Runnable action) {
    actions.add(action);
  }

  private volatile MiningEvent.Ddl refreshCause;

  /**
   * During a refresh of the object ids after a DDL step cut, the DDL that caused it; null during a
   * refresh asked for by a signal or outside a refresh (SRC-SEL-4).
   */
  public MiningEvent.Ddl refreshCause() {
    return refreshCause;
  }

  /** SRC-SIG-1 refresh-tables: the object ids are re-resolved before the next step. */
  public void requestRefresh() {
    pendingRefresh = true;
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
    for (Runnable a = actions.poll(); a != null; a = actions.poll()) {
      a.run();
    }
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
      return idle();
    }
    if (recycler.due()) {
      source.recycle();
      recycler.recycled();
      metrics.sessionRecycles.incrementAndGet();
    }
    LogSet logs = inventory.forRange(cursor.scn(), end);
    if (logs.endScn() < end) {
      end = logs.endScn(); // a log switch is under way: mine to the newest listed log's end
      if (end <= cursor.scn()) {
        return idle();
      }
    }
    StepPlan plan = scheduler.plan(cursor.scn(), end, logs.logs());
    metrics.windowLogs.set(plan.windowLogs());
    Instant t0 = clock.get();
    StepOutcome outcome = scoped(runner.run(source, cursor, plan.endScn()));
    java.util.Set<sh.oso.connect.oracle.core.model.TableId> lag = lagTables(outcome);
    if (!lag.isEmpty()) {
      outcome = scoped(replay(plan.endScn(), lag));
    }
    if (firstStart != null
        && outcome.applies()
        && outcome.next() != null
        && outcome.next().scn() >= firstStart.floor()) {
      firstStart = null; // every later event is at or above the floor
    }
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
    try {
      warmSchemas(outcome); // every database read happens before the buffer changes
      decoded = preDecode(outcome);
      apply(outcome);
    } finally {
      decoded = java.util.Map.of();
      replayed = java.util.Set.of();
    }
    cursor = outcome.next();
    Duration elapsed = Duration.between(t0, clock.get());
    metrics.lastStepMillis.set(elapsed.toMillis());
    metrics.steps.incrementAndGet();
    metrics.minedToScn.set(cursor.scn());
    if (outcome.kind() == StepOutcome.Kind.CUT) {
      metrics.stepCuts.incrementAndGet();
      java.util.List<MiningEvent> events = outcome.events();
      refreshCause =
          events.isEmpty() || !(events.get(events.size() - 1) instanceof MiningEvent.Ddl d)
              ? null
              : d;
      try {
        refreshIds();
      } finally {
        refreshCause = null;
      }
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

  /**
   * P1-17, ADR-0008: DML rows LogMiner could not map to today's dictionary (STATUS 2 with generic
   * {@code "COL n"} names) were written before a later DDL on their table. LOB rows are STATUS 2
   * for another reason (ADR-0015) and keep their names.
   */
  static java.util.Set<sh.oso.connect.oracle.core.model.TableId> lagTables(StepOutcome outcome) {
    java.util.Set<sh.oso.connect.oracle.core.model.TableId> out = new java.util.LinkedHashSet<>();
    for (MiningEvent e : outcome.events()) {
      if (e instanceof MiningEvent.Dml d
          && !d.undo()
          && d.status() == 2
          && !d.op().isLobOp()
          && d.op() != sh.oso.connect.oracle.core.model.Operation.SELECT_LOB_LOCATOR
          && d.sqlRedo() != null
          && d.sqlRedo().contains("\"COL ")) {
        out.add(d.table());
      }
    }
    return out;
  }

  /**
   * PRD-03 section 3 step 4: mines the step again with the dictionary from the newest build in the
   * redo, with DDL tracking, so its rows carry the names of their moment; the next step returns to
   * the online catalog. Rows of the lag tables decode with the version valid at their SCN ({@link
   * SchemaRegistry#at}), and a DDL inside the step adds its version before the rows after it.
   */
  /**
   * Rows with a partial XID go to the transaction open in their undo slot first; then, by ADR-0019,
   * a step below the first-start floor keeps only transactions open at the start.
   */
  private StepOutcome scoped(StepOutcome outcome) {
    StepOutcome resolved =
        partialXids.resolve(
            outcome, buffer.open().stream().map(TransactionBuffer.OpenTransaction::key).toList());
    return firstStart == null ? resolved : firstStart.filter(resolved);
  }

  /** Nothing to mine yet: journaling, age and orphan checks, and the idle position. */
  private Progress idle() throws SQLException {
    metrics.idlePolls.incrementAndGet();
    buffer.flushJournal(clock.get()); // age-based journaling must not wait for new redo
    enforceTransactionAge();
    checkOrphans();
    sink.idle(cursor.scn(), ResumeCalculator.resume(cursor, oldestOpen()));
    publishBuffer();
    return Progress.IDLE;
  }

  private StepOutcome replay(
      long endScn, java.util.Set<sh.oso.connect.oracle.core.model.TableId> lag) {
    metrics.lagReplays.incrementAndGet();
    StepOutcome again;
    try {
      again = runner.run(source.redoDictionary(), cursor, endScn);
    } catch (sh.oso.connect.oracle.core.errors.OracleCdcPurgedException e) {
      // the online pass has just read the step's own logs: what is missing lies between the
      // dictionary build and the step
      throw new sh.oso.connect.oracle.core.errors.DictionaryUnavailableException(
          "The logs from the last dictionary build up to SCN "
              + cursor.scn()
              + " cannot all be read: "
              + e.getMessage(),
          sh.oso.connect.oracle.core.mining.event.LogMinerEventSource.DICTIONARY_ACTION,
          e);
    }
    if (again.kind() == StepOutcome.Kind.COMPLETE || again.kind() == StepOutcome.Kind.CUT) {
      java.util.Set<sh.oso.connect.oracle.core.model.TableId> still = lagTables(again);
      if (!still.isEmpty()) {
        throw new sh.oso.connect.oracle.core.errors.DictionaryUnavailableException(
            "Rows of "
                + still
                + " after SCN "
                + cursor.scn()
                + " still have generic column names with the dictionary from the redo.",
            sh.oso.connect.oracle.core.mining.event.LogMinerEventSource.DICTIONARY_ACTION);
      }
      replayed = lag;
      sink.dictionaryReplayed(cursor.scn(), again.next().scn(), lag);
    }
    return again;
  }

  /** The version a row decodes with: today's, or for a replayed row the one valid at its SCN. */
  private TableSchema schemaFor(MiningEvent.Dml d) throws SQLException {
    TableSchema schema =
        replayed.contains(d.table()) ? schemas.at(d.table(), d.scn()) : schemas.current(d.table());
    excluded.project(schema); // SRC-SEL-2: the version's key must keep every column
    return schema;
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
        // the dictionary reads; a replayed row's version is chosen in redo order, after any DDL
        // of the step before it has been applied. SRC-SEL-2: a key column cannot be excluded
        excluded.project(schemas.current(d.table()));
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
          lobs.flush(d.tx()).ifPresent(held -> add(d.tx(), held));
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
        lobs.flush(c.tx()).ifPresent(held -> add(c.tx(), held));
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
        sink.ddl(d);
        applyDdl(d);
      } else if (e instanceof MiningEvent.Unsupported u) {
        if (settings.onDecodeError() == DecodeErrorAction.FAIL) {
          throw new DecodeException(
              "LogMiner marked a row of "
                  + u.table().fqn()
                  + " (object "
                  + u.dataObj()
                  + ", transaction "
                  + u.tx()
                  + ") UNSUPPORTED at "
                  + u.id()
                  + (u.info() == null ? "" : " (" + u.info() + ")"),
              "The table has a column type LogMiner cannot reconstruct (DOC-5); exclude the table"
                  + " or set cdc.on.decode.error=dlq.");
        }
        metrics.decodeFailures.incrementAndGet();
        sink.unsupported(u);
      } else if (e instanceof MiningEvent.Other o
          && PartialXidResolver.PARTIAL_ROLLBACK.equals(o.operation())) {
        partialRollback(o);
      }
      // MissingScn never reaches here (the runner stops); other Other rows and LogBoundary are
      // ignored
    }
  }

  /**
   * ADR-0022: a ROLLBACK row with the partial XID ends its transaction only when every buffered
   * change of it has been undone, as the undo rows of a full rollback leave it; otherwise it closed
   * a rollback to a savepoint and the transaction carries on. A transaction that really rolled back
   * with changes still buffered (an undo row that matched nothing) waits for the orphan check,
   * which releases it only once the database no longer has it open. A spilled transaction resolves
   * its undo rows at commit, so it is left alone.
   */
  private void partialRollback(MiningEvent.Other o) {
    boolean emptied =
        buffer.open().stream()
            .filter(t -> t.key().equals(o.tx()))
            .anyMatch(t -> t.events() == 0 && t.spilledBytes() == 0);
    if (emptied) {
      lobs.discard(o.tx());
      buffer.rollback(new MiningEvent.Rollback(o.tx(), o.id(), 0, null));
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
      tables[i] = schemaFor(rows.get(i));
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
          ? sh.oso.connect.oracle.core.decode.RowDecoder.decodeLob(d, schema, excluded)
          : decoder.decode(d, schema, excluded);
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
    TableSchema schema = schemaFor(d);
    RowChange change;
    try {
      Object pre = predecoded(d);
      change = pre != null ? (RowChange) pre : decoder.decode(d, schema, excluded);
    } catch (DecodeException ex) {
      if (settings.onDecodeError() == DecodeErrorAction.FAIL) {
        throw ex;
      }
      metrics.decodeFailures.incrementAndGet();
      sink.decodeFailed(d, ex);
      return;
    }
    for (RowChange ready : lobs.accept(d.tx(), change, schema)) {
      add(d.tx(), ready);
    }
    metrics.lobInsertsMerged.set(lobs.merged());
  }

  /**
   * Every change enters the buffer through here. SRC-SEL-2: the decoder has already left the
   * excluded columns out; the projection is the guard for a substitute decoder.
   */
  private void add(sh.oso.connect.oracle.core.model.TxKey tx, RowChange change) {
    buffer.add(tx, excluded.project(change));
  }

  /** CORE-DEC-6: a LOB_WRITE, LOB_TRIM or LOB_ERASE row joins the change of its statement. */
  private void decodeLobAndBuffer(MiningEvent.Dml d) throws SQLException {
    TableSchema schema = schemaFor(d);
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
              : sh.oso.connect.oracle.core.decode.RowDecoder.decodeLob(d, schema, excluded);
    } catch (DecodeException ex) {
      if (settings.onDecodeError() == DecodeErrorAction.FAIL) {
        throw ex;
      }
      metrics.decodeFailures.incrementAndGet();
      sink.decodeFailed(d, ex);
      return;
    }
    for (RowChange ready : lobs.acceptLob(d.tx(), f, schema)) {
      add(d.tx(), ready);
    }
    metrics.lobRowsApplied.set(lobs.fragments());
  }

  /**
   * PRD-03 section 3: a DDL on a captured table is classified; a structural change stores a new
   * schema version effective from the DDL's SCN, read from the dictionary, so the rows after it
   * decode with the new layout; an unknown statement stops the task (SCH-5).
   */
  private void applyDdl(MiningEvent.Ddl d) throws SQLException {
    if (d.objectName() == null || d.owner() == null) {
      return;
    }
    sh.oso.connect.oracle.core.model.TableId table =
        new sh.oso.connect.oracle.core.model.TableId(d.pdb(), d.owner(), d.objectName());
    if (!captured.test(table) && !schemas.known(table)) {
      return;
    }
    sh.oso.connect.oracle.core.schema.DdlClassifier.Kind kind =
        sh.oso.connect.oracle.core.schema.DdlClassifier.classify(d.sql());
    switch (kind) {
      case CREATE_TABLE:
      case COLUMNS:
      case CONSTRAINTS:
      case SUPPLEMENTAL_LOG:
        Optional<TableSchema> next = schemas.applyDdl(table, d.scn());
        if (next.isPresent()) {
          excluded.project(
              next.get()); // SRC-SEL-2: a DDL that moves the key onto an excluded column
          sink.schemaChanged(next.get(), d);
        }
        break;
      case RENAME_TABLE:
      case DROP_TABLE:
        schemas.forget(table);
        sink.schemaChanged(null, d);
        break;
      case UNKNOWN:
        throw new sh.oso.connect.oracle.core.errors.UnsupportedDdlException(
            "DDL on captured table "
                + table.fqn()
                + " at SCN "
                + d.scn()
                + " could not be classified: "
                + abbreviate(d.sql()),
            "Report the statement. To continue, exclude the table, or resnapshot it after"
                + " resetting the offsets past this SCN; the connector never guesses a layout.");
      default:
        break; // truncate, partition maintenance, indexes, comments: the layout is unchanged
    }
  }

  private static String abbreviate(String s) {
    String one = s == null ? "" : s.replaceAll("\\s+", " ").trim();
    return one.length() <= 300 ? one : one.substring(0, 300) + " ...";
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
              excluded,
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
