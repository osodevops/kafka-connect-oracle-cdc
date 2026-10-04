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
  private final LobInsertCoalescer coalescer = new LobInsertCoalescer();
  private final Supplier<Instant> clock;
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
    this.cursor = StepCursor.at(start.resumeScn());
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
      sink.idle(cursor.scn(), ResumeCalculator.resumeScn(cursor.scn(), oldestOpen()));
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
    apply(outcome);
    cursor = outcome.next();
    Duration elapsed = Duration.between(t0, clock.get());
    metrics.lastStepMillis.set(elapsed.toMillis());
    metrics.steps.incrementAndGet();
    metrics.minedToScn.set(cursor.scn());
    if (outcome.kind() == StepOutcome.Kind.CUT) {
      metrics.stepCuts.incrementAndGet();
      java.util.Set<String> owners = idRefresher.refresh();
      runner =
          new StepRunner(classifier, new sh.oso.connect.oracle.core.mining.step.DdlStepCut(owners));
    } else {
      scheduler.stepCompleted(elapsed);
    }
    sink.stepApplied(cursor.scn(), ResumeCalculator.resumeScn(cursor.scn(), oldestOpen()));
    return Progress.STEP_APPLIED;
  }

  private void refreshIds() throws SQLException {
    java.util.Set<String> owners = idRefresher.refresh();
    runner =
        new StepRunner(classifier, new sh.oso.connect.oracle.core.mining.step.DdlStepCut(owners));
    pendingRefresh = false;
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
        if (d.undo()) {
          coalescer.flush(d.tx()).ifPresent(held -> buffer.add(d.tx(), held));
          buffer.undo(d.tx(), d.id(), d.rowId());
        } else {
          decodeAndBuffer(d);
        }
      } else if (e instanceof MiningEvent.Commit c) {
        coalescer.flush(c.tx()).ifPresent(held -> buffer.add(c.tx(), held));
        Optional<CommittedTransaction> tx = buffer.commit(c);
        if (tx.isPresent()) {
          emit(tx.get());
        }
      } else if (e instanceof MiningEvent.Rollback r) {
        coalescer.discard(r.tx());
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

  private void decodeAndBuffer(MiningEvent.Dml d) throws SQLException {
    TableSchema schema = schemas.current(d.table());
    RowChange change;
    try {
      change = decoder.decode(d, schema);
    } catch (DecodeException ex) {
      if (settings.onDecodeError() == DecodeErrorAction.FAIL) {
        throw ex;
      }
      metrics.decodeFailures.incrementAndGet();
      sink.decodeFailed(d, ex);
      return;
    }
    java.util.Set<String> lobs = new java.util.HashSet<>();
    for (sh.oso.connect.oracle.core.schema.ColumnSpec col : schema.columns()) {
      if (col.type().isLob()) {
        lobs.add(col.name());
      }
    }
    for (RowChange ready : coalescer.accept(d.tx(), change, lobs)) {
      buffer.add(d.tx(), ready);
    }
    metrics.lobInsertsMerged.set(coalescer.merged());
  }

  /** Oldest unfinished work: open buffer entries and inserts held by the coalescer. */
  private Optional<sh.oso.connect.oracle.core.model.RedoRecordId> oldestOpen() {
    Optional<sh.oso.connect.oracle.core.model.RedoRecordId> oldest = buffer.oldestFirstCaptured();
    Optional<sh.oso.connect.oracle.core.model.RedoRecordId> pending = coalescer.oldestPending();
    if (pending.isPresent() && (oldest.isEmpty() || pending.get().compareTo(oldest.get()) < 0)) {
      return pending;
    }
    return oldest;
  }

  private void emit(CommittedTransaction tx) {
    int skip = SkipRule.eventsToSkip(start, tx);
    if (skip >= tx.size()) {
      metrics.transactionsSkipped.incrementAndGet();
      return;
    }
    metrics.transactionsCommitted.incrementAndGet();
    long resume = ResumeCalculator.resumeScn(tx.commitScn(), oldestOpen());
    sink.committed(tx, skip, resume);
  }
}
