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
package sh.oso.connect.oracle.delivery;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.engine.EventSink;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.schema.SchemaRegistry;
import sh.oso.connect.oracle.core.schema.TableSchema;
import sh.oso.connect.oracle.envelope.DebeziumEnvelope;
import sh.oso.connect.oracle.heartbeat.HeartbeatEmitter;
import sh.oso.connect.oracle.ops.OpsEvent;
import sh.oso.connect.oracle.ops.OpsEventWriter;

/**
 * The engine's sink on the Connect side: committed transactions become records on a bounded queue
 * that poll() drains. Each record carries the position a restart may use once it is acknowledged:
 * the transaction's resume candidate, its commit and the index after this event (CORE-POS-3). The
 * queue bound applies back-pressure to the engine thread. Heartbeats make the position durable at
 * start and on quiet periods (SRC-HB-1); their position names the last emitted commit so a restart
 * from a heartbeat offset skips nothing and repeats nothing.
 */
public final class RecordQueueSink
    implements EventSink,
        sh.oso.connect.oracle.core.buffer.JournalSink,
        sh.oso.connect.oracle.core.metrics.SinkMetrics {

  private final DebeziumEnvelope envelope;
  private final SchemaRegistry schemas;
  private Position base;
  private final BlockingQueue<SourceRecord> queue;
  private final HeartbeatEmitter heartbeats;
  private final OpsEventWriter ops;
  private final sh.oso.connect.oracle.journal.JournalRecords journal;
  private final sh.oso.connect.oracle.dlq.DecodeDlqWriter dlq;
  private final long heartbeatIntervalMs;
  private final LongSupplier clock;
  private volatile long lastMinedTo;
  private volatile RedoRecordId lastResumeCandidate;
  private long lastQueuedAt;
  private long lastHeartbeatAt;
  private Position lastEmittedCommit;
  private long heartbeatsSent;
  private long opsEvents;
  private long journalChunks;
  private long journalTombstones;
  private long dlqRecords;
  private long lastCommitTimestamp = -1;
  private long millisBehindSource = -1;

  public RecordQueueSink(
      DebeziumEnvelope envelope,
      SchemaRegistry schemas,
      Position base,
      int capacity,
      HeartbeatEmitter heartbeats,
      OpsEventWriter ops,
      sh.oso.connect.oracle.journal.JournalRecords journal,
      sh.oso.connect.oracle.dlq.DecodeDlqWriter dlq,
      long heartbeatIntervalMs,
      LongSupplier clock) {
    this.envelope = envelope;
    this.schemas = schemas;
    this.base = base;
    this.queue = new LinkedBlockingQueue<>(capacity);
    this.heartbeats = heartbeats;
    this.ops = ops;
    this.journal = journal;
    this.dlq = dlq;
    this.heartbeatIntervalMs = heartbeatIntervalMs;
    this.clock = clock;
    this.lastEmittedCommit = base;
    this.lastResumeCandidate = base.resumePoint();
    this.lastMinedTo = base.resumeScn();
  }

  /** The start heartbeat: makes the start position durable before any change record. */
  public void heartbeatAtStart() {
    heartbeat(base, null, "start");
  }

  /**
   * Publishes an ops event. Its offset is the quiet-heartbeat position: the last emitted commit
   * with the engine's current resume candidate, which every record queued before it allows.
   */
  public synchronized void ops(OpsEvent.Type type, String... details) {
    long now = clock.getAsLong();
    Position at = lastEmittedCommit.withResume(lastResumeCandidate);
    put(ops.record(OpsEvent.of(type, now, at.resumeScn(), details), at));
    opsEvents++;
  }

  /** The position every record queued so far allows: the quiet-heartbeat rule. */
  private Position safePosition() {
    return lastEmittedCommit.withResume(lastResumeCandidate);
  }

  /**
   * ADR-0003: a journal chunk rides the queue like any record. Its offset is the position before
   * this chunk counted, so the resume SCN passes the chunk only through a later record, which
   * Connect commits after this one was acknowledged.
   */
  @Override
  public synchronized void chunk(sh.oso.connect.oracle.core.buffer.JournalChunk c) {
    put(journal.chunk(c, safePosition()));
    journalChunks++;
  }

  @Override
  public synchronized void tombstones(
      TxKey key, List<sh.oso.connect.oracle.core.buffer.JournalChunk.Ref> chunks) {
    for (sh.oso.connect.oracle.core.buffer.JournalChunk.Ref ref : chunks) {
      put(
          journal.tombstone(
              new sh.oso.connect.oracle.journal.JournalRecords.ChunkKey(
                  key, ref.chunk(), ref.generation()),
              safePosition()));
    }
    journalTombstones += chunks.size();
  }

  /** Tombstones for stale chunks found at start (ADR-0003: unacknowledged or re-mined writes). */
  public synchronized void tombstone(sh.oso.connect.oracle.journal.JournalRecords.ChunkKey k) {
    put(journal.tombstone(k, safePosition()));
    journalTombstones++;
  }

  /**
   * CORE-TX-7: the released ledger travels in every later offset so a restart still refuses a late
   * COMMIT for the released transaction; the ops topic carries the full details.
   */
  @Override
  public synchronized void orphanReleased(
      sh.oso.connect.oracle.core.orphan.OrphanDetector.Release r, List<String> released) {
    base = base.withReleased(released);
    lastEmittedCommit = lastEmittedCommit.withReleased(released);
    ops(
        OpsEvent.Type.TRANSACTION_ORPHAN_RELEASED,
        "xid",
        r.tx().key().xid().toString(),
        "con_id",
        Integer.toString(r.tx().key().srcConId()),
        "user",
        r.tx().username(),
        "client_id",
        r.tx().clientId(),
        "first_scn",
        Long.toString(r.tx().firstScn()),
        "last_scn",
        Long.toString(r.tx().lastScn()),
        "events",
        Integer.toString(r.tx().events()),
        "absent_at_scn",
        Long.toString(r.absentAtScn()),
        "reason",
        r.reason());
  }

  /** CORE-TX-6: the discard is recorded on the ops topic and the DLQ, and the ledger travels on. */
  @Override
  public synchronized void transactionDiscarded(
      sh.oso.connect.oracle.core.buffer.TransactionBuffer.OpenTransaction t,
      java.time.Duration age,
      List<String> released) {
    base = base.withReleased(released);
    lastEmittedCommit = lastEmittedCommit.withReleased(released);
    if (dlq != null) {
      put(dlq.discarded(t, age, safePosition(), clock.getAsLong()));
      dlqRecords++;
    }
    ops(
        OpsEvent.Type.TRANSACTION_DISCARDED,
        "xid",
        t.key().xid().toString(),
        "con_id",
        Integer.toString(t.key().srcConId()),
        "user",
        t.username(),
        "first_scn",
        Long.toString(t.firstScn()),
        "last_scn",
        Long.toString(t.lastScn()),
        "events",
        Integer.toString(t.events()),
        "age_ms",
        Long.toString(age.toMillis()));
  }

  /** The stop event (SRC-OPS): exception class, error code, runbook link and operator action. */
  public void stopped(Throwable t) {
    String code = null;
    String runbook = null;
    String action = null;
    if (t instanceof sh.oso.connect.oracle.core.errors.OracleCdcException oe) {
      code = oe.code().code();
      runbook = oe.runbookUrl();
      action = oe.operatorAction();
    }
    ops(
        OpsEvent.Type.STOP,
        "exception",
        t.getClass().getName(),
        "message",
        t.getMessage(),
        "code",
        code,
        "runbook",
        runbook,
        "operator_action",
        action);
  }

  @Override
  public void ddl(sh.oso.connect.oracle.core.mining.event.MiningEvent.Ddl d) {
    ops(
        OpsEvent.Type.DDL_SEEN,
        "pdb",
        d.pdb(),
        "owner",
        d.owner(),
        "object",
        d.objectName(),
        "scn",
        Long.toString(d.scn()),
        "sql",
        d.sql() == null ? null : d.sql().length() > 2000 ? d.sql().substring(0, 2000) : d.sql());
  }

  @Override
  public void decodeFailed(
      sh.oso.connect.oracle.core.mining.event.MiningEvent.Dml dml,
      sh.oso.connect.oracle.core.errors.DecodeException cause) {
    if (dlq != null) {
      synchronized (this) {
        Position at = safePosition();
        put(dlq.decodeError(dml, cause, at.schemaEpoch(), at, clock.getAsLong()));
        dlqRecords++;
      }
    }
    ops(
        OpsEvent.Type.DECODE_ERROR_DLQ,
        "table",
        dml.table().fqn(),
        "xid",
        dml.tx().xid().toString(),
        "scn",
        Long.toString(dml.scn()),
        "operation",
        dml.op().name(),
        "error",
        cause.getMessage());
  }

  @Override
  public void unsupported(sh.oso.connect.oracle.core.mining.event.MiningEvent.Unsupported u) {
    if (dlq != null) {
      synchronized (this) {
        put(dlq.unsupported(u, safePosition(), clock.getAsLong()));
        dlqRecords++;
      }
    }
    ops(
        OpsEvent.Type.UNSUPPORTED_ROW,
        "table",
        u.table().fqn(),
        "xid",
        u.tx().xid().toString(),
        "scn",
        Long.toString(u.scn()),
        "status",
        Integer.toString(u.status()),
        "info",
        u.info());
  }

  @Override
  public void idsRefreshed(java.util.Set<String> owners) {
    ops(OpsEvent.Type.IDS_REFRESHED, "owners", String.join(",", new java.util.TreeSet<>(owners)));
  }

  @Override
  public void reconnected(String cause) {
    ops(OpsEvent.Type.RECONNECTED, "cause", cause);
  }

  @Override
  public synchronized void committed(
      CommittedTransaction tx, int skipped, RedoRecordId resumeCandidate) {
    if (tx.commitTimestamp() != null) {
      lastCommitTimestamp = tx.commitTimestamp().toEpochMilli();
      millisBehindSource = Math.max(0, clock.getAsLong() - lastCommitTimestamp);
    }
    Map<TableId, Long> perTable = new HashMap<>();
    for (int i = 0; i < tx.size(); i++) {
      RowChange c = tx.events().get(i);
      long order = perTable.merge(c.table(), 1L, Long::sum);
      if (i < skipped) {
        continue;
      }
      TableSchema schema;
      try {
        schema = schemas.current(c.table());
      } catch (SQLException e) {
        throw new ConnectException("Reading the schema of " + c.table().fqn(), e);
      }
      // until the last record of the transaction is acknowledged, a restart must re-mine the
      // transaction itself, so its first capture bounds the resume point (CORE-POS-2, CORE-POS-3)
      RedoRecordId resume =
          i + 1 < tx.size() ? earlier(resumeCandidate, tx.firstCaptured()) : resumeCandidate;
      Position offset =
          base.withCommit(tx.commitId(), tx.thread(), tx.key(), i + 1).withResume(resume);
      for (SourceRecord r : envelope.records(tx, i, order, schema, offset)) {
        put(r);
      }
      lastQueuedAt = clock.getAsLong();
    }
    lastEmittedCommit =
        base.withCommit(tx.commitId(), tx.thread(), tx.key(), tx.size())
            .withResume(resumeCandidate);
  }

  /** The earlier of two resume points in redo order, with the lower SCN as its floor. */
  static RedoRecordId earlier(RedoRecordId a, RedoRecordId b) {
    long scn = Math.min(a.scn(), b.scn());
    if (!a.hasRba() || !b.hasRba()) {
      return a.scn() <= b.scn()
          ? new RedoRecordId(scn, a.rsId(), a.ssn())
          : new RedoRecordId(scn, b.rsId(), b.ssn());
    }
    return a.compareTo(b) <= 0
        ? new RedoRecordId(scn, a.rsId(), a.ssn())
        : new RedoRecordId(scn, b.rsId(), b.ssn());
  }

  private void put(SourceRecord r) {
    try {
      queue.put(r);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new ConnectException("interrupted while queueing records", e);
    }
  }

  @Override
  public void idle(long minedToScn, RedoRecordId resumeCandidate) {
    stepApplied(minedToScn, resumeCandidate);
  }

  @Override
  public synchronized void stepApplied(long minedToScn, RedoRecordId resumeCandidate) {
    lastMinedTo = minedToScn;
    lastResumeCandidate = resumeCandidate;
    if (heartbeatIntervalMs <= 0) {
      return;
    }
    long now = clock.getAsLong();
    boolean quiet = now - lastQueuedAt >= heartbeatIntervalMs;
    if (quiet && now - lastHeartbeatAt >= heartbeatIntervalMs) {
      // every emitted record precedes this heartbeat in the queue, so Connect commits this offset
      // only after those records; resume never passes an open transaction (the candidate)
      heartbeat(lastEmittedCommit.withResume(resumeCandidate), minedToScn, "quiet");
    }
  }

  private synchronized void heartbeat(Position position, Long minedTo, String reason) {
    long now = clock.getAsLong();
    put(heartbeats.record(position, minedTo, reason, now));
    lastHeartbeatAt = now;
    heartbeatsSent++;
  }

  /** Drains up to {@code max} records, waiting up to {@code lingerMs} for the first. */
  public List<SourceRecord> drain(int max, long lingerMs) throws InterruptedException {
    List<SourceRecord> out = new java.util.ArrayList<>();
    SourceRecord first = queue.poll(lingerMs, TimeUnit.MILLISECONDS);
    if (first == null) {
      return out;
    }
    out.add(first);
    queue.drainTo(out, max - 1);
    return out;
  }

  public int queued() {
    return queue.size();
  }

  public long lastMinedTo() {
    return lastMinedTo;
  }

  public long lastResumeCandidate() {
    return lastResumeCandidate.scn();
  }

  /** The resume point every record queued so far allows, with its redo byte address. */
  public RedoRecordId lastResumePoint() {
    return lastResumeCandidate;
  }

  public synchronized long heartbeatsSent() {
    return heartbeatsSent;
  }

  public synchronized long opsEvents() {
    return opsEvents;
  }

  public synchronized long journalChunks() {
    return journalChunks;
  }

  public synchronized long journalTombstones() {
    return journalTombstones;
  }

  @Override
  public synchronized long lastCommitTimestampMillis() {
    return lastCommitTimestamp;
  }

  @Override
  public synchronized long millisBehindSource() {
    return millisBehindSource;
  }

  public synchronized long dlqRecords() {
    return dlqRecords;
  }
}
