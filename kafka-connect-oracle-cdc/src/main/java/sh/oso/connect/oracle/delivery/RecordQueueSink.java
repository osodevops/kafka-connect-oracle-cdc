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
  private final BlockingQueue<Queued> queue;
  private boolean eos;
  private long splitMaxRecords = Long.MAX_VALUE;
  private long splitMaxBytes = Long.MAX_VALUE;

  /**
   * A queued record. {@code boundary}: a Kafka transaction may end after it (it is not inside an
   * Oracle transaction); {@code force}: it must end there (a split, SRC-EOS-4); {@code bytes}: the
   * change data it carries, for cdc.eos.batch.max.bytes.
   */
  public record Queued(SourceRecord record, boolean boundary, boolean force, long bytes) {}

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

  private sh.oso.connect.oracle.schema.SchemaRecords.Writer schemaTopic;

  /** PRD-03: where schema versions go (the compacted schema topic). */
  public synchronized void schemaTopic(sh.oso.connect.oracle.schema.SchemaRecords.Writer w) {
    this.schemaTopic = w;
  }

  /** A table's versions as one schema topic record; between transactions, so a boundary. */
  public synchronized void schemaVersions(TableId table, List<TableSchema> versions) {
    if (schemaTopic != null) {
      put(
          schemaTopic.versions(
              table,
              versions,
              sh.oso.connect.oracle.core.position.PositionCodec.write(safePosition())));
    }
  }

  /** A tombstone for a table dropped or renamed away. */
  public synchronized void schemaRemoved(TableId table) {
    if (schemaTopic != null) {
      put(
          schemaTopic.removed(
              table, sh.oso.connect.oracle.core.position.PositionCodec.write(safePosition())));
    }
  }

  /** PRD-03: a captured table's schema version changed, or the table left under this name. */
  @Override
  public void schemaChanged(
      TableSchema schema, sh.oso.connect.oracle.core.mining.event.MiningEvent.Ddl d) {
    ops(
        OpsEvent.Type.DDL_APPLIED,
        "pdb",
        d.pdb(),
        "owner",
        d.owner(),
        "object",
        d.objectName(),
        "scn",
        Long.toString(d.scn()),
        "version",
        schema == null ? "removed" : Integer.toString(schema.version()),
        "columns",
        schema == null ? null : Integer.toString(schema.columns().size()));
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

  /** P1-17: a step mined again with a dictionary from the redo (PRD-03 lag case). */
  @Override
  public void dictionaryReplayed(long fromScn, long toScn, java.util.Set<TableId> tables) {
    java.util.TreeSet<String> names = new java.util.TreeSet<>();
    for (TableId t : tables) {
      names.add(t.fqn());
    }
    ops(
        OpsEvent.Type.DICTIONARY_REPLAY,
        "from_scn",
        Long.toString(fromScn),
        "to_scn",
        Long.toString(toScn),
        "tables",
        String.join(",", names));
  }

  @Override
  public void reconnected(String cause) {
    ops(OpsEvent.Type.RECONNECTED, "cause", cause);
  }

  private sh.oso.connect.oracle.core.snapshot.SnapshotCoordinator snapshot;
  private sh.oso.connect.oracle.core.snapshot.SnapshotProgress snapshotProgress;

  /**
   * PRD-02: chunks of {@code coordinator} are published here as streaming passes their SCN; {@code
   * progress} is the snapshot block the offsets carry from now on.
   */
  public synchronized void snapshot(
      sh.oso.connect.oracle.core.snapshot.SnapshotCoordinator coordinator,
      sh.oso.connect.oracle.core.snapshot.SnapshotProgress progress) {
    this.snapshot = coordinator;
    this.snapshotProgress = progress;
    this.snapshotStarted = !progress.untouched();
    carrySnapshot(); // the next offset records that this snapshot runs
  }

  /** A snapshot is running: started and not yet complete or stopped. */
  public synchronized boolean snapshotRunning() {
    return snapshot != null;
  }

  /** SRC-SIG-1 snapshot-stop: the running snapshot ends here, its remaining chunks unread. */
  public synchronized void snapshotStopped() {
    snapshot = null;
    snapshotProgress = snapshotProgress.completed();
    carrySnapshot();
    ops(OpsEvent.Type.SNAPSHOT_COMPLETE, "table", "*", "stopped", "true");
  }

  /**
   * SRC-SIG-3: the signal at {@code offset} of the signal topic is handled; every offset from here
   * on records it, so an acknowledged signal is never handled again.
   */
  public synchronized void signalProcessed(long offset) {
    base = base.withExtra(SIGNAL_OFFSET, offset);
    lastEmittedCommit = lastEmittedCommit.withExtra(SIGNAL_OFFSET, offset);
  }

  /** The key of the last handled signal's offset in the position's extras. */
  public static final String SIGNAL_OFFSET = "signal_offset";

  /** SRC-SEL-4: new tables waiting for their snapshot, comma-separated, in the extras. */
  public static final String SNAPSHOT_PENDING = "snapshot_pending";

  /** The tables waiting for a snapshot, as recorded in the offsets from here on. */
  public synchronized void pendingSnapshot(java.util.Collection<TableId> tables) {
    String value =
        tables.isEmpty()
            ? null
            : String.join(",", tables.stream().map(TableId::fqn).sorted().toList());
    base = base.withExtra(SNAPSHOT_PENDING, value);
    lastEmittedCommit = lastEmittedCommit.withExtra(SNAPSHOT_PENDING, value);
  }

  /** The first snapshot record has gone out (SNAP-8 marks it {@code first}). */
  private boolean snapshotStarted;

  /** SNAP-1 snapshot_only: publishes every ready chunk; there is no streaming to wait for. */
  public synchronized boolean publishSnapshot() {
    emitSnapshot(Long.MAX_VALUE);
    return snapshot == null;
  }

  /**
   * PRD-02 section 3 step 4: every chunk read as of an SCN at or below {@code scn} goes out now,
   * before any commit at or after its SCN. A chunk then holds the rows as they were at its SCN, and
   * every later change follows it. Records of a chunk carry the progress before it, the last one
   * the progress after it.
   */
  private void emitSnapshot(long scn) {
    if (snapshot == null) {
      return;
    }
    for (sh.oso.connect.oracle.core.snapshot.SnapshotCoordinator.Batch b : snapshot.ready(scn)) {
      long now = clock.getAsLong();
      for (sh.oso.connect.oracle.core.snapshot.SnapshotCoordinator.Chunk c : b.chunks()) {
        sh.oso.connect.oracle.core.snapshot.SnapshotProgress after =
            snapshotProgress.advance(b.table(), c.range().upper());
        List<sh.oso.connect.oracle.core.snapshot.SnapshotRow> rows = c.rows();
        for (int i = 0; i < rows.size(); i++) {
          boolean lastRow = i + 1 == rows.size();
          String marker =
              snapshotProgress.scoped()
                  ? "incremental"
                  : !snapshotStarted
                      ? "first"
                      : b.last() && lastRow && c == b.chunks().get(b.chunks().size() - 1)
                          ? "last"
                          : "true";
          snapshotStarted = true;
          Position at = safePosition().withSnapshot((lastRow ? after : snapshotProgress).toMap());
          SourceRecord rec =
              envelope.snapshotRecord(b.table(), rows.get(i), b.schema(), b.scn(), now, marker, at);
          put(new Queued(rec, lastRow, false, 0));
        }
        snapshotProgress = after;
        carrySnapshot();
        snapshotRows += rows.size();
        ops(
            OpsEvent.Type.SNAPSHOT_CHUNK_DONE,
            "table",
            b.table().fqn(),
            "scn",
            Long.toString(b.scn()),
            "rows",
            Integer.toString(rows.size()));
      }
      if (b.tableDone()) {
        snapshotProgress = snapshotProgress.advance(b.table(), null);
        carrySnapshot();
        ops(OpsEvent.Type.SNAPSHOT_COMPLETE, "table", b.table().fqn(), "skipped", b.skipped());
      }
      lastQueuedAt = now;
    }
    if (snapshot.finished()) {
      snapshotProgress = snapshotProgress.completed();
      carrySnapshot();
      ops(OpsEvent.Type.SNAPSHOT_COMPLETE, "table", "*");
      snapshot = null;
    }
  }

  /** Every offset from here on carries the snapshot progress. */
  private void carrySnapshot() {
    Map<String, Object> block = snapshotProgress.toMap();
    base = base.withSnapshot(block);
    lastEmittedCommit = lastEmittedCommit.withSnapshot(block);
  }

  private long snapshotRows;

  public synchronized long snapshotRows() {
    return snapshotRows;
  }

  @Override
  public synchronized void committed(
      CommittedTransaction tx, int skipped, RedoRecordId resumeCandidate) {
    emitSnapshot(tx.commitScn());
    if (tx.commitTimestamp() != null) {
      lastCommitTimestamp = tx.commitTimestamp().toEpochMilli();
      millisBehindSource = Math.max(0, clock.getAsLong() - lastCommitTimestamp);
    }
    Map<TableId, Long> perTable = new HashMap<>();
    // SRC-EOS-4: a transaction above the record limit is split; one above the byte limit is split
    // from the point it crosses it
    boolean split = eos && tx.size() - skipped > splitMaxRecords;
    long sinceRecords = 0;
    long sinceBytes = 0;
    int splits = 0;
    for (int i = 0; i < tx.size(); i++) {
      RowChange c = tx.events().get(i);
      long order = perTable.merge(c.table(), 1L, Long::sum);
      if (i < skipped) {
        continue;
      }
      TableSchema schema;
      try {
        // PRD-03: the version the row was decoded with, so a later DDL never hides its values
        schema = schemas.version(c.table(), c.schemaVersion());
      } catch (SQLException e) {
        throw new ConnectException("Reading the schema of " + c.table().fqn(), e);
      }
      // until the last record of the transaction is acknowledged, a restart must re-mine the
      // transaction itself, so its first capture bounds the resume point (CORE-POS-2, CORE-POS-3)
      RedoRecordId resume =
          i + 1 < tx.size() ? earlier(resumeCandidate, tx.firstCaptured()) : resumeCandidate;
      Position offset =
          base.withCommit(tx.commitId(), tx.thread(), tx.key(), i + 1).withResume(resume);
      List<SourceRecord> records = envelope.records(tx, i, order, schema, offset);
      long bytes = sh.oso.connect.oracle.core.buffer.SizeEstimate.of(c);
      sinceRecords++;
      sinceBytes += bytes;
      boolean last = i + 1 == tx.size();
      boolean cut =
          eos && !last && (sinceRecords >= splitMaxRecords || sinceBytes >= splitMaxBytes);
      if (cut) {
        split = true;
        splits++;
        sinceRecords = 0;
        sinceBytes = 0;
      }
      for (int r = 0; r < records.size(); r++) {
        SourceRecord rec = records.get(r);
        if (split) {
          rec.headers().addBoolean("cdc.split", true);
        }
        boolean end = r + 1 == records.size();
        put(new Queued(rec, end && (last || cut), end && cut, r == 0 ? bytes : 0));
      }
      lastQueuedAt = clock.getAsLong();
    }
    lastEmittedCommit =
        base.withCommit(tx.commitId(), tx.thread(), tx.key(), tx.size())
            .withResume(resumeCandidate);
    if (splits > 0) {
      ops(
          OpsEvent.Type.TRANSACTION_SPLIT,
          "xid",
          tx.key().xid().toString(),
          "commit_scn",
          Long.toString(tx.commitScn()),
          "events",
          Integer.toString(tx.size()),
          "kafka_transactions",
          Integer.toString(splits + 1));
    }
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

  /** SRC-EOS-4: split Oracle transactions above these sizes into several Kafka transactions. */
  public synchronized void exactlyOnce(long maxRecords, long maxBytes) {
    this.eos = true;
    this.splitMaxRecords = maxRecords;
    this.splitMaxBytes = maxBytes;
  }

  private void put(SourceRecord r) {
    put(new Queued(r, true, false, 0));
  }

  private void put(Queued q) {
    try {
      queue.put(q);
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
    emitSnapshot(minedToScn);
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
    for (Queued q : drainQueued(max, lingerMs)) {
      out.add(q.record());
    }
    return out;
  }

  /** As {@link #drain}, with each record's transaction boundary marks (SRC-EOS-2). */
  public List<Queued> drainQueued(int max, long lingerMs) throws InterruptedException {
    List<Queued> out = new java.util.ArrayList<>();
    Queued first = queue.poll(lingerMs, TimeUnit.MILLISECONDS);
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
