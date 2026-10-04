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
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.schema.SchemaRegistry;
import sh.oso.connect.oracle.core.schema.TableSchema;
import sh.oso.connect.oracle.envelope.DebeziumEnvelope;
import sh.oso.connect.oracle.heartbeat.HeartbeatEmitter;

/**
 * The engine's sink on the Connect side: committed transactions become records on a bounded queue
 * that poll() drains. Each record carries the position a restart may use once it is acknowledged:
 * the transaction's resume candidate, its commit and the index after this event (CORE-POS-3). The
 * queue bound applies back-pressure to the engine thread. Heartbeats make the position durable at
 * start and on quiet periods (SRC-HB-1); their position names the last emitted commit so a restart
 * from a heartbeat offset skips nothing and repeats nothing.
 */
public final class RecordQueueSink implements EventSink {

  private final DebeziumEnvelope envelope;
  private final SchemaRegistry schemas;
  private final Position base;
  private final BlockingQueue<SourceRecord> queue;
  private final HeartbeatEmitter heartbeats;
  private final long heartbeatIntervalMs;
  private final LongSupplier clock;
  private volatile long lastMinedTo;
  private volatile long lastResumeCandidate;
  private long lastQueuedAt;
  private long lastHeartbeatAt;
  private Position lastEmittedCommit;
  private long heartbeatsSent;

  public RecordQueueSink(
      DebeziumEnvelope envelope,
      SchemaRegistry schemas,
      Position base,
      int capacity,
      HeartbeatEmitter heartbeats,
      long heartbeatIntervalMs,
      LongSupplier clock) {
    this.envelope = envelope;
    this.schemas = schemas;
    this.base = base;
    this.queue = new LinkedBlockingQueue<>(capacity);
    this.heartbeats = heartbeats;
    this.heartbeatIntervalMs = heartbeatIntervalMs;
    this.clock = clock;
    this.lastEmittedCommit = base;
    this.lastResumeCandidate = base.resumeScn();
    this.lastMinedTo = base.resumeScn();
  }

  /** The start heartbeat: makes the start position durable before any change record. */
  public void heartbeatAtStart() {
    heartbeat(base, null, "start");
  }

  @Override
  public synchronized void committed(CommittedTransaction tx, int skipped, long resumeCandidate) {
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
      // transaction itself, so its first capture bounds the resume SCN (CORE-POS-2, CORE-POS-3)
      long resume =
          i + 1 < tx.size() ? Math.min(resumeCandidate, tx.firstCaptured().scn()) : resumeCandidate;
      Position offset =
          base.withCommit(tx.commitScn(), tx.thread(), tx.key(), i + 1).withResumeScn(resume);
      for (SourceRecord r : envelope.records(tx, i, order, schema, offset)) {
        put(r);
      }
      lastQueuedAt = clock.getAsLong();
    }
    lastEmittedCommit =
        base.withCommit(tx.commitScn(), tx.thread(), tx.key(), tx.size())
            .withResumeScn(resumeCandidate);
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
  public void idle(long minedToScn, long resumeCandidate) {
    stepApplied(minedToScn, resumeCandidate);
  }

  @Override
  public synchronized void stepApplied(long minedToScn, long resumeCandidate) {
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
      heartbeat(lastEmittedCommit.withResumeScn(resumeCandidate), minedToScn, "quiet");
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
    return lastResumeCandidate;
  }

  public synchronized long heartbeatsSent() {
    return heartbeatsSent;
  }
}
