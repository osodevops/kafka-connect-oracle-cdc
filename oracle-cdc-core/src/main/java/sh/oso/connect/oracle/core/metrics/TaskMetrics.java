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
package sh.oso.connect.oracle.core.metrics;

import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import javax.management.InstanceAlreadyExistsException;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import sh.oso.connect.oracle.core.buffer.BufferMetricsSnapshot;
import sh.oso.connect.oracle.core.buffer.TransactionBuffer;
import sh.oso.connect.oracle.core.engine.EngineMetrics;

/**
 * {@link TaskMetricsMXBean} over the engine's counters, the buffer snapshot the engine publishes
 * after every step (the buffer itself belongs to the engine thread) and the sink's counters.
 */
public final class TaskMetrics implements TaskMetricsMXBean {

  private static final BufferMetricsSnapshot EMPTY =
      new BufferMetricsSnapshot(0, 0, 0, -1, 0, 0, 0, 0, 0, 0, 0, 0);

  private final EngineMetrics e;
  private final SinkMetrics sink;
  private final long memoryMax;
  private final long spillMax;
  private final Supplier<Instant> clock;

  public TaskMetrics(
      EngineMetrics engine,
      SinkMetrics sink,
      long memoryMax,
      long spillMax,
      Supplier<Instant> clock) {
    this.e = engine;
    this.sink = sink;
    this.memoryMax = memoryMax;
    this.spillMax = spillMax;
    this.clock = clock;
  }

  public static ObjectName name(String server) {
    try {
      // topic prefixes are plain names; quote anything else so the name stays valid
      String value = server.matches("[A-Za-z0-9._-]+") ? server : ObjectName.quote(server);
      return new ObjectName("sh.oso.cdc:type=task,server=" + value);
    } catch (JMException ex) {
      throw new IllegalArgumentException("bad server name for JMX: " + server, ex);
    }
  }

  /** Registers under {@link #name}, replacing a leftover registration of a stopped task. */
  public ObjectName register(String server) throws JMException {
    MBeanServer s = ManagementFactory.getPlatformMBeanServer();
    ObjectName n = name(server);
    try {
      s.registerMBean(this, n);
    } catch (InstanceAlreadyExistsException ex) {
      s.unregisterMBean(n);
      s.registerMBean(this, n);
    }
    return n;
  }

  public static void unregister(ObjectName n) {
    try {
      MBeanServer s = ManagementFactory.getPlatformMBeanServer();
      if (n != null && s.isRegistered(n)) {
        s.unregisterMBean(n);
      }
    } catch (JMException ignore) {
      // already gone
    }
  }

  private BufferMetricsSnapshot b() {
    BufferMetricsSnapshot s = e.buffer;
    return s == null ? EMPTY : s;
  }

  @Override
  public long getSteps() {
    return e.steps.get();
  }

  @Override
  public long getRowsMined() {
    return e.rowsMined.get();
  }

  @Override
  public long getEventsApplied() {
    return e.eventsApplied.get();
  }

  @Override
  public long getStepRetries() {
    return e.stepRetries.get();
  }

  @Override
  public long getStepTimeouts() {
    return e.stepTimeouts.get();
  }

  @Override
  public long getStepCuts() {
    return e.stepCuts.get();
  }

  @Override
  public long getReconnects() {
    return e.reconnects.get();
  }

  @Override
  public long getIdlePolls() {
    return e.idlePolls.get();
  }

  @Override
  public long getSessionRecycles() {
    return e.sessionRecycles.get();
  }

  @Override
  public long getTransactionsCommitted() {
    return e.transactionsCommitted.get();
  }

  @Override
  public long getTransactionsSkipped() {
    return e.transactionsSkipped.get();
  }

  @Override
  public long getDecodeFailures() {
    return e.decodeFailures.get();
  }

  @Override
  public long getLobInsertsMerged() {
    return e.lobInsertsMerged.get();
  }

  @Override
  public long getLobRowsApplied() {
    return e.lobRowsApplied.get();
  }

  @Override
  public long getOrphansReleased() {
    return e.orphansReleased.get();
  }

  @Override
  public long getTransactionsDiscarded() {
    return e.transactionsDiscarded.get();
  }

  @Override
  public long getLastStepMillis() {
    return e.lastStepMillis.get();
  }

  @Override
  public int getWindowLogs() {
    return e.windowLogs.get();
  }

  @Override
  public long getMinedToScn() {
    return e.minedToScn.get();
  }

  @Override
  public long getSafeEndScn() {
    return e.safeEndScn.get();
  }

  @Override
  public long getScnLag() {
    return Math.max(0, e.safeEndScn.get() - e.minedToScn.get());
  }

  @Override
  public int getOpenTransactions() {
    return b().openTransactions();
  }

  @Override
  public long getBufferedEvents() {
    return b().bufferedEvents();
  }

  @Override
  public long getBufferHeapBytes() {
    return b().estimatedBytes();
  }

  @Override
  public long getBufferMemoryMaxBytes() {
    return memoryMax;
  }

  @Override
  public long getOldestOpenScn() {
    return b().oldestOpenScn();
  }

  @Override
  public long getRolledBackTransactions() {
    return b().rolledBackTransactions();
  }

  @Override
  public long getUndoneEvents() {
    return b().undoneEvents();
  }

  @Override
  public long getUnmatchedUndo() {
    return b().unmatchedUndo();
  }

  @Override
  public int getSpilledTransactions() {
    return b().spilledTransactions();
  }

  @Override
  public long getSpilledBytes() {
    return b().spilledBytes();
  }

  @Override
  public long getSpillMaxBytes() {
    return spillMax;
  }

  @Override
  public int getJournaledTransactions() {
    return b().journaledTransactions();
  }

  @Override
  public long getHeartbeatsSent() {
    return sink.heartbeatsSent();
  }

  @Override
  public long getOpsEvents() {
    return sink.opsEvents();
  }

  @Override
  public long getJournalChunks() {
    return sink.journalChunks();
  }

  @Override
  public long getJournalTombstones() {
    return sink.journalTombstones();
  }

  @Override
  public long getDlqRecords() {
    return sink.dlqRecords();
  }

  @Override
  public int getQueueDepth() {
    return sink.queued();
  }

  @Override
  public long getLastCommitTimestampMillis() {
    return sink.lastCommitTimestampMillis();
  }

  @Override
  public long getMillisBehindSource() {
    return sink.millisBehindSource();
  }

  @Override
  public List<TransactionInfo> getLargestTransactions() {
    List<TransactionBuffer.OpenTransaction> open = e.largest;
    List<TransactionInfo> out = new ArrayList<>();
    if (open == null) {
      return out;
    }
    Instant now = clock.get();
    for (TransactionBuffer.OpenTransaction t : open) {
      out.add(
          new TransactionInfo(
              t.key().xid().toString(),
              t.username(),
              t.clientId(),
              t.firstSeenAt() == null
                  ? -1
                  : Math.max(0, now.toEpochMilli() - t.firstSeenAt().toEpochMilli()),
              t.firstScn(),
              t.events(),
              t.heapBytes(),
              t.spilledBytes(),
              t.journaled()));
    }
    return out;
  }
}
