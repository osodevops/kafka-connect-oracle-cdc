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
package sh.oso.connect.oracle.core.buffer;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import sh.oso.connect.oracle.core.errors.BufferExhaustedException;
import sh.oso.connect.oracle.core.errors.JournalCorruptionException;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TxKey;

/**
 * Transaction buffer keyed by (container, XID) with heap-first storage, largest-first spill and an
 * optional journal (CORE-TX-1 to CORE-TX-5). Changes stay on heap until the budget is exceeded;
 * then the largest open transaction moves to a {@link SpillStore} file and keeps appending there.
 * Exceeding the spill cap is {@link BufferExhaustedException} naming the ten largest transactions.
 * Transactions crossing the {@link JournalPolicy} thresholds are written to the {@link JournalSink}
 * in chunks and stop pinning the resume position at their start (ADR-0003). Without a store or a
 * policy the buffer is heap only, unlimited and never journals (tests and the fake).
 */
public final class HeapTransactionBuffer implements TransactionBuffer {

  private final Map<TxKey, TransactionEntry> open = new HashMap<>();
  private final Map<TxKey, MiningEvent.TxStart> starts = new HashMap<>();
  private final Map<TxKey, TransactionEntry> released = new HashMap<>();
  private final long memoryBudget;
  private final SpillStore store;
  private final JournalPolicy policy;
  private final JournalSink journal;
  private final long generation;
  private final Supplier<Instant> clock;
  private long bufferedEvents;
  private long estimatedBytes;
  private long committed;
  private long rolledBack;
  private long undone;
  private long unmatchedUndo;
  private long zeroXidStarts;
  private long spills;
  private long chunksWritten;

  /** Heap only, no budget, no journal. */
  public HeapTransactionBuffer() {
    this(Long.MAX_VALUE, null);
  }

  /** Heap up to {@code memoryBudgetBytes}, then the largest transactions spill to the store. */
  public HeapTransactionBuffer(long memoryBudgetBytes, SpillStore store) {
    this(memoryBudgetBytes, store, JournalPolicy.NEVER, JournalSink.NONE, 0, Instant::now);
  }

  /** Full configuration: budget and spill, journal policy and sink, the task generation. */
  public HeapTransactionBuffer(
      long memoryBudgetBytes,
      SpillStore store,
      JournalPolicy policy,
      JournalSink journal,
      long generation,
      Supplier<Instant> clock) {
    this.memoryBudget = memoryBudgetBytes;
    this.store = store;
    this.policy = policy;
    this.journal = journal;
    this.generation = generation;
    this.clock = clock;
  }

  @Override
  public void start(MiningEvent.TxStart start) {
    if (start.tx().xid().isZero()) {
      zeroXidStarts++;
      return;
    }
    starts.put(start.tx(), start);
  }

  @Override
  public void add(TxKey key, RowChange change) {
    if (key.xid().isZero()) {
      throw new IllegalArgumentException("a change cannot belong to the zero XID: " + change.id());
    }
    TransactionEntry e = open.get(key);
    if (e == null) {
      e = new TransactionEntry(key, change, starts.get(key), clock.get());
      open.put(key, e);
    }
    long before = e.estimatedBytes();
    try {
      e.add(change);
    } catch (IOException ex) {
      throw spillFailed(ex);
    }
    bufferedEvents++;
    estimatedBytes += e.estimatedBytes() - before;
    enforceBudget();
  }

  @Override
  public String undo(
      TxKey key,
      RedoRecordId undoId,
      String rowId,
      sh.oso.connect.oracle.core.model.TableId table,
      sh.oso.connect.oracle.core.model.Operation op) {
    TransactionEntry e = open.get(key);
    if (e == null) {
      unmatchedUndo++;
      return null;
    }
    String target = e.resolve(rowId, table, op);
    return apply(e, undoId, target) == TransactionEntry.NONE ? null : target;
  }

  private int apply(TransactionEntry e, RedoRecordId undoId, String target) {
    long before = e.estimatedBytes();
    int result;
    try {
      result = e.undo(undoId, target);
    } catch (IOException ex) {
      throw spillFailed(ex);
    }
    if (result == TransactionEntry.NONE) {
      unmatchedUndo++;
      return result;
    }
    if (!e.spilled()) {
      // spilled undos are counted when the file is resolved at commit
      undone++;
      if (result == TransactionEntry.REMOVED) {
        bufferedEvents--;
      }
      estimatedBytes -= before - e.estimatedBytes();
    }
    enforceBudget();
    return result;
  }

  @Override
  public Optional<CommittedTransaction> commit(MiningEvent.Commit commit) {
    TxKey key = commit.tx();
    MiningEvent.TxStart start = starts.remove(key);
    TransactionEntry e = open.remove(key);
    if (e == null) {
      return Optional.empty();
    }
    committed++;
    bufferedEvents -= e.size();
    estimatedBytes -= e.estimatedBytes();
    if (e.startId == null && start != null) {
      e.startId = start.id();
      e.thread = start.thread();
      e.username = start.username();
      e.clientId = start.clientId();
    }
    List<RowChange> events;
    if (e.spilled()) {
      SpillStore.Resolved r;
      try {
        r = e.spill.resolve();
      } catch (IOException ex) {
        throw spillFailed(ex);
      }
      undone += r.undone();
      unmatchedUndo += r.unmatchedUndo();
      events = r.changes();
    } else {
      events = e.changes();
    }
    if (e.spilled() || e.journaled) {
      released.put(key, e); // the file and the tombstones wait for release()
    }
    return Optional.of(
        new CommittedTransaction(
            key,
            e.firstCaptured,
            e.startId,
            commit.id(),
            commit.timestamp(),
            e.thread == 0 ? commit.thread() : e.thread,
            e.username,
            e.clientId,
            events));
  }

  @Override
  public void release(TxKey key) {
    TransactionEntry e = released.remove(key);
    if (e == null) {
      return;
    }
    if (e.spill != null) {
      e.spill.delete();
    }
    if (e.journaled) {
      journal.tombstones(key, List.copyOf(e.chunkRefs));
    }
  }

  @Override
  public void discard(TxKey key) {
    starts.remove(key);
    TransactionEntry e = open.remove(key);
    if (e != null) {
      drop(key, e);
    }
  }

  @Override
  public void rollback(MiningEvent.Rollback rollback) {
    starts.remove(rollback.tx());
    TransactionEntry e = open.remove(rollback.tx());
    if (e != null) {
      drop(rollback.tx(), e);
    }
  }

  private void drop(TxKey key, TransactionEntry e) {
    rolledBack++;
    bufferedEvents -= e.size();
    estimatedBytes -= e.estimatedBytes();
    if (e.spill != null) {
      // the undo rows of a rolled-back spilled transaction are never resolved (the file goes
      // unread); count them as undone so the undo totals still add up
      undone += e.spilledUndos();
      e.spill.delete();
    }
    if (e.journaled) {
      journal.tombstones(key, List.copyOf(e.chunkRefs));
    }
  }

  @Override
  public Optional<RedoRecordId> oldestFirstCaptured() {
    RedoRecordId oldest = null;
    for (TransactionEntry e : open.values()) {
      RedoRecordId bound = e.resumeBound();
      if (oldest == null || bound.compareTo(oldest) < 0) {
        oldest = bound;
      }
    }
    return Optional.ofNullable(oldest);
  }

  @Override
  public void flushJournal(Instant now) {
    if (!policy.enabled()) {
      return;
    }
    for (TransactionEntry e : open.values()) {
      try {
        if (!e.journaled) {
          if (policy.due(Duration.between(e.firstSeenAt, now), e.size())) {
            List<JournalFrames.Frame> all = e.allFrames();
            e.journaled = true;
            writeChunks(e, all);
          }
        } else if (!e.pendingJournal.isEmpty()) {
          List<JournalFrames.Frame> pending = new ArrayList<>(e.pendingJournal);
          e.pendingJournal.clear();
          writeChunks(e, pending);
        }
      } catch (IOException ex) {
        throw spillFailed(ex);
      }
    }
  }

  /** Splits frames into chunks of about the policy's size and hands them to the sink. */
  private void writeChunks(TransactionEntry e, List<JournalFrames.Frame> frames) {
    if (frames.isEmpty()) {
      if (e.lastJournaled == null) {
        e.lastJournaled = e.firstCaptured;
      }
      return;
    }
    int from = 0;
    while (from < frames.size()) {
      int bytes = 0;
      int to = from;
      while (to < frames.size()
          && (to == from
              || bytes + JournalFrames.sizeOf(frames.get(to)) <= policy.chunkMaxBytes())) {
        bytes += JournalFrames.sizeOf(frames.get(to));
        to++;
      }
      List<JournalFrames.Frame> slice = frames.subList(from, to);
      int undos = 0;
      for (JournalFrames.Frame f : slice) {
        if (f.isUndo()) {
          undos++;
        }
      }
      JournalChunk chunk =
          new JournalChunk(
              e.key,
              e.chunkRefs.size(),
              generation,
              slice.get(0).id(),
              slice.get(slice.size() - 1).id(),
              slice.size() - undos,
              undos,
              e.firstCaptured,
              e.startId,
              e.thread,
              e.username,
              e.clientId,
              JournalFrames.encode(slice));
      journal.chunk(chunk);
      e.chunkRefs.add(chunk.ref());
      chunksWritten++;
      e.lastJournaled = chunk.last();
      from = to;
    }
  }

  @Override
  public void restore(List<JournalChunk> chunks) {
    if (chunks.isEmpty()) {
      return;
    }
    List<JournalChunk> ordered = new ArrayList<>(chunks);
    ordered.sort(Comparator.comparingInt(JournalChunk::chunk));
    JournalChunk first = ordered.get(0);
    TxKey key = first.key();
    for (int i = 0; i < ordered.size(); i++) {
      JournalChunk c = ordered.get(i);
      if (!c.key().equals(key)) {
        throw new IllegalArgumentException(
            "chunks of different transactions: " + key + " and " + c.key());
      }
      if (c.chunk() != i) {
        throw new JournalCorruptionException(
            "Journal for transaction "
                + key
                + " is missing chunk "
                + i
                + " (found chunk "
                + c.chunk()
                + ").",
            "The journal topic lost a chunk, probably to retention or compaction settings. Restore"
                + " cleanup.policy=compact with unlimited retention on the journal topic, then"
                + " reset the offsets to a position before the transaction started.");
      }
    }
    if (open.containsKey(key)) {
      throw new IllegalStateException("transaction already open: " + key);
    }
    TransactionEntry e = new TransactionEntry(key, first.firstCaptured(), null, clock.get());
    e.startId = first.startId();
    e.thread = first.thread();
    e.username = first.username();
    e.clientId = first.clientId();
    open.put(key, e);
    try {
      for (JournalChunk c : ordered) {
        for (JournalFrames.Frame f :
            JournalFrames.decode(c.payload(), "Journal chunk " + c.chunk() + " of " + key)) {
          long before = e.estimatedBytes();
          if (f.isUndo()) {
            int result = e.undo(f.undoId(), f.undoRowId());
            if (result != TransactionEntry.NONE && !e.spilled()) {
              undone++;
              if (result == TransactionEntry.REMOVED) {
                bufferedEvents--;
              }
              estimatedBytes -= before - e.estimatedBytes();
            } else if (result == TransactionEntry.NONE) {
              unmatchedUndo++;
            }
          } else {
            e.add(f.change());
            bufferedEvents++;
            estimatedBytes += e.estimatedBytes() - before;
          }
          enforceBudget();
        }
      }
    } catch (IOException ex) {
      throw spillFailed(ex);
    }
    JournalChunk last = ordered.get(ordered.size() - 1);
    e.journaled = true;
    for (JournalChunk c : ordered) {
      e.chunkRefs.add(c.ref()); // tombstoned later under the generation that wrote each chunk
    }
    e.lastJournaled = last.last();
    e.pendingJournal.clear(); // the restored frames are already in the journal
  }

  @Override
  public int openTransactions() {
    return open.size();
  }

  /** START rows remembered for transactions without captured changes yet. */
  public int pendingStarts() {
    return starts.size();
  }

  /** How many times a transaction was moved to disk. */
  public long spills() {
    return spills;
  }

  /** Journal chunks written so far. */
  public long chunksWritten() {
    return chunksWritten;
  }

  @Override
  public List<OpenTransaction> largest(int n) {
    List<OpenTransaction> all = new ArrayList<>(open.size());
    for (TransactionEntry e : open.values()) {
      all.add(
          new OpenTransaction(
              e.key,
              e.username,
              e.clientId,
              e.firstCaptured.scn(),
              e.lastSeen == null ? e.firstCaptured.scn() : e.lastSeen.scn(),
              e.firstSeenAt,
              e.sessionNo,
              e.serialNo,
              e.size(),
              e.estimatedBytes(),
              e.spilledBytes(),
              e.journaled));
    }
    all.sort(
        Comparator.comparingLong(OpenTransaction::totalBytes)
            .reversed()
            .thenComparingLong(OpenTransaction::firstScn));
    return all.size() > n ? List.copyOf(all.subList(0, n)) : List.copyOf(all);
  }

  @Override
  public BufferMetricsSnapshot metrics() {
    int spilled = 0;
    long spilledBytes = 0;
    int journaled = 0;
    for (TransactionEntry e : open.values()) {
      if (e.spilled()) {
        spilled++;
        spilledBytes += e.spilledBytes();
      }
      if (e.journaled) {
        journaled++;
      }
    }
    return new BufferMetricsSnapshot(
        open.size(),
        bufferedEvents,
        estimatedBytes,
        oldestFirstCaptured().map(RedoRecordId::scn).orElse(-1L),
        committed,
        rolledBack,
        undone,
        unmatchedUndo,
        zeroXidStarts,
        spilled,
        spilledBytes,
        journaled);
  }

  /** Largest-first spill until the heap is within budget, then the spill cap check. */
  private void enforceBudget() {
    if (store == null) {
      return;
    }
    while (estimatedBytes > memoryBudget) {
      TransactionEntry largest = null;
      for (TransactionEntry e : open.values()) {
        if (!e.spilled() && (largest == null || e.estimatedBytes() > largest.estimatedBytes())) {
          largest = e;
        }
      }
      if (largest == null || largest.estimatedBytes() == 0) {
        break; // everything is on disk already; the heap holds only entry shells
      }
      try {
        long freed = largest.estimatedBytes();
        largest.spillTo(store.create(largest.key));
        estimatedBytes -= freed;
        spills++;
      } catch (IOException ex) {
        throw spillFailed(ex);
      }
    }
    if (store.totalBytes() > store.maxBytes()) {
      throw new BufferExhaustedException(
          "Spilled uncommitted changes exceed cdc.buffer.spill.max.bytes ("
              + store.maxBytes()
              + " bytes): "
              + store.totalBytes()
              + " bytes on disk under "
              + store.dir()
              + ". Largest open transactions: "
              + describe(largest(10)),
          "Raise cdc.buffer.spill.max.bytes or cdc.buffer.memory.max.bytes, commit or roll back"
              + " the transactions named, or set cdc.transaction.max.age.ms to bound them; then"
              + " restart the task.");
    }
  }

  private BufferExhaustedException spillFailed(IOException ex) {
    return new BufferExhaustedException(
        "Writing spilled changes under "
            + (store == null ? "(no spill directory)" : store.dir())
            + " failed: "
            + ex.getMessage()
            + ". Largest open transactions: "
            + describe(largest(10)),
        "Free space on the spill volume or point cdc.buffer.spill.dir at a larger one, then"
            + " restart the task; it re-mines the open transactions from the position.",
        ex);
  }

  static String describe(List<OpenTransaction> top) {
    StringBuilder sb = new StringBuilder();
    for (OpenTransaction t : top) {
      if (sb.length() > 0) {
        sb.append("; ");
      }
      sb.append(t.key().xid())
          .append(" con ")
          .append(t.key().srcConId())
          .append(t.username() == null ? "" : " user " + t.username())
          .append(" from SCN ")
          .append(t.firstScn())
          .append(", ")
          .append(t.events())
          .append(" events, ")
          .append(t.heapBytes())
          .append(" heap bytes, ")
          .append(t.spilledBytes())
          .append(" spilled bytes")
          .append(t.journaled() ? ", journaled" : "");
    }
    return sb.length() == 0 ? "none" : sb.toString();
  }
}
