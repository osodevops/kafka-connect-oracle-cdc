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

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TxKey;

/**
 * In-memory {@link TransactionBuffer}. START rows are remembered only until their COMMIT or
 * ROLLBACK so the connector can stamp a transaction with its start position and user without ever
 * creating an entry for transactions that touch no captured table (dbz#2683, dbz#24).
 */
public final class HeapTransactionBuffer implements TransactionBuffer {

  private final Map<TxKey, TransactionEntry> open = new HashMap<>();
  private final Map<TxKey, MiningEvent.TxStart> starts = new HashMap<>();
  private long bufferedEvents;
  private long estimatedBytes;
  private long committed;
  private long rolledBack;
  private long undone;
  private long unmatchedUndo;
  private long zeroXidStarts;

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
      e = new TransactionEntry(key, change, starts.get(key));
      open.put(key, e);
    }
    long before = e.estimatedBytes();
    e.add(change);
    bufferedEvents++;
    estimatedBytes += e.estimatedBytes() - before;
  }

  @Override
  public void undo(TxKey key, RedoRecordId undoId, String rowId) {
    TransactionEntry e = open.get(key);
    if (e == null) {
      unmatchedUndo++;
      return;
    }
    long before = e.estimatedBytes();
    if (e.undo(rowId)) {
      undone++;
      bufferedEvents--;
      estimatedBytes -= before - e.estimatedBytes();
    } else {
      unmatchedUndo++;
    }
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
            e.changes()));
  }

  @Override
  public void rollback(MiningEvent.Rollback rollback) {
    starts.remove(rollback.tx());
    TransactionEntry e = open.remove(rollback.tx());
    if (e != null) {
      rolledBack++;
      bufferedEvents -= e.size();
      estimatedBytes -= e.estimatedBytes();
    }
  }

  @Override
  public Optional<RedoRecordId> oldestFirstCaptured() {
    RedoRecordId oldest = null;
    for (TransactionEntry e : open.values()) {
      if (oldest == null || e.firstCaptured.compareTo(oldest) < 0) {
        oldest = e.firstCaptured;
      }
    }
    return Optional.ofNullable(oldest);
  }

  @Override
  public int openTransactions() {
    return open.size();
  }

  /** START rows remembered for transactions without captured changes yet. */
  public int pendingStarts() {
    return starts.size();
  }

  @Override
  public BufferMetricsSnapshot metrics() {
    return new BufferMetricsSnapshot(
        open.size(),
        bufferedEvents,
        estimatedBytes,
        oldestFirstCaptured().map(RedoRecordId::scn).orElse(-1L),
        committed,
        rolledBack,
        undone,
        unmatchedUndo,
        zeroXidStarts);
  }
}
