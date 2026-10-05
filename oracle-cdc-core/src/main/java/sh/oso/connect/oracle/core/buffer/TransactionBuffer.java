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

import java.util.Optional;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TxKey;

/**
 * Buffers decoded changes per open transaction until COMMIT (CORE-TX-1, CORE-TX-2). Entries exist
 * only for transactions with at least one captured change; START rows and all-zero XIDs never
 * create one. The heap implementation is Phase 1a; spill and the journal layer on top of it.
 */
public interface TransactionBuffer {

  /** Records a START row; no entry is created. Zero XIDs are counted and ignored. */
  void start(MiningEvent.TxStart start);

  /** Adds a captured change; creates the entry on the first one. */
  void add(TxKey key, RowChange change);

  /**
   * Applies a ROLLBACK=1 row: removes the latest earlier change of the same transaction with the
   * same ROWID (reference/operation-codes.md). An undo with no match is counted, not fatal: the
   * change it undoes predates what this connector buffered.
   */
  void undo(TxKey key, RedoRecordId undoId, String rowId);

  /** COMMIT: releases the entry, if any, with its surviving changes in redo order. */
  Optional<CommittedTransaction> commit(MiningEvent.Commit commit);

  /** ROLLBACK: discards the entry, if any. */
  void rollback(MiningEvent.Rollback rollback);

  /**
   * The earliest record a restart must re-mine for the open entries: the first captured record of a
   * non-journaled transaction, the last journaled record of a journaled one (ADR-0003). Drives
   * {@code resume_scn} (CORE-POS-2).
   */
  Optional<RedoRecordId> oldestFirstCaptured();

  /**
   * Journals what the policy says is due (CORE-TX-4): transactions crossing the age or size
   * threshold get their whole history written, journaled ones get the changes since the last chunk.
   * Called by the engine at the end of every step and when idle.
   */
  default void flushJournal(java.time.Instant now) {}

  /** Rebuilds an open transaction from its journal chunks, in chunk order (CORE-TX-5). */
  default void restore(java.util.List<JournalChunk> chunks) {
    throw new UnsupportedOperationException("this buffer does not journal");
  }

  int openTransactions();

  BufferMetricsSnapshot metrics();

  /**
   * The committed transaction's events have been consumed by the sink; a spilled copy can go.
   * Default: nothing to release.
   */
  default void release(TxKey key) {}

  /** The largest open transactions, by bytes buffered on heap and disk (CORE-TX-8). */
  default java.util.List<OpenTransaction> largest(int n) {
    return java.util.List.of();
  }

  /** Every open transaction, for the orphan check. */
  default java.util.List<OpenTransaction> open() {
    return largest(Integer.MAX_VALUE);
  }

  /** Drops an open transaction as if it had rolled back (orphan release, CORE-TX-7). */
  default void discard(TxKey key) {}

  /** One open transaction as reported in metrics and in the buffer-exhausted message. */
  record OpenTransaction(
      TxKey key,
      String username,
      String clientId,
      long firstScn,
      long lastScn,
      java.time.Instant firstSeenAt,
      long sessionNo,
      long serialNo,
      int events,
      long heapBytes,
      long spilledBytes,
      boolean journaled) {
    public long totalBytes() {
      return heapBytes + spilledBytes;
    }
  }
}
