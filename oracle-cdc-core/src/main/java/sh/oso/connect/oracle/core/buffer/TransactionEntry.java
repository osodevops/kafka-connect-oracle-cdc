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
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.RowIds;
import sh.oso.connect.oracle.core.model.TxKey;

/**
 * One open transaction: identity, where it started, its changes in redo order (on heap or in a
 * spill file) and its journal state.
 */
final class TransactionEntry {

  final TxKey key;
  final RedoRecordId firstCaptured;
  final Instant firstSeenAt;
  RedoRecordId startId;
  int thread;
  String username;
  String clientId;
  long sessionNo;
  long serialNo;
  RedoRecordId lastSeen;
  private final List<RowChange> changes = new ArrayList<>();
  private long estimatedBytes;

  /** Non-null once the entry's changes live on disk; later changes append there. */
  SpillStore.SpillFile spill;

  private int spilledChanges;
  private int spilledUndos;

  /**
   * Journal state (CORE-TX-4): chunks written, the last journaled record, frames not yet written.
   */
  boolean journaled;

  /** Every chunk record written or restored for this transaction, with its generation. */
  final List<JournalChunk.Ref> chunkRefs = new ArrayList<>();

  RedoRecordId lastJournaled;
  final List<JournalFrames.Frame> pendingJournal = new ArrayList<>();

  TransactionEntry(TxKey key, RowChange first, MiningEvent.TxStart start, Instant now) {
    this(key, first.id(), start, now);
  }

  TransactionEntry(TxKey key, RedoRecordId firstCaptured, MiningEvent.TxStart start, Instant now) {
    this.key = key;
    this.firstCaptured = firstCaptured;
    this.firstSeenAt = now;
    this.lastSeen = firstCaptured;
    if (start != null) {
      this.startId = start.id();
      this.thread = start.thread();
      this.username = start.username();
      this.clientId = start.clientId();
      this.sessionNo = start.sessionNo();
      this.serialNo = start.serialNo();
    }
  }

  void add(RowChange c) throws IOException {
    if (c.id() != null) {
      lastSeen = c.id();
    }
    track(c);
    if (journaled) {
      pendingJournal.add(JournalFrames.Frame.of(c));
    }
    if (spill != null) {
      spill.append(c);
      spilledChanges++;
      return;
    }
    changes.add(c);
    estimatedBytes += SizeEstimate.of(c);
  }

  /**
   * Removes the latest earlier change with this ROWID; returns false when none matches. For a
   * spilled entry the undo is appended and resolved at commit, so it returns true and the match is
   * counted then.
   */
  /** Undo results: nothing matched, the change was removed, or a LOB group was downgraded. */
  static final int NONE = 0;

  static final int REMOVED = 1;
  static final int DOWNGRADED = 2;

  /** Synthetic-ROWID changes in order, with runs of other changes between them (ADR-0015). */
  private final java.util.ArrayDeque<Object> stack = new java.util.ArrayDeque<>();

  private record Synthetic(
      String rowId,
      sh.oso.connect.oracle.core.model.TableId table,
      sh.oso.connect.oracle.core.model.Operation op) {}

  private static final class Run {
    int n;
  }

  /** The ROWID an undo of {@code table} with {@code op} targets: the newest change if it fits. */
  String resolve(
      String rowId,
      sh.oso.connect.oracle.core.model.TableId table,
      sh.oso.connect.oracle.core.model.Operation op) {
    if (table != null
        && op != null
        && stack.peekLast() instanceof Synthetic s
        && s.table().equals(table)
        && s.op().inverse() == op
        && (RowIds.real(s.rowId()) == null || RowIds.real(s.rowId()).equals(rowId))) {
      return s.rowId();
    }
    return rowId;
  }

  private void track(RowChange c) {
    if (RowIds.isInert(c.rowId())) {
      return; // a downgraded LOB group is no longer undone by anything
    }
    if (RowIds.isSynthetic(c.rowId())) {
      stack.addLast(new Synthetic(c.rowId(), c.table(), c.op()));
    } else if (stack.peekLast() instanceof Run r) {
      r.n++;
    } else {
      Run r = new Run();
      r.n = 1;
      stack.addLast(r);
    }
  }

  private void untrack(String target) {
    java.util.Iterator<Object> it = stack.descendingIterator();
    while (it.hasNext()) {
      Object o = it.next();
      if (RowIds.isSynthetic(target)) {
        if (o instanceof Synthetic s && s.rowId().equals(target)) {
          it.remove();
          return;
        }
      } else if (o instanceof Run r) {
        if (--r.n == 0) {
          it.remove();
        }
        return;
      }
    }
  }

  /** A LOB group after an undo: the row as it was, its LOB values unavailable (ADR-0015). */
  static RowChange inert(RowChange c) {
    return new RowChange(
        c.table(),
        sh.oso.connect.oracle.core.model.Operation.UPDATE,
        c.before(),
        c.before(),
        c.partial(),
        RowIds.inert(c.rowId()),
        c.id(),
        c.tx(),
        c.timestamp(),
        c.schemaVersion());
  }

  /**
   * Applies an undo whose target ROWID is already resolved (live, or replayed from the journal):
   * removes the latest earlier change with that ROWID, or downgrades it when it is a LOB group. For
   * a spilled entry the undo is appended and resolved at commit, so the result is {@link #REMOVED}
   * and the match is counted then.
   */
  int undo(RedoRecordId undoId, String rowId) throws IOException {
    if (rowId == null) {
      return NONE;
    }
    if (undoId != null) {
      lastSeen = undoId;
    }
    if (journaled) {
      pendingJournal.add(JournalFrames.Frame.undo(undoId, rowId));
    }
    if (spill != null) {
      untrack(rowId); // resolved at commit; the undo stack follows the decision now
      spill.appendUndo(undoId, rowId);
      spilledUndos++;
      return REMOVED;
    }
    for (int i = changes.size() - 1; i >= 0; i--) {
      RowChange c = changes.get(i);
      if (rowId.equals(c.rowId())) {
        untrack(rowId);
        estimatedBytes -= SizeEstimate.of(c);
        if (RowIds.isLobGroup(rowId)) {
          RowChange d = inert(c);
          changes.set(i, d);
          estimatedBytes += SizeEstimate.of(d);
          return DOWNGRADED;
        }
        changes.remove(i);
        return REMOVED;
      }
    }
    return NONE;
  }

  List<RowChange> changes() {
    return changes;
  }

  /** Changes buffered on heap plus changes appended to the spill file, before undo resolution. */
  int size() {
    return changes.size() + spilledChanges;
  }

  int spilledUndos() {
    return spilledUndos;
  }

  /** Heap bytes; zero once spilled. */
  long estimatedBytes() {
    return estimatedBytes;
  }

  long spilledBytes() {
    return spill == null ? 0 : spill.bytes();
  }

  boolean spilled() {
    return spill != null;
  }

  /** Moves every heap change to the given file; the heap list is emptied. */
  void spillTo(SpillStore.SpillFile file) throws IOException {
    for (RowChange c : changes) {
      file.append(c);
    }
    spilledChanges += changes.size();
    changes.clear();
    estimatedBytes = 0;
    spill = file;
  }

  /** Every frame of the transaction so far, for its first journal chunk. */
  List<JournalFrames.Frame> allFrames() throws IOException {
    if (spill != null) {
      return spill.frames();
    }
    List<JournalFrames.Frame> out = new ArrayList<>(changes.size());
    for (RowChange c : changes) {
      out.add(JournalFrames.Frame.of(c));
    }
    return out;
  }

  /** The record a restart must re-mine from for this entry (ADR-0003 resume rule). */
  RedoRecordId resumeBound() {
    return journaled && lastJournaled != null ? lastJournaled : firstCaptured;
  }
}
