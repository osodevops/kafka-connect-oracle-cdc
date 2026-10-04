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

import java.util.ArrayList;
import java.util.List;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TxKey;

/** One open transaction: identity, where it started and its changes in redo order. */
final class TransactionEntry {

  final TxKey key;
  final RedoRecordId firstCaptured;
  RedoRecordId startId;
  int thread;
  String username;
  String clientId;
  private final List<RowChange> changes = new ArrayList<>();
  private long estimatedBytes;

  TransactionEntry(TxKey key, RowChange first, MiningEvent.TxStart start) {
    this.key = key;
    this.firstCaptured = first.id();
    if (start != null) {
      this.startId = start.id();
      this.thread = start.thread();
      this.username = start.username();
      this.clientId = start.clientId();
    }
  }

  void add(RowChange c) {
    changes.add(c);
    estimatedBytes += SizeEstimate.of(c);
  }

  /** Removes the latest earlier change with this ROWID; returns false when none matches. */
  boolean undo(String rowId) {
    if (rowId == null) {
      return false;
    }
    for (int i = changes.size() - 1; i >= 0; i--) {
      RowChange c = changes.get(i);
      if (rowId.equals(c.rowId())) {
        changes.remove(i);
        estimatedBytes -= SizeEstimate.of(c);
        return true;
      }
    }
    return false;
  }

  List<RowChange> changes() {
    return changes;
  }

  int size() {
    return changes.size();
  }

  long estimatedBytes() {
    return estimatedBytes;
  }
}
