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

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TxKey;

/**
 * LogMiner reconstructs an INSERT into a table with LOB columns as two rows: the INSERT with
 * EMPTY_CLOB or EMPTY_BLOB placeholders and the all-A placeholder ROWID, then an UPDATE of the LOB
 * columns carrying the real ROWID (observed by EngineCorrectnessEngineIT on 23ai). Buffered as two
 * changes, a later savepoint undo could not match the insert by ROWID. This stage holds such an
 * insert until the next change of its transaction and folds the locator update into it. Pending
 * inserts count as open work for the resume position.
 */
public final class LobInsertCoalescer {

  public static final String PLACEHOLDER_ROWID = "AAAAAAAAAAAAAAAAAA";

  private final Map<TxKey, RowChange> pending = new HashMap<>();
  private long merged;

  /**
   * Returns the changes ready for the buffer, in order; {@code lobColumns} names the table's LOBs.
   */
  public List<RowChange> accept(TxKey key, RowChange change, Set<String> lobColumns) {
    RowChange held = pending.remove(key);
    if (held != null) {
      if (isLocatorUpdate(held, change, lobColumns)) {
        merged++;
        return List.of(merge(held, change));
      }
      if (isPlaceholderInsert(change)) {
        pending.put(key, change);
        return List.of(held);
      }
      return List.of(held, change);
    }
    if (isPlaceholderInsert(change)) {
      pending.put(key, change);
      return List.of();
    }
    return List.of(change);
  }

  /** Releases a held insert unchanged (before a commit or an undo of the same transaction). */
  public Optional<RowChange> flush(TxKey key) {
    return Optional.ofNullable(pending.remove(key));
  }

  public void discard(TxKey key) {
    pending.remove(key);
  }

  /** Earliest held insert, so the resume SCN never passes it (CORE-POS-2). */
  public Optional<RedoRecordId> oldestPending() {
    return pending.values().stream().map(RowChange::id).min(RedoRecordId::compareTo);
  }

  public int pendingCount() {
    return pending.size();
  }

  public long merged() {
    return merged;
  }

  static boolean isPlaceholderInsert(RowChange c) {
    return c.op() == Operation.INSERT && PLACEHOLDER_ROWID.equals(c.rowId());
  }

  /** The UPDATE right after the insert, same table, before image equal on every non-LOB column. */
  static boolean isLocatorUpdate(RowChange insert, RowChange next, Set<String> lobColumns) {
    if (next.op() != Operation.UPDATE
        || !next.table().equals(insert.table())
        || next.before() == null
        || insert.after() == null) {
      return false;
    }
    boolean onlyLobsSet = true;
    for (String col : next.after().keySet()) {
      if (!lobColumns.contains(col)
          && !Objects.equals(next.after().get(col), next.before().get(col))) {
        onlyLobsSet = false;
      }
    }
    if (!onlyLobsSet) {
      return false;
    }
    for (Map.Entry<String, Object> e : next.before().entrySet()) {
      if (lobColumns.contains(e.getKey())) {
        continue;
      }
      if (!insert.after().containsKey(e.getKey())
          || !valuesEqual(insert.after().get(e.getKey()), e.getValue())) {
        return false;
      }
    }
    return true;
  }

  private static boolean valuesEqual(Object a, Object b) {
    if (a instanceof java.math.BigDecimal x && b instanceof java.math.BigDecimal y) {
      return x.compareTo(y) == 0;
    }
    if (a instanceof byte[] x && b instanceof byte[] y) {
      return java.util.Arrays.equals(x, y);
    }
    return Objects.equals(a, b);
  }

  private static RowChange merge(RowChange insert, RowChange locator) {
    Map<String, Object> after = new LinkedHashMap<>(insert.after());
    after.putAll(locator.after());
    return new RowChange(
        insert.table(),
        Operation.INSERT,
        null,
        after,
        insert.partial(),
        locator.rowId(),
        insert.id(),
        insert.tx(),
        insert.timestamp());
  }
}
