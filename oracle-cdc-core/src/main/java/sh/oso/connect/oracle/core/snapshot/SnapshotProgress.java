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
package sh.oso.connect.oracle.core.snapshot;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import sh.oso.connect.oracle.core.errors.OracleCdcCorruptionException;
import sh.oso.connect.oracle.core.model.TableId;

/**
 * PRD-02 SNAP-3: a snapshot's progress as the offset's snapshot block. Chunks of a table are
 * emitted in key order, so a table's progress is one frontier: the lower bound of its next chunk.
 * Captured tables absent from the block have not started. Immutable.
 *
 * <pre>{"v": 1, "complete": false, "tables": {"PDB.OWNER.T": {"done": false, "frontier": [...]}}}
 * </pre>
 */
public final class SnapshotProgress {

  public static final int VERSION = 1;

  /** A table's state; {@code frontier} is null before its first chunk. */
  public record TableState(boolean done, List<String> frontier) {}

  private final boolean complete;
  private final Map<String, TableState> tables;

  private SnapshotProgress(boolean complete, Map<String, TableState> tables) {
    this.complete = complete;
    this.tables = Map.copyOf(tables);
  }

  /** A snapshot of every captured table, nothing read yet. */
  public static SnapshotProgress begin() {
    return new SnapshotProgress(false, Map.of());
  }

  /** The block of a stored position, or null when it has none (no snapshot was ever started). */
  @SuppressWarnings("unchecked")
  public static SnapshotProgress of(Map<String, Object> block) {
    if (block == null) {
      return null;
    }
    Object v = block.get("v");
    if (!(v instanceof Number n) || n.intValue() != VERSION) {
      throw new OracleCdcCorruptionException(
          "The offset's snapshot block has version " + v + ", expected " + VERSION + ".",
          "Run a connector version that wrote this offset, or reset the offsets.");
    }
    Map<String, TableState> tables = new LinkedHashMap<>();
    Object raw = block.get("tables");
    if (raw instanceof Map<?, ?> m) {
      for (Map.Entry<?, ?> e : m.entrySet()) {
        Map<String, Object> t = (Map<String, Object>) e.getValue();
        List<String> frontier = null;
        if (t.get("frontier") instanceof List<?> l) {
          frontier = new ArrayList<>();
          for (Object o : l) {
            frontier.add(String.valueOf(o));
          }
        }
        tables.put(
            String.valueOf(e.getKey()),
            new TableState(Boolean.TRUE.equals(t.get("done")), frontier));
      }
    }
    return new SnapshotProgress(Boolean.TRUE.equals(block.get("complete")), tables);
  }

  public Map<String, Object> toMap() {
    Map<String, Object> out = new LinkedHashMap<>();
    out.put("v", VERSION);
    out.put("complete", complete);
    Map<String, Object> ts = new LinkedHashMap<>();
    for (Map.Entry<String, TableState> e : tables.entrySet()) {
      Map<String, Object> t = new LinkedHashMap<>();
      t.put("done", e.getValue().done());
      if (e.getValue().frontier() != null) {
        t.put("frontier", e.getValue().frontier());
      }
      ts.put(e.getKey(), t);
    }
    out.put("tables", ts);
    return out;
  }

  public boolean complete() {
    return complete;
  }

  public boolean done(TableId t) {
    TableState s = tables.get(t.fqn());
    return complete || (s != null && s.done());
  }

  /** The lower bound of the table's next chunk; null when it has not started. */
  public List<String> frontier(TableId t) {
    TableState s = tables.get(t.fqn());
    return s == null ? null : s.frontier();
  }

  /** True when no table has emitted a chunk yet. */
  public boolean untouched() {
    return !complete && tables.isEmpty();
  }

  /** After a chunk ending at {@code upper}: the next chunk starts there, or the table is done. */
  public SnapshotProgress advance(TableId t, List<String> upper) {
    Map<String, TableState> next = new LinkedHashMap<>(tables);
    next.put(t.fqn(), upper == null ? new TableState(true, null) : new TableState(false, upper));
    return new SnapshotProgress(false, next);
  }

  /** Every captured table is done; the block shrinks to the flag. */
  public SnapshotProgress completed() {
    return new SnapshotProgress(true, Map.of());
  }

  @Override
  public boolean equals(Object o) {
    return o instanceof SnapshotProgress p && p.complete == complete && p.tables.equals(tables);
  }

  @Override
  public int hashCode() {
    return tables.hashCode() * 31 + Boolean.hashCode(complete);
  }

  @Override
  public String toString() {
    return toMap().toString();
  }
}
