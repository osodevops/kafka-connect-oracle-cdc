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
package sh.oso.connect.oracle.schema;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.schema.SchemaStore;
import sh.oso.connect.oracle.core.schema.TableSchema;

/**
 * The schema topic as the registry's store (PRD-03 section 2): every version of every table
 * decoded, written to the compacted topic on change through {@link Writer}, and pruned on write to
 * the version valid at the resume point plus the ones after it, so the topic stays small.
 */
public final class SchemaTopicStore implements SchemaStore {

  /** Where versions go: the task's record queue. */
  public interface Writer {
    void versions(TableId table, List<TableSchema> versions);

    void removed(TableId table);

    Writer NONE =
        new Writer() {
          public void versions(TableId table, List<TableSchema> versions) {}

          public void removed(TableId table) {}
        };
  }

  private final Map<TableId, List<TableSchema>> versions = new ConcurrentHashMap<>();
  private final LongSupplier resumeScn;
  private volatile Writer writer = Writer.NONE;

  /** Tables saved or removed while held, written by {@link #release()}; guarded by this. */
  private final Set<TableId> held = new java.util.LinkedHashSet<>();

  private boolean holding; // guarded by this

  public SchemaTopicStore(LongSupplier resumeScn) {
    this.resumeScn = resumeScn;
  }

  public void writeTo(Writer w) {
    this.writer = w;
  }

  /**
   * Keeps changes without writing them until {@link #release()}. The task reads every captured
   * table's layout in start() (ADR-0016 amendment), before poll() runs: written there, thousands of
   * tables would fill the record queue and start() would wait on it for ever.
   */
  public synchronized void hold() {
    holding = true;
  }

  /**
   * Writes each table changed while held, as it stands now, and writes every later change at once.
   * Called on a thread that may wait for poll() to drain the queue (the engine's or the
   * snapshot's).
   */
  public synchronized void release() {
    holding = false;
    for (TableId t : held) {
      List<TableSchema> v = versions(t);
      if (v.isEmpty()) {
        writer.removed(t);
      } else {
        writer.versions(t, v);
      }
    }
    held.clear();
  }

  /** Versions read back from the topic at start; nothing is written. */
  public void seed(TableId table, List<TableSchema> loaded) {
    if (loaded.isEmpty()) {
      versions.remove(table);
    } else {
      List<TableSchema> sorted = new ArrayList<>(loaded);
      sorted.sort(Comparator.comparingInt(TableSchema::version));
      versions.put(table, sorted);
    }
  }

  public Set<TableId> tables() {
    return Set.copyOf(versions.keySet());
  }

  @Override
  public List<TableSchema> versions(TableId table) {
    return List.copyOf(versions.getOrDefault(table, List.of()));
  }

  @Override
  public Optional<TableSchema> load(TableId table) {
    List<TableSchema> l = versions.get(table);
    return l == null || l.isEmpty() ? Optional.empty() : Optional.of(l.get(l.size() - 1));
  }

  @Override
  public synchronized void save(TableSchema schema) {
    List<TableSchema> l = new ArrayList<>(versions.getOrDefault(schema.table(), List.of()));
    l.removeIf(s -> s.version() == schema.version());
    l.add(schema);
    l.sort(Comparator.comparingInt(TableSchema::version));
    List<TableSchema> kept = prune(l, resumeScn.getAsLong());
    versions.put(schema.table(), kept);
    if (holding) {
      held.add(schema.table());
    } else {
      writer.versions(schema.table(), kept);
    }
  }

  @Override
  public synchronized void remove(TableId table) {
    if (versions.remove(table) != null) {
      if (holding) {
        held.add(table);
      } else {
        writer.removed(table);
      }
    }
  }

  /** The newest version valid at {@code scn} and every later one. */
  static List<TableSchema> prune(List<TableSchema> sorted, long scn) {
    int from = 0;
    for (int i = 0; i < sorted.size(); i++) {
      if (sorted.get(i).validFromScn() <= scn) {
        from = i;
      }
    }
    return List.copyOf(sorted.subList(from, sorted.size()));
  }
}
