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
package sh.oso.connect.oracle.core.schema;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import sh.oso.connect.oracle.core.errors.DecodeException;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TableId;

/**
 * PRD-01 SRC-SEL-2, ADR-0018: the columns {@code cdc.columns.exclude} keeps out of every record.
 * Each pattern is a regular expression matched against the whole of {@code PDB.SCHEMA.TABLE.COLUMN}
 * in a CDB and {@code SCHEMA.TABLE.COLUMN} otherwise, case-insensitively unless the table patterns
 * are case-sensitive, exactly as {@code cdc.tables.include} matches tables.
 *
 * <p>The decoder skips an excluded column before its literal is converted, so its value never
 * reaches the buffer, the spill files, the transaction journal, an exception message or a record.
 * The schema registry keeps the full dictionary layout (names and types only); {@link
 * #project(TableSchema)} removes the excluded columns where a layout is rendered or selected, and
 * refuses a layout whose record key would lose a column. Decisions are cached per table and column,
 * so the patterns run once per column.
 */
public final class ColumnFilter {

  private static final ColumnFilter NONE = new ColumnFilter(List.of());

  private final List<Pattern> patterns;
  private final Map<TableId, Map<String, Boolean>> decisions = new ConcurrentHashMap<>();
  private final Map<TableId, Boolean> tables = new ConcurrentHashMap<>();
  private final Map<TableSchema, TableSchema> projections = new ConcurrentHashMap<>();

  private ColumnFilter(List<Pattern> patterns) {
    this.patterns = List.copyOf(patterns);
  }

  /** Excludes nothing. */
  public static ColumnFilter none() {
    return NONE;
  }

  /**
   * The filter for {@code patterns}; blank entries are ignored.
   *
   * @throws java.util.regex.PatternSyntaxException when a pattern is not a regular expression
   */
  public static ColumnFilter of(List<String> patterns, boolean caseSensitive) {
    int flags = caseSensitive ? 0 : Pattern.CASE_INSENSITIVE;
    List<Pattern> compiled = new ArrayList<>();
    for (String p : patterns) {
      if (p != null && !p.isBlank()) {
        compiled.add(Pattern.compile(p.trim(), flags));
      }
    }
    return compiled.isEmpty() ? NONE : new ColumnFilter(compiled);
  }

  public boolean isEmpty() {
    return patterns.isEmpty();
  }

  /** Whether {@code column} of {@code table} is kept out of records. */
  public boolean excludes(TableId table, String column) {
    if (patterns.isEmpty()) {
      return false;
    }
    return decisions
        .computeIfAbsent(table, t -> new ConcurrentHashMap<>())
        .computeIfAbsent(column, c -> matchesAny(table.fqn() + "." + c));
  }

  /** {@link #excludes(TableId, String)} for a column written as {@code TABLE_FQN.COLUMN}. */
  public boolean excludesName(String qualifiedColumn) {
    return matchesAny(qualifiedColumn);
  }

  private boolean matchesAny(String qualified) {
    for (Pattern p : patterns) {
      if (p.matcher(qualified).matches()) {
        return true;
      }
    }
    return false;
  }

  /**
   * Whether some column of {@code table} could be excluded, judged from the name alone: a pattern
   * that matches, or could match, the table's name followed by a dot and more characters. Raw redo
   * of such a table (SQL_REDO, SQL_UNDO, parser messages quoting it) may hold an excluded value, so
   * it is withheld from DLQ records and error messages.
   */
  public boolean mayExclude(TableId table) {
    if (patterns.isEmpty()) {
      return false;
    }
    return tables.computeIfAbsent(
        table,
        t -> {
          String prefix = t.fqn() + ".";
          for (Pattern p : patterns) {
            Matcher m = p.matcher(prefix);
            // hitEnd: the matcher read the whole name and wanted more, so a column could match
            if (m.matches() || m.hitEnd()) {
              return true;
            }
          }
          return false;
        });
  }

  /** The key columns of {@code schema} this filter would exclude, in key order. */
  public List<String> excludedKeyColumns(TableSchema schema) {
    List<String> out = new ArrayList<>();
    for (String k : schema.keyColumns()) {
      if (excludes(schema.table(), k)) {
        out.add(k);
      }
    }
    return out;
  }

  /**
   * {@code schema} without its excluded columns: the layout records are rendered with and snapshots
   * select. The same instance when nothing is excluded.
   *
   * @throws DecodeException (CDC-3001) when a column of the record key is excluded
   */
  public TableSchema project(TableSchema schema) {
    if (patterns.isEmpty()) {
      return schema;
    }
    return projections.computeIfAbsent(schema, this::projection);
  }

  private TableSchema projection(TableSchema schema) {
    List<String> key = excludedKeyColumns(schema);
    if (!key.isEmpty()) {
      throw new DecodeException(
          "cdc.columns.exclude matches "
              + (key.size() == 1 ? "column " : "columns ")
              + String.join(", ", key)
              + " of "
              + schema.table().fqn()
              + ", which "
              + (key.size() == 1 ? "is" : "are")
              + " part of the record key ("
              + schema.keySource()
              + ")",
          "A key column cannot be excluded. Change cdc.columns.exclude so it no longer matches"
              + " the key, or key the table by other columns with cdc.key.columns; then restart"
              + " the task.");
    }
    List<ColumnSpec> kept = new ArrayList<>(schema.columns().size());
    for (ColumnSpec c : schema.columns()) {
      if (!excludes(schema.table(), c.name())) {
        kept.add(c);
      }
    }
    if (kept.size() == schema.columns().size()) {
      return schema;
    }
    return new TableSchema(
        schema.table(),
        kept,
        schema.keyColumns(),
        schema.keySource(),
        schema.supplementalAllColumns(),
        schema.supplementalPrimaryKey(),
        schema.version(),
        schema.validFromScn(),
        schema.exact());
  }

  /** {@code image} without the excluded columns of {@code table}; the same map when none is. */
  public Map<String, Object> project(TableId table, Map<String, Object> image) {
    if (patterns.isEmpty() || image == null) {
      return image;
    }
    boolean any = false;
    for (String column : image.keySet()) {
      if (excludes(table, column)) {
        any = true;
        break;
      }
    }
    if (!any) {
      return image;
    }
    Map<String, Object> out = new LinkedHashMap<>();
    for (Map.Entry<String, Object> e : image.entrySet()) {
      if (!excludes(table, e.getKey())) {
        out.put(e.getKey(), e.getValue());
      }
    }
    return out;
  }

  /**
   * {@code change} without excluded columns in either image: the guard every change passes before
   * it enters the transaction buffer. The decoder already leaves them out, so this only copies a
   * change a substitute decoder produced.
   */
  public RowChange project(RowChange change) {
    if (patterns.isEmpty()) {
      return change;
    }
    Map<String, Object> before = project(change.table(), change.before());
    Map<String, Object> after = project(change.table(), change.after());
    if (before == change.before() && after == change.after()) {
      return change;
    }
    return new RowChange(
        change.table(),
        change.op(),
        before,
        after,
        change.partial(),
        change.rowId(),
        change.id(),
        change.tx(),
        change.timestamp(),
        change.schemaVersion());
  }

  @Override
  public String toString() {
    return patterns.isEmpty()
        ? "ColumnFilter[none]"
        : "ColumnFilter" + patterns.stream().map(Pattern::pattern).toList();
  }
}
