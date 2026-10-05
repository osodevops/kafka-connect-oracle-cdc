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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import sh.oso.connect.oracle.core.model.TableId;

/**
 * The structure of a captured table at one point in time: columns in position order, the chosen key
 * and the supplemental logging level, which decides whether before images are complete.
 */
public record TableSchema(
    TableId table,
    List<ColumnSpec> columns,
    List<String> keyColumns,
    KeySource keySource,
    boolean supplementalAllColumns,
    boolean supplementalPrimaryKey,
    int version,
    long validFromScn) {

  public TableSchema {
    Objects.requireNonNull(table, "table");
    columns = List.copyOf(columns);
    keyColumns = List.copyOf(keyColumns);
    Objects.requireNonNull(keySource, "keySource");
  }

  /** The first version of a table, valid from any SCN (PRD-03). */
  public TableSchema(
      TableId table,
      List<ColumnSpec> columns,
      List<String> keyColumns,
      KeySource keySource,
      boolean supplementalAllColumns,
      boolean supplementalPrimaryKey) {
    this(
        table,
        columns,
        keyColumns,
        keySource,
        supplementalAllColumns,
        supplementalPrimaryKey,
        1,
        0);
  }

  /** This layout as version {@code version}, effective from {@code scn} (a DDL's SCN). */
  public TableSchema withVersion(int version, long scn) {
    return new TableSchema(
        table,
        columns,
        keyColumns,
        keySource,
        supplementalAllColumns,
        supplementalPrimaryKey,
        version,
        scn);
  }

  /**
   * Same columns, key and supplemental logging: a DDL that changed none of them adds no version.
   */
  public boolean sameLayout(TableSchema other) {
    return table.equals(other.table)
        && columns.equals(other.columns)
        && keyColumns.equals(other.keyColumns)
        && keySource == other.keySource
        && supplementalAllColumns == other.supplementalAllColumns
        && supplementalPrimaryKey == other.supplementalPrimaryKey;
  }

  public ColumnSpec column(String name) {
    for (ColumnSpec c : columns) {
      if (c.name().equals(name)) {
        return c;
      }
    }
    return null;
  }

  public Map<String, ColumnSpec> columnsByName() {
    Map<String, ColumnSpec> m = new LinkedHashMap<>();
    for (ColumnSpec c : columns) {
      m.put(c.name(), c);
    }
    return m;
  }

  public TableSchema withKey(List<String> key, KeySource source) {
    return new TableSchema(
        table,
        columns,
        key,
        source,
        supplementalAllColumns,
        supplementalPrimaryKey,
        version,
        validFromScn);
  }
}
