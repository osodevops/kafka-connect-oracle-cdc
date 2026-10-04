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
    boolean supplementalPrimaryKey) {

  public TableSchema {
    Objects.requireNonNull(table, "table");
    columns = List.copyOf(columns);
    keyColumns = List.copyOf(keyColumns);
    Objects.requireNonNull(keySource, "keySource");
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
        table, columns, key, source, supplementalAllColumns, supplementalPrimaryKey);
  }
}
