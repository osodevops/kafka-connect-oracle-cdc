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
package sh.oso.connect.oracle.core.decode;

import java.util.LinkedHashMap;
import java.util.Map;
import sh.oso.connect.oracle.core.errors.DecodeException;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.TableSchema;

/**
 * Turns a DML event plus the table schema into a {@link RowChange} (CORE-DEC-2 to CORE-DEC-4). An
 * unknown column or an undecodable literal is a {@link DecodeException}; STATUS 2 or 3 rows are
 * refused before parsing. The before image of an UPDATE or DELETE is partial when the WHERE clause
 * does not cover every column, which is what primary-key-only supplemental logging produces.
 */
public final class RowDecoder {

  private RowDecoder() {}

  public static RowChange decode(MiningEvent.Dml dml, TableSchema schema) {
    if (dml.status() == 2 || dml.status() == 3) {
      throw new DecodeException(
          "LogMiner could not reconstruct "
              + dml.op()
              + " on "
              + schema.table().fqn()
              + " (STATUS "
              + dml.status()
              + (dml.info() == null ? "" : ", " + dml.info())
              + ")",
          "The dictionary no longer matches the redo (DDL since the change) or the type is"
              + " unsupported; see the runbook for decode errors.");
    }
    ParsedDml p = SqlRedoParser.parse(dml.sqlRedo());
    if (p.op() != dml.op()) {
      throw new DecodeException(
          "SQL_REDO is a " + p.op() + " but OPERATION_CODE says " + dml.op() + " at " + dml.id(),
          "Report the row; the connector stops rather than guess.");
    }
    Map<String, ColumnSpec> columns = schema.columnsByName();
    Map<String, Object> set = values(p.set(), columns, schema);
    Map<String, Object> where = values(p.where(), columns, schema);
    Map<String, Object> before;
    Map<String, Object> after;
    boolean partial;
    switch (p.op()) {
      case INSERT:
        before = null;
        after = set;
        partial = false;
        break;
      case UPDATE:
        before = where;
        after = new LinkedHashMap<>(where);
        after.putAll(set);
        partial = where.size() < columns.size();
        break;
      default:
        before = where;
        after = null;
        partial = where.size() < columns.size();
        break;
    }
    String rowId = p.rowId() != null ? p.rowId() : dml.rowId();
    return new RowChange(
        schema.table(), p.op(), before, after, partial, rowId, dml.id(), dml.tx(), dml.timestamp());
  }

  private static Map<String, Object> values(
      Iterable<ColumnValue> cvs, Map<String, ColumnSpec> columns, TableSchema schema) {
    Map<String, Object> out = new LinkedHashMap<>();
    for (ColumnValue cv : cvs) {
      ColumnSpec col = columns.get(cv.column());
      if (col == null) {
        throw new DecodeException(
            "SQL_REDO names column "
                + cv.column()
                + " which "
                + schema.table().fqn()
                + " does not have in the current schema",
            "A DDL changed the table after this redo was written; the connector needs the redo"
                + " dictionary (PRD-03) or a coordinated DDL.");
      }
      out.put(col.name(), OracleTypeCodec.decode(col, cv.value()));
    }
    // table order for deterministic records
    Map<String, Object> ordered = new LinkedHashMap<>();
    for (ColumnSpec c : schema.columns()) {
      if (out.containsKey(c.name())) {
        ordered.put(c.name(), out.get(c.name()));
      }
    }
    return ordered;
  }

  /** Whether a DML operation on this schema can ever yield a full before image. */
  public static boolean fullBeforeImagePossible(TableSchema schema, Operation op) {
    return op == Operation.INSERT || schema.supplementalAllColumns();
  }
}
