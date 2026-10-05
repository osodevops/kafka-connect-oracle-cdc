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
        partial = where.size() < nonLobColumns(schema);
        break;
      default:
        before = where;
        after = null;
        partial = where.size() < nonLobColumns(schema);
        break;
    }
    String rowId = p.rowId() != null ? p.rowId() : dml.rowId();
    return new RowChange(
        schema.table(), p.op(), before, after, partial, rowId, dml.id(), dml.tx(), dml.timestamp());
  }

  /**
   * Decodes a LOB_WRITE, LOB_TRIM or LOB_ERASE row (ADR-0015). Its STATUS is 2 with INFO "LOB
   * sql_redo not re-executable" although the block is complete, so the status is not checked; a
   * buffer whose length differs from the call's amount is a {@link DecodeException}.
   */
  public static LobFragment decodeLob(MiningEvent.Dml dml, TableSchema schema) {
    if (dml.sqlRedo() == null) {
      throw new DecodeException(
          dml.op()
              + " row of "
              + schema.table().fqn()
              + " at "
              + dml.id()
              + " has no SQL_REDO (STATUS "
              + dml.status()
              + (dml.info() == null ? "" : ", " + dml.info())
              + ")",
          "Report the row with the Oracle version (reference/lob-redo-shapes.md).");
    }
    LobRedo r = SqlRedoParser.parseLob(dml.sqlRedo());
    if (!r.owner().equals(schema.table().schema()) || !r.table().equals(schema.table().table())) {
      throw new DecodeException(
          "LOB row at "
              + dml.id()
              + " names "
              + r.owner()
              + "."
              + r.table()
              + " but belongs to "
              + schema.table().fqn(),
          "Report the row; the connector stops rather than guess.");
    }
    Map<String, ColumnSpec> columns = schema.columnsByName();
    ColumnSpec col = columns.get(r.column());
    if (col == null || !col.type().isLob()) {
      throw new DecodeException(
          "LOB row at "
              + dml.id()
              + " writes "
              + r.column()
              + ", which is not a LOB column of "
              + schema.table().fqn(),
          "A DDL changed the table after this redo was written; see the runbook for decode"
              + " errors.");
    }
    boolean binary = col.type() == sh.oso.connect.oracle.core.schema.OracleType.BLOB;
    java.util.List<LobFragment.Edit> edits = new java.util.ArrayList<>();
    for (LobRedo.Op op : r.ops()) {
      if (op instanceof LobRedo.Write w) {
        Object data = OracleTypeCodec.decode(col, w.data());
        if (data == null) {
          throw new DecodeException(
              "LOB write at " + dml.id() + " has a NULL buffer", "Report the row shape.");
        }
        LobFragment.Write write = new LobFragment.Write(w.offset(), data);
        if (write.length() != w.amount()) {
          throw new DecodeException(
              "LOB write at "
                  + dml.id()
                  + " declares "
                  + w.amount()
                  + (binary ? " bytes" : " characters")
                  + " but its buffer holds "
                  + write.length(),
              "Report the row shape with the database character set; the connector stops"
                  + " rather than guess.");
        }
        edits.add(write);
      } else if (op instanceof LobRedo.Trim t) {
        edits.add(new LobFragment.Trim(t.length()));
      } else if (op instanceof LobRedo.Erase e) {
        edits.add(new LobFragment.Erase(e.amount(), e.offset()));
      }
    }
    return new LobFragment(
        schema.table(),
        col.name(),
        binary,
        values(r.where(), columns, schema),
        edits,
        dml.rowId(),
        dml.id(),
        dml.tx(),
        dml.timestamp());
  }

  /** LOB columns never appear in a WHERE clause, so they do not make an image partial. */
  static int nonLobColumns(TableSchema schema) {
    int n = 0;
    for (ColumnSpec c : schema.columns()) {
      if (!c.type().isLob()) {
        n++;
      }
    }
    return n;
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
