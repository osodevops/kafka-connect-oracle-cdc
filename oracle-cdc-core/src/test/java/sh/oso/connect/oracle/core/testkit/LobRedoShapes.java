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
package sh.oso.connect.oracle.core.testkit;

import java.util.List;
import java.util.Map;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.KeySource;
import sh.oso.connect.oracle.core.schema.OracleType;
import sh.oso.connect.oracle.core.schema.TableSchema;

/**
 * SQL_REDO in the shapes LogMiner writes for a table with LOB columns on 23ai
 * (reference/lob-redo-shapes.md): the row pieces with the placeholder ROWID and the PL/SQL blocks
 * of LOB_WRITE, LOB_TRIM and LOB_ERASE rows.
 */
public final class LobRedoShapes {

  public static final TableId DOCS = new TableId("FREEPDB1", "APP", "DOCS");

  /** DOCS(ID NUMBER key, NAME VARCHAR2, C CLOB, NC NCLOB, B BLOB) with ALL COLUMNS logging. */
  public static final TableSchema SCHEMA =
      new TableSchema(
          DOCS,
          List.of(
              ColumnSpec.of("ID", 1, OracleType.NUMBER),
              ColumnSpec.of("NAME", 2, OracleType.VARCHAR2),
              ColumnSpec.of("C", 3, OracleType.CLOB),
              ColumnSpec.of("NC", 4, OracleType.NCLOB),
              ColumnSpec.of("B", 5, OracleType.BLOB)),
          List.of("ID"),
          KeySource.PRIMARY_KEY,
          true,
          false);

  private LobRedoShapes() {}

  public static String where(int id, String name) {
    return "\"ID\" = '" + id + "' and \"NAME\" = '" + name.replace("'", "''") + "'";
  }

  public static String insertEmpty(int id, String name) {
    return "insert into \"APP\".\"DOCS\"(\"ID\",\"NAME\",\"C\",\"NC\",\"B\") values ('"
        + id
        + "','"
        + name
        + "',EMPTY_CLOB(),EMPTY_CLOB(),EMPTY_BLOB())";
  }

  public static String update(String set, int id, String name) {
    return "update \"APP\".\"DOCS\" set " + set + " where " + where(id, name);
  }

  public static String undoUpdate(String set) {
    return "update \"APP\".\"DOCS\" set " + set;
  }

  public static String undoDelete(String rowId) {
    return "delete from \"APP\".\"DOCS\" where ROWID = '" + rowId + "'";
  }

  private static String block(String column, String where, String body) {
    String loc = column.equals("B") ? "loc_b" : column.equals("NC") ? "loc_nc" : "loc_c";
    return "DECLARE \n loc_c CLOB; \n buf_c VARCHAR2(6162); \n loc_b BLOB; \n buf_b RAW(6162); \n"
        + " loc_nc NCLOB; \n buf_nc NVARCHAR2(6162); \n e_len NUMBER; \nBEGIN\n select \""
        + column
        + "\" into "
        + loc
        + " from \"APP\".\"DOCS\" where "
        + where
        + " for update;\n\n"
        + body.replace("LOC", loc)
        + "END;\n\n";
  }

  /** A text write; quotes in {@code data} are doubled as LogMiner does. */
  public static String write(String column, String where, long offset, String data) {
    String buf = column.equals("NC") ? "buf_nc" : "buf_c";
    return block(
        column,
        where,
        " "
            + buf
            + " := '"
            + data.replace("'", "''")
            + "'; \n  dbms_lob.write(LOC, "
            + data.codePointCount(0, data.length())
            + ", "
            + offset
            + ", "
            + buf
            + ");\n");
  }

  public static String writeBytes(String where, long offset, byte[] data) {
    StringBuilder hex = new StringBuilder();
    for (byte b : data) {
      hex.append(String.format("%02x", b));
    }
    return block(
        "B",
        where,
        " buf_b := HEXTORAW('"
            + hex
            + "'); \n  dbms_lob.write(LOC, "
            + data.length
            + ", "
            + offset
            + ", buf_b);\n");
  }

  public static String trim(String column, String where, long length) {
    return block(column, where, "  dbms_lob.trim(LOC, " + length + ");\n");
  }

  public static String erase(String column, String where, long amount, long offset) {
    return block(
        column,
        where,
        "  e_len :=  " + amount + ";\n  dbms_lob.erase(LOC, e_len, " + offset + ");\n");
  }

  public static Map<String, Object> row(int id, String name) {
    return Map.of("ID", new java.math.BigDecimal(id), "NAME", name);
  }
}
