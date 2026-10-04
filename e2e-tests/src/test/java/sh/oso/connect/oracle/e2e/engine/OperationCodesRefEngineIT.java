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
package sh.oso.connect.oracle.e2e.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.ReferenceDoc;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * CORE-REF-1 (second half): runs one scripted workload per statement kind and records the
 * (OPERATION, OPERATION_CODE, ROLLBACK, STATUS) tuples LogMiner emits for it. The DML window is
 * mined before any DDL runs, because the online catalog decodes rows written before a DDL on the
 * same table with generic column names and STATUS 2; that behaviour is then provoked on purpose and
 * recorded as evidence for the dictionary lag case (PRD-03 section 3, ADR-0008).
 */
@Tag("engine")
class OperationCodesRefEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void operationCodesPerStatementKindAreRecorded() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    Map<String, long[]> ranges = new LinkedHashMap<>();
    try (Connection root = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      w.setAutoCommit(false);
      ddl(
          w,
          "CREATE TABLE t (id NUMBER PRIMARY KEY, name VARCHAR2(100), amount NUMBER(12,2), note"
              + " CLOB, created DATE)",
          "ALTER TABLE t ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
          "CREATE TABLE lagcase (id NUMBER PRIMARY KEY, name VARCHAR2(50))",
          "ALTER TABLE lagcase ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      w.commit();
      OracleSql.archiveLogCurrent(db);
      String filter =
          "(SEG_OWNER = '"
              + schema
              + "' OR USERNAME = '"
              + schema
              + "') AND OPERATION <> 'INTERNAL'";

      // ---- window 1: DML only ----
      long dmlStart = LogMinerHelper.currentScn(root);
      scenario(
          root,
          ranges,
          "insert",
          () ->
              exec(
                  w,
                  "INSERT INTO t (id, name, amount, created) VALUES (1, 'one', 10.5, SYSDATE)",
                  "INSERT INTO t (id, name, amount, created) VALUES (2, 'two', 20, SYSDATE)"));
      scenario(
          root,
          ranges,
          "update",
          () -> exec(w, "UPDATE t SET name = 'uno', amount = 11 WHERE id = 1"));
      scenario(root, ranges, "update-key", () -> exec(w, "UPDATE t SET id = 3 WHERE id = 2"));
      scenario(root, ranges, "delete", () -> exec(w, "DELETE FROM t WHERE id = 3"));
      scenario(
          root,
          ranges,
          "lob-write",
          () -> exec(w, "UPDATE t SET note = TO_CLOB(RPAD('x', 6000, 'y')) WHERE id = 1"));
      scenario(
          root,
          ranges,
          "savepoint-rollback",
          () -> {
            try (Statement s = w.createStatement()) {
              s.execute("INSERT INTO t (id, name, amount) VALUES (10, 'kept', 1)");
              s.execute("SAVEPOINT sp1");
              s.execute("INSERT INTO t (id, name, amount) VALUES (11, 'rolled back', 2)");
              s.execute("UPDATE t SET name = 'rolled back' WHERE id = 10");
              s.execute("DELETE FROM t WHERE id = 1");
              s.execute("ROLLBACK TO SAVEPOINT sp1");
              s.execute("INSERT INTO t (id, name, amount) VALUES (12, 'after savepoint', 3)");
            }
            w.commit();
          });
      scenario(
          root,
          ranges,
          "full-rollback",
          () -> {
            try (Statement s = w.createStatement()) {
              s.execute("INSERT INTO t (id, name, amount) VALUES (20, 'never', 0)");
            }
            w.rollback();
          });
      scenario(
          root,
          ranges,
          "lagcase-dml",
          () ->
              exec(
                  w,
                  "INSERT INTO lagcase VALUES (1, 'before ddl')",
                  "UPDATE lagcase SET name = 'still before ddl' WHERE id = 1"));
      OracleSql.archiveLogCurrent(db);
      long dmlEnd = LogMinerHelper.currentScn(root);

      // mine now, while the online catalog still matches the tables as they were
      LogMinerHelper.start(root, dmlStart, dmlEnd);
      List<Map<String, String>> dmlRows = LogMinerHelper.rows(root, filter);
      LogMinerHelper.end(root);

      // ---- window 2: DDL ----
      long ddlStart = LogMinerHelper.currentScn(root);
      scenario(root, ranges, "truncate", () -> ddl(w, "TRUNCATE TABLE t"));
      scenario(root, ranges, "ddl-alter", () -> ddl(w, "ALTER TABLE t ADD (extra VARCHAR2(10))"));
      scenario(
          root,
          ranges,
          "ddl-create-drop",
          () -> ddl(w, "CREATE TABLE t2 (id NUMBER)", "DROP TABLE t2 PURGE"));
      scenario(
          root,
          ranges,
          "lagcase-ddl",
          () ->
              ddl(
                  w,
                  "ALTER TABLE lagcase ADD (added NUMBER)",
                  "ALTER TABLE lagcase DROP COLUMN name"));
      OracleSql.archiveLogCurrent(db);
      long ddlEnd = LogMinerHelper.currentScn(root);

      LogMinerHelper.start(root, ddlStart, ddlEnd);
      List<Map<String, String>> ddlRows = LogMinerHelper.rows(root, filter);
      LogMinerHelper.end(root);

      // lag case: the DML window again, now that LAGCASE has a different column layout
      LogMinerHelper.start(root, dmlStart, dmlEnd);
      List<Map<String, String>> lagRows =
          LogMinerHelper.rows(root, filter + " AND TABLE_NAME = 'LAGCASE'");
      LogMinerHelper.end(root);
      assertThat(dmlRows).isNotEmpty();
      assertThat(ddlRows).isNotEmpty();

      List<List<String>> tupleRows = new ArrayList<>();
      tupleRows.addAll(tuples(dmlRows, ranges));
      tupleRows.addAll(tuples(ddlRows, ranges));

      List<List<String>> undo = undoRelationships(dmlRows, ranges.get("savepoint-rollback"));
      List<List<String>> lagBefore = decodeShape(dmlRows, "LAGCASE");
      List<List<String>> lagAfter = decodeShape(lagRows, "LAGCASE");

      new ReferenceDoc(
              "operation-codes",
              "LogMiner operation codes by statement kind",
              "Every `(OPERATION, OPERATION_CODE, ROLLBACK, STATUS)` tuple LogMiner emitted for a"
                  + " scripted workload on a heap table with a CLOB and `ALL COLUMNS` supplemental"
                  + " logging, mined with `DICT_FROM_ONLINE_CATALOG`, `NO_ROWID_IN_STMT` and"
                  + " `NO_SQL_DELIMITER`. Fixes the operation-code list in PRD-00 CORE-MINE-2 and"
                  + " the rollback matching rule in CORE-TX-2. Only rows for the workload tables"
                  + " and the transaction control rows are listed; recursive dictionary SQL from"
                  + " the same session is excluded.")
          .section("Tuples observed")
          .table(
              List.of("OPERATION", "OPERATION_CODE", "ROLLBACK", "STATUS", "Scenarios"), tupleRows)
          .section("Savepoint rollback: how ROLLBACK=1 rows relate to the rows they undo")
          .paragraph(
              "Scenario: insert 10, savepoint, insert 11, update 10, delete 1, rollback to"
                  + " savepoint, insert 12, commit. Each undo row is matched to the latest earlier"
                  + " row in the transaction with the same ROW_ID; that is the matching rule"
                  + " CORE-TX-2 implements. RS_ID differs between an undo row and its original, so"
                  + " RS_ID cannot be the key.")
          .table(List.of("Undo row", "Undoes", "Same RS_ID", "Same SSN", "SQL_REDO shape"), undo)
          .section("Online catalog after DDL (dictionary lag case)")
          .paragraph(
              "The same DML rows on table LAGCASE mined twice with the online catalog: before any"
                  + " DDL, then after ALTER TABLE ADD and DROP COLUMN on that table. This is the"
                  + " case PRD-03 section 3 step 4 handles with a redo dictionary; ADR-0008 records"
                  + " the decision.")
          .paragraph("Before the DDL:")
          .table(List.of("OPERATION", "STATUS", "SQL_REDO shape", "Column names"), lagBefore)
          .paragraph("After the DDL:")
          .table(List.of("OPERATION", "STATUS", "SQL_REDO shape", "Column names"), lagAfter)
          .assertUpToDate();
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  private static List<List<String>> tuples(
      List<Map<String, String>> rows, Map<String, long[]> ranges) {
    TreeMap<String, TreeSet<String>> tuples = new TreeMap<>();
    for (Map<String, String> r : rows) {
      String tableName = String.valueOf(r.get("TABLE_NAME"));
      boolean onTable = List.of("T", "T2", "LAGCASE").contains(tableName);
      boolean control = List.of("START", "COMMIT", "ROLLBACK").contains(r.get("OPERATION"));
      if (!onTable && !control) {
        continue;
      }
      String scenario = scenarioFor(ranges, Long.parseLong(r.get("SCN")));
      String key =
          String.join(
              "\u0001",
              String.valueOf(r.get("OPERATION")),
              String.valueOf(r.get("OPERATION_CODE")),
              String.valueOf(r.get("ROLLBACK")),
              String.valueOf(r.get("STATUS")));
      tuples.computeIfAbsent(key, k -> new TreeSet<>()).add(scenario);
    }
    List<List<String>> out = new ArrayList<>();
    for (Map.Entry<String, TreeSet<String>> e : tuples.entrySet()) {
      String[] k = e.getKey().split("\u0001");
      out.add(List.of(k[0], k[1], k[2], k[3], String.join(", ", e.getValue())));
    }
    return out;
  }

  private static List<List<String>> undoRelationships(
      List<Map<String, String>> rows, long[] range) {
    List<Map<String, String>> spRows = new ArrayList<>();
    for (Map<String, String> r : rows) {
      long scn = Long.parseLong(r.get("SCN"));
      if (scn >= range[0] && scn <= range[1] && "T".equals(r.get("TABLE_NAME"))) {
        spRows.add(r);
      }
    }
    List<List<String>> out = new ArrayList<>();
    for (Map<String, String> r : spRows) {
      if (!"1".equals(r.get("ROLLBACK"))) {
        continue;
      }
      Map<String, String> original = null;
      for (Map<String, String> o : spRows) {
        if ("0".equals(o.get("ROLLBACK"))
            && o.get("ROW_ID") != null
            && o.get("ROW_ID").equals(r.get("ROW_ID"))
            && Long.parseLong(o.get("SCN")) <= Long.parseLong(r.get("SCN"))) {
          original = o;
        }
      }
      out.add(
          List.of(
              r.get("OPERATION") + " (undo)",
              original == null ? "(none)" : original.get("OPERATION"),
              original == null
                  ? "n/a"
                  : yesNo(
                      original.get("RS_ID") != null
                          && original.get("RS_ID").equals(r.get("RS_ID"))),
              original == null
                  ? "n/a"
                  : yesNo(original.get("SSN") != null && original.get("SSN").equals(r.get("SSN"))),
              shape(r.get("SQL_REDO"))));
    }
    return out;
  }

  private static List<List<String>> decodeShape(List<Map<String, String>> rows, String table) {
    List<List<String>> out = new ArrayList<>();
    for (Map<String, String> r : rows) {
      if (table.equals(r.get("TABLE_NAME"))
          && List.of("INSERT", "UPDATE", "DELETE").contains(r.get("OPERATION"))) {
        String sql = r.get("SQL_REDO");
        out.add(
            List.of(
                r.get("OPERATION"),
                String.valueOf(r.get("STATUS")),
                shape(sql),
                sql != null && sql.contains("\"COL ")
                    ? "generic COL n names"
                    : "real column names"));
      }
    }
    return out;
  }

  /** SQL_REDO with every literal removed, so the committed document is stable across runs. */
  static String shape(String sql) {
    if (sql == null) {
      return "(null)";
    }
    String s = sql.replaceAll("\\s+", " ");
    s = s.replaceAll("HEXTORAW\\('[0-9A-Fa-f]*'\\)", "HEXTORAW(...)");
    s = s.replaceAll("TO_DATE\\('[^']*'[^)]*\\)", "TO_DATE(...)");
    s = s.replaceAll("'[^']*'", "'...'");
    s = s.replaceAll("\"T_[A-Z0-9_]+\"", "\"<schema>\"");
    return s.length() > 110 ? s.substring(0, 110) + "..." : s;
  }

  private static String yesNo(boolean b) {
    return b ? "yes" : "no";
  }

  interface Step {
    void run() throws SQLException;
  }

  private void scenario(Connection root, Map<String, long[]> ranges, String name, Step step)
      throws SQLException {
    long from = LogMinerHelper.currentScn(root);
    step.run();
    long to = LogMinerHelper.currentScn(root);
    ranges.put(name, new long[] {from, to});
  }

  private static String scenarioFor(Map<String, long[]> ranges, long scn) {
    for (Map.Entry<String, long[]> e : ranges.entrySet()) {
      if (scn >= e.getValue()[0] && scn <= e.getValue()[1]) {
        return e.getKey();
      }
    }
    return "other";
  }

  private static void exec(Connection c, String... sql) throws SQLException {
    try (Statement s = c.createStatement()) {
      for (String q : sql) {
        s.execute(q);
      }
    }
    c.commit();
  }

  private static void ddl(Connection c, String... sql) throws SQLException {
    try (Statement s = c.createStatement()) {
      for (String q : sql) {
        s.execute(q);
      }
    }
  }
}
