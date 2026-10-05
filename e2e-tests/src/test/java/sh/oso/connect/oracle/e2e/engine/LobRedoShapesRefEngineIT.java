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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.ReferenceDoc;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * CORE-REF for LOBs (ADR-0015): one transaction per statement shape on a table with CLOB, NCLOB and
 * BLOB columns, on SecureFile and BasicFile storage, and the undo rows of savepoint rollbacks. Rows
 * are attributed to their scenario by XID and recorded as sequences of operation, ROLLBACK flag and
 * ROWID kind, with repeated rows collapsed, so only facts that hold run after run are written.
 */
@Tag("engine")
class LobRedoShapesRefEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  static final String BIG = "a".repeat(9000) + "z";
  static final String BIG2 = "b".repeat(11000) + "y";
  static final String WIDE = "x".repeat(5000) + "😀" + "y".repeat(10);
  static final String PLACEHOLDER = "AAAAAAAAAAAAAAAAAA";

  interface Step {
    void run(Connection c, String t) throws SQLException;
  }

  @Test
  void lobRowsAndTheirUndoAreRecorded() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    Map<String, String> xids = new LinkedHashMap<>();
    try (Connection root = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      w.setAutoCommit(false);
      try (Statement s = w.createStatement()) {
        s.execute(
            "CREATE TABLE lob_sf (id NUMBER PRIMARY KEY, name VARCHAR2(50), c CLOB, nc NCLOB, b"
                + " BLOB)");
        s.execute("ALTER TABLE lob_sf ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        s.execute(
            "CREATE TABLE lob_bf (id NUMBER PRIMARY KEY, name VARCHAR2(50), c CLOB, nc NCLOB, b"
                + " BLOB) LOB (c) STORE AS BASICFILE, LOB (nc) STORE AS BASICFILE, LOB (b) STORE AS"
                + " BASICFILE");
        s.execute("ALTER TABLE lob_bf ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      }
      OracleSql.archiveLogCurrent(db);
      long start = LogMinerHelper.currentScn(root);
      Map<String, Step> statements = statements();
      Map<String, Step> rollbacks = rollbacks();
      for (String t : List.of("LOB_SF", "LOB_BF")) {
        for (Map.Entry<String, Step> e : statements.entrySet()) {
          xids.put(run(w, t, e.getValue()), t + "\u0001" + e.getKey());
        }
        for (Map.Entry<String, Step> e : rollbacks.entrySet()) {
          xids.put(run(w, t, e.getValue()), t + "\u0001" + e.getKey());
        }
      }
      OracleSql.archiveLogCurrent(db);
      long end = LogMinerHelper.currentScn(root);
      List<Long> objects = new ArrayList<>();
      try (Statement s = w.createStatement();
          ResultSet rs =
              s.executeQuery(
                  "SELECT data_object_id FROM user_objects WHERE object_name IN ('LOB_SF',"
                      + " 'LOB_BF') AND object_type = 'TABLE'")) {
        while (rs.next()) {
          objects.add(rs.getLong(1));
        }
      }
      LogMinerHelper.start(root, start, end);
      List<Map<String, String>> rows =
          LogMinerHelper.rows(
              root,
              "DATA_OBJ# IN ("
                  + objects.stream()
                      .map(String::valueOf)
                      .reduce((a, b) -> a + ", " + b)
                      .orElseThrow()
                  + ") AND OPERATION_CODE <> 0");
      List<Map<String, String>> uba = uba(root, objects);
      LogMinerHelper.end(root);

      Map<String, List<Map<String, String>>> byScenario = new LinkedHashMap<>();
      for (Map<String, String> r : rows) {
        String scenario = xids.get(r.get("XIDUSN") + "." + r.get("XIDSLT") + "." + r.get("XIDSQN"));
        if (scenario != null) {
          byScenario.computeIfAbsent(scenario, k -> new ArrayList<>()).add(r);
        }
      }
      assertThat(byScenario.keySet()).hasSize(xids.size());

      new ReferenceDoc(
              "lob-redo-shapes",
              "LogMiner rows for LOB statements",
              "The rows LogMiner emits for statements on a table with CLOB, NCLOB and BLOB columns"
                  + " and `ALL COLUMNS` supplemental logging, on SecureFile (`LOB_SF`) and"
                  + " BasicFile (`LOB_BF`) storage, mined with `DICT_FROM_ONLINE_CATALOG`,"
                  + " `NO_ROWID_IN_STMT` and `NO_SQL_DELIMITER`, and the undo rows of savepoint"
                  + " rollbacks. Fixes the LOB assembly, grouping and undo rules of PRD-00"
                  + " CORE-DEC-6 and CORE-TX-2 (ADR-0015). Rows are attributed to their statement"
                  + " by XID; `+` marks a row that repeats, a ROWID is `placeholder` (all A) or"
                  + " `real`.")
          .section("LOB row fields")
          .table(List.of("Fact", "Observed"), facts(rows, uba))
          .section("Rows per statement")
          .table(
              List.of("Table", "Statement", "Rows in redo order"),
              shapes(byScenario, statements.keySet()))
          .section("Savepoint and statement rollbacks")
          .paragraph(
              "Each undo row reverses one statement's change to one row, newest first, and names"
                  + " the real ROWID even where the rows it reverses had the placeholder. The SET"
                  + " list of an undo row is not a reliable list of the LOB columns it restores.")
          .table(
              List.of("Table", "Scenario", "Rows in redo order"),
              shapes(byScenario, rollbacks.keySet()))
          .assertUpToDate();
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  private static Map<String, Step> statements() {
    Map<String, Step> m = new LinkedHashMap<>();
    m.put(
        "insert small literals",
        (c, t) ->
            exec(
                c, "INSERT INTO " + t + " VALUES (1, 'one', 'small', N'n', HEXTORAW('DEADBEEF'))"));
    m.put(
        "insert large values (bind)",
        (c, t) -> insert(c, t, 2, BIG, "n".repeat(5000), bytes(9000)));
    m.put("insert one large value, others null", (c, t) -> insert(c, t, 3, BIG, null, null));
    m.put(
        "insert a temporary CLOB (createClob)",
        (c, t) -> {
          java.sql.Clob clob = c.createClob();
          clob.setString(1, BIG);
          try (PreparedStatement ps =
              c.prepareStatement("INSERT INTO " + t + " (id, name, c) VALUES (4, 'four', ?)")) {
            ps.setClob(1, clob);
            ps.executeUpdate();
          }
          clob.free();
        });
    m.put(
        "update with a temporary CLOB (createClob)",
        (c, t) -> {
          java.sql.Clob clob = c.createClob();
          clob.setString(1, BIG2);
          try (PreparedStatement ps =
              c.prepareStatement("UPDATE " + t + " SET name = 'four b', c = ? WHERE id = 4")) {
            ps.setClob(1, clob);
            ps.executeUpdate();
          }
          clob.free();
        });
    m.put(
        "update small literal",
        (c, t) -> exec(c, "UPDATE " + t + " SET c = 'changed' WHERE id = 1"));
    m.put(
        "update large value (bind)",
        (c, t) -> bind(c, "UPDATE " + t + " SET c = ? WHERE id = 2", BIG2));
    m.put(
        "update a column and a large value",
        (c, t) -> bind(c, "UPDATE " + t + " SET name = 'both', c = ? WHERE id = 3", BIG));
    m.put("update a null NCLOB to a large value", (c, t) -> nclob(c, t, 3, "é".repeat(3000)));
    m.put(
        "update text with a supplementary character",
        (c, t) -> bind(c, "UPDATE " + t + " SET c = ? WHERE id = 2", WIDE));
    m.put(
        "update a non-LOB column",
        (c, t) -> exec(c, "UPDATE " + t + " SET name = 'renamed' WHERE id = 2"));
    m.put(
        "DBMS_LOB.WRITEAPPEND",
        (c, t) -> exec(c, plsql(t, "c", "DBMS_LOB.WRITEAPPEND(l, 5, 'hello');")));
    m.put(
        "DBMS_LOB.WRITE in the middle",
        (c, t) -> exec(c, plsql(t, "c", "DBMS_LOB.WRITE(l, 4, 101, 'MIDL');")));
    m.put("DBMS_LOB.TRIM", (c, t) -> exec(c, plsql(t, "c", "DBMS_LOB.TRIM(l, 700);")));
    m.put("DBMS_LOB.ERASE", (c, t) -> exec(c, plsql(t, "c", "DBMS_LOB.ERASE(l, n, 1);")));
    m.put(
        "DBMS_LOB.WRITE on a BLOB",
        (c, t) ->
            exec(
                c,
                plsql(t, "b", "DBMS_LOB.WRITE(l, 4, 1, HEXTORAW('CAFEBABE'));")
                    .replace("l CLOB", "l BLOB")));
    m.put(
        "update two rows' large values",
        (c, t) -> bind(c, "UPDATE " + t + " SET c = ? WHERE id IN (2, 3)", BIG));
    m.put(
        "update large value to small literal",
        (c, t) -> exec(c, "UPDATE " + t + " SET c = 'short' WHERE id = 3"));
    m.put(
        "update large value to EMPTY_CLOB",
        (c, t) -> exec(c, "UPDATE " + t + " SET nc = EMPTY_CLOB() WHERE id = 2"));
    m.put("update to NULL", (c, t) -> exec(c, "UPDATE " + t + " SET b = NULL WHERE id = 2"));
    m.put("delete", (c, t) -> exec(c, "DELETE FROM " + t + " WHERE id = 3"));
    return m;
  }

  private static Map<String, Step> rollbacks() {
    Map<String, Step> m = new LinkedHashMap<>();
    m.put(
        "savepoint, large value over a small one",
        (c, t) -> {
          exec(c, "SAVEPOINT s");
          bind(c, "UPDATE " + t + " SET c = ? WHERE id = 1", BIG2);
          exec(c, "ROLLBACK TO SAVEPOINT s", "UPDATE " + t + " SET name = 'after' WHERE id = 1");
        });
    m.put(
        "update, savepoint, large value over a large one",
        (c, t) -> {
          exec(c, "UPDATE " + t + " SET name = 'kept' WHERE id = 2", "SAVEPOINT s");
          bind(c, "UPDATE " + t + " SET c = ? WHERE id = 2", BIG2);
          exec(c, "ROLLBACK TO SAVEPOINT s");
        });
    m.put(
        "savepoint, a column and a large value",
        (c, t) -> {
          exec(c, "SAVEPOINT s");
          bind(c, "UPDATE " + t + " SET name = 'x', c = ? WHERE id = 2", BIG);
          exec(c, "ROLLBACK TO SAVEPOINT s", "UPDATE " + t + " SET name = 'y' WHERE id = 2");
        });
    m.put(
        "savepoint, insert of large values",
        (c, t) -> {
          exec(c, "SAVEPOINT s");
          insert(c, t, 11, BIG, "n".repeat(5000), bytes(9000));
          exec(
              c,
              "ROLLBACK TO SAVEPOINT s",
              "INSERT INTO " + t + " (id, name) VALUES (12, 'twelve')");
        });
    m.put(
        "savepoint, two rows' large values",
        (c, t) -> {
          exec(c, "SAVEPOINT s");
          bind(c, "UPDATE " + t + " SET c = ? WHERE id IN (1, 2)", BIG);
          exec(c, "ROLLBACK TO SAVEPOINT s", "UPDATE " + t + " SET name = 'z' WHERE id = 1");
        });
    m.put(
        "large value, savepoint, large value",
        (c, t) -> {
          bind(c, "UPDATE " + t + " SET c = ? WHERE id = 1", BIG);
          exec(c, "SAVEPOINT s");
          bind(c, "UPDATE " + t + " SET c = ? WHERE id = 1", BIG2);
          exec(c, "ROLLBACK TO SAVEPOINT s");
        });
    m.put(
        "append, savepoint, append",
        (c, t) -> {
          exec(
              c,
              plsql(t, "c", "DBMS_LOB.WRITEAPPEND(l, 3, 'xxx');").replace("id = 2", "id = 1"),
              "SAVEPOINT s");
          exec(
              c,
              plsql(t, "c", "DBMS_LOB.WRITEAPPEND(l, 3, 'yyy');").replace("id = 2", "id = 1"),
              "ROLLBACK TO SAVEPOINT s");
        });
    m.put(
        "failed insert (ORA-00001) of a large value",
        (c, t) -> {
          exec(c, "UPDATE " + t + " SET name = 'before' WHERE id = 12");
          try {
            insert(c, t, 1, BIG, null, null);
          } catch (SQLException expected) {
            // the statement is rolled back; the transaction stays open
          }
        });
    return m;
  }

  private static String plsql(String t, String col, String call) {
    return "DECLARE l CLOB; n INTEGER := 10; BEGIN SELECT "
        + col
        + " INTO l FROM "
        + t
        + " WHERE id = 2 FOR UPDATE; "
        + call
        + " END;";
  }

  /** Runs a step in its own transaction and returns its XID as USN.SLT.SQN. */
  private static String run(Connection w, String t, Step step) throws SQLException {
    step.run(w, t);
    String xid;
    try (Statement s = w.createStatement();
        ResultSet rs = s.executeQuery("SELECT DBMS_TRANSACTION.LOCAL_TRANSACTION_ID FROM dual")) {
      rs.next();
      xid = rs.getString(1);
    }
    w.commit();
    return xid;
  }

  private static List<List<String>> facts(
      List<Map<String, String>> rows, List<Map<String, String>> uba) {
    Map<String, Set<String>> codes = new LinkedHashMap<>();
    Set<String> lobStatus = new TreeSet<>();
    Set<String> locator = new TreeSet<>();
    Set<String> blockShape = new TreeSet<>();
    String amounts = "not observed";
    for (Map<String, String> r : joined(rows)) {
      String op = r.get("OPERATION");
      codes.computeIfAbsent(op, k -> new TreeSet<>()).add(r.get("OPERATION_CODE"));
      if (op.startsWith("LOB_")) {
        lobStatus.add("STATUS " + r.get("STATUS") + ", INFO \"" + r.get("INFO") + "\"");
        String redo = r.get("SQL_REDO") == null ? "" : r.get("SQL_REDO");
        if (redo.startsWith("DECLARE")) {
          blockShape.add(
              redo.contains(" for update;") && redo.contains("dbms_lob.")
                  ? "DECLARE ... BEGIN select \"<column>\" into <locator> from <table> where"
                      + " <non-LOB columns> for update; <buffer assignment>; dbms_lob.<call>;"
                      + " END;"
                  : "other");
        }
        java.util.regex.Matcher m =
            java.util.regex.Pattern.compile(
                    "buf_c := '(.*)'; \\s*dbms_lob\\.write\\(loc_c, (\\d+),",
                    java.util.regex.Pattern.DOTALL)
                .matcher(redo);
        if (m.find()) {
          String data = m.group(1).replace("''", "'");
          int amount = Integer.parseInt(m.group(2));
          if (data.length() != data.codePointCount(0, data.length())) {
            amounts =
                amount == data.codePointCount(0, data.length())
                    ? "characters (code points): a character outside the BMP counts once"
                    : "UTF-16 code units";
          }
        }
      }
      if (r.get("OPERATION_CODE").equals("9")) {
        locator.add(
            "STATUS "
                + r.get("STATUS")
                + ", SQL_REDO "
                + (r.get("SQL_REDO") == null ? "null" : "present"));
      }
    }
    Set<String> ubas = new TreeSet<>();
    for (Map<String, String> r : uba) {
      ubas.add(r.get("OPERATION") + " " + ("0".equals(r.get("UBABLK")) ? "zero" : "set"));
    }
    List<List<String>> out = new ArrayList<>();
    for (Map.Entry<String, Set<String>> e : codes.entrySet()) {
      out.add(List.of("OPERATION_CODE of " + e.getKey(), String.join(", ", e.getValue())));
    }
    out.add(
        List.of(
            "STATUS and INFO of LOB_WRITE, LOB_TRIM and LOB_ERASE rows",
            String.join("; ", lobStatus)));
    out.add(List.of("SQL_REDO of those rows", String.join("; ", blockShape)));
    out.add(
        List.of(
            "Rows with OPERATION_CODE 9 (documented as SEL_LOB_LOCATOR, labelled INTERNAL)",
            locator.isEmpty() ? "not observed" : String.join("; ", locator)));
    out.add(List.of("Text write amounts count", amounts));
    out.add(List.of("Undo block address (UBABLK) of LOB rows", String.join("; ", ubas)));
    return out;
  }

  /** Rows with CSF continuation pieces joined into the row that completes them. */
  private static List<Map<String, String>> joined(List<Map<String, String>> rows) {
    List<Map<String, String>> out = new ArrayList<>();
    StringBuilder pending = null;
    for (Map<String, String> r : rows) {
      String redo = r.get("SQL_REDO") == null ? "" : r.get("SQL_REDO");
      if ("1".equals(r.get("CSF"))) {
        pending = pending == null ? new StringBuilder(redo) : pending.append(redo);
        continue;
      }
      Map<String, String> whole = new LinkedHashMap<>(r);
      if (pending != null) {
        whole.put("SQL_REDO", pending.append(redo).toString());
        pending = null;
      }
      out.add(whole);
    }
    return out;
  }

  private static List<Map<String, String>> uba(Connection root, List<Long> objects)
      throws SQLException {
    List<Map<String, String>> out = new ArrayList<>();
    try (Statement st = root.createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT DISTINCT operation, CASE WHEN ubablk = 0 THEN '0' ELSE 'x' END FROM"
                    + " v$logmnr_contents WHERE data_obj# IN ("
                    + objects.stream()
                        .map(String::valueOf)
                        .reduce((a, b) -> a + ", " + b)
                        .orElseThrow()
                    + ") AND operation LIKE 'LOB%'")) {
      while (rs.next()) {
        out.add(Map.of("OPERATION", rs.getString(1), "UBABLK", rs.getString(2)));
      }
    }
    return out;
  }

  private static List<List<String>> shapes(
      Map<String, List<Map<String, String>>> byScenario, Set<String> names) {
    List<List<String>> out = new ArrayList<>();
    int order = 0;
    for (String name : names) {
      order++;
      for (String t : List.of("LOB_SF", "LOB_BF")) {
        List<Map<String, String>> rows = byScenario.getOrDefault(t + "\u0001" + name, List.of());
        List<String> seq = new ArrayList<>();
        for (Map<String, String> r : rows) {
          if ("1".equals(r.get("CSF"))) {
            continue; // continuation rows join the next row
          }
          String rowId = r.get("ROW_ID");
          String kind = rowId == null ? "none" : PLACEHOLDER.equals(rowId) ? "placeholder" : "real";
          String s =
              r.get("OPERATION")
                  + ("1".equals(r.get("ROLLBACK")) ? " (undo)" : "")
                  + " ["
                  + kind
                  + "]";
          if ("1".equals(r.get("ROLLBACK")) && r.get("SQL_REDO") != null) {
            s += " " + undoShape(r.get("SQL_REDO"));
          }
          if (!seq.isEmpty()
              && (seq.get(seq.size() - 1).equals(s) || seq.get(seq.size() - 1).equals(s + "+"))) {
            seq.set(seq.size() - 1, s + "+");
          } else {
            seq.add(s);
          }
        }
        out.add(List.of(t, String.format("%02d %s", order, name), String.join(", ", seq)));
      }
    }
    return out;
  }

  /** The SET columns of an undo row, with values masked except EMPTY_CLOB and EMPTY_BLOB. */
  static String undoShape(String sql) {
    String s = sql.replaceAll("\\s+", " ").trim();
    if (s.startsWith("delete")) {
      return "where ROWID";
    }
    int set = s.indexOf(" set ");
    if (set < 0) {
      return "";
    }
    String body = s.substring(set + 5).replaceAll("'[^']*'", "'...'");
    return "set " + body.replaceAll("\\s+", " ");
  }

  private static byte[] bytes(int n) {
    byte[] b = new byte[n];
    for (int i = 0; i < n; i++) {
      b[i] = (byte) i;
    }
    return b;
  }

  private static void insert(Connection c, String t, int id, String clob, String nclob, byte[] blob)
      throws SQLException {
    try (PreparedStatement ps =
        c.prepareStatement("INSERT INTO " + t + " VALUES (?, ?, ?, ?, ?)")) {
      ps.setInt(1, id);
      ps.setString(2, "row" + id);
      ps.setString(3, clob);
      ps.setNString(4, nclob);
      ps.setBytes(5, blob);
      ps.executeUpdate();
    }
  }

  private static void nclob(Connection c, String t, int id, String value) throws SQLException {
    try (PreparedStatement ps = c.prepareStatement("UPDATE " + t + " SET nc = ? WHERE id = ?")) {
      ps.setNString(1, value);
      ps.setInt(2, id);
      ps.executeUpdate();
    }
  }

  private static void bind(Connection c, String sql, String value) throws SQLException {
    try (PreparedStatement ps = c.prepareStatement(sql)) {
      ps.setString(1, value);
      ps.executeUpdate();
    }
  }

  private static void exec(Connection c, String... sql) throws SQLException {
    try (Statement s = c.createStatement()) {
      for (String q : sql) {
        s.execute(q);
      }
    }
  }
}
