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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.ReferenceDoc;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * ADR-0008 gate: can redo written before a DDL be decoded correctly after that DDL by mining with a
 * dictionary stored in the redo logs (DBMS_LOGMNR_D.BUILD with STORE_IN_REDO_LOGS) plus
 * DDL_DICT_TRACKING, where the online catalog yields generic column names and STATUS 2? This is the
 * recovery path of PRD-03 section 3 step 4.
 */
@Tag("engine")
class DictionaryReplayRefEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void redoDictionaryDecodesRowsWrittenBeforeADdl() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection root = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection sys = db.sysdba(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      w.setAutoCommit(false);
      try (Statement s = w.createStatement()) {
        s.execute("CREATE TABLE lag (id NUMBER PRIMARY KEY, name VARCHAR2(50), amount NUMBER)");
        s.execute("ALTER TABLE lag ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      }
      w.commit();
      OracleSql.archiveLogCurrent(db);

      // dictionary build into the redo stream: as the capture user if its grants allow, else SYS
      String buildBy;
      try (Statement s = root.createStatement()) {
        s.execute("BEGIN DBMS_LOGMNR_D.BUILD(OPTIONS => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS); END;");
        buildBy = "the capture user (EXECUTE ON DBMS_LOGMNR_D)";
      } catch (SQLException e) {
        try (Statement s = sys.createStatement()) {
          s.execute("BEGIN DBMS_LOGMNR_D.BUILD(OPTIONS => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS); END;");
        }
        buildBy = "SYSDBA only; the capture user got " + e.getMessage().split("\n")[0].trim();
      }
      OracleSql.archiveLogCurrent(db);
      long dictEnd = LogMinerHelper.currentScn(root);

      long dmlStart = LogMinerHelper.currentScn(root);
      try (Statement s = w.createStatement()) {
        s.execute("INSERT INTO lag VALUES (1, 'before ddl', 10)");
        s.execute("UPDATE lag SET amount = 11 WHERE id = 1");
        s.execute("DELETE FROM lag WHERE id = 1");
        s.execute("INSERT INTO lag VALUES (2, 'also before ddl', 20)");
      }
      w.commit();
      OracleSql.archiveLogCurrent(db);
      long dmlEnd = LogMinerHelper.currentScn(root);

      try (Statement s = w.createStatement()) {
        s.execute("ALTER TABLE lag ADD (added NUMBER)");
        s.execute("ALTER TABLE lag DROP COLUMN name");
        s.execute("INSERT INTO lag VALUES (3, 30, 3)");
      }
      w.commit();
      OracleSql.archiveLogCurrent(db);
      long ddlEnd = LogMinerHelper.currentScn(root);

      // dictionary location in the archived logs
      List<List<String>> dictLogs = new ArrayList<>();
      try (Statement s = root.createStatement();
          ResultSet rs =
              s.executeQuery(
                  "SELECT sequence#, dictionary_begin, dictionary_end FROM v$archived_log WHERE"
                      + " dest_id = 1 AND (dictionary_begin = 'YES' OR dictionary_end = 'YES') AND"
                      + " next_change# >= "
                      + (dictEnd - 100000)
                      + " ORDER BY sequence#")) {
        int n = 0;
        while (rs.next()) {
          dictLogs.add(List.of("log " + (++n), rs.getString(2), rs.getString(3)));
        }
      }

      String where = "SEG_OWNER = '" + schema + "' AND TABLE_NAME = 'LAG' ORDER BY SCN, RS_ID, SSN";
      // 1. online catalog after the DDL
      LogMinerHelper.start(root, dmlStart, dmlEnd);
      List<Map<String, String>> online = LogMinerHelper.rows(root, where);
      LogMinerHelper.end(root);
      // 2. redo dictionary with DDL tracking, logs added from the dictionary build onwards
      String replayError = null;
      List<Map<String, String>> replay = new ArrayList<>();
      try {
        startWithRedoDictionary(root, dictEnd - 1, dmlEnd);
        replay = LogMinerHelper.rows(root, where);
        LogMinerHelper.end(root);
      } catch (SQLException e) {
        replayError = e.getMessage().split("\n")[0].trim();
        try {
          LogMinerHelper.end(root);
        } catch (SQLException ignore) {
          // session already closed
        }
      }

      List<List<String>> table = new ArrayList<>();
      for (Map<String, String> r : online) {
        table.add(
            List.of(
                "online catalog",
                r.get("OPERATION"),
                String.valueOf(r.get("STATUS")),
                columnNames(r.get("SQL_REDO"))));
      }
      for (Map<String, String> r : replay) {
        table.add(
            List.of(
                "redo dictionary + DDL tracking",
                r.get("OPERATION"),
                String.valueOf(r.get("STATUS")),
                columnNames(r.get("SQL_REDO"))));
      }
      boolean replayDecodes =
          !replay.isEmpty()
              && replay.stream()
                  .allMatch(
                      r ->
                          "0".equals(r.get("STATUS"))
                              && r.get("SQL_REDO") != null
                              && !r.get("SQL_REDO").contains("\"COL "));

      new ReferenceDoc(
              "dictionary-replay",
              "Decoding redo written before a DDL",
              "Rows written to a table, then `ALTER TABLE ADD` and `DROP COLUMN` on that table,"
                  + " mined afterwards (a) with the online catalog and (b) with a dictionary stored"
                  + " in the redo logs by `DBMS_LOGMNR_D.BUILD(STORE_IN_REDO_LOGS)` plus"
                  + " `DDL_DICT_TRACKING`. This is the lag-case recovery path of PRD-03 section 3"
                  + " step 4 and the ADR-0008 gate.")
          .section("Decoding per mode")
          .table(List.of("Mode", "OPERATION", "STATUS", "Column names in SQL_REDO"), table)
          .section("Facts")
          .bullet("Dictionary build executed by: " + buildBy + ".")
          .bullet(
              "Archived logs flagged DICTIONARY_BEGIN or DICTIONARY_END around the build: "
                  + (dictLogs.isEmpty() ? "none found" : "found")
                  + "; a build can span several logs and every one of them must be added to the"
                  + " session.")
          .bullet(
              "Redo-dictionary replay decoded every pre-DDL row with real column names and STATUS"
                  + " 0: "
                  + (replayDecodes
                      ? "yes"
                      : "NO" + (replayError != null ? " (" + replayError + ")" : ""))
                  + ".")
          .paragraph(
              replayDecodes
                  ? "Decision (ADR-0008): the lag case is recoverable when a dictionary build"
                      + " exists in redo before the position; PRD-03 section 3 steps 4 and 5"
                      + " stand."
                  : "Decision (ADR-0008): redo-dictionary replay did not decode the pre-DDL rows on"
                      + " this version; 1.0 stops with DictionaryUnavailableException and"
                      + " documents coordinated DDL.")
          .assertUpToDate();
      assertThat(online).isNotEmpty();
      System.out.println(
          "dictionary-replay: redo dictionary decodes pre-DDL rows = "
              + replayDecodes
              + (replayError != null ? " error=" + replayError : ""));
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  /**
   * Adds every archived log from the one holding the dictionary build to the end SCN, then starts
   * with the redo dictionary.
   */
  private static void startWithRedoDictionary(Connection root, long fromScn, long endScn)
      throws SQLException {
    List<String> logs = new ArrayList<>();
    try (Statement s = root.createStatement();
        ResultSet rs =
            s.executeQuery(
                "SELECT name FROM v$archived_log WHERE dest_id = 1 AND name IS NOT NULL AND deleted"
                    + " = 'NO' AND next_change# >= (SELECT MAX(first_change#) FROM v$archived_log"
                    + " WHERE dictionary_begin = 'YES' AND first_change# <= "
                    + fromScn
                    + ")"
                    + " AND first_change# <= "
                    + endScn
                    + " ORDER BY sequence#")) {
      while (rs.next()) {
        logs.add(rs.getString(1));
      }
    }
    if (logs.isEmpty()) {
      throw new SQLException("no archived logs from the dictionary build to SCN " + endScn);
    }
    try (Statement st = root.createStatement()) {
      boolean first = true;
      for (String log : logs) {
        st.execute(
            "BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => '"
                + log.replace("'", "''")
                + "', OPTIONS => "
                + (first ? "DBMS_LOGMNR.NEW" : "DBMS_LOGMNR.ADDFILE")
                + "); END;");
        first = false;
      }
      st.execute(
          "BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN => "
              + fromScn
              + ", ENDSCN => "
              + endScn
              + ", OPTIONS => DBMS_LOGMNR.DICT_FROM_REDO_LOGS + DBMS_LOGMNR.DDL_DICT_TRACKING"
              + " + DBMS_LOGMNR.NO_ROWID_IN_STMT + DBMS_LOGMNR.NO_SQL_DELIMITER); END;");
    }
  }

  private static String columnNames(String sql) {
    if (sql == null) {
      return "(null)";
    }
    java.util.regex.Matcher m = java.util.regex.Pattern.compile("\"([A-Z_ 0-9]+)\"").matcher(sql);
    java.util.TreeSet<String> names = new java.util.TreeSet<>();
    while (m.find()) {
      String n = m.group(1);
      if (!n.startsWith("T_") && !"LAG".equals(n)) {
        names.add(n);
      }
    }
    return String.join(", ", names);
  }
}
