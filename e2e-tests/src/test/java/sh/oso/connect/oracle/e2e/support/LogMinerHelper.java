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
package sh.oso.connect.oracle.e2e.support;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Minimal LogMiner driver for the spikes: archive everything, add the archived logs that cover an
 * SCN range, start with the online catalog, fetch rows as maps, end. The engine proper lives in
 * {@code oracle-cdc-core}; this is deliberately dumb and transparent.
 */
public final class LogMinerHelper {

  private LogMinerHelper() {}

  /** Columns fetched for every spike row. Missing columns are reported, not fatal. */
  public static final List<String> COLUMNS =
      List.of(
          "SCN",
          "START_SCN",
          "COMMIT_SCN",
          "TIMESTAMP",
          "COMMIT_TIMESTAMP",
          "THREAD#",
          "XIDUSN",
          "XIDSLT",
          "XIDSQN",
          "OPERATION",
          "OPERATION_CODE",
          "ROLLBACK",
          "STATUS",
          "INFO",
          "SEG_OWNER",
          "SEG_NAME",
          "TABLE_NAME",
          "USERNAME",
          "SESSION#",
          "SERIAL#",
          "CLIENT_ID",
          "ROW_ID",
          "RS_ID",
          "SSN",
          "CSF",
          "DATA_OBJ#",
          "DATA_OBJD#",
          "DATA_OBJV#",
          "OBJECT_ID",
          "CON_ID",
          "SRC_CON_ID",
          "SRC_CON_NAME",
          "SRC_CON_DBID",
          "SQL_REDO",
          "SQL_UNDO");

  public static long currentScn(Connection c) throws SQLException {
    return OracleSql.currentScn(c);
  }

  /** Adds every archived log (dest 1, thread 1) overlapping the range and starts a session. */
  public static void start(Connection root, long startScn, long endScn) throws SQLException {
    List<String> logs = new ArrayList<>();
    try (PreparedStatement ps =
        root.prepareStatement(
            "SELECT name FROM v$archived_log WHERE dest_id = 1 AND name IS NOT NULL AND"
                + " next_change# >= ? AND first_change# <= ? AND deleted = 'NO' ORDER BY"
                + " sequence#")) {
      ps.setLong(1, startScn);
      ps.setLong(2, endScn);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          logs.add(rs.getString(1));
        }
      }
    }
    if (logs.isEmpty()) {
      throw new IllegalStateException("no archived logs cover SCN " + startScn + " to " + endScn);
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
              + startScn
              + ", ENDSCN => "
              + endScn
              + ", OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.NO_ROWID_IN_STMT"
              + " + DBMS_LOGMNR.NO_SQL_DELIMITER); END;");
    }
  }

  /**
   * Like {@link #start} but also adds the online redo log members whose range overlaps, so redo
   * that is not archived yet can be mined (the engine's online mode).
   */
  public static void startWithOnline(Connection root, long startScn, long endScn)
      throws SQLException {
    List<String> logs = new ArrayList<>();
    try (PreparedStatement ps =
        root.prepareStatement(
            "SELECT name FROM v$archived_log WHERE dest_id = 1 AND name IS NOT NULL AND"
                + " next_change# >= ? AND first_change# <= ? AND deleted = 'NO' ORDER BY"
                + " sequence#")) {
      ps.setLong(1, startScn);
      ps.setLong(2, endScn);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          logs.add(rs.getString(1));
        }
      }
    }
    try (PreparedStatement ps =
        root.prepareStatement(
            "SELECT f.member FROM v$log l JOIN v$logfile f ON f.group# = l.group# WHERE"
                + " l.status IN ('CURRENT', 'ACTIVE') AND l.first_change# <= ? AND l.archived ="
                + " 'NO' ORDER BY l.sequence#, f.member")) {
      ps.setLong(1, endScn);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          logs.add(rs.getString(1));
        }
      }
    }
    if (logs.isEmpty()) {
      throw new IllegalStateException("no logs cover SCN " + startScn + " to " + endScn);
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
              + startScn
              + ", ENDSCN => "
              + endScn
              + ", OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.NO_ROWID_IN_STMT"
              + " + DBMS_LOGMNR.NO_SQL_DELIMITER); END;");
    }
  }

  public static void end(Connection root) throws SQLException {
    try (Statement st = root.createStatement()) {
      st.execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;");
    }
  }

  /** Fetches rows matching the WHERE clause as ordered column maps (string values). */
  public static List<Map<String, String>> rows(Connection root, String where) throws SQLException {
    List<Map<String, String>> out = new ArrayList<>();
    String sql = "SELECT " + String.join(", ", COLUMNS) + " FROM v$logmnr_contents WHERE " + where;
    try (Statement st = root.createStatement();
        ResultSet rs = st.executeQuery(sql)) {
      ResultSetMetaData md = rs.getMetaData();
      while (rs.next()) {
        Map<String, String> row = new LinkedHashMap<>();
        for (int i = 1; i <= md.getColumnCount(); i++) {
          row.put(md.getColumnLabel(i), rs.getString(i));
        }
        out.add(row);
      }
    }
    return out;
  }
}
