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
package sh.oso.connect.oracle.core.mining;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import sh.oso.connect.oracle.core.logs.RedoLog;
import sh.oso.connect.oracle.core.model.Xid;

/**
 * {@link LogMinerSource} over DBMS_LOGMNR. Options follow CORE-MINE-1: online catalog,
 * NO_ROWID_IN_STMT, NO_SQL_DELIMITER, never COMMITTED_DATA_ONLY or SKIP_CORRUPTION. JDBC-bound;
 * proven by the engine tier and excluded from the unit coverage gate.
 */
public final class JdbcLogMinerSession implements LogMinerSource {

  private static final String ADD_NEW =
      "BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => ?, OPTIONS => DBMS_LOGMNR.NEW); END;";
  private static final String ADD_MORE =
      "BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => ?, OPTIONS => DBMS_LOGMNR.ADDFILE); END;";
  private static final String START_ONLINE =
      "BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN => ?, ENDSCN => ?, OPTIONS =>"
          + " DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.NO_ROWID_IN_STMT"
          + " + DBMS_LOGMNR.NO_SQL_DELIMITER); END;";
  private static final String START_REDO_DICT =
      "BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN => ?, ENDSCN => ?, OPTIONS =>"
          + " DBMS_LOGMNR.DICT_FROM_REDO_LOGS + DBMS_LOGMNR.DDL_DICT_TRACKING"
          + " + DBMS_LOGMNR.NO_ROWID_IN_STMT + DBMS_LOGMNR.NO_SQL_DELIMITER); END;";
  private static final String END = "BEGIN DBMS_LOGMNR.END_LOGMNR; END;";
  private static final String PGA =
      "SELECT p.pga_used_mem FROM v$process p JOIN v$session s ON s.paddr = p.addr"
          + " WHERE s.sid = SYS_CONTEXT('USERENV', 'SID')";

  private final Connection c;
  private final int fetchSize;
  private final int queryTimeoutSeconds;
  private final Set<String> added = new LinkedHashSet<>();
  private boolean started;

  public JdbcLogMinerSession(Connection miningConnection, int fetchSize, Duration queryTimeout) {
    this.c = miningConnection;
    this.fetchSize = Math.max(1, fetchSize);
    this.queryTimeoutSeconds = (int) Math.max(0, queryTimeout.toSeconds());
  }

  @Override
  public void setLogs(List<RedoLog> logs) throws SQLException {
    Set<String> wanted = new LinkedHashSet<>();
    for (RedoLog l : logs) {
      wanted.add(l.path());
    }
    if (wanted.equals(added)) {
      return;
    }
    if (started) {
      end();
    }
    boolean first = true;
    for (String path : wanted) {
      try (CallableStatement cs = c.prepareCall(first ? ADD_NEW : ADD_MORE)) {
        cs.setString(1, path);
        cs.execute();
      }
      first = false;
    }
    added.clear();
    added.addAll(wanted);
  }

  @Override
  public void start(long startScn, long endScn, DictionaryMode mode) throws SQLException {
    try (CallableStatement cs =
        c.prepareCall(mode == DictionaryMode.ONLINE_CATALOG ? START_ONLINE : START_REDO_DICT)) {
      cs.setLong(1, startScn);
      cs.setLong(2, endScn);
      cs.execute();
    }
    started = true;
  }

  @Override
  public RowCursor query(MiningFilter filter, long startScn, long endScn) throws SQLException {
    PreparedStatement ps = c.prepareStatement(LogMinerQuery.sql(filter));
    try {
      ps.setFetchSize(fetchSize);
      if (queryTimeoutSeconds > 0) {
        ps.setQueryTimeout(queryTimeoutSeconds);
      }
      ps.setLong(1, startScn);
      ps.setLong(2, endScn);
      ResultSet rs = ps.executeQuery();
      return new JdbcRowCursor(ps, rs);
    } catch (SQLException | RuntimeException e) {
      ps.close();
      throw e;
    }
  }

  @Override
  public void end() throws SQLException {
    try (Statement s = c.createStatement()) {
      s.execute(END);
    } catch (SQLException e) {
      if (e.getErrorCode() != 1307) { // no LogMiner session is currently active
        throw e;
      }
    } finally {
      started = false;
    }
  }

  @Override
  public void reset() throws SQLException {
    try {
      end();
    } finally {
      added.clear();
    }
  }

  @Override
  public long pgaUsedBytes() throws SQLException {
    try (Statement s = c.createStatement();
        ResultSet rs = s.executeQuery(PGA)) {
      return rs.next() ? rs.getLong(1) : -1;
    } catch (SQLException e) {
      if (e.getErrorCode() == 942) {
        return -1;
      }
      throw e;
    }
  }

  @Override
  public void close() throws SQLException {
    try {
      end();
    } finally {
      c.close();
    }
  }

  static LogMinerRow map(ResultSet rs) throws SQLException {
    int i = 1;
    long scn = rs.getLong(i++);
    long startScn = rs.getLong(i++);
    long commitScn = rs.getLong(i++);
    Timestamp ts = rs.getTimestamp(i++);
    Timestamp cts = rs.getTimestamp(i++);
    int thread = rs.getInt(i++);
    Xid xid = new Xid(rs.getLong(i++), rs.getLong(i++), rs.getLong(i++));
    String operation = rs.getString(i++);
    int code = rs.getInt(i++);
    boolean rollback = rs.getInt(i++) == 1;
    int status = rs.getInt(i++);
    String info = rs.getString(i++);
    String segOwner = rs.getString(i++);
    String segName = rs.getString(i++);
    String tableName = rs.getString(i++);
    String username = rs.getString(i++);
    long sessionNo = rs.getLong(i++);
    long serialNo = rs.getLong(i++);
    String clientId = rs.getString(i++);
    String rowId = rs.getString(i++);
    String rsId = rs.getString(i++);
    long ssn = rs.getLong(i++);
    boolean csf = rs.getInt(i++) == 1;
    long dataObj = rs.getLong(i++);
    long dataObjd = rs.getLong(i++);
    long dataObjv = rs.getLong(i++);
    int srcConId = rs.getInt(i++);
    String srcConName = rs.getString(i++);
    long srcConDbid = rs.getLong(i++);
    int conId = rs.getInt(i++);
    String sqlRedo = rs.getString(i++);
    String sqlUndo = rs.getString(i);
    return new LogMinerRow(
        scn,
        startScn,
        commitScn,
        ts == null ? null : ts.toInstant(),
        cts == null ? null : cts.toInstant(),
        thread,
        xid,
        operation,
        code,
        rollback,
        status,
        info,
        segOwner,
        segName,
        tableName,
        username,
        sessionNo,
        serialNo,
        clientId,
        rowId,
        rsId,
        ssn,
        csf,
        dataObj,
        dataObjd,
        dataObjv,
        srcConId,
        srcConName,
        srcConDbid,
        conId,
        sqlRedo,
        sqlUndo);
  }

  private static final class JdbcRowCursor implements RowCursor {
    private final PreparedStatement ps;
    private final ResultSet rs;
    private LogMinerRow current;

    JdbcRowCursor(PreparedStatement ps, ResultSet rs) {
      this.ps = ps;
      this.rs = rs;
    }

    @Override
    public boolean next() throws SQLException {
      if (rs.next()) {
        current = map(rs);
        return true;
      }
      current = null;
      return false;
    }

    @Override
    public LogMinerRow row() {
      return current;
    }

    @Override
    public void cancel() throws SQLException {
      ps.cancel();
    }

    @Override
    public void close() throws SQLException {
      try {
        rs.close();
      } finally {
        ps.close();
      }
    }
  }
}
