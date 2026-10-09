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
package sh.oso.connect.oracle.core.doctor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import sh.oso.connect.oracle.core.logs.RedoLog;
import sh.oso.connect.oracle.core.topology.ArchiveDestination;
import sh.oso.connect.oracle.core.topology.DatabaseInfo;
import sh.oso.connect.oracle.core.topology.JdbcCatalogSource;
import sh.oso.connect.oracle.core.topology.PdbInfo;
import sh.oso.connect.oracle.core.topology.ThreadInfo;

/**
 * {@link DoctorCatalog} over a metadata connection. Reads the CDB_ views so one session at CDB$ROOT
 * covers every PDB; in a non-CDB the same views behave like their DBA_ counterparts with CON_ID 0.
 * Exercised by the engine tier, excluded from the unit coverage gate.
 */
public final class JdbcDoctorCatalog implements DoctorCatalog {

  /** Fixed views the engine selects from; V$LOGMNR_CONTENTS is probed too (ORA-01306 is fine). */
  static final List<String> REQUIRED_VIEWS =
      List.of(
          "V$DATABASE",
          "V$LOG",
          "V$LOGFILE",
          "V$ARCHIVED_LOG",
          "V$ARCHIVE_DEST_STATUS",
          "V$THREAD",
          "V$TRANSACTION",
          "GV$TRANSACTION",
          "V$SESSION",
          "V$PROCESS",
          "V$LOGMNR_CONTENTS",
          "V$LOGMNR_LOGS",
          "V$LOGMNR_PARAMETERS",
          "V$PDBS",
          "V$VERSION",
          "V$PARAMETER");

  private final Connection c;
  private final JdbcCatalogSource base;
  private sh.oso.connect.oracle.core.topology.Platform platform;

  public JdbcDoctorCatalog(Connection metadataConnection) {
    this.c = metadataConnection;
    this.base = new JdbcCatalogSource(metadataConnection);
  }

  @Override
  public List<CapturedTable> capturedTables(
      List<Pattern> include, List<Pattern> exclude, List<String> pdbs) throws SQLException {
    List<CapturedTable> out = new ArrayList<>();
    String sql =
        "SELECT t.con_id, c.name AS pdb, t.owner, t.table_name, t.row_movement, t.iot_type"
            + " FROM cdb_tables t"
            + " JOIN cdb_users u ON u.username = t.owner AND u.con_id = t.con_id"
            + " LEFT JOIN v$containers c ON c.con_id = t.con_id"
            + " WHERE u.oracle_maintained = 'N' AND t.temporary = 'N' AND t.nested = 'NO'"
            + " AND (t.iot_type IS NULL OR t.iot_type = 'IOT')"
            + " ORDER BY t.con_id, t.owner, t.table_name";
    List<Object[]> candidates = new ArrayList<>();
    try (Statement s = c.createStatement();
        ResultSet rs = s.executeQuery(sql)) {
      while (rs.next()) {
        String pdb = rs.getString(2);
        if (pdb != null && !pdbs.isEmpty() && pdbs.stream().noneMatch(pdb::equalsIgnoreCase)) {
          continue;
        }
        if ("CDB$ROOT".equals(pdb)) {
          continue;
        }
        String fqn = (pdb == null ? "" : pdb + ".") + rs.getString(3) + "." + rs.getString(4);
        boolean in = include.isEmpty() || include.stream().anyMatch(p -> p.matcher(fqn).matches());
        boolean ex = exclude.stream().anyMatch(p -> p.matcher(fqn).matches());
        if (in && !ex) {
          candidates.add(
              new Object[] {
                rs.getInt(1),
                pdb,
                rs.getString(3),
                rs.getString(4),
                "ENABLED".equals(rs.getString(5)),
                rs.getString(6) != null
              });
        }
      }
    }
    for (Object[] t : candidates) {
      int conId = (Integer) t[0];
      String owner = (String) t[2];
      String name = (String) t[3];
      out.add(
          new CapturedTable(
              (String) t[1],
              owner,
              name,
              columns(conId, owner, name),
              hasLogGroup(conId, owner, name, "ALL COLUMN LOGGING"),
              hasLogGroup(conId, owner, name, "PRIMARY KEY LOGGING"),
              hasPrimaryKey(conId, owner, name),
              hasNotNullUniqueIndex(conId, owner, name),
              (Boolean) t[4],
              (Boolean) t[5]));
    }
    return out;
  }

  private List<CapturedTable.Column> columns(int conId, String owner, String table)
      throws SQLException {
    List<CapturedTable.Column> cols = new ArrayList<>();
    try (PreparedStatement ps =
        c.prepareStatement(
            "SELECT column_name, data_type, identity_column FROM cdb_tab_cols WHERE con_id = ?"
                + " AND owner = ? AND table_name = ? AND hidden_column = 'NO' AND virtual_column ="
                + " 'NO' ORDER BY column_id")) {
      ps.setInt(1, conId);
      ps.setString(2, owner);
      ps.setString(3, table);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          cols.add(
              new CapturedTable.Column(
                  rs.getString(1), rs.getString(2), "YES".equals(rs.getString(3))));
        }
      }
    }
    return cols;
  }

  private boolean hasLogGroup(int conId, String owner, String table, String type)
      throws SQLException {
    try (PreparedStatement ps =
        c.prepareStatement(
            "SELECT COUNT(*) FROM cdb_log_groups WHERE con_id = ? AND owner = ? AND table_name = ?"
                + " AND log_group_type = ?")) {
      ps.setInt(1, conId);
      ps.setString(2, owner);
      ps.setString(3, table);
      ps.setString(4, type);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() && rs.getInt(1) > 0;
      }
    }
  }

  private boolean hasPrimaryKey(int conId, String owner, String table) throws SQLException {
    try (PreparedStatement ps =
        c.prepareStatement(
            "SELECT COUNT(*) FROM cdb_constraints WHERE con_id = ? AND owner = ? AND table_name ="
                + " ? AND constraint_type = 'P' AND status = 'ENABLED'")) {
      ps.setInt(1, conId);
      ps.setString(2, owner);
      ps.setString(3, table);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() && rs.getInt(1) > 0;
      }
    }
  }

  private boolean hasNotNullUniqueIndex(int conId, String owner, String table) throws SQLException {
    try (PreparedStatement ps =
        c.prepareStatement(
            "SELECT COUNT(*) FROM cdb_indexes i WHERE i.con_id = ? AND i.table_owner = ? AND"
                + " i.table_name = ? AND i.uniqueness = 'UNIQUE' AND i.status = 'VALID' AND NOT"
                + " EXISTS (SELECT 1 FROM cdb_ind_columns ic JOIN cdb_tab_cols tc ON tc.con_id ="
                + " ic.con_id AND tc.owner = ic.table_owner AND tc.table_name = ic.table_name AND"
                + " tc.column_name = ic.column_name WHERE ic.con_id = i.con_id AND ic.index_owner"
                + " = i.owner AND ic.index_name = i.index_name AND tc.nullable = 'Y')")) {
      ps.setInt(1, conId);
      ps.setString(2, owner);
      ps.setString(3, table);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() && rs.getInt(1) > 0;
      }
    }
  }

  @Override
  public List<String> grantedPrivileges() throws SQLException {
    List<String> out = new ArrayList<>();
    try (Statement s = c.createStatement();
        ResultSet rs =
            s.executeQuery(
                "SELECT privilege FROM session_privs UNION ALL SELECT role FROM session_roles")) {
      while (rs.next()) {
        out.add(rs.getString(1));
      }
    }
    return out;
  }

  @Override
  public List<String> inaccessibleViews() {
    List<String> out = new ArrayList<>();
    for (String v : REQUIRED_VIEWS) {
      try (Statement s = c.createStatement();
          ResultSet rs = s.executeQuery("SELECT 1 FROM " + v + " WHERE ROWNUM = 0")) {
        rs.next();
      } catch (SQLException e) {
        if (e.getErrorCode() == 942 || e.getErrorCode() == 1031) {
          out.add(v);
        }
        // ORA-01306 (no LogMiner session) and friends mean the view itself is readable
      }
    }
    return out;
  }

  @Override
  public sh.oso.connect.oracle.core.topology.Platform platform() throws SQLException {
    if (platform == null) {
      platform = sh.oso.connect.oracle.core.topology.Platform.detect(c);
    }
    return platform;
  }

  @Override
  public String rdsConfiguration(String name) throws SQLException {
    if (platform() != sh.oso.connect.oracle.core.topology.Platform.RDS) {
      return null;
    }
    try (PreparedStatement ps =
        c.prepareStatement("SELECT value FROM rdsadmin.rds_configuration WHERE name = ?")) {
      ps.setString(1, name);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getString(1) : null;
      }
    } catch (SQLException e) {
      if (e.getErrorCode() == 942 || e.getErrorCode() == 1031) {
        return null; // readable by the master user; the capture user usually cannot
      }
      throw e;
    }
  }

  @Override
  public boolean containerDataAll() throws SQLException {
    if (!base.database().cdb()) {
      return true;
    }
    int all = count("SELECT COUNT(*) FROM dba_pdbs WHERE pdb_name <> 'PDB$SEED'");
    int visible;
    try {
      visible = count("SELECT COUNT(*) FROM v$pdbs WHERE con_id > 2");
    } catch (SQLException e) {
      if (e.getErrorCode() == 942) {
        return false;
      }
      throw e;
    }
    return visible >= all;
  }

  @Override
  public boolean commonUser() throws SQLException {
    try (Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("SELECT common FROM dba_users WHERE username = USER")) {
      if (rs.next()) {
        return "YES".equals(rs.getString(1));
      }
    } catch (SQLException e) {
      if (e.getErrorCode() != 942) {
        throw e;
      }
    }
    return count("SELECT COUNT(*) FROM dual WHERE USER LIKE 'C##%'") > 0;
  }

  private int count(String sql) throws SQLException {
    try (Statement s = c.createStatement();
        ResultSet rs = s.executeQuery(sql)) {
      return rs.next() ? rs.getInt(1) : 0;
    }
  }

  @Override
  public DatabaseInfo database() throws SQLException {
    return base.database();
  }

  @Override
  public long currentScn() throws SQLException {
    return base.currentScn();
  }

  @Override
  public List<ThreadInfo> threads() throws SQLException {
    return base.threads();
  }

  @Override
  public List<PdbInfo> pdbs() throws SQLException {
    return base.pdbs();
  }

  @Override
  public List<ArchiveDestination> archiveDestinations() throws SQLException {
    return base.archiveDestinations();
  }

  @Override
  public List<RedoLog> onlineLogs() throws SQLException {
    return base.onlineLogs();
  }

  @Override
  public List<RedoLog> archivedLogs(long startScn, long endScn, int destId) throws SQLException {
    return base.archivedLogs(startScn, endScn, destId);
  }

  @Override
  public List<RedoLog> archivedSince(Instant since, int destId) throws SQLException {
    return base.archivedSince(since, destId);
  }

  @Override
  public List<RedoLog> dictionaryLogs(int destId) throws SQLException {
    return base.dictionaryLogs(destId);
  }

  @Override
  public List<ArchiveStat> archiveHistory(Instant since, int destId) throws SQLException {
    List<ArchiveStat> out = new ArrayList<>();
    try (PreparedStatement ps =
        c.prepareStatement(
            "SELECT thread#, sequence#, first_change#, next_change#, first_time, next_time,"
                + " blocks * block_size, deleted, status FROM v$archived_log WHERE dest_id = ?"
                + " AND standby_dest = 'NO' AND next_time >= ? ORDER BY thread#, sequence#")) {
      ps.setInt(1, destId);
      ps.setTimestamp(2, Timestamp.from(since));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          Timestamp first = rs.getTimestamp(5);
          Timestamp next = rs.getTimestamp(6);
          String status = rs.getString(9);
          out.add(
              new ArchiveStat(
                  rs.getInt(1),
                  rs.getLong(2),
                  rs.getLong(3),
                  rs.getLong(4),
                  first == null ? null : first.toInstant(),
                  next == null ? null : next.toInstant(),
                  rs.getLong(7),
                  "YES".equals(rs.getString(8)) || "D".equals(status) || "X".equals(status)));
        }
      }
    }
    return out;
  }

  @Override
  public List<OnlineLogGroup> onlineLogGroups() throws SQLException {
    List<OnlineLogGroup> out = new ArrayList<>();
    try (Statement s = c.createStatement();
        ResultSet rs =
            s.executeQuery(
                "SELECT thread#, group#, bytes, status FROM v$log ORDER BY thread#, group#")) {
      while (rs.next()) {
        out.add(new OnlineLogGroup(rs.getInt(1), rs.getInt(2), rs.getLong(3), rs.getString(4)));
      }
    }
    return out;
  }

  @Override
  public String parameter(String name) throws SQLException {
    try (PreparedStatement ps =
        c.prepareStatement("SELECT value FROM v$parameter WHERE name = ?")) {
      ps.setString(1, name);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getString(1) : null;
      }
    }
  }

  @Override
  public int fixedTablesWithStatistics() throws SQLException {
    try {
      return count(
          "SELECT COUNT(*) FROM dba_tab_statistics WHERE object_type = 'FIXED TABLE' AND"
              + " last_analyzed IS NOT NULL");
    } catch (SQLException e) {
      if (e.getErrorCode() == 942) {
        return -1;
      }
      throw e;
    }
  }

  @Override
  public List<PdbState> pdbStates() throws SQLException {
    List<PdbState> out = new ArrayList<>();
    try (Statement st = c.createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT p.name, p.open_mode, CASE WHEN EXISTS (SELECT 1 FROM dba_pdb_saved_states"
                    + " s WHERE s.con_name = p.name) THEN 1 ELSE 0 END FROM v$pdbs p WHERE p.name"
                    + " <> 'PDB$SEED' ORDER BY p.name")) {
      while (rs.next()) {
        out.add(new PdbState(rs.getString(1), rs.getString(2), rs.getInt(3) == 1));
      }
      return out;
    } catch (SQLException e) {
      if (e.getErrorCode() == 942) {
        return null;
      }
      throw e;
    }
  }

  @Override
  public boolean canExecute(String owner, String name) throws SQLException {
    // ALL_OBJECTS lists a package only when the session may execute it, directly, through an
    // enabled role or through EXECUTE ANY PROCEDURE
    try (PreparedStatement ps =
        c.prepareStatement(
            "SELECT COUNT(*) FROM all_objects WHERE owner = ? AND object_name = ? AND object_type"
                + " = 'PACKAGE'")) {
      ps.setString(1, owner);
      ps.setString(2, name);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() && rs.getInt(1) > 0;
      }
    }
  }
}
