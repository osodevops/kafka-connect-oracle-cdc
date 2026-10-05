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
package sh.oso.connect.oracle.core.topology;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import sh.oso.connect.oracle.core.logs.RedoLog;

/** {@link CatalogSource} over a metadata connection at CDB$ROOT (or the non-CDB root). */
public final class JdbcCatalogSource implements CatalogSource {

  private final java.util.function.Supplier<Connection> conn;

  public JdbcCatalogSource(Connection metadataConnection) {
    this(() -> metadataConnection);
  }

  /** Reads the connection on every call so the owner can reconnect underneath (CORE-CONN-6). */
  public JdbcCatalogSource(java.util.function.Supplier<Connection> metadataConnection) {
    this.conn = metadataConnection;
  }

  private Connection c() {
    return conn.get();
  }

  @Override
  public DatabaseInfo database() throws SQLException {
    try (Statement st = c().createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT d.dbid, d.name, d.cdb, d.log_mode, d.open_mode, d.database_role,"
                    + " i.version_full, d.resetlogs_change#, d.supplemental_log_data_min,"
                    + " d.platform_name FROM v$database d CROSS JOIN v$instance i")) {
      rs.next();
      return new DatabaseInfo(
          rs.getLong(1),
          rs.getString(2),
          "YES".equals(rs.getString(3)),
          rs.getString(4),
          rs.getString(5),
          rs.getString(6),
          rs.getString(7),
          rs.getLong(8),
          "YES".equals(rs.getString(9)),
          rs.getString(10));
    }
  }

  @Override
  public long currentScn() throws SQLException {
    try (Statement st = c().createStatement();
        ResultSet rs = st.executeQuery("SELECT current_scn FROM v$database")) {
      rs.next();
      return rs.getLong(1);
    }
  }

  @Override
  public List<ThreadInfo> threads() throws SQLException {
    List<ThreadInfo> out = new ArrayList<>();
    try (Statement st = c().createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT thread#, enabled, status, sequence# FROM v$thread ORDER BY thread#")) {
      while (rs.next()) {
        out.add(
            new ThreadInfo(
                rs.getInt(1), !"DISABLED".equals(rs.getString(2)), rs.getString(3), rs.getLong(4)));
      }
    }
    return out;
  }

  @Override
  public List<PdbInfo> pdbs() throws SQLException {
    List<PdbInfo> out = new ArrayList<>();
    try (Statement st = c().createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT con_id, name, open_mode, dbid FROM v$pdbs WHERE con_id > 2 ORDER BY"
                    + " con_id")) {
      while (rs.next()) {
        out.add(new PdbInfo(rs.getInt(1), rs.getString(2), rs.getString(3), rs.getLong(4)));
      }
    }
    return out;
  }

  @Override
  public List<ArchiveDestination> archiveDestinations() throws SQLException {
    List<ArchiveDestination> out = new ArrayList<>();
    try (Statement st = c().createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT dest_id, dest_name, destination, status, type, archived_thread# FROM"
                    + " v$archive_dest_status WHERE status <> 'INACTIVE' ORDER BY dest_id")) {
      while (rs.next()) {
        out.add(
            new ArchiveDestination(
                rs.getInt(1),
                rs.getString(2),
                rs.getString(3),
                rs.getString(4),
                rs.getString(5),
                rs.getString(6)));
      }
    }
    return out;
  }

  @Override
  public List<RedoLog> onlineLogs() throws SQLException {
    List<RedoLog> out = new ArrayList<>();
    try (Statement st = c().createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT l.thread#, l.sequence#, l.first_change#, l.next_change#, f.member,"
                    + " l.status, l.archived FROM v$log l JOIN v$logfile f ON f.group# = l.group#"
                    + " AND f.type = 'ONLINE' WHERE f.status IS NULL OR f.status NOT IN ('INVALID',"
                    + " 'DELETED') ORDER BY l.thread#, l.sequence#")) {
      long lastGroupSeq = -1;
      while (rs.next()) {
        long seq = rs.getLong(2);
        if (seq == lastGroupSeq) {
          continue; // one member per group is enough
        }
        lastGroupSeq = seq;
        long next =
            rs.getObject(4) == null || "CURRENT".equals(rs.getString(6))
                ? Long.MAX_VALUE
                : rs.getLong(4);
        out.add(
            new RedoLog(
                rs.getInt(1),
                seq,
                rs.getLong(3),
                next,
                rs.getString(5),
                false,
                rs.getString(6),
                false,
                0,
                false,
                false));
      }
    }
    return out;
  }

  @Override
  public List<RedoLog> archivedLogs(long startScn, long endScn, int destId) throws SQLException {
    List<RedoLog> out = new ArrayList<>();
    try (PreparedStatement ps =
        c().prepareStatement(
                "SELECT thread#, sequence#, first_change#, next_change#, name, status, deleted,"
                    + " dest_id, dictionary_begin, dictionary_end FROM v$archived_log WHERE dest_id"
                    + " = ? AND next_change# > ? AND first_change# <= ? AND standby_dest = 'NO'"
                    + " ORDER BY thread#, sequence#")) {
      ps.setInt(1, destId);
      ps.setLong(2, startScn);
      ps.setLong(3, endScn);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          out.add(
              new RedoLog(
                  rs.getInt(1),
                  rs.getLong(2),
                  rs.getLong(3),
                  rs.getLong(4),
                  rs.getString(5),
                  true,
                  rs.getString(6),
                  "YES".equals(rs.getString(7)),
                  rs.getInt(8),
                  "YES".equals(rs.getString(9)),
                  "YES".equals(rs.getString(10))));
        }
      }
    }
    return out;
  }

  @Override
  public List<RedoLog> dictionaryLogs(int destId) throws SQLException {
    List<RedoLog> out = new ArrayList<>();
    try (PreparedStatement ps =
        c().prepareStatement(
                "SELECT thread#, sequence#, first_change#, next_change#, name, status, deleted,"
                    + " dest_id, dictionary_begin, dictionary_end FROM v$archived_log WHERE dest_id"
                    + " = ? AND (dictionary_begin = 'YES' OR dictionary_end = 'YES') AND"
                    + " standby_dest = 'NO' ORDER BY thread#, sequence#")) {
      ps.setInt(1, destId);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          out.add(
              new RedoLog(
                  rs.getInt(1),
                  rs.getLong(2),
                  rs.getLong(3),
                  rs.getLong(4),
                  rs.getString(5),
                  true,
                  rs.getString(6),
                  "YES".equals(rs.getString(7)),
                  rs.getInt(8),
                  "YES".equals(rs.getString(9)),
                  "YES".equals(rs.getString(10))));
        }
      }
    }
    return out;
  }

  @Override
  public List<RedoLog> archivedSince(Instant since, int destId) throws SQLException {
    List<RedoLog> out = new ArrayList<>();
    try (PreparedStatement ps =
        c().prepareStatement(
                "SELECT thread#, sequence#, first_change#, next_change#, name, status, deleted,"
                    + " dest_id, dictionary_begin, dictionary_end FROM v$archived_log WHERE dest_id"
                    + " = ? AND completion_time >= ? ORDER BY thread#, sequence#")) {
      ps.setInt(1, destId);
      ps.setTimestamp(2, Timestamp.from(since));
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          out.add(
              new RedoLog(
                  rs.getInt(1),
                  rs.getLong(2),
                  rs.getLong(3),
                  rs.getLong(4),
                  rs.getString(5),
                  true,
                  rs.getString(6),
                  "YES".equals(rs.getString(7)),
                  rs.getInt(8),
                  "YES".equals(rs.getString(9)),
                  "YES".equals(rs.getString(10))));
        }
      }
    }
    return out;
  }
}
