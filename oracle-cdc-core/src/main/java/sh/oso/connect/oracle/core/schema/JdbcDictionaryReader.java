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
package sh.oso.connect.oracle.core.schema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import sh.oso.connect.oracle.core.model.TableId;

/**
 * {@link DictionaryReader} over the CDB_ views from a CDB$ROOT (or non-CDB) session. JDBC-bound;
 * proven by the engine tier and excluded from the unit coverage gate.
 */
public final class JdbcDictionaryReader implements DictionaryReader {

  /**
   * Tables per query of {@link #readAll}: two binds each, well inside Oracle's limit of 1,000
   * expressions in a list.
   */
  static final int READ_ALL_CHUNK = 500;

  private final java.util.function.Supplier<Connection> conn;
  private final int chunk;

  public JdbcDictionaryReader(Connection metadataConnection) {
    this(() -> metadataConnection);
  }

  /** Reads the connection on every call so the owner can reconnect underneath (CORE-CONN-6). */
  public JdbcDictionaryReader(java.util.function.Supplier<Connection> metadataConnection) {
    this(metadataConnection, READ_ALL_CHUNK);
  }

  /** With {@code chunk} tables per query of {@link #readAll} (tests). */
  JdbcDictionaryReader(java.util.function.Supplier<Connection> metadataConnection, int chunk) {
    this.conn = metadataConnection;
    this.chunk = chunk;
  }

  private Connection c() {
    return conn.get();
  }

  private int conId(TableId t) throws SQLException {
    if (t.pdb() == null) {
      return 0;
    }
    try (PreparedStatement ps =
        c().prepareStatement("SELECT con_id FROM v$containers WHERE UPPER(name) = UPPER(?)")) {
      ps.setString(1, t.pdb());
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          throw new SQLException("no container named " + t.pdb());
        }
        return rs.getInt(1);
      }
    }
  }

  @Override
  public Optional<java.time.Instant> lastDdlTime(TableId t) throws SQLException {
    try (PreparedStatement ps =
        c().prepareStatement(
                "SELECT last_ddl_time FROM cdb_objects WHERE con_id = ? AND owner = ? AND"
                    + " object_name = ? AND object_type = 'TABLE'")) {
      ps.setInt(1, conId(t));
      ps.setString(2, t.schema());
      ps.setString(3, t.table());
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() && rs.getTimestamp(1) != null
            ? Optional.of(rs.getTimestamp(1).toInstant())
            : Optional.empty();
      }
    }
  }

  @Override
  public Optional<Long> scnAt(java.time.Instant time) throws SQLException {
    try (PreparedStatement ps =
            c().prepareStatement("SELECT SYSDATE, current_scn FROM v$database");
        ResultSet rs = ps.executeQuery()) {
      rs.next();
      if (!time.isBefore(rs.getTimestamp(1).toInstant())) {
        return Optional.of(rs.getLong(2)); // not reached yet: valid from now at the earliest
      }
    }
    try (PreparedStatement ps = c().prepareStatement("SELECT TIMESTAMP_TO_SCN(?) FROM dual")) {
      ps.setTimestamp(1, java.sql.Timestamp.from(time));
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? Optional.of(rs.getLong(1)) : Optional.empty();
      }
    } catch (SQLException e) {
      if (e.getErrorCode() == 8180) {
        return Optional.empty(); // older than the SCN-to-time mapping keeps
      }
      throw e;
    }
  }

  @Override
  public Optional<java.time.Instant> timeOfScn(long scn) throws SQLException {
    try (PreparedStatement ps = c().prepareStatement("SELECT SCN_TO_TIMESTAMP(?) FROM dual")) {
      ps.setLong(1, scn);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? Optional.of(rs.getTimestamp(1).toInstant()) : Optional.empty();
      }
    } catch (SQLException e) {
      if (e.getErrorCode() == 8181) {
        return Optional.empty(); // older than the SCN-to-time mapping keeps
      }
      throw e;
    }
  }

  @Override
  public Optional<TableSchema> read(TableId t) throws SQLException {
    int con = conId(t);
    List<ColumnSpec> cols = new ArrayList<>();
    try (PreparedStatement ps =
        c().prepareStatement(
                "SELECT column_name, column_id, data_type, data_length, data_precision, data_scale,"
                    + " nullable FROM cdb_tab_cols WHERE con_id = ? AND owner = ? AND table_name ="
                    + " ? AND hidden_column = 'NO' AND virtual_column = 'NO' ORDER BY column_id")) {
      ps.setInt(1, con);
      ps.setString(2, t.schema());
      ps.setString(3, t.table());
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          cols.add(column(rs, 1));
        }
      }
    }
    if (cols.isEmpty()) {
      return Optional.empty();
    }
    boolean all = hasLogGroup(con, t, "ALL COLUMN LOGGING");
    boolean pk = hasLogGroup(con, t, "PRIMARY KEY LOGGING");
    return Optional.of(new TableSchema(t, cols, List.of(), KeySource.NONE, all, pk));
  }

  /**
   * One CDB_TAB_COLS row from column {@code first} on: name, id, type, length, precision, scale and
   * nullability.
   */
  private static ColumnSpec column(ResultSet rs, int first) throws SQLException {
    String type = rs.getString(first + 2);
    return new ColumnSpec(
        rs.getString(first),
        rs.getInt(first + 1),
        OracleType.fromDictionary(type),
        type,
        rs.getInt(first + 3),
        rs.getObject(first + 4) == null ? -1 : rs.getInt(first + 4),
        rs.getObject(first + 5) == null ? -1 : rs.getInt(first + 5),
        "Y".equals(rs.getString(first + 6)));
  }

  /**
   * ADR-0016 amendment, the read at start: per container one lookup of its id, then for every
   * {@link #READ_ALL_CHUNK} tables five queries (columns, supplemental log groups, primary key,
   * unique indexes, and last DDL times last), where the per-table methods take about a dozen round
   * trips for each table. The predicates are those of {@link #read}, {@link #keyCandidates} and
   * {@link #lastDdlTime}, so both paths give the same layout.
   */
  @Override
  public Map<TableId, Layout> readAll(Collection<TableId> tables) throws SQLException {
    Map<String, List<TableId>> byContainer = new LinkedHashMap<>();
    for (TableId t : new LinkedHashSet<>(tables)) {
      byContainer.computeIfAbsent(Objects.toString(t.pdb(), ""), k -> new ArrayList<>()).add(t);
    }
    Map<TableId, Layout> out = new LinkedHashMap<>();
    for (List<TableId> group : byContainer.values()) {
      int con = conId(group.get(0));
      for (int from = 0; from < group.size(); from += chunk) {
        readChunk(con, group.subList(from, Math.min(group.size(), from + chunk)), out);
      }
    }
    return out;
  }

  private void readChunk(int con, List<TableId> tables, Map<TableId, Layout> out)
      throws SQLException {
    Map<Name, TableId> byName = new HashMap<>();
    for (TableId t : tables) {
      byName.put(key(t.schema(), t.table()), t);
    }
    String in = String.join(", ", Collections.nCopies(tables.size(), "(?, ?)"));
    Map<TableId, List<ColumnSpec>> columns = new HashMap<>();
    try (PreparedStatement ps =
        c().prepareStatement(
                "SELECT owner, table_name, column_name, column_id, data_type, data_length,"
                    + " data_precision, data_scale, nullable FROM cdb_tab_cols WHERE con_id = ? AND"
                    + " hidden_column = 'NO' AND virtual_column = 'NO' AND (owner, table_name) IN ("
                    + in
                    + ") ORDER BY owner, table_name, column_id")) {
      bind(ps, con, tables);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          TableId t = byName.get(key(rs.getString(1), rs.getString(2)));
          if (t != null) {
            columns.computeIfAbsent(t, k -> new ArrayList<>()).add(column(rs, 3));
          }
        }
      }
    }
    Set<TableId> allColumns = new HashSet<>();
    Set<TableId> primaryKeyColumns = new HashSet<>();
    try (PreparedStatement ps =
        c().prepareStatement(
                "SELECT owner, table_name, log_group_type FROM cdb_log_groups WHERE con_id = ? AND"
                    + " (owner, table_name) IN ("
                    + in
                    + ") AND log_group_type IN ('ALL COLUMN LOGGING', 'PRIMARY KEY LOGGING')")) {
      bind(ps, con, tables);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          TableId t = byName.get(key(rs.getString(1), rs.getString(2)));
          if (t != null) {
            ("ALL COLUMN LOGGING".equals(rs.getString(3)) ? allColumns : primaryKeyColumns).add(t);
          }
        }
      }
    }
    Map<TableId, List<String>> primaryKeys = new HashMap<>();
    try (PreparedStatement ps =
        c().prepareStatement(
                "SELECT k.owner, k.table_name, cc.column_name FROM cdb_constraints k JOIN"
                    + " cdb_cons_columns cc ON cc.con_id = k.con_id AND cc.owner = k.owner AND"
                    + " cc.constraint_name = k.constraint_name WHERE k.con_id = ? AND (k.owner,"
                    + " k.table_name) IN ("
                    + in
                    + ") AND k.constraint_type = 'P' AND k.status = 'ENABLED' ORDER BY k.owner,"
                    + " k.table_name, cc.position")) {
      bind(ps, con, tables);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          TableId t = byName.get(key(rs.getString(1), rs.getString(2)));
          if (t != null) {
            primaryKeys.computeIfAbsent(t, k -> new ArrayList<>()).add(rs.getString(3));
          }
        }
      }
    }
    Map<TableId, List<List<String>>> uniques = new HashMap<>();
    try (PreparedStatement ps =
        c().prepareStatement(
                "SELECT i.table_owner, i.table_name, i.index_name, ic.column_name FROM cdb_indexes"
                    + " i JOIN cdb_ind_columns ic ON ic.con_id = i.con_id AND ic.index_owner ="
                    + " i.owner AND ic.index_name = i.index_name WHERE i.con_id = ? AND"
                    + " (i.table_owner, i.table_name) IN ("
                    + in
                    + ") AND i.uniqueness = 'UNIQUE' AND i.status = 'VALID' AND NOT EXISTS (SELECT"
                    + " 1 FROM cdb_ind_columns x JOIN cdb_tab_cols tc ON tc.con_id = x.con_id AND"
                    + " tc.owner = x.table_owner AND tc.table_name = x.table_name AND"
                    + " tc.column_name = x.column_name WHERE x.con_id = i.con_id AND x.index_owner"
                    + " = i.owner AND x.index_name = i.index_name AND tc.nullable = 'Y') ORDER BY"
                    + " i.table_owner, i.table_name, i.index_name, ic.column_position")) {
      bind(ps, con, tables);
      try (ResultSet rs = ps.executeQuery()) {
        TableId currentTable = null;
        String currentIndex = null;
        List<String> cols = null;
        while (rs.next()) {
          TableId t = byName.get(key(rs.getString(1), rs.getString(2)));
          if (t == null) {
            continue;
          }
          String idx = rs.getString(3);
          if (!t.equals(currentTable) || !idx.equals(currentIndex)) {
            cols = new ArrayList<>();
            uniques.computeIfAbsent(t, k -> new ArrayList<>()).add(cols);
            currentTable = t;
            currentIndex = idx;
          }
          cols.add(rs.getString(4));
        }
      }
    }
    // read last: a DDL after the queries above shows as a later time, never as an older layout
    // paired with an older time
    Map<TableId, java.time.Instant> lastDdl = new HashMap<>();
    try (PreparedStatement ps =
        c().prepareStatement(
                "SELECT owner, object_name, last_ddl_time FROM cdb_objects WHERE con_id = ? AND"
                    + " (owner, object_name) IN ("
                    + in
                    + ") AND object_type = 'TABLE'")) {
      bind(ps, con, tables);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          TableId t = byName.get(key(rs.getString(1), rs.getString(2)));
          java.sql.Timestamp ts = rs.getTimestamp(3);
          if (t != null && ts != null) {
            lastDdl.put(t, ts.toInstant());
          }
        }
      }
    }
    for (TableId t : tables) {
      List<ColumnSpec> cols = columns.get(t);
      if (cols == null) {
        continue; // not in the dictionary, as read() reports with an empty result
      }
      out.put(
          t,
          new Layout(
              new TableSchema(
                  t,
                  cols,
                  List.of(),
                  KeySource.NONE,
                  allColumns.contains(t),
                  primaryKeyColumns.contains(t)),
              new KeySelector.Candidates(
                  primaryKeys.getOrDefault(t, List.of()), uniques.getOrDefault(t, List.of())),
              lastDdl.get(t)));
    }
  }

  /** The container id, then each table's owner and name, for a list of {@code (?, ?)} pairs. */
  private static void bind(PreparedStatement ps, int con, List<TableId> tables)
      throws SQLException {
    int i = 1;
    ps.setInt(i++, con);
    for (TableId t : tables) {
      ps.setString(i++, t.schema());
      ps.setString(i++, t.table());
    }
  }

  /** A table by owner and name within one container. */
  private record Name(String owner, String table) {}

  private static Name key(String owner, String table) {
    return new Name(owner, table);
  }

  private boolean hasLogGroup(int con, TableId t, String type) throws SQLException {
    try (PreparedStatement ps =
        c().prepareStatement(
                "SELECT COUNT(*) FROM cdb_log_groups WHERE con_id = ? AND owner = ? AND table_name"
                    + " = ? AND log_group_type = ?")) {
      ps.setInt(1, con);
      ps.setString(2, t.schema());
      ps.setString(3, t.table());
      ps.setString(4, type);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() && rs.getInt(1) > 0;
      }
    }
  }

  @Override
  public KeySelector.Candidates keyCandidates(TableId t) throws SQLException {
    int con = conId(t);
    List<String> pk = new ArrayList<>();
    try (PreparedStatement ps =
        c().prepareStatement(
                "SELECT cc.column_name FROM cdb_constraints k JOIN cdb_cons_columns cc ON cc.con_id"
                    + " = k.con_id AND cc.owner = k.owner AND cc.constraint_name ="
                    + " k.constraint_name WHERE k.con_id = ? AND k.owner = ? AND k.table_name = ?"
                    + " AND k.constraint_type = 'P' AND k.status = 'ENABLED' ORDER BY"
                    + " cc.position")) {
      ps.setInt(1, con);
      ps.setString(2, t.schema());
      ps.setString(3, t.table());
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          pk.add(rs.getString(1));
        }
      }
    }
    List<List<String>> uniques = new ArrayList<>();
    try (PreparedStatement ps =
        c().prepareStatement(
                "SELECT i.index_name, ic.column_name FROM cdb_indexes i JOIN cdb_ind_columns ic ON"
                    + " ic.con_id = i.con_id AND ic.index_owner = i.owner AND ic.index_name ="
                    + " i.index_name WHERE i.con_id = ? AND i.table_owner = ? AND i.table_name = ?"
                    + " AND i.uniqueness = 'UNIQUE' AND i.status = 'VALID' AND NOT EXISTS (SELECT 1"
                    + " FROM cdb_ind_columns x JOIN cdb_tab_cols tc ON tc.con_id = x.con_id AND"
                    + " tc.owner = x.table_owner AND tc.table_name = x.table_name AND"
                    + " tc.column_name = x.column_name WHERE x.con_id = i.con_id AND x.index_owner"
                    + " = i.owner AND x.index_name = i.index_name AND tc.nullable = 'Y') ORDER BY"
                    + " i.index_name, ic.column_position")) {
      ps.setInt(1, con);
      ps.setString(2, t.schema());
      ps.setString(3, t.table());
      try (ResultSet rs = ps.executeQuery()) {
        String current = null;
        List<String> cols = null;
        while (rs.next()) {
          String idx = rs.getString(1);
          if (!idx.equals(current)) {
            cols = new ArrayList<>();
            uniques.add(cols);
            current = idx;
          }
          cols.add(rs.getString(2));
        }
      }
    }
    return new KeySelector.Candidates(pk, uniques);
  }
}
