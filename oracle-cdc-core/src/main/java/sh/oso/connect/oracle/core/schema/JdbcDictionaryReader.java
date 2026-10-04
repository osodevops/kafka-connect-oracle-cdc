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
import java.util.List;
import java.util.Optional;
import sh.oso.connect.oracle.core.model.TableId;

/**
 * {@link DictionaryReader} over the CDB_ views from a CDB$ROOT (or non-CDB) session. JDBC-bound;
 * proven by the engine tier and excluded from the unit coverage gate.
 */
public final class JdbcDictionaryReader implements DictionaryReader {

  private final Connection c;

  public JdbcDictionaryReader(Connection metadataConnection) {
    this.c = metadataConnection;
  }

  private int conId(TableId t) throws SQLException {
    if (t.pdb() == null) {
      return 0;
    }
    try (PreparedStatement ps =
        c.prepareStatement("SELECT con_id FROM v$containers WHERE UPPER(name) = UPPER(?)")) {
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
  public Optional<TableSchema> read(TableId t) throws SQLException {
    int con = conId(t);
    List<ColumnSpec> cols = new ArrayList<>();
    try (PreparedStatement ps =
        c.prepareStatement(
            "SELECT column_name, column_id, data_type, data_length, data_precision, data_scale,"
                + " nullable FROM cdb_tab_cols WHERE con_id = ? AND owner = ? AND table_name = ?"
                + " AND hidden_column = 'NO' AND virtual_column = 'NO' ORDER BY column_id")) {
      ps.setInt(1, con);
      ps.setString(2, t.schema());
      ps.setString(3, t.table());
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          String type = rs.getString(3);
          cols.add(
              new ColumnSpec(
                  rs.getString(1),
                  rs.getInt(2),
                  OracleType.fromDictionary(type),
                  type,
                  rs.getInt(4),
                  rs.getInt(5),
                  rs.getInt(6),
                  "Y".equals(rs.getString(7))));
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

  private boolean hasLogGroup(int con, TableId t, String type) throws SQLException {
    try (PreparedStatement ps =
        c.prepareStatement(
            "SELECT COUNT(*) FROM cdb_log_groups WHERE con_id = ? AND owner = ? AND table_name = ?"
                + " AND log_group_type = ?")) {
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
        c.prepareStatement(
            "SELECT cc.column_name FROM cdb_constraints k JOIN cdb_cons_columns cc ON cc.con_id ="
                + " k.con_id AND cc.owner = k.owner AND cc.constraint_name = k.constraint_name"
                + " WHERE k.con_id = ? AND k.owner = ? AND k.table_name = ? AND k.constraint_type"
                + " = 'P' AND k.status = 'ENABLED' ORDER BY cc.position")) {
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
        c.prepareStatement(
            "SELECT i.index_name, ic.column_name FROM cdb_indexes i JOIN cdb_ind_columns ic ON"
                + " ic.con_id = i.con_id AND ic.index_owner = i.owner AND ic.index_name ="
                + " i.index_name WHERE i.con_id = ? AND i.table_owner = ? AND i.table_name = ? AND"
                + " i.uniqueness = 'UNIQUE' AND i.status = 'VALID' AND NOT EXISTS (SELECT 1 FROM"
                + " cdb_ind_columns x JOIN cdb_tab_cols tc ON tc.con_id = x.con_id AND tc.owner ="
                + " x.table_owner AND tc.table_name = x.table_name AND tc.column_name ="
                + " x.column_name WHERE x.con_id = i.con_id AND x.index_owner = i.owner AND"
                + " x.index_name = i.index_name AND tc.nullable = 'Y') ORDER BY i.index_name,"
                + " ic.column_position")) {
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
