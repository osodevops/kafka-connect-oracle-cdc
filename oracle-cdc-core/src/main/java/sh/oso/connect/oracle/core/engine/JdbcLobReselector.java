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
package sh.oso.connect.oracle.core.engine;

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
import java.util.function.Supplier;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.RowIds;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.KeySource;
import sh.oso.connect.oracle.core.schema.OracleType;
import sh.oso.connect.oracle.core.schema.TableSchema;

/**
 * {@link LobReselector} over a connection of its own: switches to the row's PDB (the capture user
 * holds SET CONTAINER, SELECT ANY TABLE and FLASHBACK ANY TABLE) and runs {@code SELECT ... AS OF
 * SCN} by key, or by ROWID for a ROWID-keyed table. JDBC-bound; proven by the engine tier.
 */
public final class JdbcLobReselector implements LobReselector {

  /** Undo too old, SCN out of range, or the table changed since: the value stays unavailable. */
  static final Set<Integer> UNAVAILABLE = Set.of(1555, 8181, 1466);

  private final Supplier<Connection> connection;
  private Connection current;
  private String container;

  public JdbcLobReselector(Supplier<Connection> connection) {
    this.connection = connection;
  }

  @Override
  public Map<String, Object> reselect(
      TableSchema schema, RowChange change, long scn, List<String> columns) throws SQLException {
    List<String> keys = new ArrayList<>();
    List<Object> values = new ArrayList<>();
    if (schema.keySource() == KeySource.ROWID || schema.keyColumns().isEmpty()) {
      String rowId = RowIds.real(change.rowId());
      if (rowId == null) {
        return Map.of();
      }
      keys.add("ROWID");
      values.add(rowId);
    } else {
      for (String k : schema.keyColumns()) {
        if (!change.after().containsKey(k) || change.after().get(k) == null) {
          return Map.of();
        }
        keys.add(quote(k));
        values.add(change.after().get(k));
      }
    }
    Connection c = connection.get();
    if (c != current) {
      current = c; // a reconnect: the new session starts in the root container
      container = null;
    }
    String pdb = schema.table().pdb();
    if (pdb != null && container == null) {
      // a PDB-local session is already in its container (ADR-0027)
      try (Statement st = c.createStatement();
          java.sql.ResultSet rs =
              st.executeQuery("SELECT SYS_CONTEXT('USERENV', 'CON_NAME') FROM dual")) {
        container = rs.next() ? rs.getString(1) : null;
      }
    }
    if (pdb != null && !pdb.equals(container)) {
      try (Statement st = c.createStatement()) {
        st.execute("ALTER SESSION SET CONTAINER = " + quote(pdb));
      }
      container = pdb;
    }
    StringBuilder sql = new StringBuilder("SELECT ");
    for (int i = 0; i < columns.size(); i++) {
      sql.append(i == 0 ? "" : ", ").append(quote(columns.get(i)));
    }
    sql.append(" FROM ")
        .append(quote(schema.table().schema()))
        .append('.')
        .append(quote(schema.table().table()))
        .append(" AS OF SCN ? WHERE ");
    for (int i = 0; i < keys.size(); i++) {
      sql.append(i == 0 ? "" : " AND ").append(keys.get(i)).append(" = ?");
    }
    try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
      ps.setLong(1, scn);
      for (int i = 0; i < values.size(); i++) {
        ps.setObject(i + 2, values.get(i));
      }
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return Map.of(); // the row did not exist at the commit SCN
        }
        Map<String, Object> out = new LinkedHashMap<>();
        for (String col : columns) {
          ColumnSpec spec = schema.column(col);
          Object v =
              spec.type() == OracleType.BLOB
                  ? rs.getBytes(col)
                  : spec.type() == OracleType.NCLOB ? rs.getNString(col) : rs.getString(col);
          out.put(col, v);
        }
        return out;
      }
    } catch (SQLException e) {
      if (UNAVAILABLE.contains(OraErrorClassifier.oraCode(e))) {
        return Map.of();
      }
      throw e;
    }
  }

  private static String quote(String identifier) {
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }
}
