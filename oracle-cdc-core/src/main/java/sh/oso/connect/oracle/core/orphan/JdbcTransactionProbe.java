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
package sh.oso.connect.oracle.core.orphan;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;

/** {@link TransactionProbe} over GV$TRANSACTION and GV$SESSION; exercised against Oracle only. */
public final class JdbcTransactionProbe implements TransactionProbe {

  private final Supplier<Connection> connection;

  public JdbcTransactionProbe(Supplier<Connection> connection) {
    this.connection = connection;
  }

  @Override
  public Set<TxKey> activeTransactions() throws SQLException {
    Set<TxKey> out = new HashSet<>();
    try (Statement st = connection.get().createStatement();
        ResultSet rs =
            st.executeQuery("SELECT con_id, xidusn, xidslot, xidsqn FROM gv$transaction")) {
      while (rs.next()) {
        out.add(new TxKey(rs.getInt(1), new Xid(rs.getLong(2), rs.getLong(3), rs.getLong(4))));
      }
    }
    return out;
  }

  @Override
  public boolean sessionExists(long sessionNo, long serialNo) throws SQLException {
    try (PreparedStatement ps =
        connection
            .get()
            .prepareStatement("SELECT 1 FROM gv$session WHERE sid = ? AND serial# = ?")) {
      ps.setLong(1, sessionNo);
      ps.setLong(2, serialNo);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  @Override
  public long currentScn() throws SQLException {
    try (Statement st = connection.get().createStatement();
        ResultSet rs = st.executeQuery("SELECT current_scn FROM v$database")) {
      rs.next();
      return rs.getLong(1);
    }
  }
}
