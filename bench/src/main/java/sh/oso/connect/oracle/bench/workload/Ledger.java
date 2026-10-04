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
package sh.oso.connect.oracle.bench.workload;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * The ledger of committed transactions. One row is inserted inside every generator transaction, so
 * a row exists exactly when Oracle committed that transaction. The XID is {@code usn.slot.sqn} from
 * {@code DBMS_TRANSACTION.LOCAL_TRANSACTION_ID}, the same three numbers LogMiner exposes as XIDUSN,
 * XIDSLT and XIDSQN. The table lives next to the data tables and is excluded from capture.
 */
public final class Ledger {

  private Ledger() {}

  static String ddl(String table) {
    return "CREATE TABLE "
        + table
        + " (xid VARCHAR2(40) PRIMARY KEY, session_id NUMBER NOT NULL, seq NUMBER NOT NULL,"
        + " ops NUMBER NOT NULL, inserts NUMBER NOT NULL, updates NUMBER NOT NULL,"
        + " deletes NUMBER NOT NULL, effects VARCHAR2(4000), committed_at TIMESTAMP(6) NOT NULL)";
  }

  /** Reads the current transaction id; null when the session has no open transaction. */
  static String currentXid(Connection c) throws SQLException {
    try (Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("SELECT DBMS_TRANSACTION.LOCAL_TRANSACTION_ID FROM dual")) {
      return rs.next() ? rs.getString(1) : null;
    }
  }

  static void insert(
      Connection c,
      String table,
      String xid,
      int session,
      long seq,
      int ops,
      int inserts,
      int updates,
      int deletes,
      String effects)
      throws SQLException {
    try (PreparedStatement ps =
        c.prepareStatement(
            "INSERT INTO "
                + table
                + " (xid, session_id, seq, ops, inserts, updates, deletes, effects, committed_at)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, SYSTIMESTAMP)")) {
      ps.setString(1, xid);
      ps.setInt(2, session);
      ps.setLong(3, seq);
      ps.setInt(4, ops);
      ps.setInt(5, inserts);
      ps.setInt(6, updates);
      ps.setInt(7, deletes);
      ps.setString(8, effects);
      ps.executeUpdate();
    }
  }

  /** Committed XIDs recorded in {@code owner.table}, in {@code usn.slot.sqn} form. */
  public static Set<String> committedXids(Connection c, String owner, String table)
      throws SQLException {
    Set<String> out = new LinkedHashSet<>();
    try (Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("SELECT xid FROM " + owner + "." + table + " ORDER BY seq")) {
      while (rs.next()) {
        out.add(rs.getString(1));
      }
    }
    return out;
  }

  /** Formats LogMiner's three XID columns the way the ledger stores them. */
  public static String xid(long usn, long slot, long sqn) {
    return usn + "." + slot + "." + sqn;
  }
}
