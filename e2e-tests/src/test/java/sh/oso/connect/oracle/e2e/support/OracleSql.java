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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/** Database-level helpers used by tests and fault injection, all through SYSDBA at CDB$ROOT. */
public final class OracleSql {

  private OracleSql() {}

  /** Forces a log switch and waits for the previous log to be archived. */
  public static void archiveLogCurrent(OracleTestDatabase db) throws SQLException {
    try (Connection c = db.sysdba(OracleTestDatabase.CDB_SERVICE);
        Statement st = c.createStatement()) {
      st.execute("ALTER SYSTEM ARCHIVE LOG CURRENT");
    }
  }

  public static long currentScn(Connection c) throws SQLException {
    try (Statement st = c.createStatement();
        ResultSet rs = st.executeQuery("SELECT current_scn FROM v$database")) {
      rs.next();
      return rs.getLong(1);
    }
  }

  public static long maxArchivedSequence(Connection c) throws SQLException {
    try (Statement st = c.createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT NVL(MAX(sequence#), 0) FROM v$archived_log WHERE thread# = 1 AND dest_id ="
                    + " 1")) {
      rs.next();
      return rs.getLong(1);
    }
  }
}
