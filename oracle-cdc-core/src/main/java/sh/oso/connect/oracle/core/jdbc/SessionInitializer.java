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
package sh.oso.connect.oracle.core.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Fixed session settings every connection gets (PRD-00 CORE-CONN-4). SQL_REDO renders temporal
 * literals with the session's NLS formats and LOCAL TIME ZONE values in the session's zone ({@code
 * reference/sql-redo-shapes.md}), so the decoder can only rely on these exact values.
 */
public final class SessionInitializer {

  public static final String DATE_FORMAT = "YYYY-MM-DD HH24:MI:SS";
  public static final String TIMESTAMP_FORMAT = "YYYY-MM-DD HH24:MI:SS.FF9";
  public static final String TIMESTAMP_TZ_FORMAT = "YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM";
  public static final String NUMERIC_CHARACTERS = ".,";
  public static final String TIME_ZONE = "UTC";

  public static final List<String> STATEMENTS =
      List.of(
          "ALTER SESSION SET NLS_DATE_FORMAT = '" + DATE_FORMAT + "'",
          "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = '" + TIMESTAMP_FORMAT + "'",
          "ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = '" + TIMESTAMP_TZ_FORMAT + "'",
          "ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '" + NUMERIC_CHARACTERS + "'",
          "ALTER SESSION SET TIME_ZONE = '" + TIME_ZONE + "'",
          "ALTER SESSION SET NLS_LENGTH_SEMANTICS = 'CHAR'");

  private SessionInitializer() {}

  public static void apply(Connection c, ConnectionRole role) throws SQLException {
    try (Statement st = c.createStatement()) {
      for (String sql : STATEMENTS) {
        st.execute(sql);
      }
    }
    try (java.sql.CallableStatement cs =
        c.prepareCall("{call DBMS_APPLICATION_INFO.SET_MODULE(?, ?)}")) {
      cs.setString(1, "oso-cdc");
      cs.setString(2, role.name().toLowerCase(java.util.Locale.ROOT));
      cs.execute();
    }
    c.setAutoCommit(false);
  }
}
