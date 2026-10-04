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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

/**
 * Per-test schemas inside a PDB. Every test class owns one schema named after itself, created
 * through SYSDBA, so suites never collide and never need to restart the database.
 */
public final class SchemaFixtures {

  private SchemaFixtures() {}

  /** Schema name for a test class: {@code T_} plus the simple class name, upper case, max 30. */
  public static String nameFor(Class<?> testClass) {
    String n = "T_" + testClass.getSimpleName().toUpperCase(Locale.ROOT);
    return n.length() > 30 ? n.substring(0, 30) : n;
  }

  /** Drops (if present) and recreates the schema in the PDB with the grants tables need. */
  public static void recreate(OracleTestDatabase db, String pdb, String schema)
      throws SQLException {
    try (Connection c = db.sysdbaInPdb(pdb);
        Statement st = c.createStatement()) {
      dropIfExists(st, schema);
      st.execute(
          "CREATE USER " + schema + " IDENTIFIED BY \"" + schema + "\" QUOTA UNLIMITED ON users");
      st.execute(
          "GRANT CREATE SESSION, CREATE TABLE, CREATE SEQUENCE, CREATE VIEW, CREATE PROCEDURE TO "
              + schema);
    }
  }

  public static void drop(OracleTestDatabase db, String pdb, String schema) throws SQLException {
    try (Connection c = db.sysdbaInPdb(pdb);
        Statement st = c.createStatement()) {
      dropIfExists(st, schema);
    }
  }

  /** Kills the user's sessions first (a connector may still hold one), then drops with retries. */
  private static void dropIfExists(Statement st, String schema) throws SQLException {
    SQLException last = null;
    for (int attempt = 0; attempt < 10; attempt++) {
      try (java.sql.ResultSet rs =
          st.executeQuery("SELECT sid, serial# FROM v$session WHERE username = '" + schema + "'")) {
        java.util.List<String> sessions = new java.util.ArrayList<>();
        while (rs.next()) {
          sessions.add(rs.getLong(1) + "," + rs.getLong(2));
        }
        for (String s : sessions) {
          try (Statement kill = st.getConnection().createStatement()) {
            kill.execute("ALTER SYSTEM KILL SESSION '" + s + "' IMMEDIATE");
          } catch (SQLException ignore) {
            // already gone
          }
        }
      }
      try {
        st.execute(
            "BEGIN EXECUTE IMMEDIATE 'DROP USER "
                + schema
                + " CASCADE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -1918 THEN RAISE; END IF;"
                + " END;");
        return;
      } catch (SQLException e) {
        if (e.getErrorCode() != 1940) {
          throw e;
        }
        last = e;
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw e;
        }
      }
    }
    throw last;
  }
}
