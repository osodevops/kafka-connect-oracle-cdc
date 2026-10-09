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
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.doctor.SetupSql;
import sh.oso.connect.oracle.core.topology.Platform;

/**
 * {@link TestDatabase} over a database the suite does not own, reached by JDBC URL: Amazon RDS for
 * Oracle in the temporary dev-account lab, reached through an SSM port forward. Non-CDB only for
 * now. Passwords come from the environment, never from a property, so they stay out of build logs.
 *
 * <ul>
 *   <li>{@code e2e.external.url}: JDBC URL, for example {@code
 *       jdbc:oracle:thin:@//localhost:15210/CDCLAB} (an SSM port forward)
 *   <li>{@code e2e.external.user} (default {@code CDC}) and env {@code CDC_E2E_PASSWORD}: the
 *       capture user
 *   <li>{@code e2e.external.admin.user} and env {@code CDC_E2E_ADMIN_PASSWORD}: a user that may
 *       create users and switch logs (the RDS master user)
 *   <li>{@code e2e.external.platform}: {@code rds}, {@code rds-cdb} (the tenant database of an RDS
 *       CDB, mined in range mode, ADR-0027) or {@code onprem} (default), the platform the doctor
 *       must detect
 *   <li>{@code e2e.external.pdb}: with {@code rds-cdb}, the tenant database's name (default: the
 *       service name in the URL)
 *   <li>{@code e2e.external.apply.setup=true}: run {@code setup-sql} for the platform as the admin
 *       user first, so the run proves the script as well
 * </ul>
 */
final class ExternalTestDatabase implements TestDatabase {

  private static volatile ExternalTestDatabase instance;

  private final String url;
  private final String user;
  private final String password;
  private final String adminUser;
  private final String adminPassword;
  private final Platform platform;
  private final String pdb; // null for a non-CDB; the tenant database when connected to a PDB
  private final Map<String, String> schemaPasswords = new ConcurrentHashMap<>();

  private ExternalTestDatabase(
      String url,
      String user,
      String password,
      String adminUser,
      String adminPassword,
      Platform platform,
      String pdb) {
    this.url = url;
    this.user = user;
    this.password = password;
    this.adminUser = adminUser;
    this.adminPassword = adminPassword;
    this.platform = platform;
    this.pdb = pdb;
  }

  static ExternalTestDatabase fromSystemProperties() {
    if (instance == null) {
      synchronized (ExternalTestDatabase.class) {
        if (instance == null) {
          String url = System.getProperty("e2e.external.url");
          String kind =
              System.getProperty("e2e.external.platform", "onprem").toLowerCase(Locale.ROOT);
          boolean inPdb = kind.endsWith("-cdb");
          ExternalTestDatabase db =
              new ExternalTestDatabase(
                  url,
                  System.getProperty("e2e.external.user", "CDC"),
                  required("CDC_E2E_PASSWORD"),
                  System.getProperty("e2e.external.admin.user"),
                  System.getenv("CDC_E2E_ADMIN_PASSWORD"),
                  Platform.valueOf(kind.replace("-cdb", "").toUpperCase(Locale.ROOT)),
                  inPdb
                      ? System.getProperty(
                              "e2e.external.pdb", url.substring(url.lastIndexOf('/') + 1))
                          .toUpperCase(Locale.ROOT)
                      : null);
          if (Boolean.getBoolean("e2e.external.apply.setup")) {
            db.applySetup();
          }
          instance = db;
        }
      }
    }
    return instance;
  }

  private static String required(String env) {
    String v = System.getenv(env);
    if (v == null || v.isEmpty()) {
      throw new IllegalStateException(env + " must be set for an external database run");
    }
    return v;
  }

  private Connection admin() throws SQLException {
    if (adminUser == null || adminPassword == null) {
      throw new IllegalStateException(
          "e2e.external.admin.user and CDC_E2E_ADMIN_PASSWORD are needed to create schemas and"
              + " switch logs");
    }
    return DriverManager.getConnection(url, adminUser, adminPassword);
  }

  /** Runs the platform's setup script as the admin user; a capture user that exists is kept. */
  private void applySetup() {
    String script =
        SetupSql.generate(user, password, false, SetupSql.Profile.PRODUCTION, platform, List.of());
    try (Connection c = admin();
        Statement st = c.createStatement()) {
      for (String sql : SqlScript.statements(script)) {
        try {
          st.execute(sql);
        } catch (SQLException e) {
          if (e.getErrorCode() != 1920) { // ORA-01920: the user exists from an earlier run
            throw e;
          }
        }
      }
    } catch (SQLException e) {
      throw new IllegalStateException("applying setup-sql --platform " + platform + ": " + e, e);
    }
  }

  @Override
  public Platform platform() {
    return platform;
  }

  @Override
  public Connection capture() throws SQLException {
    return DriverManager.getConnection(url, user, password);
  }

  @Override
  public Connection workload(String schema) throws SQLException {
    return DriverManager.getConnection(url, schema, passwordOf(schema));
  }

  private String passwordOf(String schema) {
    return schemaPasswords.computeIfAbsent(
        schema, s -> "Q" + UUID.randomUUID().toString().replace("-", "").substring(0, 20));
  }

  @Override
  public void recreateSchema(String schema) throws SQLException {
    try (Connection c = admin();
        Statement st = c.createStatement()) {
      drop(c, st, schema);
      st.execute(
          "CREATE USER "
              + schema
              + " IDENTIFIED BY \""
              + passwordOf(schema)
              + "\" QUOTA UNLIMITED ON users");
      st.execute(
          "GRANT CREATE SESSION, CREATE TABLE, CREATE SEQUENCE, CREATE VIEW, CREATE PROCEDURE TO "
              + schema);
    }
  }

  @Override
  public void dropSchema(String schema) throws SQLException {
    try (Connection c = admin();
        Statement st = c.createStatement()) {
      drop(c, st, schema);
    }
  }

  /** Ends the schema's sessions (rdsadmin on RDS, which has no ALTER SYSTEM), then drops it. */
  private void drop(Connection c, Statement st, String schema) throws SQLException {
    SQLException last = null;
    for (int attempt = 0; attempt < 10; attempt++) {
      try (PreparedStatement ps =
          c.prepareStatement("SELECT sid, serial# FROM v$session WHERE username = ?")) {
        ps.setString(1, schema);
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            kill(c, rs.getLong(1), rs.getLong(2));
          }
        }
      }
      try {
        st.execute("DROP USER " + schema + " CASCADE");
        return;
      } catch (SQLException e) {
        if (e.getErrorCode() == 1918) { // ORA-01918: no such user
          return;
        }
        if (e.getErrorCode() != 1940) { // ORA-01940: still connected
          throw e;
        }
        last = e;
      }
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new SQLException("interrupted dropping " + schema, e);
      }
    }
    throw last;
  }

  private void kill(Connection c, long sid, long serial) {
    String sql =
        platform == Platform.RDS
            ? "BEGIN rdsadmin.rdsadmin_util.kill(sid => "
                + sid
                + ", serial => "
                + serial
                + "); END;"
            : "ALTER SYSTEM KILL SESSION '" + sid + "," + serial + "' IMMEDIATE";
    try (Statement k = c.createStatement()) {
      k.execute(sql);
    } catch (SQLException ignore) {
      // already gone
    }
  }

  @Override
  public List<String> pdbs() {
    return pdb == null ? List.of() : List.of(pdb);
  }

  @Override
  public String include(String schema, String table) {
    return (pdb == null ? "" : pdb + "\\.") + schema + "\\." + table;
  }

  @Override
  public boolean rangeMode() {
    return pdb != null;
  }

  /**
   * Switches logs (rdsadmin on RDS) and waits up to three minutes until the log that was current
   * appears in V$ARCHIVED_LOG.
   */
  @Override
  public void archiveLogCurrent() throws SQLException {
    try (Connection c = admin();
        Statement st = c.createStatement()) {
      long current;
      try (ResultSet rs =
          st.executeQuery("SELECT sequence# FROM v$log WHERE status = 'CURRENT' AND thread# = 1")) {
        rs.next();
        current = rs.getLong(1);
      }
      st.execute(
          platform == Platform.RDS
              ? "BEGIN rdsadmin.rdsadmin_util.switch_logfile; END;"
              : "ALTER SYSTEM ARCHIVE LOG CURRENT");
      long deadline = System.nanoTime() + Duration.ofMinutes(3).toNanos();
      while (System.nanoTime() < deadline) {
        try (PreparedStatement ps =
            c.prepareStatement(
                "SELECT COUNT(*) FROM v$archived_log WHERE thread# = 1 AND sequence# = ?"
                    + " AND standby_dest = 'NO' AND status = 'A'")) {
          ps.setLong(1, current);
          try (ResultSet rs = ps.executeQuery()) {
            if (rs.next() && rs.getInt(1) > 0) {
              return;
            }
          }
        }
        try {
          Thread.sleep(2000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new SQLException("interrupted waiting for the archiver", e);
        }
      }
      throw new SQLException("log sequence " + current + " was not archived within 3 minutes");
    }
  }

  @Override
  public CoreConfig coreConfig() {
    return new CoreConfig(
        Map.of(
            CoreConfig.DATABASE_URL,
            url,
            CoreConfig.DATABASE_USER,
            user,
            CoreConfig.DATABASE_PASSWORD,
            password,
            CoreConfig.DATABASE_PDBS,
            pdb == null ? "" : pdb));
  }

  @Override
  public String describe() {
    return platform.name().toLowerCase(Locale.ROOT)
        + (pdb == null ? "" : " cdb pdb " + pdb)
        + " "
        + url;
  }
}
