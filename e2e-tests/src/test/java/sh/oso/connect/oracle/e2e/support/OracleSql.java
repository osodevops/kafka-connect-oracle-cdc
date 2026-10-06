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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.testcontainers.containers.Container.ExecResult;

/** Database-level helpers used by tests and fault injection, all through SYSDBA at CDB$ROOT. */
public final class OracleSql {

  private static final String HIDDEN = ".hidden-by-test";
  private static final List<String> hidden = new ArrayList<>();

  private OracleSql() {}

  /**
   * Makes an archived log unreadable the way an rm outside RMAN does (the catalog still lists it),
   * but keeps the file aside: the container is shared by every suite in the JVM, and a later suite
   * mining an older range must still find every log the catalog lists. Restore with {@link
   * #restoreHiddenLogs}.
   */
  public static synchronized void hideArchivedLog(OracleTestDatabase db, String path)
      throws IOException, InterruptedException {
    exec(db, "mv", path, path + HIDDEN);
    hidden.add(path);
  }

  /** Puts back every log {@link #hideArchivedLog} moved aside; call it from a finally block. */
  public static synchronized void restoreHiddenLogs(OracleTestDatabase db)
      throws IOException, InterruptedException {
    while (!hidden.isEmpty()) {
      String path = hidden.remove(hidden.size() - 1);
      exec(db, "mv", path + HIDDEN, path);
    }
  }

  private static void exec(OracleTestDatabase db, String... command)
      throws IOException, InterruptedException {
    ExecResult r = db.container().execInContainer(command);
    if (r.getExitCode() != 0) {
      throw new IllegalStateException(
          String.join(" ", command) + " failed: " + r.getStderr().trim());
    }
  }

  /** Forces a log switch and waits for the previous log to be archived. */
  public static void archiveLogCurrent(OracleTestDatabase db) throws SQLException {
    try (Connection c = db.sysdba(OracleTestDatabase.CDB_SERVICE);
        Statement st = c.createStatement()) {
      st.execute("ALTER SYSTEM ARCHIVE LOG CURRENT");
    }
  }

  /**
   * Forces a log switch and waits for the previous log to be archived, on a SYSDBA connection at
   * CDB$ROOT the caller keeps open: loops of switches must not open a connection per statement.
   */
  public static void archiveLogCurrent(Connection sysRoot) throws SQLException {
    try (Statement st = sysRoot.createStatement()) {
      st.execute("ALTER SYSTEM ARCHIVE LOG CURRENT");
    }
  }

  /** A log switch without waiting for the archiver, on a SYSDBA connection at CDB$ROOT. */
  public static void switchLogfile(Connection sysRoot) throws SQLException {
    try (Statement st = sysRoot.createStatement()) {
      st.execute("ALTER SYSTEM SWITCH LOGFILE");
    }
  }

  /** Number of online redo log groups (six on the test image). */
  public static int onlineLogGroups(Connection c) throws SQLException {
    try (Statement st = c.createStatement();
        ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM v$log")) {
      rs.next();
      return rs.getInt(1);
    }
  }

  /**
   * Switches and archives one more time than there are online groups, so every change made so far
   * exists only in archived logs: no online group still holds a copy (the dbz-2713 lesson).
   */
  public static void recycleOnlineLogs(Connection sysRoot) throws SQLException {
    int groups = onlineLogGroups(sysRoot);
    for (int i = 0; i <= groups; i++) {
      archiveLogCurrent(sysRoot);
    }
  }

  /**
   * SHUTDOWN ABORT and STARTUP of the shared instance through SQL*Plus inside the container, then
   * waits until the database is back in the state every suite assumes. Returns the SQL*Plus output.
   * Every session, the connector's included, is cut off.
   */
  public static String abortAndRestartInstance(OracleTestDatabase db, Duration timeout)
      throws Exception {
    String out = sqlplus(db, "SHUTDOWN ABORT", "STARTUP", "ALTER PLUGGABLE DATABASE ALL OPEN;");
    awaitOpen(db, timeout);
    return out;
  }

  /**
   * Restores an instance a failed suite may have left down or mounted (finally blocks): a no-op
   * when the readiness probe already passes.
   */
  public static void ensureOpen(OracleTestDatabase db, Duration timeout) throws Exception {
    if (probe(db)) {
      return;
    }
    // each statement may fail harmlessly (already started, already open)
    sqlplus(db, "STARTUP", "ALTER DATABASE OPEN;", "ALTER PLUGGABLE DATABASE ALL OPEN;");
    awaitOpen(db, timeout);
  }

  /**
   * Waits until the capture user passes the image's readiness probe (ARCHIVELOG, supplemental
   * logging, the three PDBs READ WRITE, a LogMiner session) and the FREEPDB1 service accepts
   * logons. One attempt every three seconds: a tight loop keeps the listener blocked (ORA-12516).
   */
  public static void awaitOpen(OracleTestDatabase db, Duration timeout) throws Exception {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    while (!probe(db)) {
      if (System.currentTimeMillis() > deadline) {
        throw new IllegalStateException(
            "the Oracle test database did not reopen within " + timeout.toSeconds() + " s");
      }
      Thread.sleep(3000);
    }
  }

  private static boolean probe(OracleTestDatabase db) {
    try {
      ArchivelogReadyWaitStrategy.probe(db.jdbcUrl(OracleTestDatabase.CDB_SERVICE));
      try (Connection c =
          DriverManager.getConnection(
              db.jdbcUrl(OracleTestDatabase.PDB1),
              OracleTestDatabase.CAPTURE_USER,
              OracleTestDatabase.CAPTURE_PASSWORD)) {
        return c.isValid(10);
      }
    } catch (SQLException | IllegalStateException e) {
      return false;
    }
  }

  /** Runs statements through {@code sqlplus / as sysdba} in the container; returns the output. */
  private static String sqlplus(OracleTestDatabase db, String... statements)
      throws IOException, InterruptedException {
    String script =
        "export ORACLE_SID=\"${ORACLE_SID:-FREE}\"\n"
            + "command -v sqlplus >/dev/null 2>&1 || export PATH=\"$ORACLE_HOME/bin:$PATH\"\n"
            + "sqlplus -s / as sysdba <<'SQL'\n"
            + String.join("\n", statements)
            + "\nEXIT\nSQL\n";
    ExecResult r = db.container().execInContainer("bash", "-c", script);
    return r.getStdout() + r.getStderr();
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
