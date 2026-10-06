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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;

/**
 * Oracle accepts listener connections before the database is usable, and the gvenzl entrypoint
 * prints {@code DATABASE IS READY TO USE!} only after the first-start hooks have run. This strategy
 * waits for that line, then proves through JDBC, as the capture user, that the database is in the
 * state the suites assume: ARCHIVELOG, minimal supplemental logging, the three PDBs open, and a
 * LogMiner session that can be started and ended.
 */
public final class ArchivelogReadyWaitStrategy extends AbstractWaitStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(ArchivelogReadyWaitStrategy.class);
  private static final Duration PROBE_INTERVAL = Duration.ofSeconds(3);

  @Override
  protected void waitUntilReady() {
    Instant start = Instant.now();
    new LogMessageWaitStrategy()
        .withRegEx(".*DATABASE IS READY TO USE!.*\\s")
        .withStartupTimeout(startupTimeout)
        .waitUntilReady(waitStrategyTarget);
    Duration remaining = startupTimeout.minus(Duration.between(start, Instant.now()));
    String url =
        "jdbc:oracle:thin:@//"
            + waitStrategyTarget.getHost()
            + ":"
            + waitStrategyTarget.getMappedPort(1521)
            + "/"
            + OracleTestDatabase.CDB_SERVICE;
    // One attempt every few seconds. A tight retry loop floods the listener with logons and can
    // keep its handler reported as blocked (ORA-12516) for as long as the loop runs.
    Instant deadline =
        Instant.now().plus(remaining.isNegative() ? Duration.ofSeconds(30) : remaining);
    SQLException last = null;
    int attempt = 0;
    while (Instant.now().isBefore(deadline)) {
      attempt++;
      try {
        probe(url);
        LOG.info("Oracle test database passed the CDC readiness probe on attempt {}", attempt);
        return;
      } catch (SQLException | IllegalStateException e) {
        last = e instanceof SQLException ? (SQLException) e : new SQLException(e.getMessage(), e);
        LOG.debug("readiness probe attempt {} failed: {}", attempt, e.getMessage());
        try {
          Thread.sleep(PROBE_INTERVAL.toMillis());
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new ContainerLaunchException("interrupted while waiting for Oracle", ie);
        }
      }
    }
    throw new ContainerLaunchException(
        "Oracle test database never became ready for CDC after " + attempt + " probes", last);
  }

  static void probe(String url) throws SQLException {
    try (Connection c =
            DriverManager.getConnection(
                url, OracleTestDatabase.CAPTURE_USER, OracleTestDatabase.CAPTURE_PASSWORD);
        Statement st = c.createStatement()) {
      try (ResultSet rs =
          st.executeQuery("SELECT log_mode, supplemental_log_data_min FROM v$database")) {
        rs.next();
        require("ARCHIVELOG".equals(rs.getString(1)), "LOG_MODE is " + rs.getString(1));
        require("YES".equals(rs.getString(2)), "SUPPLEMENTAL_LOG_DATA_MIN is " + rs.getString(2));
      }
      int open = 0;
      try (ResultSet rs =
          st.executeQuery(
              "SELECT name FROM v$pdbs WHERE open_mode = 'READ WRITE' AND name IN ('FREEPDB1',"
                  + " 'FREEPDB2', 'FREEPDB3')")) {
        while (rs.next()) {
          open++;
        }
      }
      require(open == 3, "only " + open + " of the three test PDBs are READ WRITE");
      // A trivial LogMiner session over the newest archived log proves the LOGMINING grants.
      st.execute(
          "BEGIN  FOR r IN (SELECT name FROM (SELECT name FROM v$archived_log WHERE name IS NOT"
              + " NULL ORDER BY sequence# DESC) WHERE ROWNUM = 1) LOOP   "
              + " DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => r.name, OPTIONS => DBMS_LOGMNR.NEW);  END"
              + " LOOP;  DBMS_LOGMNR.START_LOGMNR(OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG);"
              + "  DBMS_LOGMNR.END_LOGMNR;END;");
    }
  }

  private static void require(boolean ok, String message) {
    if (!ok) {
      throw new IllegalStateException("not ready: " + message);
    }
  }
}
