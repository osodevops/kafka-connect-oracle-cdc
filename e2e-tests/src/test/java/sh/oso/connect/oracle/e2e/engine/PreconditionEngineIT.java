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
package sh.oso.connect.oracle.e2e.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;

/**
 * Asserts the state every other suite assumes (testing strategy section 2). If this fails, nothing
 * else is worth running.
 */
@Tag("engine")
class PreconditionEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void databaseIsInArchivelogModeWithMinimalSupplementalLogging() throws SQLException {
    try (Connection c = db.capture(OracleTestDatabase.CDB_SERVICE);
        Statement st = c.createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT log_mode, supplemental_log_data_min, supplemental_log_data_all, cdb FROM"
                    + " v$database")) {
      assertThat(rs.next()).isTrue();
      assertThat(rs.getString("LOG_MODE")).isEqualTo("ARCHIVELOG");
      assertThat(rs.getString("SUPPLEMENTAL_LOG_DATA_MIN")).isEqualTo("YES");
      assertThat(rs.getString("SUPPLEMENTAL_LOG_DATA_ALL"))
          .as("ALL COLUMNS logging is per table, never database wide")
          .isEqualTo("NO");
      assertThat(rs.getString("CDB")).isEqualTo("YES");
    }
  }

  @Test
  void bothTestPdbsAreOpenReadWrite() throws SQLException {
    assertThat(query("SELECT name || '=' || open_mode FROM v$pdbs WHERE con_id > 2 ORDER BY name"))
        .containsExactly("FREEPDB1=READ WRITE", "FREEPDB2=READ WRITE");
  }

  @Test
  void archiveDestinationIsAnExplicitLocationOutsideTheRecoveryArea() throws SQLException {
    assertThat(query("SELECT value FROM v$parameter WHERE name = 'log_archive_dest_1'"))
        .containsExactly("LOCATION=/opt/oracle/archive");
    assertThat(query("SELECT destination || ':' || status FROM v$archive_dest WHERE dest_id = 1"))
        .containsExactly("/opt/oracle/archive:VALID");
  }

  @Test
  void onlineRedoLogsAreUniformFiftyMegabyteGroups() throws SQLException {
    List<String> sizes = query("SELECT DISTINCT TO_CHAR(bytes / 1048576) FROM v$log");
    assertThat(sizes).containsExactly("50");
    assertThat(query("SELECT TO_CHAR(COUNT(*)) FROM v$log"))
        .first()
        .satisfies(n -> assertThat(Integer.parseInt(n)).isGreaterThanOrEqualTo(3));
  }

  @Test
  void captureUserCanMineTheLatestArchivedLog() throws SQLException {
    try (Connection c = db.capture(OracleTestDatabase.CDB_SERVICE);
        Statement st = c.createStatement()) {
      st.execute(
          "BEGIN  FOR r IN (SELECT name FROM (SELECT name FROM v$archived_log WHERE name IS NOT"
              + " NULL ORDER BY sequence# DESC) WHERE ROWNUM = 1) LOOP   "
              + " DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => r.name, OPTIONS => DBMS_LOGMNR.NEW);  END"
              + " LOOP;  DBMS_LOGMNR.START_LOGMNR(OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG);"
              + "END;");
      try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM v$logmnr_contents")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getLong(1)).as("rows mined from the newest archived log").isGreaterThan(0);
      }
      st.execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;");
    }
  }

  @Test
  void workloadSchemaExistsInEveryTestPdb() throws SQLException {
    for (String pdb : List.of(OracleTestDatabase.PDB1, OracleTestDatabase.PDB2)) {
      try (Connection c = db.capture(pdb);
          Statement st = c.createStatement();
          ResultSet rs =
              st.executeQuery("SELECT COUNT(*) FROM all_users WHERE username = 'WORKLOAD'")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt(1)).as("WORKLOAD user in %s", pdb).isEqualTo(1);
      }
    }
  }

  @Test
  void logMinerPackagesArePresent() throws SQLException {
    assertThat(
            query(
                "SELECT object_name FROM dba_objects WHERE owner = 'SYS' AND object_type ="
                    + " 'PACKAGE' AND object_name IN ('DBMS_LOGMNR', 'DBMS_LOGMNR_D') ORDER BY 1"))
        .containsExactly("DBMS_LOGMNR", "DBMS_LOGMNR_D");
  }

  private List<String> query(String sql) throws SQLException {
    List<String> out = new ArrayList<>();
    try (Connection c = db.capture(OracleTestDatabase.CDB_SERVICE);
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery(sql)) {
      while (rs.next()) {
        out.add(rs.getString(1));
      }
    }
    return out;
  }
}
