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
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.ReferenceDoc;

/**
 * P0-09: the ORA codes and behaviours the engine's error classifier and session manager depend on
 * (PRD-00 CORE-ERR, CORE-MINE-7, CORE-LOG-4): restarting a session without END_LOGMNR, a range no
 * added log covers, an archived log deleted from disk behind V$ARCHIVED_LOG's back, mining the
 * current online log, and whether the mining session's PGA is visible.
 */
@Tag("engine")
class MiningBehavioursRefEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void miningErrorCodesAndSessionBehavioursAreRecorded() throws Exception {
    List<List<String>> facts = new ArrayList<>();
    try (Connection root = db.capture(OracleTestDatabase.CDB_SERVICE)) {
      OracleSql.archiveLogCurrent(db);
      OracleSql.archiveLogCurrent(db);
      long scn = LogMinerHelper.currentScn(root);

      // 1. START_LOGMNR twice in one session without END_LOGMNR
      String restart;
      try {
        LogMinerHelper.start(root, scn - 5000, scn);
        LogMinerHelper.start(root, scn - 4000, scn);
        restart =
            "allowed: a second START_LOGMNR in the same session replaces the range (options are not"
                + " persistent)";
      } catch (SQLException e) {
        restart = code(e);
      } finally {
        quietEnd(root);
      }
      facts.add(List.of("START_LOGMNR again without END_LOGMNR", restart));

      // 2. a range that no added log covers
      String noLog;
      try (Statement st = root.createStatement()) {
        String oldest =
            scalar(
                root,
                "SELECT name FROM (SELECT name FROM v$archived_log WHERE dest_id = 1 AND name IS"
                    + " NOT NULL AND deleted = 'NO' ORDER BY sequence#) WHERE ROWNUM = 1");
        st.execute(
            "BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => '"
                + oldest
                + "', OPTIONS => DBMS_LOGMNR.NEW); END;");
        st.execute(
            "BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN => "
                + (scn + 1000000)
                + ", ENDSCN => "
                + (scn + 1000100)
                + ", OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG); END;");
        noLog = "no error raised";
      } catch (SQLException e) {
        noLog = code(e);
      } finally {
        quietEnd(root);
      }
      facts.add(List.of("START_LOGMNR for an SCN range outside every added log", noLog));

      // 3. archived log deleted from disk while V$ARCHIVED_LOG still lists it
      String deletedAdd;
      String deletedListed;
      String victim =
          scalar(
              root,
              "SELECT name FROM (SELECT name FROM v$archived_log WHERE dest_id = 1 AND name IS NOT"
                  + " NULL AND deleted = 'NO' ORDER BY sequence# DESC) WHERE ROWNUM = 1");
      db.container().execInContainer("rm", "-f", victim);
      try (Statement st = root.createStatement()) {
        st.execute(
            "BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => '"
                + victim
                + "', OPTIONS => DBMS_LOGMNR.NEW); END;");
        deletedAdd = "ADD_LOGFILE accepted the missing file";
      } catch (SQLException e) {
        deletedAdd = code(e);
      } finally {
        quietEnd(root);
      }
      deletedListed =
          scalar(
              root,
              "SELECT deleted || '/' || status FROM v$archived_log WHERE name = '"
                  + victim.replace("'", "''")
                  + "'");
      facts.add(List.of("ADD_LOGFILE of an archived log removed from disk", deletedAdd));
      facts.add(
          List.of(
              "V$ARCHIVED_LOG DELETED/STATUS for that file afterwards",
              deletedListed
                  + " (the catalog does not notice an rm; the doctor must check the file,"
                  + " DOC-12)"));
      // RMAN CROSSCHECK is the supported way to reconcile the catalog with the disk
      org.testcontainers.containers.Container.ExecResult rman =
          db.container()
              .execInContainer(
                  "bash",
                  "-lc",
                  "printf 'CROSSCHECK ARCHIVELOG ALL;\\nDELETE NOPROMPT EXPIRED ARCHIVELOG ALL;\\n'"
                      + " | rman target /");
      String afterCrosscheck =
          scalar(
              root,
              "SELECT deleted || '/' || status FROM v$archived_log WHERE name = '"
                  + victim.replace("'", "''")
                  + "'");
      facts.add(
          List.of(
              "V$ARCHIVED_LOG DELETED/STATUS after RMAN CROSSCHECK and DELETE EXPIRED",
              (rman.getExitCode() == 0 && rman.getStdout().contains("rosschecked")
                      ? afterCrosscheck
                      : "RMAN did not run: " + rman.getStderr().trim())
                  + " (the engine treats DELETED = YES or STATUS = D or X as purged, CORE-LOG-4)"));

      // 4. mining the current online redo log with the online catalog
      String online;
      try (Statement st = root.createStatement()) {
        String current =
            scalar(
                root,
                "SELECT f.member FROM v$log l JOIN v$logfile f ON f.group# = l.group# WHERE"
                    + " l.status = 'CURRENT' AND ROWNUM = 1");
        long first =
            Long.parseLong(
                scalar(root, "SELECT first_change# FROM v$log WHERE status = 'CURRENT'"));
        st.execute(
            "BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => '"
                + current
                + "', OPTIONS => DBMS_LOGMNR.NEW); END;");
        st.execute(
            "BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN => "
                + first
                + ", ENDSCN => "
                + LogMinerHelper.currentScn(root)
                + ", OPTIONS => DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG); END;");
        try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM v$logmnr_contents")) {
          rs.next();
          online =
              "allowed; rows mined from the current online log: "
                  + (rs.getLong(1) > 0 ? "some" : "none");
        }
      } catch (SQLException e) {
        online = code(e);
      } finally {
        quietEnd(root);
      }
      facts.add(List.of("Mining the CURRENT online redo log", online));

      // 5. is the mining session's PGA visible to the capture user?
      String pga;
      try {
        pga =
            scalar(
                root,
                "SELECT CASE WHEN p.pga_used_mem IS NULL THEN 'not visible' ELSE 'visible' END FROM"
                    + " v$session s JOIN v$process p ON p.addr = s.paddr WHERE s.sid ="
                    + " SYS_CONTEXT('USERENV','SID')");
      } catch (SQLException e) {
        pga = code(e);
      }
      facts.add(List.of("V$PROCESS.PGA_USED_MEM of the mining session", pga));

      new ReferenceDoc(
              "mining-errors",
              "LogMiner session behaviours and error codes",
              "Behaviours and ORA codes the engine depends on, observed on Oracle Database Free"
                  + " with the capture user's grants (PRD-00 CORE-ERR classifier, CORE-MINE-7"
                  + " session reuse, CORE-LOG-4 purge detection, PRD-05 DOC-12).")
          .section("Observed")
          .table(List.of("Situation", "Behaviour"), facts)
          .section("Not covered here")
          .bullet(
              "An online log overwritten during a slow fetch (ORA-00310, ORA-00334, ORA-01289) is"
                  + " provoked with FaultyJdbc and a log switch storm in the P1-08 suites, not by"
                  + " this spike.")
          .assertUpToDate();
      assertThat(facts).hasSize(7);
    }
  }

  private static String code(SQLException e) {
    String m = e.getMessage() == null ? "" : e.getMessage().split("\n")[0].trim();
    java.util.regex.Matcher x = java.util.regex.Pattern.compile("ORA-\\d{5}").matcher(m);
    return x.find()
        ? x.group()
            + ": "
            + m.replace(x.group() + ": ", "")
                .replaceAll("\\s*\\(CONNECTION_ID=.*", "")
                .replaceAll("/opt/oracle/archive/[^ ]+", "<file>")
        : m;
  }

  private static String scalar(Connection c, String sql) throws SQLException {
    try (Statement st = c.createStatement();
        ResultSet rs = st.executeQuery(sql)) {
      return rs.next() ? rs.getString(1) : null;
    }
  }

  private static void quietEnd(Connection c) {
    try (Statement st = c.createStatement()) {
      st.execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;");
    } catch (SQLException ignore) {
      // no session
    }
  }
}
