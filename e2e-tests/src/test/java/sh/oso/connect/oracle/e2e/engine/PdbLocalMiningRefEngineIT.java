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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.ReferenceDoc;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * CORE-REF for per-PDB mining (Phase 4, Amazon RDS with the CDB architecture and Autonomous
 * Database): connected to a PDB as a local user, does DBMS_LOGMNR.START_LOGMNR work with an SCN
 * range and no ADD_LOGFILE, with which dictionary options, and what do V$LOGMNR_LOGS, the row
 * columns, an uncovered range, a second session and the PDB's view of the log catalog look like?
 * Records facts only, never timings; it decides the shape of the range mining mode (ADR-0027).
 */
@Tag("engine")
class PdbLocalMiningRefEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  /** A local user of FREEPDB1; the password is generated per run and never written anywhere. */
  private static final String MINER = "T_PDBLOCALMINER";

  @Test
  void aLocalUserMinesItsPdbWithAnScnRangeOnly() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    String minerPassword = "M" + UUID.randomUUID().toString().replace("-", "").substring(0, 20);
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection sys = db.sysdbaInPdb(OracleTestDatabase.PDB1);
        Statement st = sys.createStatement()) {
      try {
        st.execute("DROP USER " + MINER + " CASCADE");
      } catch (SQLException ignore) {
        // first run
      }
      st.execute("CREATE USER " + MINER + " IDENTIFIED BY \"" + minerPassword + "\"");
      st.execute("GRANT CREATE SESSION, LOGMINING, SELECT ANY TRANSACTION TO " + MINER);
      st.execute("GRANT EXECUTE ON DBMS_LOGMNR TO " + MINER);
      st.execute("GRANT EXECUTE ON DBMS_LOGMNR_D TO " + MINER);
      for (String v :
          List.of(
              "V_$LOGMNR_CONTENTS",
              "V_$LOGMNR_LOGS",
              "V_$DATABASE",
              "V_$ARCHIVED_LOG",
              "V_$LOG",
              "V_$LOGFILE",
              "V_$THREAD",
              "V_$ARCHIVE_DEST_STATUS",
              "V_$CONTAINERS")) {
        st.execute("GRANT SELECT ON " + v + " TO " + MINER);
      }
    }
    List<List<String>> facts = new ArrayList<>();
    try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Connection m = db.connect(OracleTestDatabase.PDB1, MINER, minerPassword);
        Connection m2 = db.connect(OracleTestDatabase.PDB1, MINER, minerPassword)) {
      try (Statement s = w.createStatement()) {
        s.execute("CREATE TABLE pl (id NUMBER PRIMARY KEY, v VARCHAR2(20))");
        s.execute("ALTER TABLE pl ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      }
      OracleSql.archiveLogCurrent(db);
      long from = scn(m);
      try (Statement s = w.createStatement()) {
        s.execute("INSERT INTO pl VALUES (1, 'one')");
        s.execute("UPDATE pl SET v = 'uno' WHERE id = 1");
      }
      long committedAt = scn(m);
      long to = committedAt + 1;

      facts.add(row("SYS_CONTEXT CON_NAME of the session", context(m, "CON_NAME")));
      facts.add(
          row("SYS_CONTEXT CLOUD_SERVICE on Oracle Database Free", context(m, "CLOUD_SERVICE")));
      for (String v :
          List.of("V$DATABASE", "V$ARCHIVED_LOG", "V$LOG", "V$THREAD", "V$ARCHIVE_DEST_STATUS")) {
        facts.add(row(v + " readable from the PDB", readable(m, v)));
      }
      facts.add(
          row(
              "ADD_LOGFILE of an archived log from the PDB",
              outcome(
                  m,
                  "BEGIN DBMS_LOGMNR.ADD_LOGFILE(LOGFILENAME => '/nonexistent.arc',"
                      + " OPTIONS => DBMS_LOGMNR.NEW); END;")));

      // the change is in the current online log: no log switch before mining
      String online = start(m, from, to, "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG");
      facts.add(row("START_LOGMNR, SCN range, online catalog, no ADD_LOGFILE", online));
      if ("ok".equals(online)) {
        facts.add(
            row(
                "V$LOGMNR_LOGS rows after the start",
                count(m, "SELECT COUNT(*) FROM v$logmnr_logs") > 0 ? "populated" : "empty"));
        facts.add(row("Rows of the table seen before a log switch", rowsOf(m, schema)));
        facts.add(row("Column values of those rows", columnsOf(m, schema)));
        facts.add(
            row(
                "A second session's START_LOGMNR while the first is open",
                start(m2, from, to, "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG")));
        end(m2);
        end(m);
      }
      OracleSql.archiveLogCurrent(db);
      String archived = start(m, from, to, "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG");
      facts.add(row("START_LOGMNR again after the log is archived", archived));
      if ("ok".equals(archived)) {
        facts.add(row("Rows of the table after the log switch", rowsOf(m, schema)));
        end(m);
      }
      facts.add(
          row(
              "START_LOGMNR with DICT_FROM_REDO_LOGS + DDL_DICT_TRACKING, no ADD_LOGFILE",
              startAndEnd(
                  m, from, to, "DBMS_LOGMNR.DICT_FROM_REDO_LOGS + DBMS_LOGMNR.DDL_DICT_TRACKING")));
      facts.add(
          row(
              "START_LOGMNR for a range below the oldest redo (STARTSCN 1)",
              startAndEnd(m, 1, 1000, "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG")));
      facts.add(
          row(
              "START_LOGMNR with an end SCN beyond the current SCN",
              startAndEnd(m, from, scn(m) + 1_000_000, "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG")));
      facts.add(
          row(
              "DBMS_LOGMNR_D.BUILD into the redo from the PDB",
              outcome(
                  m,
                  "BEGIN DBMS_LOGMNR_D.BUILD(OPTIONS => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS); END;")));
    } finally {
      try (Connection sys = db.sysdbaInPdb(OracleTestDatabase.PDB1);
          Statement st = sys.createStatement()) {
        st.execute("DROP USER " + MINER + " CASCADE");
      } catch (SQLException ignore) {
        // a session may still hold it; the next run drops it first
      }
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
    ReferenceDoc doc =
        new ReferenceDoc(
            "pdb-local-mining",
            "LogMiner from inside a PDB as a local user",
            "What per-PDB mining (START_LOGMNR with an SCN range and no ADD_LOGFILE, connected to"
                + " the PDB as a local user with LOGMINING) does on Oracle Database Free. It"
                + " decides the range mining mode for Amazon RDS with the CDB architecture and"
                + " Autonomous Database (ADR-0027).");
    doc.section("Facts").table(List.of("Question", "Answer"), facts);
    doc.assertUpToDate();
    assertThat(facts).as("the spike ran to the end").hasSizeGreaterThan(10);
  }

  private static List<String> row(String q, String a) {
    return List.of(q, a);
  }

  private static long scn(Connection c) throws SQLException {
    return count(c, "SELECT current_scn FROM v$database");
  }

  private static long count(Connection c, String sql) throws SQLException {
    try (Statement s = c.createStatement();
        ResultSet rs = s.executeQuery(sql)) {
      rs.next();
      return rs.getLong(1);
    }
  }

  private static String context(Connection c, String parameter) {
    try (Statement s = c.createStatement();
        ResultSet rs =
            s.executeQuery("SELECT SYS_CONTEXT('USERENV', '" + parameter + "') FROM dual")) {
      rs.next();
      String v = rs.getString(1);
      return v == null ? "null" : v;
    } catch (SQLException e) {
      return "ORA-" + String.format("%05d", OraErrorClassifier.oraCode(e));
    }
  }

  private static String readable(Connection c, String view) {
    try (Statement s = c.createStatement();
        ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM " + view)) {
      rs.next();
      return rs.getLong(1) > 0 ? "yes, with rows" : "yes, empty";
    } catch (SQLException e) {
      return "no, ORA-" + String.format("%05d", OraErrorClassifier.oraCode(e));
    }
  }

  private static String outcome(Connection c, String plsql) {
    try (Statement s = c.createStatement()) {
      s.execute(plsql);
      return "ok";
    } catch (SQLException e) {
      return "ORA-" + String.format("%05d", OraErrorClassifier.oraCode(e));
    }
  }

  private static String start(Connection c, long from, long to, String options) {
    try (CallableStatement cs =
        c.prepareCall(
            "BEGIN DBMS_LOGMNR.START_LOGMNR(STARTSCN => ?, ENDSCN => ?, OPTIONS => "
                + options
                + "); END;")) {
      cs.setLong(1, from);
      cs.setLong(2, to);
      cs.execute();
      return "ok";
    } catch (SQLException e) {
      return "ORA-" + String.format("%05d", OraErrorClassifier.oraCode(e));
    }
  }

  private static String startAndEnd(Connection c, long from, long to, String options) {
    String r = start(c, from, to, options);
    end(c);
    return r;
  }

  private static void end(Connection c) {
    try (Statement s = c.createStatement()) {
      s.execute("BEGIN DBMS_LOGMNR.END_LOGMNR; END;");
    } catch (SQLException ignore) {
      // ORA-01307: no session to end
    }
  }

  private static String rowsOf(Connection c, String schema) {
    try (PreparedStatement ps =
        c.prepareStatement(
            "SELECT operation FROM v$logmnr_contents WHERE seg_owner = ? AND table_name = 'PL'"
                + " AND operation IN ('INSERT', 'UPDATE') ORDER BY scn, ssn")) {
      ps.setString(1, schema);
      try (ResultSet rs = ps.executeQuery()) {
        List<String> ops = new ArrayList<>();
        while (rs.next()) {
          ops.add(rs.getString(1));
        }
        return ops.isEmpty() ? "none" : String.join(", ", ops);
      }
    } catch (SQLException e) {
      return "ORA-" + String.format("%05d", OraErrorClassifier.oraCode(e));
    }
  }

  private static String columnsOf(Connection c, String schema) {
    try (PreparedStatement ps =
        c.prepareStatement(
            "SELECT rs_id, ssn, thread#, xidusn, src_con_id, src_con_name, con_id"
                + " FROM v$logmnr_contents WHERE seg_owner = ? AND table_name = 'PL'"
                + " AND operation = 'INSERT'")) {
      ps.setString(1, schema);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return "no row";
        }
        return "RS_ID "
            + (rs.getString(1) == null || rs.getString(1).isBlank() ? "empty" : "set")
            + ", SSN "
            + (rs.getObject(2) == null ? "null" : "set")
            + ", THREAD# "
            + rs.getInt(3)
            + ", XIDUSN "
            + (rs.getObject(4) == null ? "null" : "set")
            + ", SRC_CON_ID "
            + (rs.getInt(5) > 2 ? "the PDB's" : String.valueOf(rs.getInt(5)))
            + ", SRC_CON_NAME "
            + rs.getString(6)
            + ", CON_ID "
            + (rs.getInt(7) > 2 ? "the PDB's" : String.valueOf(rs.getInt(7)));
      }
    } catch (SQLException e) {
      return "ORA-" + String.format("%05d", OraErrorClassifier.oraCode(e));
    }
  }
}
