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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.io.TempDir;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.doctor.Doctor;
import sh.oso.connect.oracle.core.doctor.DoctorContext;
import sh.oso.connect.oracle.core.doctor.Finding;
import sh.oso.connect.oracle.core.doctor.JdbcDoctorCatalog;
import sh.oso.connect.oracle.core.doctor.Report;
import sh.oso.connect.oracle.core.doctor.Rules;
import sh.oso.connect.oracle.core.doctor.Severity;
import sh.oso.connect.oracle.doctor.cli.DoctorMain;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * P0-12: every fast-mode doctor rule against real dictionary fixtures, and the {@code check}
 * command end to end. Exit 0 on a clean table set, exit 1 on each seeded fixture.
 */
@Tag("engine")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DoctorEngineIT {

  private static final String NOPRIV = "c##t_docnopriv";

  private final OracleTestDatabase db = OracleTestDatabase.get();
  private final String schema = SchemaFixtures.nameFor(getClass());

  @BeforeAll
  void fixtures() throws SQLException {
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Statement s = w.createStatement()) {
      s.execute("CREATE TABLE clean (id NUMBER PRIMARY KEY, name VARCHAR2(50))");
      s.execute("ALTER TABLE clean ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      s.execute("CREATE TABLE uq (a NUMBER NOT NULL, b NUMBER)");
      s.execute("CREATE UNIQUE INDEX uq_a ON uq (a)");
      s.execute("ALTER TABLE uq ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      s.execute("CREATE TABLE nolog (id NUMBER PRIMARY KEY, v NUMBER)");
      s.execute("CREATE TABLE pkonly (id NUMBER PRIMARY KEY, v NUMBER)");
      s.execute("ALTER TABLE pkonly ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS");
      s.execute(
          "CREATE TABLE ident (id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY, v NUMBER)");
      s.execute("ALTER TABLE ident ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      s.execute("CREATE TABLE boolt (id NUMBER PRIMARY KEY, flag BOOLEAN)");
      s.execute("ALTER TABLE boolt ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      s.execute("CREATE TABLE a_table_name_that_is_longer_than_thirty (id NUMBER PRIMARY KEY)");
      s.execute(
          "ALTER TABLE a_table_name_that_is_longer_than_thirty ADD SUPPLEMENTAL LOG DATA (ALL)"
              + " COLUMNS");
      s.execute("CREATE TABLE nokey (a NUMBER, b NUMBER)");
      s.execute("ALTER TABLE nokey ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      s.execute("CREATE TABLE mover (a NUMBER, b NUMBER) ENABLE ROW MOVEMENT");
      s.execute("ALTER TABLE mover ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
    }
    try (Connection sys = db.sysdba(OracleTestDatabase.CDB_SERVICE);
        Statement s = sys.createStatement()) {
      try {
        s.execute("DROP USER " + NOPRIV + " CASCADE");
      } catch (SQLException ignore) {
        // first run
      }
      s.execute("CREATE USER " + NOPRIV + " IDENTIFIED BY \"x\" CONTAINER=ALL");
      s.execute("GRANT CREATE SESSION TO " + NOPRIV + " CONTAINER=ALL");
    }
  }

  @AfterAll
  void cleanup() throws SQLException {
    SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    try (Connection sys = db.sysdba(OracleTestDatabase.CDB_SERVICE);
        Statement s = sys.createStatement()) {
      s.execute("DROP USER " + NOPRIV + " CASCADE");
    }
  }

  private Map<String, String> props(String include) {
    Map<String, String> p = new HashMap<>();
    p.put(CoreConfig.DATABASE_URL, db.jdbcUrl(OracleTestDatabase.CDB_SERVICE));
    p.put(CoreConfig.DATABASE_USER, OracleTestDatabase.CAPTURE_USER);
    p.put(CoreConfig.DATABASE_PASSWORD, OracleTestDatabase.CAPTURE_PASSWORD);
    p.put(CoreConfig.DATABASE_PDBS, OracleTestDatabase.PDB1);
    p.put("cdc.tables.include", include);
    return p;
  }

  private Report run(Connection c, Map<String, String> props, String keyMissing) {
    DoctorContext ctx =
        new DoctorContext(
            new CoreConfig(props),
            new JdbcDoctorCatalog(c),
            List.of(props.get("cdc.tables.include").split(",")),
            List.of(),
            keyMissing);
    return new Doctor(Rules.fastMode()).run(ctx);
  }

  @Test
  void doc21ReadsThePluggableDatabasesOpenAndSaved() throws SQLException {
    // the test image opens both PDBs and saves their state, as the setup guide asks
    try (Connection c = db.capture(OracleTestDatabase.CDB_SERVICE)) {
      List<sh.oso.connect.oracle.core.doctor.PdbState> pdbs = new JdbcDoctorCatalog(c).pdbStates();
      assertThat(pdbs)
          .extracting(sh.oso.connect.oracle.core.doctor.PdbState::name)
          .contains(OracleTestDatabase.PDB1, "FREEPDB2")
          .doesNotContain("PDB$SEED");
      assertThat(pdbs).allMatch(p -> p.open() && p.savedState(), "open with a saved state");
      DoctorContext ctx =
          new DoctorContext(
              new CoreConfig(props("FREEPDB1\\." + schema + "\\.CLEAN")),
              new JdbcDoctorCatalog(c),
              List.of("FREEPDB1\\." + schema + "\\.CLEAN"),
              List.of(),
              "fail");
      assertThat(new Doctor(Rules.all()).run(ctx).findings())
          .noneMatch(f -> f.rule().equals("DOC-21"));
    }
  }

  @Test
  void cleanTablesProduceNoFindings() throws SQLException {
    try (Connection c = db.capture(OracleTestDatabase.CDB_SERVICE)) {
      Report r =
          run(c, props("FREEPDB1\\." + schema + "\\.CLEAN,FREEPDB1\\." + schema + "\\.UQ"), "fail");
      assertThat(r.findings()).isEmpty();
      assertThat(r.exitCode()).isZero();
    }
  }

  @Test
  void eachFixtureTripsItsRule() throws SQLException {
    try (Connection c = db.capture(OracleTestDatabase.CDB_SERVICE)) {
      Report r = run(c, props("FREEPDB1\\." + schema + "\\..*"), "fail");
      Map<String, List<Finding>> byTable = new HashMap<>();
      for (Finding f : r.findings()) {
        for (String t :
            List.of("NOLOG", "PKONLY", "IDENT", "BOOLT", "A_TABLE_NAME", "NOKEY", "MOVER")) {
          if (f.message().contains("." + t)) {
            byTable.computeIfAbsent(t, k -> new java.util.ArrayList<>()).add(f);
          }
        }
      }
      assertThat(byTable.get("NOLOG"))
          .extracting(Finding::rule, Finding::severity)
          .contains(org.assertj.core.groups.Tuple.tuple("DOC-3", Severity.BLOCKING));
      assertThat(byTable.get("PKONLY"))
          .extracting(Finding::rule, Finding::severity)
          .contains(org.assertj.core.groups.Tuple.tuple("DOC-3", Severity.WARNING));
      assertThat(byTable.get("IDENT")).extracting(Finding::rule).contains("DOC-5");
      assertThat(byTable.get("BOOLT")).extracting(Finding::rule).contains("DOC-5");
      assertThat(byTable.get("A_TABLE_NAME")).extracting(Finding::rule).contains("DOC-6");
      assertThat(byTable.get("NOKEY"))
          .extracting(Finding::rule, Finding::severity)
          .contains(org.assertj.core.groups.Tuple.tuple("DOC-7", Severity.BLOCKING));
      assertThat(r.findings()).noneMatch(f -> f.message().contains(".CLEAN"));
      assertThat(r.findings()).noneMatch(f -> f.message().contains(".UQ"));
      assertThat(r.exitCode()).isEqualTo(Report.EXIT_BLOCKING);

      Report rowid = run(c, props("FREEPDB1\\." + schema + "\\.MOVER"), "rowid");
      assertThat(rowid.findings())
          .extracting(Finding::rule, Finding::severity)
          .containsExactly(org.assertj.core.groups.Tuple.tuple("DOC-7", Severity.WARNING));
      assertThat(rowid.findings().get(0).fixSql()).contains("DISABLE ROW MOVEMENT");
    }
  }

  @Test
  void aUserWithoutTheGrantProfileFailsDoc4WithTheGrantsToRun() throws SQLException {
    Map<String, String> p = props("FREEPDB1\\." + schema + "\\.CLEAN");
    p.put(CoreConfig.DATABASE_USER, NOPRIV);
    p.put(CoreConfig.DATABASE_PASSWORD, "x");
    try (Connection c = db.connect(OracleTestDatabase.CDB_SERVICE, NOPRIV, "x")) {
      Report r = run(c, p, "fail");
      List<Finding> doc4 = r.findings().stream().filter(f -> f.rule().equals("DOC-4")).toList();
      assertThat(doc4).extracting(Finding::message).anyMatch(m -> m.contains("LOGMINING"));
      assertThat(doc4).extracting(Finding::message).anyMatch(m -> m.contains("V$DATABASE"));
      assertThat(doc4)
          .extracting(Finding::fixSql)
          .anyMatch(s -> s != null && s.contains("GRANT SELECT ON V_$DATABASE TO " + NOPRIV));
      assertThat(r.exitCode()).isEqualTo(Report.EXIT_BLOCKING);
    }
  }

  @Test
  void checkCommandEndToEnd(@TempDir Path dir) throws Exception {
    Path ok = dir.resolve("ok.json");
    Files.writeString(
        ok,
        new com.fasterxml.jackson.databind.ObjectMapper()
            .writeValueAsString(
                Map.of("name", "doctor", "config", props("FREEPDB1\\." + schema + "\\.CLEAN"))));
    StringWriter out = new StringWriter();
    int exit =
        DoctorMain.run(
            new PrintWriter(out),
            new PrintWriter(new StringWriter()),
            "check",
            "--config",
            ok.toString(),
            "--rules",
            "fast",
            "--format",
            "json");
    assertThat(exit).isZero();
    assertThat(out.toString()).startsWith("{\"exitCode\":0,\"findings\":[]}");

    Path bad = dir.resolve("bad.json");
    Files.writeString(
        bad,
        new com.fasterxml.jackson.databind.ObjectMapper()
            .writeValueAsString(props("FREEPDB1\\." + schema + "\\.NOLOG")));
    StringWriter md = new StringWriter();
    exit =
        DoctorMain.run(
            new PrintWriter(md),
            new PrintWriter(new StringWriter()),
            "check",
            "--config",
            bad.toString());
    assertThat(exit).isEqualTo(Report.EXIT_BLOCKING);
    assertThat(md.toString())
        .contains("| DOC-3 | BLOCKING |")
        .contains("ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;");

    Map<String, String> unreachable = props(".*");
    unreachable.put(CoreConfig.DATABASE_PASSWORD, "wrong");
    Path wrong = dir.resolve("wrong.json");
    Files.writeString(
        wrong, new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(unreachable));
    StringWriter j = new StringWriter();
    exit =
        DoctorMain.run(
            new PrintWriter(j),
            new PrintWriter(new StringWriter()),
            "check",
            "--config",
            wrong.toString(),
            "--format",
            "junit");
    assertThat(exit).isEqualTo(Report.EXIT_BLOCKING);
    assertThat(j.toString()).contains("<failure").contains("CONNECT");
  }
}
