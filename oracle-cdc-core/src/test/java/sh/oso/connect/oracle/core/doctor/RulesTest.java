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
package sh.oso.connect.oracle.core.doctor;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.topology.ArchiveDestination;
import sh.oso.connect.oracle.core.topology.DatabaseInfo;
import sh.oso.connect.oracle.core.topology.ThreadInfo;

class RulesTest {

  private static CoreConfig config(Map<String, String> extra) {
    Map<String, String> p =
        new java.util.HashMap<>(
            Map.of(
                CoreConfig.DATABASE_HOST,
                "h",
                CoreConfig.DATABASE_SERVICE,
                "FREE",
                CoreConfig.DATABASE_USER,
                "C##CDC",
                CoreConfig.DATABASE_PASSWORD,
                "x",
                CoreConfig.DATABASE_PDBS,
                "FREEPDB1"));
    p.putAll(extra);
    return new CoreConfig(p);
  }

  private static CapturedTable table(
      String name, boolean allCols, boolean pk, List<CapturedTable.Column> cols) {
    return new CapturedTable(
        "FREEPDB1", "APP", name, cols, allCols, !allCols, pk, false, false, false);
  }

  private static final CapturedTable.Column ID = new CapturedTable.Column("ID", "NUMBER", false);

  private DoctorContext ctx(FakeDoctorCatalog cat, String keyMissing) {
    return new DoctorContext(
        config(Map.of()),
        cat,
        List.of("FREEPDB1\\.APP\\..*"),
        List.of("FREEPDB1\\.APP\\.SKIP.*"),
        keyMissing);
  }

  @Test
  void aSecondEnabledRedoThreadIsBlockingInFastMode() {
    FakeDoctorCatalog cat = new FakeDoctorCatalog();
    cat.tables.add(table("ORDERS", true, true, List.of(ID)));
    cat.base.threads.add(new ThreadInfo(2, true, "OPEN", 3));
    Report r = new Doctor(Rules.fastMode()).run(ctx(cat, "fail"));
    assertThat(r.findings())
        .filteredOn(f -> f.rule().equals("DOC-13"))
        .extracting(Finding::severity)
        .containsExactly(Severity.BLOCKING);
    assertThat(r.findings())
        .filteredOn(f -> f.rule().equals("DOC-13"))
        .extracting(Finding::message)
        .allMatch(m -> m.contains("2 enabled redo threads (thread 1 OPEN, thread 2 OPEN)"))
        .allMatch(m -> m.contains("CDC-5001"));
    assertThat(r.exitCode()).isEqualTo(Report.EXIT_BLOCKING);

    // a disabled second thread (an instance removed from the cluster) is information only
    cat.base.threads.set(1, new ThreadInfo(2, false, "CLOSED", 3));
    Report info = new Doctor(Rules.fastMode()).run(ctx(cat, "fail"));
    assertThat(info.findings())
        .filteredOn(f -> f.rule().equals("DOC-13"))
        .extracting(Finding::severity)
        .containsExactly(Severity.INFO);
    assertThat(info.exitCode()).isEqualTo(Report.EXIT_OK);
  }

  @Test
  void onAmazonRdsEveryFixIsOneTheMasterUserCanRun() {
    FakeDoctorCatalog cat = new FakeDoctorCatalog();
    cat.platform = sh.oso.connect.oracle.core.topology.Platform.RDS;
    cat.rdsConfiguration.put("archivelog retention hours", "48");
    cat.tables.add(table("ORDERS", true, true, List.of(ID)));
    cat.base.database =
        new DatabaseInfo(
            1,
            "CDCLAB",
            false,
            "NOARCHIVELOG",
            "READ WRITE",
            "PRIMARY",
            "19.0.0.0.0",
            1,
            false,
            "Linux x86 64-bit");
    cat.inaccessible.add("V$LOGMNR_CONTENTS");
    cat.parameters.put("undo_retention", "60");
    cat.base.destinations.clear();
    cat.dictionaryPackage = false;
    Report r = new Doctor(Rules.all()).run(ctx(cat, "fail"));
    List<Finding> withFix = r.findings().stream().filter(f -> f.fixSql() != null).toList();
    assertThat(withFix)
        .extracting(Finding::rule)
        .contains("DOC-1", "DOC-2", "DOC-4", "DOC-11", "DOC-12");
    assertThat(withFix)
        .extracting(Finding::fixSql)
        .noneMatch(f -> f.contains("ALTER SYSTEM"))
        .noneMatch(f -> f.contains("ALTER DATABASE"))
        .noneMatch(f -> f.contains("SHUTDOWN"))
        .noneMatch(f -> f.contains("GRANT SELECT ON"));
    assertThat(fixOf(r, "DOC-1")).contains("--backup-retention-period 1");
    assertThat(fixOf(r, "DOC-2")).contains("alter_supplemental_logging(p_action => 'ADD')");
    assertThat(fixOf(r, "DOC-4"))
        .contains("grant_sys_object('V_$LOGMNR_CONTENTS', 'C##CDC', 'SELECT')");
    assertThat(fixOf(r, "DOC-11")).contains("ParameterName=undo_retention,ParameterValue=900");
  }

  private static String fixOf(Report r, String rule) {
    return r.findings().stream()
        .filter(f -> f.rule().equals(rule) && f.fixSql() != null)
        .map(Finding::fixSql)
        .findFirst()
        .orElseThrow();
  }

  @Test
  void rdsArchiveRetentionMustCoverTheJournalThresholdAndTheDowntime() {
    FakeDoctorCatalog cat = new FakeDoctorCatalog();
    // not RDS: the rule has nothing to say
    assertThat(new Doctor(List.of(Rules.rdsArchiveRetention())).run(ctx(cat, "fail")).findings())
        .isEmpty();
    cat.platform = sh.oso.connect.oracle.core.topology.Platform.RDS;
    // the master user can read the setting; the capture user usually cannot
    assertThat(doc23(cat)).extracting(Finding::severity).containsExactly(Severity.INFO);
    cat.rdsConfiguration.put("archivelog retention hours", "0");
    assertThat(doc23(cat)).extracting(Finding::severity).containsExactly(Severity.BLOCKING);
    assertThat(doc23(cat).get(0).fixSql())
        .contains("name  => 'archivelog retention hours'")
        .contains("value => '25'")
        .contains("COMMIT;");
    cat.rdsConfiguration.put("archivelog retention hours", "24");
    assertThat(doc23(cat)).extracting(Finding::severity).containsExactly(Severity.WARNING);
    assertThat(doc23(cat).get(0).message()).contains("24 hours").contains("needs 25");
    cat.rdsConfiguration.put(
        "archivelog retention hours", String.valueOf(SetupSql.RDS_RETENTION_HOURS));
    assertThat(doc23(cat)).as("the setup script's value satisfies the rule").isEmpty();
  }

  private List<Finding> doc23(FakeDoctorCatalog cat) {
    return new Doctor(List.of(Rules.rdsArchiveRetention())).run(ctx(cat, "fail")).findings();
  }

  @Test
  void aCleanDatabaseHasNoFindings() {
    FakeDoctorCatalog cat = new FakeDoctorCatalog();
    cat.tables.add(table("ORDERS", true, true, List.of(ID)));
    cat.tables.add(table("SKIPPED", false, false, List.of(ID)));
    Report r = new Doctor(Rules.fastMode()).run(ctx(cat, "fail"));
    assertThat(r.findings()).isEmpty();
    assertThat(r.exitCode()).isEqualTo(Report.EXIT_OK);
    assertThat(r.toMarkdown()).contains("No findings");
  }

  @Test
  void everyRuleFiresOnItsFixture() {
    FakeDoctorCatalog cat = new FakeDoctorCatalog();
    cat.base.database =
        new DatabaseInfo(
            1,
            "FREE",
            true,
            "NOARCHIVELOG",
            "READ WRITE",
            "PRIMARY",
            "18.0.0.0.0",
            1,
            false,
            "Linux");
    cat.base.destinations.clear();
    cat.base.destinations.add(
        new ArchiveDestination(1, "LOG_ARCHIVE_DEST_1", "/a", "ERROR", "LOCAL", "PRIMARY"));
    cat.privileges.remove("LOGMINING");
    cat.inaccessible.add("V$LOGMNR_CONTENTS");
    cat.containerDataAll = false;
    cat.tables.add(table("NOLOG", false, true, List.of(ID)));
    cat.tables.add(
        new CapturedTable(
            "FREEPDB1",
            "APP",
            "IDENT",
            List.of(new CapturedTable.Column("ID", "NUMBER", true)),
            true,
            false,
            true,
            false,
            false,
            false));
    cat.tables.add(
        table("BOOL", true, true, List.of(ID, new CapturedTable.Column("FLAG", "BOOLEAN", false))));
    cat.tables.add(table("A_VERY_LONG_TABLE_NAME_OVER_THIRTY_CHARS", true, true, List.of(ID)));
    cat.tables.add(table("NOKEY", true, false, List.of(ID)));
    Report r = new Doctor(Rules.fastMode()).run(ctx(cat, "fail"));
    assertThat(r.findings())
        .extracting(Finding::rule)
        .contains(
            "DOC-1", "DOC-2", "DOC-3", "DOC-4", "DOC-5", "DOC-6", "DOC-7", "DOC-12", "DOC-15");
    assertThat(r.findings())
        .filteredOn(f -> f.rule().equals("DOC-4"))
        .extracting(Finding::message)
        .anyMatch(m -> m.contains("LOGMINING"))
        .anyMatch(m -> m.contains("V$LOGMNR_CONTENTS"))
        .anyMatch(m -> m.contains("CONTAINER_DATA"));
    assertThat(r.findings())
        .filteredOn(f -> f.rule().equals("DOC-5"))
        .extracting(Finding::message)
        .anyMatch(m -> m.contains("identity"))
        .anyMatch(m -> m.contains("BOOLEAN"));
    assertThat(r.exitCode()).isEqualTo(Report.EXIT_BLOCKING);
    assertThat(r.toMarkdown()).contains("## SQL to run").contains("ALTER DATABASE ARCHIVELOG");
    assertThat(r.toJson()).startsWith("{\"exitCode\":1").contains("\"rule\":\"DOC-1\"");
    assertThat(r.toJUnitXml()).contains("<failure");
  }

  @Test
  void primaryKeyOnlyLoggingAndRowidKeysAreWarnings() {
    FakeDoctorCatalog cat = new FakeDoctorCatalog();
    cat.tables.add(table("PKONLY", false, true, List.of(ID)));
    cat.tables.add(
        new CapturedTable(
            "FREEPDB1", "APP", "MOVER", List.of(ID), true, false, false, false, true, false));
    Report r = new Doctor(Rules.fastMode()).run(ctx(cat, "rowid"));
    assertThat(r.findings()).extracting(Finding::severity).containsOnly(Severity.WARNING);
    assertThat(r.exitCode()).isEqualTo(Report.EXIT_WARNINGS);
    // ADR-0004: a keyless table is snapshotted in ROWID ranges, so ROW MOVEMENT is a warning
    // under every key policy that accepts keyless tables; without it the table is only noted
    cat.tables.add(
        new CapturedTable(
            "FREEPDB1", "APP", "STILL", List.of(ID), true, false, false, false, false, false));
    Report none = new Doctor(Rules.fastMode()).run(ctx(cat, "none"));
    assertThat(none.findings())
        .filteredOn(f -> f.rule().equals("DOC-7"))
        .extracting(Finding::message, Finding::severity)
        .containsExactlyInAnyOrder(
            org.assertj.core.groups.Tuple.tuple(
                "FREEPDB1.APP.MOVER has no key and ROW MOVEMENT enabled. Snapshots of a keyless"
                    + " table read ROWID ranges, so a row that moves while a snapshot runs can be"
                    + " read twice or missed.",
                Severity.WARNING),
            org.assertj.core.groups.Tuple.tuple(
                "FREEPDB1.APP.STILL has no key; records are keyed by none.", Severity.INFO));
    assertThat(r.findings())
        .filteredOn(f -> f.rule().equals("DOC-7"))
        .singleElement()
        .satisfies(
            f -> {
              assertThat(f.message()).contains("keyed by ROWID").contains("ROWID ranges");
              assertThat(f.fixSql()).contains("DISABLE ROW MOVEMENT");
            });
  }

  @Test
  void standbyNeedsArchiveOnlyModeAndAnOpenDatabase() {
    FakeDoctorCatalog cat = new FakeDoctorCatalog();
    // DoctorContext caches the database facts, so every case gets a fresh one
    // an Active Data Guard standby: archive_only only
    cat.base.database = standby("READ ONLY WITH APPLY");
    assertThat(doc14(ctx(cat, "fail")))
        .extracting(Finding::severity)
        .containsExactly(Severity.BLOCKING);
    assertThat(doc14(archiveOnly(cat))).isEmpty();
    // read only without apply: accepted, but the safe end stands still
    cat.base.database = standby("READ ONLY");
    assertThat(doc14(archiveOnly(cat)))
        .extracting(Finding::severity)
        .containsExactly(Severity.WARNING);
    // mounted: no dictionary to read, in either mode
    cat.base.database = standby("MOUNTED");
    assertThat(doc14(ctx(cat, "fail")))
        .extracting(Finding::severity)
        .containsExactly(Severity.BLOCKING);
    assertThat(doc14(archiveOnly(cat)))
        .extracting(Finding::message)
        .singleElement()
        .asString()
        .contains("no dictionary to read");
  }

  private DoctorContext archiveOnly(FakeDoctorCatalog cat) {
    return new DoctorContext(
        config(Map.of(CoreConfig.CAPTURE_MODE, "archive_only")),
        cat,
        List.of(".*"),
        List.of(),
        "fail");
  }

  private static DatabaseInfo standby(String openMode) {
    return new DatabaseInfo(
        1,
        "FREE",
        true,
        "ARCHIVELOG",
        openMode,
        "PHYSICAL STANDBY",
        "19.0.0.0.0",
        1,
        true,
        "Linux");
  }

  private static List<Finding> doc14(DoctorContext ctx) {
    return new Doctor(List.of(Rules.roleAndOpenMode())).run(ctx).findings();
  }

  @Test
  void aRuleThatThrowsBecomesABlockingFinding() {
    Rule broken =
        new Rule() {
          public String id() {
            return "DOC-X";
          }

          public List<Finding> evaluate(DoctorContext ctx) {
            throw new IllegalStateException("boom");
          }
        };
    Report r = new Doctor(List.of(broken)).run(ctx(new FakeDoctorCatalog(), "fail"));
    assertThat(r.findings())
        .singleElement()
        .satisfies(
            f -> {
              assertThat(f.rule()).isEqualTo("DOC-X");
              assertThat(f.severity()).isEqualTo(Severity.BLOCKING);
              assertThat(f.message()).contains("boom");
            });
  }

  @Test
  void setupSqlProfiles() {
    String lab =
        SetupSql.generate(
            "c##cdc",
            "cdc",
            true,
            SetupSql.Profile.LAB,
            sh.oso.connect.oracle.core.topology.Platform.ONPREM,
            List.of("FREEPDB1", "FREEPDB2"));
    assertThat(lab)
        .contains("CREATE USER c##cdc IDENTIFIED BY \"cdc\" CONTAINER=ALL;")
        .contains("SET CONTAINER_DATA=ALL")
        .contains("GRANT ALTER SYSTEM")
        .contains("ALTER SESSION SET CONTAINER = FREEPDB2;");
    String prod =
        SetupSql.generate(
            "c##cdc",
            "secret",
            true,
            SetupSql.Profile.PRODUCTION,
            sh.oso.connect.oracle.core.topology.Platform.ONPREM,
            List.of());
    assertThat(prod)
        .doesNotContain("ALTER SYSTEM")
        .doesNotContain("workload")
        .contains("SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
    String nonCdb =
        SetupSql.generate(
            "cdc",
            "x",
            false,
            SetupSql.Profile.PRODUCTION,
            sh.oso.connect.oracle.core.topology.Platform.ONPREM,
            List.of());
    assertThat(nonCdb).doesNotContain("CONTAINER").doesNotContain("SET CONTAINER");
    String rds =
        SetupSql.generate(
            "cdc",
            "secret",
            false,
            SetupSql.Profile.PRODUCTION,
            sh.oso.connect.oracle.core.topology.Platform.RDS,
            List.of());
    assertThat(rds)
        .contains("CREATE USER cdc IDENTIFIED BY \"secret\";")
        .contains("GRANT LOGMINING TO cdc;")
        .contains("rdsadmin.rdsadmin_util.grant_sys_object('DBMS_LOGMNR_D', 'CDC', 'EXECUTE');")
        .contains("rdsadmin.rdsadmin_util.grant_sys_object('V_$LOGMNR_CONTENTS', 'CDC', 'SELECT');")
        .contains("rdsadmin.rdsadmin_util.grant_sys_object('V_$INSTANCE', 'CDC', 'SELECT');")
        .contains("alter_supplemental_logging(p_action => 'ADD')")
        .contains("value => '" + SetupSql.RDS_RETENTION_HOURS + "'")
        .contains("COMMIT;");
    // what the master user executes, comments aside: nothing RDS refuses
    String executable =
        rds.lines()
            .filter(l -> !l.startsWith("--"))
            .collect(java.util.stream.Collectors.joining("\n"));
    assertThat(executable)
        .doesNotContain("CONTAINER=")
        .doesNotContain("SET CONTAINER")
        .doesNotContain("ALTER SYSTEM")
        .doesNotContain("ALTER DATABASE")
        .doesNotContain("GRANT SELECT ON");
    assertThat(
            SetupSql.generate(
                "u",
                "p",
                true,
                SetupSql.Profile.PRODUCTION,
                sh.oso.connect.oracle.core.topology.Platform.AUTONOMOUS,
                List.of()))
        .contains("not available yet");
  }
}
