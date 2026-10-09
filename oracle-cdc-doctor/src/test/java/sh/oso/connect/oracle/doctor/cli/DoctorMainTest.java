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
package sh.oso.connect.oracle.doctor.cli;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DoctorMainTest {

  private record Run(int exit, String out, String err) {}

  private static Run run(String... args) {
    StringWriter out = new StringWriter();
    StringWriter err = new StringWriter();
    int exit = DoctorMain.run(new PrintWriter(out), new PrintWriter(err), args);
    return new Run(exit, out.toString(), err.toString());
  }

  /** The lab grant script in docker/test-oracle is the committed output of this command. */
  @Test
  void labSetupSqlMatchesTheCommittedInitScriptByteForByte() throws Exception {
    Path repo = Path.of(System.getProperty("basedir", ".")).toAbsolutePath().getParent();
    Path script = repo.resolve("docker/test-oracle/initdb.d/04-capture-user.sql");
    assertThat(script).exists();
    Run r = run("setup-sql", "--profile", "lab");
    assertThat(r.exit()).isZero();
    assertThat(r.out()).isEqualTo(Files.readString(script));
  }

  @Test
  void productionSetupSqlUsesPlaceholderPasswordAndNoLabGrants() {
    Run r = run("setup-sql", "--user", "c##mine");
    assertThat(r.exit()).isZero();
    assertThat(r.out())
        .contains("CREATE USER c##mine IDENTIFIED BY \"<change-me>\" CONTAINER=ALL;")
        .doesNotContain("ALTER SYSTEM")
        .doesNotContain("workload");
    assertThat(run("setup-sql", "--non-cdb", "--user", "cdc").out()).doesNotContain("CONTAINER");
  }

  @Test
  void setupSqlTakesUserAndPdbsFromAConnectorConfig(@TempDir Path dir) throws Exception {
    Path cfg = dir.resolve("c.json");
    Files.writeString(
        cfg,
        "{\"name\":\"x\",\"config\":{\"cdc.database.user\":\"c##other\","
            + "\"cdc.database.pdbs\":\"PDBA,PDBB\"}}");
    Run r = run("setup-sql", "--profile", "LAB", "--config", cfg.toString());
    assertThat(r.exit()).isZero();
    assertThat(r.out())
        .contains("CREATE USER c##other")
        .contains("ALTER SESSION SET CONTAINER = PDBA;")
        .contains("ALTER SESSION SET CONTAINER = PDBB;");
  }

  @Test
  void autonomousDatabaseIsNotImplementedYet() {
    Run r = run("setup-sql", "--platform", "autonomous");
    assertThat(r.exit()).isEqualTo(DoctorMain.EXIT_NOT_IMPLEMENTED);
    assertThat(r.out()).contains("not available yet");
  }

  @Test
  void rdsScriptUsesRdsadminGrantsAndALocalUser() {
    Run r = run("setup-sql", "--platform", "rds");
    assertThat(r.exit()).isEqualTo(DoctorMain.EXIT_OK);
    assertThat(r.out())
        .contains("CREATE USER cdc IDENTIFIED BY \"<change-me>\";")
        .contains("rdsadmin.rdsadmin_util.grant_sys_object('DBMS_LOGMNR', 'CDC', 'EXECUTE');")
        .contains("archivelog retention hours")
        .doesNotContain("c##")
        .doesNotContain("CONTAINER=");
  }

  @Test
  void checkRejectsAnInvalidConfigWithUsageExitCode(@TempDir Path dir) throws Exception {
    Path cfg = dir.resolve("bad.json");
    Files.writeString(cfg, "{\"cdc.database.user\":\"u\"}");
    Run r = run("check", "--config", cfg.toString());
    assertThat(r.exit()).isEqualTo(DoctorMain.EXIT_USAGE);
    assertThat(r.err()).contains("oracle-cdc-doctor check:");
    assertThat(run("check", "--config", dir.resolve("missing.json").toString()).exit())
        .isEqualTo(DoctorMain.EXIT_USAGE);
  }

  @Test
  void readConfigAcceptsBareAndEnvelopeForms(@TempDir Path dir) throws Exception {
    Path bare = dir.resolve("bare.json");
    Files.writeString(bare, "{\"a\":\"1\",\"n\":2,\"o\":{\"x\":1}}");
    Map<String, String> m = DoctorMain.readConfig(bare);
    assertThat(m).containsEntry("a", "1").containsEntry("n", "2").containsEntry("o", "{\"x\":1}");
    assertThat(DoctorMain.csv(" a, b ,,c")).containsExactly("a", "b", "c");
    assertThat(DoctorMain.csv(null)).isEmpty();
  }

  @Test
  void noSubcommandPrintsUsage() {
    Run r = run();
    assertThat(r.exit()).isEqualTo(DoctorMain.EXIT_USAGE);
    assertThat(r.out()).contains("Usage: oracle-cdc-doctor");
  }
}
