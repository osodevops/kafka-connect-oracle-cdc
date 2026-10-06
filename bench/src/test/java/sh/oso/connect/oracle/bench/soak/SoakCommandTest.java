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
package sh.oso.connect.oracle.bench.soak;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import picocli.CommandLine;
import sh.oso.connect.oracle.bench.workload.WorkloadGenerator;
import sh.oso.connect.oracle.bench.workload.WorkloadSpec;

/** Options, the password rules and the bundled spec of {@code bench soak}. */
class SoakCommandTest {

  @TempDir Path dir;

  String[] args(String... extra) {
    List<String> a =
        new ArrayList<>(
            List.of(
                "--url", "jdbc:oracle:thin:@//localhost:1521/FREEPDB1",
                "--user", "workload",
                "--hours", "1",
                "--check-every", "0.25",
                "--bootstrap-servers", "localhost:9092",
                "--topic-prefix", "cdc",
                "--pdb", "FREEPDB1",
                "--connect-url", "http://localhost:8083",
                "--connector", "oracle-cdc",
                "--out", dir.resolve("out").toString()));
    a.addAll(List.of(extra));
    return a.toArray(String[]::new);
  }

  @Test
  void exactlyOneWayToGiveThePasswordIsRequired() {
    assertThatThrownBy(() -> SoakCommand.parse(args()))
        .isInstanceOf(CommandLine.MissingParameterException.class);
    assertThatThrownBy(
            () -> SoakCommand.parse(args("--password", "x", "--password-env", "WORKLOAD_PW")))
        .isInstanceOf(CommandLine.MutuallyExclusiveArgsException.class);
    SoakCommand c = SoakCommand.parse(args("--password-env", "WORKLOAD_PW"));
    assertThat(c.catchUpMinutes).isEqualTo(30);
    assertThat(c.checkEvery).isEqualTo(0.25);
  }

  @Test
  void thePasswordComesFromTheNamedVariable() throws Exception {
    SoakCommand c = SoakCommand.parse(args("--password-env", "WORKLOAD_PW"));
    c.env = Map.of("WORKLOAD_PW", "from-env")::get;
    assertThat(c.password()).isEqualTo("from-env");
    c.env = Map.<String, String>of()::get;
    assertThatThrownBy(c::password)
        .isInstanceOf(CommandLine.ParameterException.class)
        .hasMessageContaining("WORKLOAD_PW")
        .hasMessageContaining("not set");
  }

  @Test
  void theBundledSpecIsPacedValidAndRunOpenEnded() throws Exception {
    SoakCommand c = SoakCommand.parse(args("--password-env", "P"));
    WorkloadSpec s = c.loadSpec();

    assertThat(s.sessions).isEqualTo(2);
    assertThat(s.pauseMillisBetweenTransactions).isEqualTo(50);
    assertThat(s.lobWeight).isZero(); // the lab connector skips LOBs
    assertThat(s.truncateProbability).isZero();
    assertThat(s.ddlProbability).isZero();
    assertThat(s.largeTransactionEvery).isZero();
    assertThat(s.insertWeight + s.lobWeight).isEqualTo(s.deleteWeight);
    assertThat(s.transactionsPerSession).isZero();
    assertThat(s.durationSeconds).isGreaterThan(3600);
    new WorkloadGenerator(s, "jdbc:none", "u", "p"); // validates without connecting
    // the file also runs as a plain bench workload spec
    WorkloadSpec plain =
        WorkloadSpec.read(Path.of("src/main/resources" + SoakCommand.DEFAULT_SPEC));
    new WorkloadGenerator(plain, "jdbc:none", "u", "p");
  }

  @Test
  void theOracleMustCompareEveryTableTheWorkloadWrites() {
    SoakCommand c = SoakCommand.parse(args("--password-env", "P"));
    assertThat(c.tables(List.of("WL_T1", "WL_T2", "WL_T3")))
        .containsExactly("WL_T1", "WL_T2", "WL_T3");
    SoakCommand some = SoakCommand.parse(args("--password-env", "P", "--tables", "wl_t1,WL_T2"));
    assertThatThrownBy(() -> some.tables(List.of("WL_T1", "WL_T2", "WL_T3")))
        .isInstanceOf(CommandLine.ParameterException.class)
        .hasMessageContaining("WL_T3");
  }

  @Test
  void anOutDirectoryHoldingEvidenceIsNotReused() throws Exception {
    Path out = dir.resolve("out");
    Files.createDirectories(out);
    Files.writeString(out.resolve("check-001.json"), "{}");
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    CommandLine cl = new CommandLine(new SoakCommand());
    cl.setErr(new java.io.PrintWriter(err, true, StandardCharsets.UTF_8));
    SoakCommand cmd = cl.getCommand();
    cmd.env = Map.of("P", "pw")::get;
    int code = cl.execute(args("--password-env", "P"));
    assertThat(code).isEqualTo(CommandLine.ExitCode.USAGE);
    assertThat(err.toString(StandardCharsets.UTF_8)).contains("already holds a soak's evidence");
  }

  @Test
  void invalidDurationsAreRejectedBeforeAnythingConnects() {
    ByteArrayOutputStream err = new ByteArrayOutputStream();
    CommandLine cl = new CommandLine(new SoakCommand());
    cl.setErr(new java.io.PrintWriter(err, true, StandardCharsets.UTF_8));
    SoakCommand cmd = cl.getCommand();
    cmd.env = Map.of("P", "pw")::get;
    List<String> a = new ArrayList<>(List.of(args("--password-env", "P")));
    a.set(a.indexOf("--hours") + 1, "0");
    assertThat(cl.execute(a.toArray(String[]::new))).isEqualTo(CommandLine.ExitCode.USAGE);
    assertThat(err.toString(StandardCharsets.UTF_8)).contains("must be positive");
  }

  @Test
  void helpListsTheSoakOptions() {
    ByteArrayOutputStream bout = new ByteArrayOutputStream();
    PrintStream old = System.out;
    try {
      System.setOut(new PrintStream(bout, true, StandardCharsets.UTF_8));
      assertThat(sh.oso.connect.oracle.bench.BenchMain.run("soak", "--help")).isZero();
    } finally {
      System.setOut(old);
    }
    assertThat(bout.toString(StandardCharsets.UTF_8))
        .contains("--password-env", "--check-every", "--catch-up-timeout", "--metrics-url")
        .contains("never publish");
  }

  @Test
  void credentialsInsideUrlsAreMasked() {
    Redactor r = new Redactor().add("pw-123");
    assertThat(r.scrub("jdbc:oracle:thin:workload/pw-123@//db:1521/FREEPDB1"))
        .isEqualTo("jdbc:oracle:thin:***@//db:1521/FREEPDB1");
    assertThat(r.scrub("http://admin:secret@connect:8083/connectors"))
        .isEqualTo("http://***@connect:8083/connectors");
    assertThat(r.scrub("jdbc:oracle:thin:@//db:1521/FREEPDB1"))
        .isEqualTo("jdbc:oracle:thin:@//db:1521/FREEPDB1");
    assertThat(r.scrub("ORA-01017 for pw-123")).isEqualTo("ORA-01017 for ***");
  }
}
