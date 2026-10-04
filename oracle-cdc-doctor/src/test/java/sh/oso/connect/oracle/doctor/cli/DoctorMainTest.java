package sh.oso.connect.oracle.doctor.cli;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class DoctorMainTest {
  @Test
  void helpAndVersionExitZero() {
    assertThat(DoctorMain.run("--help")).isZero();
    assertThat(DoctorMain.run("--version")).isZero();
  }

  @Test
  void subcommandsReportNotImplementedUntilTheyLand() {
    assertThat(DoctorMain.run("check", "--config", "x.json")).isEqualTo(DoctorMain.EXIT_NOT_IMPLEMENTED);
    assertThat(DoctorMain.run("setup-sql", "--config", "x.json", "--profile", "lab"))
        .isEqualTo(DoctorMain.EXIT_NOT_IMPLEMENTED);
  }

  @Test
  void missingRequiredOptionIsAUsageError() {
    assertThat(DoctorMain.run("check")).isEqualTo(picocli.CommandLine.ExitCode.USAGE);
  }
}
