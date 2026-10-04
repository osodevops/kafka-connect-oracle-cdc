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

import org.junit.jupiter.api.Test;

class DoctorMainTest {
  @Test
  void helpAndVersionExitZero() {
    assertThat(DoctorMain.run("--help")).isZero();
    assertThat(DoctorMain.run("--version")).isZero();
  }

  @Test
  void subcommandsReportNotImplementedUntilTheyLand() {
    assertThat(DoctorMain.run("check", "--config", "x.json"))
        .isEqualTo(DoctorMain.EXIT_NOT_IMPLEMENTED);
    assertThat(DoctorMain.run("setup-sql", "--config", "x.json", "--profile", "lab"))
        .isEqualTo(DoctorMain.EXIT_NOT_IMPLEMENTED);
  }

  @Test
  void missingRequiredOptionIsAUsageError() {
    assertThat(DoctorMain.run("check")).isEqualTo(picocli.CommandLine.ExitCode.USAGE);
  }
}
