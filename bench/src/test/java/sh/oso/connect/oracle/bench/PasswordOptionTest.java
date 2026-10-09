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
package sh.oso.connect.oracle.bench;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;
import sh.oso.connect.oracle.bench.check.CheckCommand;
import sh.oso.connect.oracle.bench.workload.WorkloadCommand;

class PasswordOptionTest {

  @Test
  void thePasswordComesFromTheNamedEnvironmentVariable() {
    PasswordOption p = new PasswordOption();
    p.passwordEnv = "WORKLOAD_PASSWORD";
    assertThat(p.resolve(Map.of("WORKLOAD_PASSWORD", "s")::get)).isEqualTo("s");
    assertThatThrownBy(() -> p.resolve(Map.<String, String>of()::get))
        .hasMessageContaining("WORKLOAD_PASSWORD named by --password-env is not set");
  }

  @Test
  void workloadAndCheckTakeExactlyOneWayOfGivingThePassword() {
    String[] common = {"--url", "jdbc:oracle:thin:@//h:1521/P", "--user", "u"};
    for (Object cmd : new Object[] {new WorkloadCommand(), new CheckCommand()}) {
      CommandLine cl = new CommandLine(cmd);
      String[] extra =
          cmd instanceof CheckCommand
              ? new String[] {
                "--bootstrap-servers", "b:9092", "--topic-prefix", "cdc", "--tables", "T1"
              }
              : new String[0];
      cl.parseArgs(concat(common, extra, new String[] {"--password-env", "V"}));
      assertThatThrownBy(() -> new CommandLine(cmd).parseArgs(concat(common, extra)))
          .as("no password at all")
          .isInstanceOf(CommandLine.ParameterException.class);
      assertThatThrownBy(
              () ->
                  new CommandLine(cmd)
                      .parseArgs(
                          concat(
                              common,
                              extra,
                              new String[] {"--password-env", "V", "--password", "p"})))
          .as("both at once")
          .isInstanceOf(CommandLine.ParameterException.class);
    }
  }

  private static String[] concat(String[]... parts) {
    return java.util.Arrays.stream(parts).flatMap(java.util.Arrays::stream).toArray(String[]::new);
  }
}
