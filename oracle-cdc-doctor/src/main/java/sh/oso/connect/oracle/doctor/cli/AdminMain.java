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

import java.io.PrintWriter;
import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;
import sh.oso.connect.oracle.doctor.admin.AdminException;
import sh.oso.connect.oracle.doctor.admin.Environment;

/**
 * {@code oracle-cdc-admin}: the admin commands of PRD-05 section 4 under their own name, the same
 * commands as {@code oracle-cdc-doctor admin}. Exit codes: 0 done, 1 refused or failed, 64 usage.
 */
@Command(
    name = "oracle-cdc-admin",
    mixinStandardHelpOptions = true,
    exitCodeOnInvalidInput = 64,
    versionProvider = DoctorMain.VersionProvider.class,
    description = "Operator commands for the OSO CDC Connector for Oracle Database.",
    subcommands = {
      AdminCommands.Offsets.class,
      AdminCommands.Resnapshot.class,
      AdminCommands.Transactions.class,
      AdminCommands.Journal.class
    })
public final class AdminMain implements Callable<Integer>, AdminCommands.Rooted {

  @Spec CommandSpec spec;

  private final Environment env;

  AdminMain(Environment env) {
    this.env = env;
  }

  @Override
  public Environment env() {
    return env;
  }

  public static void main(String[] args) {
    System.exit(
        run(
            Environment.standard(),
            new PrintWriter(System.out, true),
            new PrintWriter(System.err, true),
            args));
  }

  public static int run(Environment env, PrintWriter out, PrintWriter err, String... args) {
    return new CommandLine(new AdminMain(env))
        .setUsageHelpWidth(100)
        .setCaseInsensitiveEnumValuesAllowed(true)
        .setOut(out)
        .setErr(err)
        .execute(args);
  }

  @Override
  public Integer call() {
    spec.commandLine().usage(spec.commandLine().getOut());
    return AdminException.USAGE;
  }
}
