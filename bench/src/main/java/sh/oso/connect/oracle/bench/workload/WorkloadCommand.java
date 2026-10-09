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
package sh.oso.connect.oracle.bench.workload;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** {@code bench workload}: runs a seeded workload against one PDB and prints the counters. */
@Command(
    name = "workload",
    mixinStandardHelpOptions = true,
    description = "Run a deterministic workload against an Oracle schema and print the counters.")
public final class WorkloadCommand implements Callable<Integer> {

  @Option(names = "--url", required = true, description = "JDBC URL of the PDB.")
  String url;

  @Option(names = "--user", required = true, description = "Schema owner that holds the tables.")
  String user;

  @ArgGroup(exclusive = true, multiplicity = "1")
  sh.oso.connect.oracle.bench.PasswordOption password;

  @Option(names = "--spec", description = "Workload spec JSON; defaults apply when omitted.")
  Path spec;

  @Option(names = "--seed", description = "Overrides the spec seed.")
  Long seed;

  @Option(names = "--sessions", description = "Overrides the spec session count.")
  Integer sessions;

  @Option(names = "--transactions", description = "Overrides transactions per session.")
  Integer transactions;

  @Option(
      names = "--duration",
      description = "Overrides the run time in seconds (sets transactions to 0).")
  Integer duration;

  @Option(names = "--reset", description = "Drop and recreate the tables and ledger first.")
  boolean reset;

  @Override
  public Integer call() throws IOException, SQLException, InterruptedException {
    WorkloadSpec s = spec == null ? WorkloadSpec.defaults() : WorkloadSpec.read(spec);
    if (seed != null) {
      s.seed = seed;
    }
    if (sessions != null) {
      s.sessions = sessions;
    }
    if (transactions != null) {
      s.transactionsPerSession = transactions;
    }
    if (duration != null) {
      s.durationSeconds = duration;
      s.transactionsPerSession = 0;
    }
    WorkloadGenerator g = new WorkloadGenerator(s, url, user, password.resolve(System::getenv));
    if (reset) {
      g.reset();
    }
    WorkloadResult r = g.run();
    System.out.println(r.toJson());
    return 0;
  }
}
