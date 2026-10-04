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

import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Workload generator, materialiser and correctness oracle entry point (testing strategy section 3).
 * Subcommands land with plan increments P0-13 and P1-13.
 */
@Command(
    name = "bench",
    mixinStandardHelpOptions = true,
    description = "Deterministic Oracle workload generator and Kafka correctness oracle.")
public final class BenchMain implements Callable<Integer> {

  public static void main(String[] args) {
    System.exit(new CommandLine(new BenchMain()).execute(args));
  }

  @Override
  public Integer call() {
    CommandLine.usage(this, System.out);
    return CommandLine.ExitCode.USAGE;
  }
}
