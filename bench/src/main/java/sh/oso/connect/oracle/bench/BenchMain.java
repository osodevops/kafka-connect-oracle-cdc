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
