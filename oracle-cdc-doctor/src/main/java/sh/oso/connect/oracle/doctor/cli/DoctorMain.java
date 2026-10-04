package sh.oso.connect.oracle.doctor.cli;

import java.util.concurrent.Callable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * {@code oracle-cdc-doctor}: preflight checks, DBA setup script, redo profiler and admin commands
 * for the OSO CDC Connector for Oracle Database (PRD-05). Exit codes: 0 no blocking findings, 1
 * blocking findings, 2 warnings only, 3 not implemented yet, 64 usage error.
 */
@Command(
    name = "oracle-cdc-doctor",
    mixinStandardHelpOptions = true,
    versionProvider = DoctorMain.VersionProvider.class,
    description = "Preflight checker and operations CLI for the OSO CDC Connector for Oracle Database.",
    subcommands = {DoctorMain.Check.class, DoctorMain.SetupSql.class})
public final class DoctorMain implements Callable<Integer> {

  public static final int EXIT_OK = 0;
  public static final int EXIT_BLOCKING = 1;
  public static final int EXIT_WARNINGS = 2;
  public static final int EXIT_NOT_IMPLEMENTED = 3;

  public static void main(String[] args) {
    System.exit(run(args));
  }

  /** Runs the CLI and returns the exit code without calling {@link System#exit}. */
  public static int run(String... args) {
    return new CommandLine(new DoctorMain()).setUsageHelpWidth(100).execute(args);
  }

  @Override
  public Integer call() {
    CommandLine.usage(this, System.out);
    return CommandLine.ExitCode.USAGE;
  }

  @Command(name = "check", description = "Run every preflight rule against the database and Kafka named in the connector config.")
  static final class Check implements Callable<Integer> {
    @Option(names = "--config", required = true, description = "Connector config JSON (REST envelope or bare config).")
    String config;

    @Option(names = "--format", defaultValue = "markdown", description = "markdown, json or junit.")
    String format;

    @Override
    public Integer call() {
      System.err.println("oracle-cdc-doctor check: not implemented yet (plan increment P0-12).");
      return EXIT_NOT_IMPLEMENTED;
    }
  }

  @Command(name = "setup-sql", description = "Generate the commented SQL script a DBA runs to prepare the database.")
  static final class SetupSql implements Callable<Integer> {
    @Option(names = "--config", required = true, description = "Connector config JSON.")
    String config;

    @Option(names = "--profile", defaultValue = "production", description = "production or lab.")
    String profile;

    @Option(names = "--platform", defaultValue = "onprem", description = "onprem, rds or autonomous.")
    String platform;

    @Override
    public Integer call() {
      System.err.println("oracle-cdc-doctor setup-sql: not implemented yet (plan increment P0-12).");
      return EXIT_NOT_IMPLEMENTED;
    }
  }

  static final class VersionProvider implements CommandLine.IVersionProvider {
    @Override
    public String[] getVersion() {
      String v = DoctorMain.class.getPackage().getImplementationVersion();
      return new String[] {"oracle-cdc-doctor " + (v != null ? v : "unknown")};
    }
  }
}
