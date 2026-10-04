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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.kafka.common.config.ConfigException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.doctor.Doctor;
import sh.oso.connect.oracle.core.doctor.DoctorContext;
import sh.oso.connect.oracle.core.doctor.Finding;
import sh.oso.connect.oracle.core.doctor.JdbcDoctorCatalog;
import sh.oso.connect.oracle.core.doctor.Report;
import sh.oso.connect.oracle.core.doctor.Rules;
import sh.oso.connect.oracle.core.doctor.SetupSql.Platform;
import sh.oso.connect.oracle.core.doctor.SetupSql.Profile;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.errors.OracleCdcException;
import sh.oso.connect.oracle.core.jdbc.ConnectionFactory;
import sh.oso.connect.oracle.core.jdbc.ConnectionRole;
import sh.oso.connect.oracle.core.jdbc.OracleConnectionSpec;
import sh.oso.connect.oracle.core.jdbc.RetryPolicy;

/**
 * {@code oracle-cdc-doctor}: preflight checks, DBA setup script, redo profiler and admin commands
 * for the OSO CDC Connector for Oracle Database (PRD-05). Exit codes: 0 no blocking findings, 1
 * blocking findings (or the database could not be reached), 2 warnings only, 3 not implemented yet,
 * 64 usage error.
 */
@Command(
    name = "oracle-cdc-doctor",
    mixinStandardHelpOptions = true,
    exitCodeOnInvalidInput = 64,
    versionProvider = DoctorMain.VersionProvider.class,
    description =
        "Preflight checker and operations CLI for the OSO CDC Connector for Oracle Database.",
    subcommands = {DoctorMain.Check.class, DoctorMain.SetupSqlCommand.class})
public final class DoctorMain implements Callable<Integer> {

  public static final int EXIT_OK = Report.EXIT_OK;
  public static final int EXIT_BLOCKING = Report.EXIT_BLOCKING;
  public static final int EXIT_WARNINGS = Report.EXIT_WARNINGS;
  public static final int EXIT_NOT_IMPLEMENTED = 3;
  public static final int EXIT_USAGE = 64;

  /** Connector config keys the doctor reads that belong to the connector, not the core. */
  static final String TABLES_INCLUDE = "cdc.tables.include";

  static final String TABLES_EXCLUDE = "cdc.tables.exclude";
  static final String KEY_MISSING = "cdc.key.missing";

  @Spec CommandSpec spec;

  public static void main(String[] args) {
    System.exit(run(args));
  }

  /** Runs the CLI against the standard streams and returns the exit code. */
  public static int run(String... args) {
    return commandLine().execute(args);
  }

  /** Runs the CLI with the given streams; used by tests. */
  public static int run(PrintWriter out, PrintWriter err, String... args) {
    return commandLine().setOut(out).setErr(err).execute(args);
  }

  private static CommandLine commandLine() {
    return new CommandLine(new DoctorMain())
        .setUsageHelpWidth(100)
        .setCaseInsensitiveEnumValuesAllowed(true);
  }

  @Override
  public Integer call() {
    spec.commandLine().usage(spec.commandLine().getOut());
    return EXIT_USAGE;
  }

  /** Reads a connector config file: either a bare {@code {"k": "v"}} map or a REST envelope. */
  static Map<String, String> readConfig(Path file) throws IOException {
    JsonNode root = new ObjectMapper().readTree(Files.readString(file));
    if (root == null || !root.isObject()) {
      throw new IOException(file + " does not hold a JSON object");
    }
    JsonNode cfg = root.has("config") && root.get("config").isObject() ? root.get("config") : root;
    Map<String, String> out = new LinkedHashMap<>();
    for (Iterator<Map.Entry<String, JsonNode>> it = cfg.fields(); it.hasNext(); ) {
      Map.Entry<String, JsonNode> e = it.next();
      out.put(
          e.getKey(), e.getValue().isValueNode() ? e.getValue().asText() : e.getValue().toString());
    }
    return out;
  }

  static List<String> csv(String value) {
    if (value == null || value.isBlank()) {
      return List.of();
    }
    List<String> out = new ArrayList<>();
    for (String s : value.split(",")) {
      if (!s.isBlank()) {
        out.add(s.trim());
      }
    }
    return out;
  }

  @Command(
      name = "check",
      exitCodeOnInvalidInput = 64,
      description = "Run the preflight rules against the database named in the connector config.")
  static final class Check implements Callable<Integer> {
    @Spec CommandSpec spec;

    @Option(
        names = "--config",
        required = true,
        description = "Connector config JSON (REST envelope or bare config).")
    Path config;

    @Option(names = "--format", defaultValue = "markdown", description = "markdown, json or junit.")
    String format;

    @Override
    public Integer call() throws IOException, InterruptedException {
      PrintWriter out = spec.commandLine().getOut();
      PrintWriter err = spec.commandLine().getErr();
      Map<String, String> props;
      CoreConfig cfg;
      try {
        props = readConfig(config);
        cfg = new CoreConfig(props);
      } catch (ConfigException | IOException e) {
        err.println("oracle-cdc-doctor check: " + e.getMessage());
        return EXIT_USAGE;
      }
      Report report;
      ConnectionFactory factory =
          new ConnectionFactory(
              OracleConnectionSpec.from(cfg),
              new RetryPolicy(Duration.ofSeconds(30)),
              new OraErrorClassifier(Set.copyOf(cfg.extraRetryErrorCodes())));
      try (Connection c = factory.open(ConnectionRole.METADATA)) {
        DoctorContext ctx =
            new DoctorContext(
                cfg,
                new JdbcDoctorCatalog(c),
                csv(props.get(TABLES_INCLUDE)),
                csv(props.get(TABLES_EXCLUDE)),
                props.getOrDefault(KEY_MISSING, "fail"));
        report = new Doctor(Rules.fastMode()).run(ctx);
      } catch (OracleCdcException e) {
        report = new Report(List.of(Finding.blocking("CONNECT", e.getMessage(), null)));
      } catch (SQLException e) {
        report =
            new Report(
                List.of(
                    Finding.blocking(
                        "CONNECT", "The database connection failed: " + e.getMessage(), null)));
      }
      out.print(render(report, format));
      out.flush();
      return report.exitCode();
    }

    static String render(Report report, String format) {
      switch (format == null ? "markdown" : format.toLowerCase(Locale.ROOT)) {
        case "json":
          return report.toJson() + System.lineSeparator();
        case "junit":
          return report.toJUnitXml();
        default:
          return report.toMarkdown();
      }
    }
  }

  @Command(
      name = "setup-sql",
      exitCodeOnInvalidInput = 64,
      description = "Generate the commented SQL script a DBA runs to prepare the database.")
  static final class SetupSqlCommand implements Callable<Integer> {
    @Spec CommandSpec spec;

    @Option(
        names = "--config",
        description = "Connector config JSON; supplies the user name and PDB list when given.")
    Path config;

    @Option(names = "--profile", defaultValue = "production", description = "production or lab.")
    Profile profile;

    @Option(
        names = "--platform",
        defaultValue = "onprem",
        description = "onprem (RDS and Autonomous arrive in later phases).")
    Platform platform;

    @Option(names = "--user", description = "Mining user; default c##cdc.")
    String user;

    @Option(
        names = "--password",
        description =
            "Password written into the script; default cdc for lab, a placeholder otherwise.")
    String password;

    @Option(
        names = "--non-cdb",
        description = "Generate for a non-CDB database (no CONTAINER clauses).")
    boolean nonCdb;

    @Option(
        names = "--pdbs",
        split = ",",
        description =
            "PDBs that get a workload schema in the lab profile; default FREEPDB1,FREEPDB2.")
    List<String> pdbs;

    @Override
    public Integer call() throws IOException {
      PrintWriter out = spec.commandLine().getOut();
      if (config != null) {
        Map<String, String> props = readConfig(config);
        if (user == null) {
          user = props.get(CoreConfig.DATABASE_USER);
        }
        if (pdbs == null && props.get(CoreConfig.DATABASE_PDBS) != null) {
          pdbs = csv(props.get(CoreConfig.DATABASE_PDBS));
        }
      }
      if (user == null) {
        user = "c##cdc";
      }
      if (password == null) {
        password = profile == Profile.LAB ? "cdc" : "<change-me>";
      }
      if (pdbs == null) {
        pdbs = profile == Profile.LAB ? Arrays.asList("FREEPDB1", "FREEPDB2") : List.of();
      }
      String sql =
          sh.oso.connect.oracle.core.doctor.SetupSql.generate(
              user, password, !nonCdb, profile, platform, pdbs);
      out.print(sql);
      out.flush();
      return platform == Platform.ONPREM ? EXIT_OK : EXIT_NOT_IMPLEMENTED;
    }
  }

  /** Reports the Maven version from the manifest. */
  public static final class VersionProvider implements CommandLine.IVersionProvider {
    @Override
    public String[] getVersion() {
      String v = DoctorMain.class.getPackage().getImplementationVersion();
      return new String[] {"oracle-cdc-doctor " + (v == null ? "dev" : v)};
    }
  }
}
