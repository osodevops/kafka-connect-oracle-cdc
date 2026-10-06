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

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.kafka.common.config.ConfigException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;
import sh.oso.connect.oracle.OracleCdcSourceConnector;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfig;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.doctor.Doctor;
import sh.oso.connect.oracle.core.doctor.DoctorContext;
import sh.oso.connect.oracle.core.doctor.Finding;
import sh.oso.connect.oracle.core.doctor.InternalTopic;
import sh.oso.connect.oracle.core.doctor.Report;
import sh.oso.connect.oracle.core.doctor.Rules;
import sh.oso.connect.oracle.core.doctor.SetupSql.Platform;
import sh.oso.connect.oracle.core.doctor.SetupSql.Profile;
import sh.oso.connect.oracle.core.doctor.WorkerFacts;
import sh.oso.connect.oracle.core.errors.OracleCdcException;
import sh.oso.connect.oracle.doctor.admin.ConfigFiles;
import sh.oso.connect.oracle.doctor.admin.Database;
import sh.oso.connect.oracle.doctor.admin.Environment;
import sh.oso.connect.oracle.doctor.kafka.KafkaPort;
import sh.oso.connect.oracle.topics.InternalTopics;

/**
 * {@code oracle-cdc-doctor}: preflight checks, DBA setup script, redo profiler, sizing, lag
 * explanation and the admin commands for the OSO CDC Connector for Oracle Database (PRD-05). Exit
 * codes: 0 no blocking findings, 1 blocking findings (or the database could not be reached, or an
 * admin command refused), 2 warnings only, 3 not implemented yet, 64 usage error.
 */
@Command(
    name = "oracle-cdc-doctor",
    mixinStandardHelpOptions = true,
    exitCodeOnInvalidInput = 64,
    versionProvider = DoctorMain.VersionProvider.class,
    description =
        "Preflight checker and operations CLI for the OSO CDC Connector for Oracle Database.",
    subcommands = {
      DoctorMain.Check.class,
      DoctorMain.SetupSqlCommand.class,
      DiagnosticCommands.RedoProfileCommand.class,
      DiagnosticCommands.SizingCommand.class,
      DiagnosticCommands.ExplainLag.class,
      AdminCommands.Admin.class
    })
public final class DoctorMain implements Callable<Integer>, AdminCommands.Rooted {

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

  private final Environment env;

  DoctorMain(Environment env) {
    this.env = env;
  }

  @Override
  public Environment env() {
    return env;
  }

  public static void main(String[] args) {
    System.exit(run(args));
  }

  /** Runs the CLI against the standard streams and returns the exit code. */
  public static int run(String... args) {
    return commandLine(Environment.standard()).execute(args);
  }

  /** Runs the CLI with the given streams; used by tests. */
  public static int run(PrintWriter out, PrintWriter err, String... args) {
    return run(Environment.standard(), out, err, args);
  }

  /** Runs the CLI with the given environment and streams; used by tests. */
  public static int run(Environment env, PrintWriter out, PrintWriter err, String... args) {
    return commandLine(env).setOut(out).setErr(err).execute(args);
  }

  private static CommandLine commandLine(Environment env) {
    return new CommandLine(new DoctorMain(env))
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
    return ConfigFiles.connector(file);
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

  /** {@code cdc.kafka.*} as client properties, with overrides from the command line. */
  static Properties kafkaProperties(Map<String, String> props, String bootstrap, Path commandConfig)
      throws IOException {
    Properties p = new Properties();
    for (Map.Entry<String, String> e : props.entrySet()) {
      if (e.getKey().startsWith(OracleCdcSourceConnectorConfig.KAFKA_CLIENT_PREFIX)) {
        p.put(
            e.getKey().substring(OracleCdcSourceConnectorConfig.KAFKA_CLIENT_PREFIX.length()),
            e.getValue());
      }
    }
    if (bootstrap != null) {
      p.put("bootstrap.servers", bootstrap);
    }
    if (commandConfig != null) {
      p.putAll(ConfigFiles.properties(commandConfig));
    }
    return p.containsKey("bootstrap.servers") ? p : null;
  }

  /** The internal topics the connector would use, or none when the config cannot say. */
  static List<InternalTopic> internalTopics(Map<String, String> props) {
    try {
      OracleCdcSourceConnectorConfig c = new OracleCdcSourceConnectorConfig(props);
      List<InternalTopic> out = new ArrayList<>();
      for (Map.Entry<String, InternalTopics.Spec> e : InternalTopics.of(c).entrySet()) {
        out.add(
            new InternalTopic(
                e.getKey(),
                e.getValue().name(),
                e.getValue().compacted(),
                e.getValue().retentionMs()));
      }
      return out;
    } catch (RuntimeException e) {
      return List.of();
    }
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

    @Option(
        names = "--rules",
        defaultValue = "all",
        description =
            "all (default) or fast, the subset the connector's validate runs (no Kafka or"
                + " Connect checks).")
    String rules;

    @Option(
        names = "--max-downtime",
        defaultValue = "24h",
        converter = Durations.class,
        description =
            "Longest planned stop of the connector, for the archive retention rule DOC-10;"
                + " default 24h.")
    Duration maxDowntime;

    @Option(
        names = "--bootstrap-servers",
        description = "Brokers for DOC-17 and DOC-18; default cdc.kafka.bootstrap.servers.")
    String bootstrap;

    @Option(
        names = "--command-config",
        description = "Kafka client properties file (security settings) for the broker checks.")
    Path commandConfig;

    @Option(
        names = "--connect-url",
        description =
            "Kafka Connect REST URL; with exactly.once.support=required, DOC-18 asks a worker to"
                + " validate the configuration.")
    String connectUrl;

    @Override
    public Integer call() throws IOException {
      PrintWriter out = spec.commandLine().getOut();
      PrintWriter err = spec.commandLine().getErr();
      Environment env = ((AdminCommands.Rooted) spec.root().userObject()).env();
      Map<String, String> props;
      CoreConfig cfg;
      try {
        props = readConfig(config);
        cfg = new CoreConfig(props);
      } catch (ConfigException | IOException e) {
        err.println("oracle-cdc-doctor check: " + e.getMessage());
        return EXIT_USAGE;
      }
      boolean all = !"fast".equalsIgnoreCase(rules);
      Report report;
      KafkaPort kafka = null;
      try (Database db = env.database(cfg)) {
        DoctorContext ctx =
            new DoctorContext(
                    cfg,
                    db.catalog(),
                    csv(props.get(TABLES_INCLUDE)),
                    csv(props.get(TABLES_EXCLUDE)),
                    props.getOrDefault(KEY_MISSING, "fail"))
                .withConnectorProperties(props)
                .withMaxDowntime(maxDowntime)
                .withClock(env.clock());
        if (all) {
          List<InternalTopic> topics = internalTopics(props);
          Properties kp = kafkaProperties(props, bootstrap, commandConfig);
          if (kp != null) {
            kafka = env.kafka(kp);
            ctx.withKafka(kafka.facts(), topics);
          } else {
            ctx.withInternalTopics(topics);
          }
          if (connectUrl != null
              && "required".equalsIgnoreCase(props.getOrDefault("exactly.once.support", ""))) {
            ctx.withWorker(worker(env, props, err));
          }
        }
        report = new Doctor(all ? Rules.all() : Rules.fastMode()).run(ctx);
      } catch (OracleCdcException e) {
        report = new Report(List.of(Finding.blocking("CONNECT", e.getMessage(), null)));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        report = new Report(List.of(Finding.blocking("CONNECT", "Interrupted", null)));
      } catch (Exception e) {
        report =
            new Report(
                List.of(
                    Finding.blocking(
                        "CONNECT", "The database connection failed: " + e.getMessage(), null)));
      } finally {
        if (kafka != null) {
          kafka.close();
        }
      }
      out.print(render(report, format));
      out.flush();
      return report.exitCode();
    }

    private WorkerFacts worker(Environment env, Map<String, String> props, PrintWriter err) {
      try {
        Map<String, List<String>> errors =
            env.connect(connectUrl)
                .validate(
                    props.getOrDefault("connector.class", OracleCdcSourceConnector.class.getName()),
                    props);
        List<String> eos = errors.get("exactly.once.support");
        return new WorkerFacts(eos == null || eos.isEmpty() ? null : String.join(" ", eos));
      } catch (IOException e) {
        err.println(
            "oracle-cdc-doctor check: the worker could not validate the configuration: "
                + e.getMessage());
        return null;
      }
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
        pdbs =
            profile == Profile.LAB ? Arrays.asList("FREEPDB1", "FREEPDB2", "FREEPDB3") : List.of();
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
