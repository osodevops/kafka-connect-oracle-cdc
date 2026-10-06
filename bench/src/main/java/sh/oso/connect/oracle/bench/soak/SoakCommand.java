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
package sh.oso.connect.oracle.bench.soak;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Stream;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;
import sh.oso.connect.oracle.bench.check.CheckCommand;
import sh.oso.connect.oracle.bench.check.CheckReport;
import sh.oso.connect.oracle.bench.check.CorrectnessCheck;
import sh.oso.connect.oracle.bench.workload.WorkloadGenerator;
import sh.oso.connect.oracle.bench.workload.WorkloadSpec;

/**
 * {@code bench soak}: the T3 soak against an already running connector (see {@link Soak}). Exit 0
 * pass, 1 fail, 3 inconclusive, 4 lag, 5 error.
 */
@Command(
    name = "soak",
    mixinStandardHelpOptions = true,
    description = {
      "Run a paced workload for many hours against a running connector, with the correctness"
          + " oracle at fixed check points, and write evidence, metrics and a summary.",
      "The figures are internal: never publish them.",
      "Exit codes: 0 pass, 1 fail, 3 inconclusive, 4 lag (catch-up timeout), 5 error."
    })
public final class SoakCommand implements Callable<Integer> {

  /** The bundled paced spec used when {@code --spec} is not given. */
  public static final String DEFAULT_SPEC = "/soak/default.json";

  @Spec CommandSpec cmd;

  @Option(names = "--url", required = true, description = "JDBC URL of the PDB.")
  String url;

  @Option(names = "--user", required = true, description = "Schema owner that holds the tables.")
  String user;

  @ArgGroup(exclusive = true, multiplicity = "1")
  Secret secret;

  /** Exactly one way to give the password. */
  static final class Secret {
    @Option(
        names = "--password-env",
        paramLabel = "VAR",
        description = "Environment variable holding the password.")
    String passwordEnv;

    @Option(
        names = "--password",
        description = "Password; prefer --password-env, a command line is visible to other users.")
    String password;
  }

  @Option(
      names = "--spec",
      description = "Workload spec JSON (default: the bundled paced spec soak/default.json).")
  Path spec;

  @Option(names = "--seed", description = "Overrides the spec seed.")
  Long seed;

  @Option(names = "--hours", required = true, description = "Length of the soak in hours.")
  double hours;

  @Option(
      names = "--check-every",
      defaultValue = "6",
      description = "Hours between checks (default ${DEFAULT-VALUE}).")
  double checkEvery;

  @Option(names = "--bootstrap-servers", required = true, description = "Kafka bootstrap servers.")
  String bootstrap;

  @Option(names = "--topic-prefix", required = true, description = "Connector topic prefix.")
  String prefix;

  @Option(names = "--pdb", description = "PDB name used in topic names (omit for a non-CDB).")
  String pdb;

  @Option(
      names = "--tables",
      split = ",",
      description = "Tables to compare, comma-separated (default: every table of the spec).")
  List<String> tables;

  @Option(names = "--connect-url", required = true, description = "Kafka Connect REST URL.")
  String connectUrl;

  @Option(names = "--connector", required = true, description = "Connector name.")
  String connector;

  @Option(
      names = "--metrics-url",
      description = "Prometheus text endpoint of the worker's JMX exporter (optional).")
  String metricsUrl;

  @Option(
      names = "--catch-up-timeout",
      defaultValue = "30",
      description =
          "Minutes the connector may take to pass a check SCN (default ${DEFAULT-VALUE}).")
  int catchUpMinutes;

  @Option(
      names = "--check-timeout",
      defaultValue = "180",
      description =
          "Minutes one check may spend reading the topics (default ${DEFAULT-VALUE}); the read"
              + " grows with the run.")
  int checkTimeoutMinutes;

  @Option(
      names = "--check-idle",
      defaultValue = "30",
      description =
          "Seconds without new records that end a check's read (default ${DEFAULT-VALUE}).")
  int checkIdleSeconds;

  @Option(names = "--out", required = true, description = "Directory for evidence and summary.")
  Path out;

  /** Test seams. */
  Function<String, String> env = System::getenv;

  PrintStream console = System.out;

  @Override
  public Integer call() throws Exception {
    String password = password();
    validate();
    WorkloadSpec s = loadSpec();
    List<String> specTables = new ArrayList<>();
    for (int i = 1; i <= s.tables; i++) {
      specTables.add(s.tableName(i).toUpperCase(Locale.ROOT));
    }
    List<String> compared = tables(specTables);
    String owner = user.toUpperCase(Locale.ROOT);
    List<String> topics = CheckCommand.topics(prefix, pdb, owner, compared);
    String ledger = s.ledgerTable.toUpperCase(Locale.ROOT);

    Redactor redactor = new Redactor().add(password);
    Soak.Clock clock = Soak.Clock.SYSTEM;
    SoakOutput output = new SoakOutput(out, console, redactor, clock);
    SoakSummary summary = new SoakSummary();
    summary.configuration(configuration(redactor, compared, ledger));
    summary.spec(new ObjectMapper().convertValue(s, new TypeReference<Map<String, Object>>() {}));

    WorkloadGenerator generator = new WorkloadGenerator(s, url, user, password);
    GeneratorWorkload workload = new GeneratorWorkload(generator);
    JdbcScnSource scn = new JdbcScnSource(sql -> scalar(password, sql), ledger);
    HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    ConnectRestProbe probe = new ConnectRestProbe(http, connectUrl, connector);
    Duration checkTimeout = Duration.ofMinutes(checkTimeoutMinutes);
    Soak.Checks checks =
        () -> {
          long t0 = System.currentTimeMillis();
          try (Connection c = open(password)) {
            CheckReport r =
                new CorrectnessCheck(
                        bootstrap,
                        topics,
                        c,
                        owner,
                        compared,
                        ledger,
                        checkTimeout,
                        Duration.ofSeconds(checkIdleSeconds))
                    .run();
            if (System.currentTimeMillis() - t0 >= checkTimeout.toMillis()) {
              output.log(
                  "this check read the topics until --check-timeout; a FAIL may come from an"
                      + " incomplete read. Rerun bench check with a longer --timeout on the kept"
                      + " topics before trusting it");
            }
            return r;
          }
        };
    MetricsSampler sampler =
        new MetricsSampler(
            metricsUrl == null ? null : scraper(http, metricsUrl),
            prefix,
            workload::committed,
            workload::rolledBack,
            output.metricsCsv(),
            clock,
            output::log,
            60_000);
    Soak soak =
        new Soak(
            Soak.Plan.of(hours, checkEvery, Duration.ofMinutes(catchUpMinutes)),
            workload,
            scn,
            probe,
            checks,
            new KafkaTopicPreflight(bootstrap, topics),
            sampler,
            clock,
            output,
            SoakCommand::harnessHeapPeak,
            summary);
    output.log(
        "soak of "
            + hours
            + " h against connector "
            + connector
            + ", "
            + s.sessions
            + " sessions, "
            + s.pauseMillisBetweenTransactions
            + " ms between transactions, tables "
            + compared
            + ". Keep the host awake for the whole run. Internal figures: never publish them");
    Thread hook = new Thread(soak::onSignal, "soak-shutdown");
    Runtime.getRuntime().addShutdownHook(hook);
    try {
      return soak.run().outcome().exitCode;
    } finally {
      try {
        Runtime.getRuntime().removeShutdownHook(hook);
      } catch (IllegalStateException e) {
        // the JVM is already shutting down; the hook writes the summary
      }
    }
  }

  String password() {
    if (secret.passwordEnv != null) {
      String v = env.apply(secret.passwordEnv);
      if (v == null || v.isEmpty()) {
        throw new ParameterException(
            cmd.commandLine(),
            "the environment variable "
                + secret.passwordEnv
                + " named by --password-env is not set");
      }
      return v;
    }
    return secret.password;
  }

  private void validate() throws IOException {
    if (!(hours > 0) || !(checkEvery > 0)) {
      throw new ParameterException(cmd.commandLine(), "--hours and --check-every must be positive");
    }
    if (catchUpMinutes < 1 || checkTimeoutMinutes < 1 || checkIdleSeconds < 1) {
      throw new ParameterException(
          cmd.commandLine(),
          "--catch-up-timeout, --check-timeout and --check-idle must be positive");
    }
    if (Files.isDirectory(out)) {
      try (Stream<Path> files = Files.list(out)) {
        if (files.anyMatch(
            f -> {
              String n = String.valueOf(f.getFileName());
              return n.equals(SoakOutput.SUMMARY_JSON) || n.matches("check-\\d+\\.json");
            })) {
          throw new ParameterException(
              cmd.commandLine(),
              out
                  + " already holds a soak's evidence; give another --out so nothing is"
                  + " overwritten");
        }
      }
    }
  }

  WorkloadSpec loadSpec() throws IOException {
    WorkloadSpec s;
    if (spec != null) {
      s = WorkloadSpec.read(spec);
    } else {
      try (InputStream in = SoakCommand.class.getResourceAsStream(DEFAULT_SPEC)) {
        if (in == null) {
          throw new IOException("bundled spec " + DEFAULT_SPEC + " is missing");
        }
        s = WorkloadSpec.parse(new String(in.readAllBytes(), StandardCharsets.UTF_8));
      }
    }
    if (seed != null) {
      s.seed = seed;
    }
    // open-ended: the soak ends the run itself; the duration is only a safety bound
    s.transactionsPerSession = 0;
    s.durationSeconds = (int) Math.min(Integer.MAX_VALUE, Math.ceil(hours * 3600) + 86_400);
    return s;
  }

  /**
   * The tables the oracle compares: every table the workload writes, or the ledger check would
   * count the transactions on a missing table as lost.
   */
  List<String> tables(List<String> specTables) {
    if (tables == null || tables.isEmpty()) {
      return specTables;
    }
    List<String> given = tables.stream().map(t -> t.strip().toUpperCase(Locale.ROOT)).toList();
    List<String> missing = specTables.stream().filter(t -> !given.contains(t)).toList();
    if (!missing.isEmpty()) {
      throw new ParameterException(
          cmd.commandLine(),
          "--tables must name every table the workload writes; missing " + missing);
    }
    return given;
  }

  private Map<String, Object> configuration(
      Redactor redactor, List<String> compared, String ledger) {
    Map<String, Object> c = new LinkedHashMap<>();
    c.put("url", redactor.scrub(url));
    c.put("user", user);
    c.put(
        "password",
        secret.passwordEnv != null
            ? "from environment variable " + secret.passwordEnv
            : "from the command line");
    c.put("bootstrapServers", redactor.scrub(bootstrap));
    c.put("topicPrefix", prefix);
    c.put("pdb", pdb);
    c.put("tables", compared);
    c.put("ledgerTable", ledger);
    c.put("connectUrl", redactor.scrub(connectUrl));
    c.put("connector", connector);
    c.put("metricsUrl", metricsUrl == null ? null : redactor.scrub(metricsUrl));
    c.put("checkTimeoutMinutes", checkTimeoutMinutes);
    c.put("checkIdleSeconds", checkIdleSeconds);
    return c;
  }

  private Connection open(String password) throws SQLException {
    Properties p = new Properties();
    p.setProperty("user", user);
    p.setProperty("password", password);
    return DriverManager.getConnection(url, p);
  }

  private long scalar(String password, String sql) throws SQLException {
    try (Connection c = open(password);
        Statement st = c.createStatement();
        ResultSet rs = st.executeQuery(sql)) {
      if (!rs.next()) {
        throw new SQLException("no row from the SCN query");
      }
      return rs.getLong(1);
    }
  }

  private static MetricsSampler.Scraper scraper(HttpClient http, String metricsUrl) {
    URI uri = URI.create(metricsUrl);
    return () -> {
      HttpResponse<String> r =
          http.send(
              HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(20)).GET().build(),
              HttpResponse.BodyHandlers.ofString());
      if (r.statusCode() / 100 != 2) {
        throw new IOException("metrics endpoint answered HTTP " + r.statusCode());
      }
      return r.body();
    };
  }

  /**
   * Peak heap of this JVM since the previous call, as the sum of the heap pools' peaks (an upper
   * bound): what a check needed, since the oracle holds every transaction of the run.
   */
  static long harnessHeapPeak() {
    long sum = 0;
    for (MemoryPoolMXBean p : ManagementFactory.getMemoryPoolMXBeans()) {
      if (p.getType() == MemoryType.HEAP) {
        MemoryUsage u = p.getPeakUsage();
        if (u != null) {
          sum += u.getUsed();
        }
        p.resetPeakUsage();
      }
    }
    return sum;
  }

  /** For tests: the command with its options parsed, not run. */
  static SoakCommand parse(String... args) {
    SoakCommand c = new SoakCommand();
    new CommandLine(c).parseArgs(args);
    return c;
  }
}
