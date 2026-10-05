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
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.kafka.common.config.ConfigException;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfig;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.config.CoreConfig.CaptureMode;
import sh.oso.connect.oracle.core.doctor.ArchiveStat;
import sh.oso.connect.oracle.core.doctor.DoctorCatalog;
import sh.oso.connect.oracle.core.doctor.LagExplainer;
import sh.oso.connect.oracle.core.doctor.MetricsSample;
import sh.oso.connect.oracle.core.doctor.RedoProfile;
import sh.oso.connect.oracle.core.doctor.Sizing;
import sh.oso.connect.oracle.core.errors.OracleCdcException;
import sh.oso.connect.oracle.core.logs.LogInventory;
import sh.oso.connect.oracle.core.logs.LogSet;
import sh.oso.connect.oracle.core.logs.RedoLog;
import sh.oso.connect.oracle.core.topology.ThreadInfo;
import sh.oso.connect.oracle.core.topology.TopologyProbe;
import sh.oso.connect.oracle.doctor.admin.Database;
import sh.oso.connect.oracle.doctor.admin.Environment;
import sh.oso.connect.oracle.doctor.connect.ConnectApi;
import sh.oso.connect.oracle.doctor.metrics.MetricsSource;

/** The PRD-05 diagnostic commands: {@code redo-profile}, {@code sizing} and {@code explain-lag}. */
final class DiagnosticCommands {

  private DiagnosticCommands() {}

  static Environment env(CommandSpec spec) {
    return ((AdminCommands.Rooted) spec.root().userObject()).env();
  }

  /** A connector config file read and checked. */
  record Loaded(Map<String, String> props, CoreConfig core) {}

  /** Reads the config; null after printing the reason. */
  static Loaded load(Path file, PrintWriter err, String cmd) {
    try {
      Map<String, String> props = DoctorMain.readConfig(file);
      return new Loaded(props, new CoreConfig(props));
    } catch (ConfigException | IOException e) {
      err.println("oracle-cdc-doctor " + cmd + ": " + e.getMessage());
      return null;
    }
  }

  @Command(
      name = "redo-profile",
      exitCodeOnInvalidInput = 64,
      description =
          "Archive generation per hour from V$ARCHIVED_LOG, and a LogMiner sample of the newest"
              + " archived logs with no table filter: rows by table and operation, reload jobs,"
              + " and the share of rows the connector would filter out.")
  static final class RedoProfileCommand implements Callable<Integer> {
    @Spec CommandSpec spec;

    @Option(names = "--config", required = true, description = "Connector config JSON.")
    Path config;

    @Option(
        names = "--window",
        defaultValue = "2h",
        converter = Durations.class,
        description = "Window of the hourly archive rates; default 2h.")
    Duration window;

    @Option(
        names = "--sample-logs",
        defaultValue = "1",
        description = "How many of the newest archived logs the LogMiner sample covers; default 1.")
    int sampleLogs;

    @Option(
        names = "--top",
        defaultValue = "20",
        description = "Tables listed, by rows; default 20.")
    int top;

    @Option(
        names = "--query-timeout",
        defaultValue = "10m",
        converter = Durations.class,
        description = "Timeout of the LogMiner sample query; default 10m.")
    Duration queryTimeout;

    @Override
    public Integer call() {
      PrintWriter out = spec.commandLine().getOut();
      PrintWriter err = spec.commandLine().getErr();
      Loaded loaded = load(config, err, "redo-profile");
      if (loaded == null) {
        return DoctorMain.EXIT_USAGE;
      }
      CoreConfig cfg = loaded.core();
      try (Database db = env(spec).database(cfg)) {
        DoctorCatalog cat = db.catalog();
        int dest =
            new TopologyProbe(cat, cfg.getString(CoreConfig.ARCHIVE_DESTINATION))
                .probe()
                .archiveDestId();
        Instant now = env(spec).clock().instant();
        Instant from = now.minus(window);
        List<ArchiveStat> recent = cat.archiveHistory(from, dest);
        List<RedoProfile.HourRate> hours = RedoProfile.hourly(recent, from, now);
        List<ArchiveStat> pool =
            RedoProfile.sampleRange(recent, sampleLogs) != null
                ? recent
                : cat.archiveHistory(now.minus(Duration.ofDays(7)), dest);
        long[] range = RedoProfile.sampleRange(pool, sampleLogs);
        RedoProfile.Sample sample = null;
        if (range != null) {
          LogSet set =
              new LogInventory(cat, CaptureMode.ARCHIVE_ONLY, dest).forRange(range[0], range[1]);
          long bytes = 0;
          for (RedoLog l : set.logs()) {
            for (ArchiveStat a : pool) {
              if (a.thread() == l.thread() && a.sequence() == l.sequence()) {
                bytes += a.bytes();
                break;
              }
            }
          }
          List<RedoProfile.SampleRow> rows =
              db.sampler(queryTimeout).sample(set.logs(), range[0], range[1]);
          sample =
              RedoProfile.sample(
                  rows,
                  cat.database().cdb(),
                  DoctorMain.csv(loaded.props().get(DoctorMain.TABLES_INCLUDE)),
                  DoctorMain.csv(loaded.props().get(DoctorMain.TABLES_EXCLUDE)),
                  range[0],
                  range[1],
                  set.logs().size(),
                  bytes);
        }
        out.print(RedoProfile.toMarkdown(from, now, hours, sample, top));
        out.flush();
        return DoctorMain.EXIT_OK;
      } catch (OracleCdcException e) {
        err.println("oracle-cdc-doctor redo-profile: " + e.getMessage());
        return DoctorMain.EXIT_BLOCKING;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return DoctorMain.EXIT_BLOCKING;
      } catch (Exception e) {
        err.println("oracle-cdc-doctor redo-profile: " + e.getMessage());
        return DoctorMain.EXIT_BLOCKING;
      }
    }
  }

  @Command(
      name = "sizing",
      exitCodeOnInvalidInput = 64,
      description =
          "Log switches per thread and archive generation per day from V$ARCHIVED_LOG, with the"
              + " recommended online log size and the archive retention a stated maximum"
              + " downtime needs.")
  static final class SizingCommand implements Callable<Integer> {
    @Spec CommandSpec spec;

    @Option(names = "--config", required = true, description = "Connector config JSON.")
    Path config;

    @Option(
        names = "--days",
        defaultValue = "7",
        description = "Days of archive history to read; default 7.")
    int days;

    @Option(
        names = "--max-downtime",
        defaultValue = "24h",
        converter = Durations.class,
        description = "Longest planned stop of the connector; default 24h.")
    Duration maxDowntime;

    @Override
    public Integer call() {
      PrintWriter out = spec.commandLine().getOut();
      PrintWriter err = spec.commandLine().getErr();
      Loaded loaded = load(config, err, "sizing");
      if (loaded == null) {
        return DoctorMain.EXIT_USAGE;
      }
      CoreConfig cfg = loaded.core();
      try (Database db = env(spec).database(cfg)) {
        DoctorCatalog cat = db.catalog();
        int dest =
            new TopologyProbe(cat, cfg.getString(CoreConfig.ARCHIVE_DESTINATION))
                .probe()
                .archiveDestId();
        Instant now = env(spec).clock().instant();
        Instant from = now.minus(Duration.ofDays(Math.max(1, days)));
        Duration journal = Duration.ofMillis(cfg.getLong(CoreConfig.TXJOURNAL_THRESHOLD_MS));
        Sizing.Result r =
            Sizing.compute(
                cat.archiveHistory(from, dest),
                cat.onlineLogGroups(),
                from,
                now,
                maxDowntime,
                journal);
        List<Integer> threads =
            cat.threads().stream().filter(ThreadInfo::enabled).map(ThreadInfo::thread).toList();
        Sizing.Reach reach = Sizing.reach(cat.archiveHistory(Instant.EPOCH, dest), threads, now);
        out.print(markdown(r, reach, maxDowntime, journal));
        out.flush();
        return DoctorMain.EXIT_OK;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return DoctorMain.EXIT_BLOCKING;
      } catch (Exception e) {
        err.println("oracle-cdc-doctor sizing: " + e.getMessage());
        return DoctorMain.EXIT_BLOCKING;
      }
    }

    static String markdown(
        Sizing.Result r, Sizing.Reach reach, Duration downtime, Duration journal) {
      StringBuilder sb = new StringBuilder("# oracle-cdc-doctor sizing\n\n");
      sb.append("Archived logs switched out from ")
          .append(r.from())
          .append(" to ")
          .append(r.to())
          .append(" (UTC); history observed for ")
          .append(Sizing.duration(r.observed()))
          .append(".\n\n");
      sb.append(
          "| Thread | Logs | Switches per hour | Peak switches in an hour | Peak hour (UTC) |"
              + " Archived per day | Online log size | Recommended size |\n"
              + "|---|---|---|---|---|---|---|---|\n");
      for (Sizing.ThreadFigures t : r.threads()) {
        sb.append("| ")
            .append(t.thread())
            .append(" | ")
            .append(t.logs())
            .append(" | ")
            .append(String.format(java.util.Locale.ROOT, "%.1f", t.switchesPerHour()))
            .append(" | ")
            .append(t.peakSwitchesPerHour())
            .append(" | ")
            .append(t.peakHour() == null ? "" : t.peakHour())
            .append(" | ")
            .append(Sizing.bytes(t.bytesPerDay()))
            .append(" | ")
            .append(Sizing.bytes(t.currentLogBytes()))
            .append(" | ")
            .append(Sizing.bytes(t.recommendedLogBytes()))
            .append(" |\n");
      }
      sb.append("\nThe recommended size keeps the peak hour at about ")
          .append(Sizing.TARGET_SWITCHES_PER_HOUR)
          .append(" switches; DOC-9 warns above ")
          .append(Sizing.MAX_SWITCHES_PER_HOUR)
          .append(" per hour.\n\n");
      sb.append("Archive generation, all threads: ")
          .append(Sizing.bytes(r.bytesPerDay()))
          .append(" per day on average, ")
          .append(Sizing.bytes(r.busiestDayBytes()))
          .append(" on the busiest day, ")
          .append(Sizing.bytes(r.peakHourBytes()))
          .append(" in the busiest hour.\n\n");
      sb.append("For a maximum downtime of ")
          .append(Sizing.duration(downtime))
          .append(", keep archived logs for at least ")
          .append(Sizing.duration(r.requiredRetention()))
          .append(" (the downtime plus cdc.txjournal.threshold.ms of ")
          .append(Sizing.duration(journal))
          .append("): about ")
          .append(Sizing.bytes(r.archiveSpaceBytes()))
          .append(" of archive space at the observed peak rate.\n\n");
      if (reach == null) {
        sb.append("No archived log is present at the destination.\n");
      } else if (!reach.purgeSeen()) {
        sb.append("No archived log has been deleted yet; redo reaches back ")
            .append(Sizing.duration(reach.reach()))
            .append(".\n");
      } else {
        sb.append("Archived redo now reaches back ")
            .append(Sizing.duration(reach.reach()))
            .append(
                reach.reach().compareTo(r.requiredRetention()) >= 0 ? ", enough" : ", too short")
            .append(" for this downtime.\n");
      }
      return sb.toString();
    }
  }

  @Command(
      name = "explain-lag",
      exitCodeOnInvalidInput = 64,
      description =
          "Read a running task's metrics twice and name the most likely cause of its lag: the"
              + " Kafka side, mining, a large transaction or dictionary replays.")
  static final class ExplainLag implements Callable<Integer> {
    @Spec CommandSpec spec;

    @Option(
        names = "--connect-url",
        description = "Kafka Connect REST URL; with --name, supplies the connector config.")
    String connectUrl;

    @Option(names = "--name", description = "Connector name.")
    String name;

    @Option(names = "--config", description = "Connector config JSON, instead of the REST API.")
    Path config;

    @Option(names = "--server", description = "Metrics server name; default cdc.topic.prefix.")
    String server;

    @Option(
        names = "--jmx-url",
        description =
            "JMX service URL of the worker running the task, for example"
                + " service:jmx:rmi:///jndi/rmi://worker:9999/jmxrmi.")
    String jmxUrl;

    @Option(
        names = "--metrics-url",
        description = "Prometheus endpoint of the JMX exporter on that worker.")
    String metricsUrl;

    @Option(
        names = "--interval",
        defaultValue = "10s",
        converter = Durations.class,
        description = "Time between the two readings; default 10s.")
    Duration interval;

    @Override
    public Integer call() {
      PrintWriter out = spec.commandLine().getOut();
      PrintWriter err = spec.commandLine().getErr();
      Environment env = env(spec);
      if ((jmxUrl == null) == (metricsUrl == null)) {
        err.println(
            "oracle-cdc-doctor explain-lag: give exactly one of --jmx-url and --metrics-url.");
        return DoctorMain.EXIT_USAGE;
      }
      try {
        Map<String, String> props = Map.of();
        String state = null;
        if (config != null) {
          props = DoctorMain.readConfig(config);
        } else if (connectUrl != null && name != null) {
          ConnectApi api = env.connect(connectUrl);
          props = api.config(name);
          state = api.state(name);
        }
        String srv =
            server != null ? server : props.get(OracleCdcSourceConnectorConfig.TOPIC_PREFIX);
        if (srv == null) {
          err.println(
              "oracle-cdc-doctor explain-lag: pass --server, or --config or --connect-url with"
                  + " --name so the server name can be read from cdc.topic.prefix.");
          return DoctorMain.EXIT_USAGE;
        }
        int poll = number(props, OracleCdcSourceConnectorConfig.POLL_MAX_RECORDS, defaults(true));
        long target = number(props, CoreConfig.MINING_TARGET_LATENCY_MS, defaults(false));
        LagExplainer.Settings settings =
            new LagExplainer.Settings(Math.max(1000, poll * 4), target);
        MetricsSample before;
        MetricsSample after;
        try (MetricsSource source = jmxUrl != null ? env.jmx(jmxUrl) : env.prometheus(metricsUrl)) {
          before = source.read(srv);
          env.sleep(interval);
          after = source.read(srv);
        }
        if (state != null) {
          out.println("Connector " + name + " is " + state + ".");
          out.println();
        }
        out.print(LagExplainer.toMarkdown(LagExplainer.explain(before, after, settings)));
        out.flush();
        return DoctorMain.EXIT_OK;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return DoctorMain.EXIT_BLOCKING;
      } catch (Exception e) {
        err.println("oracle-cdc-doctor explain-lag: " + e.getMessage());
        return DoctorMain.EXIT_BLOCKING;
      }
    }

    private static Map<String, Object> defaults(boolean connector) {
      return connector
          ? OracleCdcSourceConnectorConfig.configDef().defaultValues()
          : CoreConfig.configDef().defaultValues();
    }

    private static int number(Map<String, String> props, String key, Map<String, Object> dflt) {
      String v = props.get(key);
      if (v != null) {
        try {
          return Integer.parseInt(v.trim());
        } catch (NumberFormatException ignore) {
          // fall back to the default
        }
      }
      return ((Number) dflt.get(key)).intValue();
    }
  }
}
