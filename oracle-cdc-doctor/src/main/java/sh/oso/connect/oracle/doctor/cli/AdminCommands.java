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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;
import sh.oso.connect.oracle.core.errors.OracleCdcException;
import sh.oso.connect.oracle.doctor.admin.AdminException;
import sh.oso.connect.oracle.doctor.admin.AdminRecords;
import sh.oso.connect.oracle.doctor.admin.AdminSession;
import sh.oso.connect.oracle.doctor.admin.Environment;
import sh.oso.connect.oracle.doctor.admin.JournalInspector;
import sh.oso.connect.oracle.doctor.admin.OffsetsAdmin;
import sh.oso.connect.oracle.doctor.admin.ResnapshotAdmin;
import sh.oso.connect.oracle.doctor.admin.TransactionsAdmin;
import sh.oso.connect.oracle.doctor.metrics.MetricsSource;

/**
 * The PRD-05 {@code oracle-cdc-admin} commands, available as {@code oracle-cdc-doctor admin ...}
 * and through the {@link AdminMain} entry point. Each command works through the Kafka Connect REST
 * API of the worker running the connector; the connector configuration comes from the worker unless
 * {@code --config} gives it (when the worker holds externalised secrets).
 */
final class AdminCommands {

  private AdminCommands() {}

  /** The root command, which carries the environment. */
  interface Rooted {
    Environment env();
  }

  /** Options every admin command takes. */
  static final class Target {
    @Option(
        names = "--connect-url",
        required = true,
        description = "Kafka Connect REST URL, for example http://connect:8083.")
    String connectUrl;

    @Option(names = "--name", required = true, description = "Connector name.")
    String name;

    @Option(
        names = "--config",
        description =
            "Connector config JSON to use instead of the worker's copy, for example when the"
                + " worker holds the password as a config provider reference.")
    Path config;

    @Option(
        names = "--bootstrap-servers",
        description =
            "Brokers for the ops, signals and journal topics; default"
                + " cdc.kafka.bootstrap.servers.")
    String bootstrap;

    @Option(
        names = "--command-config",
        description = "Kafka client properties file (security settings).")
    Path commandConfig;

    AdminSession open(CommandSpec spec) {
      return AdminSession.open(
          ((Rooted) spec.root().userObject()).env(),
          connectUrl,
          name,
          config,
          bootstrap,
          commandConfig);
    }
  }

  interface Body {
    int run(AdminSession s, PrintWriter out) throws Exception;
  }

  /** Runs a command body, turning refusals and failures into a message and an exit code. */
  static int guard(CommandSpec spec, Target target, String command, Body body) {
    PrintWriter out = spec.commandLine().getOut();
    PrintWriter err = spec.commandLine().getErr();
    try (AdminSession s = target.open(spec)) {
      int exit = body.run(s, out);
      out.flush();
      return exit;
    } catch (AdminException e) {
      out.flush();
      err.println("oracle-cdc-admin " + command + ": " + e.getMessage());
      err.flush();
      return e.exitCode();
    } catch (OracleCdcException e) {
      out.flush();
      err.println("oracle-cdc-admin " + command + ": " + e.getMessage());
      err.flush();
      return AdminException.REFUSED;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return AdminException.REFUSED;
    } catch (Exception e) {
      out.flush();
      err.println("oracle-cdc-admin " + command + " failed: " + e);
      err.flush();
      return AdminException.REFUSED;
    }
  }

  @Command(
      name = "admin",
      exitCodeOnInvalidInput = 64,
      description = "Operator commands: offsets, resnapshot, transactions, journal.",
      subcommands = {Offsets.class, Resnapshot.class, Transactions.class, Journal.class})
  static final class Admin implements Callable<Integer> {
    @Spec CommandSpec spec;

    @Override
    public Integer call() {
      spec.commandLine().usage(spec.commandLine().getOut());
      return AdminException.USAGE;
    }
  }

  @Command(
      name = "offsets",
      exitCodeOnInvalidInput = 64,
      description = "Show or set the connector's stored offset (KIP-875).",
      subcommands = {OffsetsShow.class, OffsetsSet.class})
  static final class Offsets implements Callable<Integer> {
    @Spec CommandSpec spec;

    @Override
    public Integer call() {
      spec.commandLine().usage(spec.commandLine().getOut());
      return AdminException.USAGE;
    }
  }

  @Command(
      name = "show",
      exitCodeOnInvalidInput = 64,
      description = "Read the stored offset through GET /connectors/{name}/offsets and decode it.")
  static final class OffsetsShow implements Callable<Integer> {
    @Spec CommandSpec spec;
    @Mixin Target target;

    @Option(names = "--format", defaultValue = "text", description = "text or json.")
    String format;

    @Option(
        names = "--check-redo",
        description =
            "Also check, in V$ARCHIVED_LOG, that every log from the resume SCN on exists.")
    boolean checkRedo;

    @Override
    public Integer call() {
      return guard(
          spec,
          target,
          "offsets show",
          (s, out) -> OffsetsAdmin.show(s, out, "json".equalsIgnoreCase(format), checkRedo));
    }
  }

  @Command(
      name = "set",
      exitCodeOnInvalidInput = 64,
      description =
          "Stop the connector and set the SCN it resumes from (PATCH /connectors/{name}/offsets)."
              + " Refuses an SCN whose redo is purged, and a forward move without --allow-skip."
              + " The change is recorded on the ops topic before and after.")
  static final class OffsetsSet implements Callable<Integer> {
    @Spec CommandSpec spec;
    @Mixin Target target;

    @Option(names = "--scn", required = true, description = "The SCN to resume mining from.")
    long scn;

    @Option(names = "--reason", required = true, description = "Why, recorded on the ops topic.")
    String reason;

    @Option(
        names = "--allow-skip",
        description = "Allow a forward move, which skips the changes committed in between.")
    boolean allowSkip;

    @Option(
        names = "--forget-released",
        split = ",",
        description =
            "Released transaction ids (as the offset lists them, CON:USN.SLOT.SQN) to drop from"
                + " the offset, so a transaction mined again from an earlier SCN is not refused"
                + " with CDC-7001.")
    List<String> forgetReleased = new ArrayList<>();

    @Option(names = "--resume", description = "Resume the connector afterwards.")
    boolean resume;

    @Option(
        names = "--ops-format",
        defaultValue = "auto",
        description =
            "JSON shape of the ops event: auto (as the connector's JSON value converter), json"
                + " or json_schemas.")
    AdminRecords.OpsFormat opsFormat;

    @Override
    public Integer call() {
      return guard(
          spec,
          target,
          "offsets set",
          (s, out) ->
              OffsetsAdmin.set(
                  s,
                  new AdminRecords(s.config(), s.props(), s.connector(), opsFormat),
                  out,
                  new OffsetsAdmin.SetRequest(scn, reason, allowSkip, forgetReleased, resume)));
    }
  }

  @Command(
      name = "resnapshot",
      exitCodeOnInvalidInput = 64,
      description =
          "Ask the connector for a snapshot of the named tables through the signals topic. When"
              + " the stored offset points into purged redo, also stop the connector and move the"
              + " offset to the first SCN after the gap.")
  static final class Resnapshot implements Callable<Integer> {
    @Spec CommandSpec spec;
    @Mixin Target target;

    @Option(
        names = "--tables",
        required = true,
        split = ",",
        description = "Captured tables, PDB.OWNER.TABLE in a CDB and OWNER.TABLE otherwise.")
    List<String> tables;

    @Option(
        names = "--reason",
        required = true,
        description = "Why, recorded on the ops topic when the offset moves.")
    String reason;

    @Option(
        names = "--skip-gap-for-unlisted-tables",
        description =
            "When the offset must move past a gap, accept that captured tables not in --tables"
                + " lose their changes in the gap.")
    boolean skipGapForUnlisted;

    @Option(names = "--resume", description = "Resume the connector afterwards.")
    boolean resume;

    @Option(
        names = "--ops-format",
        defaultValue = "auto",
        description = "JSON shape of the ops event: auto, json or json_schemas.")
    AdminRecords.OpsFormat opsFormat;

    @Override
    public Integer call() {
      return guard(
          spec,
          target,
          "resnapshot",
          (s, out) ->
              ResnapshotAdmin.run(
                  s,
                  new AdminRecords(s.config(), s.props(), s.connector(), opsFormat),
                  out,
                  new ResnapshotAdmin.Request(tables, reason, skipGapForUnlisted, resume)));
    }
  }

  @Command(
      name = "transactions",
      exitCodeOnInvalidInput = 64,
      description =
          "List buffered (over JMX) and journaled (journal topic) transactions with age, size,"
              + " user and whether GV$TRANSACTION still lists them.")
  static final class Transactions implements Callable<Integer> {
    @Spec CommandSpec spec;
    @Mixin Target target;

    @Option(
        names = "--jmx-url",
        description = "JMX service URL of the worker running the task, for buffered transactions.")
    String jmxUrl;

    @Override
    public Integer call() {
      return guard(
          spec,
          target,
          "transactions",
          (s, out) -> {
            try (MetricsSource m = jmxUrl == null ? null : s.env().jmx(jmxUrl)) {
              return TransactionsAdmin.run(s, out, m);
            }
          });
    }
  }

  @Command(
      name = "journal",
      exitCodeOnInvalidInput = 64,
      description = "Transaction journal commands.",
      subcommands = {JournalInspect.class})
  static final class Journal implements Callable<Integer> {
    @Spec CommandSpec spec;

    @Override
    public Integer call() {
      spec.commandLine().usage(spec.commandLine().getOut());
      return AdminException.USAGE;
    }
  }

  @Command(
      name = "inspect",
      exitCodeOnInvalidInput = 64,
      description =
          "Read the journal topic as the task does at start and report its transactions, what the"
              + " next start restores, and whether the journal is consistent.")
  static final class JournalInspect implements Callable<Integer> {
    @Spec CommandSpec spec;
    @Mixin Target target;

    @Override
    public Integer call() {
      return guard(spec, target, "journal inspect", JournalInspector::run);
    }
  }
}
