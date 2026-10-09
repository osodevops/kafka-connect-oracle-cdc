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
package sh.oso.connect.oracle.core.doctor;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.config.CoreConfig.CaptureMode;
import sh.oso.connect.oracle.core.config.CoreConfig.LobMode;
import sh.oso.connect.oracle.core.errors.OracleCdcException;
import sh.oso.connect.oracle.core.logs.DictionaryBuild;
import sh.oso.connect.oracle.core.logs.LogInventory;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.schema.ColumnFilter;
import sh.oso.connect.oracle.core.topology.ArchiveDestination;
import sh.oso.connect.oracle.core.topology.DatabaseInfo;
import sh.oso.connect.oracle.core.topology.ThreadInfo;

/** The PRD-05 rule set. {@link #fastMode()} is what the connector's validate() runs. */
public final class Rules {

  private Rules() {}

  /**
   * Types LogMiner rejects for the whole row on 23ai Free (reference/sql-redo-shapes.md) or the
   * whole table.
   */
  static final Set<String> UNSUPPORTED_TYPES = Set.of("BOOLEAN", "JSON", "VECTOR", "BFILE");

  static final List<String> REQUIRED_PRIVILEGES =
      List.of("CREATE SESSION", "LOGMINING", "SELECT ANY TRANSACTION");

  public static List<Rule> fastMode() {
    return List.of(
        archivelog(),
        supplementalMin(),
        supplementalPerTable(),
        privileges(),
        unsupportedTables(),
        longNames(),
        keys(),
        archiveDestination(),
        rac(),
        roleAndOpenMode(),
        version(),
        avroNames(),
        rdsArchiveRetention(),
        standbyDictionarySource());
  }

  /** Every rule, in rule order: what {@code oracle-cdc-doctor check} runs by default. */
  public static List<Rule> all() {
    return List.of(
        archivelog(),
        supplementalMin(),
        supplementalPerTable(),
        privileges(),
        unsupportedTables(),
        longNames(),
        keys(),
        lobs(),
        switchRate(),
        retention(),
        undoRetention(),
        archiveDestination(),
        rac(),
        roleAndOpenMode(),
        version(),
        fixedObjectStatistics(),
        kafkaTopics(),
        exactlyOnce(),
        idleTimeout(),
        lagRecovery(),
        pdbsOpen(),
        avroNames(),
        rdsArchiveRetention(),
        standbyDictionarySource());
  }

  static Rule archivelog() {
    return rule(
        "DOC-1",
        ctx -> {
          DatabaseInfo db = ctx.database();
          return db.archivelog()
              ? List.of()
              : List.of(
                  Finding.blocking(
                      "DOC-1",
                      "The database runs in " + db.logMode() + " mode; LogMiner needs ARCHIVELOG.",
                      PlatformSql.enableArchivelog(ctx.catalog().platform())));
        });
  }

  static Rule supplementalMin() {
    return rule(
        "DOC-2",
        ctx ->
            ctx.database().supplementalLogDataMin()
                ? List.of()
                : List.of(
                    Finding.blocking(
                        "DOC-2",
                        "Minimal supplemental logging is off at database level; LogMiner cannot"
                            + " reconstruct rows without it.",
                        PlatformSql.addSupplementalLogging(ctx.catalog().platform()))));
  }

  static Rule supplementalPerTable() {
    return rule(
        "DOC-3",
        ctx -> {
          List<Finding> out = new ArrayList<>();
          for (CapturedTable t : ctx.tables()) {
            if (!t.supplementalAllColumns()) {
              String fix =
                  "ALTER TABLE "
                      + t.owner()
                      + "."
                      + t.name()
                      + " ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;";
              if (t.supplementalPrimaryKey()) {
                out.add(
                    Finding.warning(
                        "DOC-3",
                        t.fqn()
                            + " logs primary key columns only, so before images of updates and"
                            + " deletes carry the key and the changed columns only.",
                        fix));
              } else {
                out.add(
                    Finding.blocking(
                        "DOC-3",
                        t.fqn()
                            + " has no table-level supplemental logging; updates and deletes cannot"
                            + " be keyed.",
                        fix));
              }
            }
          }
          return out;
        });
  }

  static Rule privileges() {
    return rule(
        "DOC-4",
        ctx -> {
          List<Finding> out = new ArrayList<>();
          String user = ctx.config().getString(CoreConfig.DATABASE_USER);
          List<String> inaccessible = ctx.catalog().inaccessibleViews();
          boolean cdb = cdbOrGuess(ctx, inaccessible);
          String all = cdb ? " CONTAINER=ALL" : "";
          List<String> granted = ctx.catalog().grantedPrivileges();
          for (String p : REQUIRED_PRIVILEGES) {
            if (!granted.contains(p)) {
              out.add(
                  Finding.blocking(
                      "DOC-4",
                      "Privilege " + p + " is not granted to the mining user.",
                      "GRANT " + p + " TO " + user + all + ";"));
            }
          }
          for (String v : inaccessible) {
            out.add(
                Finding.blocking(
                    "DOC-4",
                    "The mining user cannot read " + v + ".",
                    PlatformSql.grantSelect(ctx.catalog().platform(), v, user, cdb)));
          }
          if (inaccessible.contains("V$DATABASE")) {
            return out; // container checks need V$DATABASE; the grant above comes first
          }
          if (cdb && ctx.catalog().connectedContainerId() > 2) {
            // ADR-0027: a local user inside a PDB mines that PDB in range mode
            if (ctx.config().miningMode() == CoreConfig.MiningMode.LOGS) {
              out.add(
                  Finding.blocking(
                      "DOC-4",
                      "The connection is to a pluggable database, where LogMiner refuses to add"
                          + " logs (ORA-65040), but cdc.mining.mode=logs. Use auto or range.",
                      null));
            }
            return out;
          }
          if (cdb) {
            if (!ctx.catalog().commonUser()) {
              out.add(
                  Finding.blocking(
                      "DOC-4",
                      "In a CDB the mining user must be a common user (C## prefix) connected to"
                          + " CDB$ROOT.",
                      null));
            } else if (!ctx.catalog().containerDataAll()) {
              out.add(
                  Finding.blocking(
                      "DOC-4",
                      "The common user sees only CDB$ROOT rows in V$PDBS, V$SESSION and"
                          + " V$LOGMNR_CONTENTS; set CONTAINER_DATA=ALL.",
                      "ALTER USER " + user + " SET CONTAINER_DATA=ALL CONTAINER=CURRENT;"));
            }
          }
          return out;
        });
  }

  private static boolean cdbOrGuess(DoctorContext ctx, List<String> inaccessible) {
    if (!inaccessible.contains("V$DATABASE")) {
      try {
        return ctx.database().cdb();
      } catch (SQLException e) {
        // fall through to the name heuristic
      }
    }
    String user = ctx.config().getString(CoreConfig.DATABASE_USER);
    return user != null && user.toUpperCase(Locale.ROOT).startsWith("C##");
  }

  static Rule unsupportedTables() {
    return rule(
        "DOC-5",
        ctx -> {
          List<Finding> out = new ArrayList<>();
          for (CapturedTable t : ctx.tables()) {
            for (CapturedTable.Column c : t.columns()) {
              String type = c.dataType() == null ? "" : c.dataType().toUpperCase();
              if (c.identity()) {
                out.add(
                    Finding.blocking(
                        "DOC-5",
                        t.fqn()
                            + " has identity column "
                            + c.name()
                            + "; LogMiner ignores the whole table.",
                        "-- replace the identity column with a sequence default, for example:\n"
                            + "-- ALTER TABLE "
                            + t.owner()
                            + "."
                            + t.name()
                            + " MODIFY "
                            + c.name()
                            + " DROP IDENTITY;"));
              } else if (UNSUPPORTED_TYPES.stream().anyMatch(type::startsWith)
                  || type.contains("NESTED")) {
                out.add(
                    Finding.blocking(
                        "DOC-5",
                        t.fqn()
                            + "."
                            + c.name()
                            + " is "
                            + type
                            + "; every DML row of this table is UNSUPPORTED in LogMiner output.",
                        null));
              }
            }
          }
          return out;
        });
  }

  static Rule longNames() {
    return rule(
        "DOC-6",
        ctx -> {
          List<Finding> out = new ArrayList<>();
          for (CapturedTable t : ctx.tables()) {
            if (t.name().length() > 30) {
              out.add(
                  Finding.blocking(
                      "DOC-6",
                      t.fqn()
                          + " has a name longer than 30 characters; LogMiner cannot mine it with"
                          + " supplemental logging.",
                      null));
            }
            for (CapturedTable.Column c : t.columns()) {
              if (c.name().length() > 30) {
                out.add(
                    Finding.blocking(
                        "DOC-6",
                        t.fqn()
                            + "."
                            + c.name()
                            + " is longer than 30 characters; LogMiner cannot mine the table.",
                        null));
              }
            }
          }
          return out;
        });
  }

  private static final java.util.regex.Pattern AVRO_NAME =
      java.util.regex.Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

  /**
   * DOC-22 (ADR-0020): with an Avro converter set on the connector, names Avro refuses need {@code
   * cdc.*.name.adjustment.mode}. A column name fails the task in the converter at the first record
   * (blocking); an owner, table or prefix part passes the converter and a default registry, and
   * only the consumer's Avro library refuses it (warning). A converter set only on the worker is
   * not visible here.
   */
  static Rule avroNames() {
    return rule(
        "DOC-22",
        ctx -> {
          if (!avroConverter(ctx.property("value.converter", null))
              && !avroConverter(ctx.property("key.converter", null))) {
            return List.of();
          }
          boolean schemaNone =
              "none".equalsIgnoreCase(ctx.property("cdc.schema.name.adjustment.mode", "none"));
          boolean fieldNone =
              "none".equalsIgnoreCase(ctx.property("cdc.field.name.adjustment.mode", "none"));
          List<String> schemaNames = new ArrayList<>();
          List<String> fieldNames = new ArrayList<>();
          // columns cdc.columns.exclude drops never reach the converter
          String patterns = ctx.property("cdc.columns.exclude", "");
          ColumnFilter excluded =
              patterns.isEmpty()
                  ? ColumnFilter.none()
                  : ColumnFilter.of(
                      Arrays.stream(patterns.split(","))
                          .map(String::trim)
                          .filter(x -> !x.isEmpty())
                          .toList(),
                      Boolean.parseBoolean(ctx.property("cdc.tables.case.sensitive", "false")));
          if (schemaNone) {
            for (String part : ctx.property("cdc.topic.prefix", "").split("\\.")) {
              if (!part.isEmpty() && !AVRO_NAME.matcher(part).matches()) {
                schemaNames.add("cdc.topic.prefix part " + part);
              }
            }
          }
          for (CapturedTable t : ctx.tables()) {
            if (schemaNone
                && (!AVRO_NAME.matcher(t.owner()).matches()
                    || !AVRO_NAME.matcher(t.name()).matches())) {
              schemaNames.add(t.fqn());
            }
            if (fieldNone) {
              TableId id = new TableId(t.pdb(), t.owner(), t.name());
              for (CapturedTable.Column c : t.columns()) {
                if (!AVRO_NAME.matcher(c.name()).matches() && !excluded.excludes(id, c.name())) {
                  fieldNames.add(t.fqn() + "." + c.name());
                }
              }
            }
          }
          List<Finding> out = new ArrayList<>();
          if (!fieldNames.isEmpty()) {
            out.add(
                Finding.blocking(
                    "DOC-22",
                    "The connector's Avro converter refuses these column names, so the task fails"
                        + " at their table's first record: "
                        + first(fieldNames)
                        + ". Set cdc.field.name.adjustment.mode=avro (or avro_unicode).",
                    null));
          }
          if (!schemaNames.isEmpty()) {
            out.add(
                Finding.warning(
                    "DOC-22",
                    "These names are not valid Avro names, and with"
                        + " cdc.schema.name.adjustment.mode=none their records reach Kafka with"
                        + " schemas a consumer on Avro for Java 1.12 or later refuses: "
                        + first(schemaNames)
                        + ". Set cdc.schema.name.adjustment.mode=avro (or avro_unicode), or turn"
                        + " on the registry's validity rule.",
                    null));
          }
          return out;
        });
  }

  private static boolean avroConverter(String className) {
    return className != null && className.trim().endsWith("AvroConverter");
  }

  /** The first twenty names, and how many more. */
  private static String first(List<String> names) {
    String shown = String.join(", ", names.subList(0, Math.min(20, names.size())));
    return names.size() > 20 ? shown + " and " + (names.size() - 20) + " more" : shown;
  }

  static Rule keys() {
    return rule(
        "DOC-7",
        ctx -> {
          List<Finding> out = new ArrayList<>();
          for (CapturedTable t : ctx.tables()) {
            if (!t.hasPrimaryKey() && !t.hasNotNullUniqueIndex()) {
              String fix =
                  "ALTER TABLE "
                      + t.owner()
                      + "."
                      + t.name()
                      + " ADD CONSTRAINT "
                      + t.name()
                      + "_PK PRIMARY KEY (...);";
              if ("fail".equals(ctx.keyMissing())) {
                out.add(
                    Finding.blocking(
                        "DOC-7",
                        t.fqn()
                            + " has no primary key or NOT NULL unique index; set cdc.key.missing to"
                            + " rowid or none, or add a key.",
                        fix));
              } else if (t.rowMovement()) {
                // ADR-0004: keyless heap tables are snapshotted in ROWID ranges
                String snapshot =
                    " Snapshots of a keyless table read ROWID ranges, so a row that moves while a"
                        + " snapshot runs can be read twice or missed.";
                out.add(
                    Finding.warning(
                        "DOC-7",
                        ("rowid".equals(ctx.keyMissing())
                                ? t.fqn()
                                    + " is keyed by ROWID but has ROW MOVEMENT enabled; a moved"
                                    + " row changes its key."
                                : t.fqn() + " has no key and ROW MOVEMENT enabled.")
                            + snapshot,
                        "ALTER TABLE " + t.owner() + "." + t.name() + " DISABLE ROW MOVEMENT;"));
              } else {
                out.add(
                    Finding.info(
                        "DOC-7",
                        t.fqn() + " has no key; records are keyed by " + ctx.keyMissing() + "."));
              }
            }
          }
          return out;
        });
  }

  static Rule archiveDestination() {
    return rule(
        "DOC-12",
        ctx -> {
          List<ArchiveDestination> dests = ctx.catalog().archiveDestinations();
          String wanted = ctx.config().getString(CoreConfig.ARCHIVE_DESTINATION);
          boolean ok =
              wanted == null || wanted.isBlank()
                  ? dests.stream().anyMatch(ArchiveDestination::validLocal)
                  : dests.stream()
                      .anyMatch(
                          d -> d.name().equalsIgnoreCase(wanted) && "VALID".equals(d.status()));
          return ok
              ? List.of()
              : List.of(
                  Finding.blocking(
                      "DOC-12",
                      "No valid local archive destination"
                          + (wanted == null || wanted.isBlank() ? "" : " named " + wanted)
                          + " in V$ARCHIVE_DEST_STATUS.",
                      PlatformSql.archiveDestination(ctx.catalog().platform())));
        });
  }

  static Rule roleAndOpenMode() {
    return rule(
        "DOC-14",
        ctx -> {
          DatabaseInfo db = ctx.database();
          CaptureMode mode = ctx.config().captureMode();
          String refusal = sh.oso.connect.oracle.core.topology.TopologyGuard.roleRefusal(db, mode);
          if (refusal != null) {
            return List.of(
                Finding.blocking(
                    "DOC-14",
                    refusal
                        + (mode == CaptureMode.ONLINE
                            ? " Use archive_only for an Active Data Guard standby open read-only."
                            : ""),
                    null));
          }
          if (mode == CaptureMode.ARCHIVE_ONLY
              && sh.oso.connect.oracle.core.topology.TopologyGuard.applyStopped(db)) {
            return List.of(
                Finding.warning(
                    "DOC-14",
                    "The physical standby is open READ ONLY without redo apply: archived logs"
                        + " arrive but are not applied, so the connector's safe end does not move"
                        + " until apply runs (READ ONLY WITH APPLY needs Active Data Guard).",
                    "ALTER DATABASE RECOVER MANAGED STANDBY DATABASE DISCONNECT FROM SESSION;"));
          }
          return List.of();
        });
  }

  static Rule version() {
    return rule(
        "DOC-15",
        ctx -> {
          String v = ctx.database().versionFull();
          int major = 0;
          try {
            major = Integer.parseInt(v.split("\\.")[0]);
          } catch (RuntimeException ignore) {
            // unknown format
          }
          return major >= 19
              ? List.of()
              : List.of(
                  Finding.blocking(
                      "DOC-15",
                      "Oracle Database " + v + " is not supported; 19c or later is required.",
                      null));
        });
  }

  /** PRD-02: the longest a snapshot chunk's flashback read is expected to take. */
  static final Duration EXPECTED_CHUNK_READ = Duration.ofSeconds(120);

  /** Default idle timeouts of common load balancers, the DOC-19 reference points. */
  static final Duration AZURE_LB_IDLE = Duration.ofMinutes(4);

  static final Duration AWS_NLB_IDLE = Duration.ofSeconds(350);

  static Rule lobs() {
    return rule(
        "DOC-8",
        ctx -> {
          List<Finding> out = new ArrayList<>();
          LobMode mode = ctx.config().lobMode();
          for (CapturedTable t : ctx.tables()) {
            List<String> lobs = new ArrayList<>();
            List<String> xml = new ArrayList<>();
            List<String> longs = new ArrayList<>();
            for (CapturedTable.Column c : t.columns()) {
              String type = c.dataType() == null ? "" : c.dataType().toUpperCase(Locale.ROOT);
              if (type.equals("CLOB") || type.equals("NCLOB") || type.equals("BLOB")) {
                lobs.add(c.name());
              } else if (type.contains("XMLTYPE")) {
                xml.add(c.name());
              } else if (type.equals("LONG") || type.equals("LONG RAW")) {
                longs.add(c.name());
              }
            }
            if (!lobs.isEmpty()) {
              String what = t.fqn() + " has LOB columns " + String.join(", ", lobs) + "; ";
              switch (mode) {
                case SKIP:
                  out.add(
                      Finding.info(
                          "DOC-8", what + "cdc.lob.mode=skip leaves them out of the records."));
                  break;
                case INLINE:
                  out.add(
                      Finding.info(
                          "DOC-8",
                          what
                              + "cdc.lob.mode=inline assembles them from redo and publishes"
                              + " cdc.unavailable.placeholder where the redo does not hold the"
                              + " value."));
                  break;
                default:
                  out.add(
                      Finding.info(
                          "DOC-8",
                          what
                              + "cdc.lob.mode=reselect assembles them from redo and queries the"
                              + " values still missing AS OF the commit SCN, one query per row,"
                              + " which needs undo back to that SCN (DOC-11)."));
              }
            }
            if (!xml.isEmpty()) {
              String what = t.fqn() + " has XMLTYPE columns " + String.join(", ", xml) + "; ";
              out.add(
                  mode == LobMode.SKIP
                      ? Finding.info(
                          "DOC-8", what + "cdc.lob.mode=skip leaves them out of the records.")
                      : Finding.warning(
                          "DOC-8",
                          what
                              + "XMLTYPE values are neither assembled from redo nor reselected,"
                              + " so records carry cdc.unavailable.placeholder for them.",
                          null));
            }
            if (!longs.isEmpty()) {
              out.add(
                  Finding.info(
                      "DOC-8",
                      t.fqn()
                          + " has LONG or LONG RAW columns "
                          + String.join(", ", longs)
                          + "; they are decoded from the SQL_REDO literal like VARCHAR2 and RAW,"
                          + " and cdc.lob.mode does not apply to them."));
            }
          }
          return out;
        });
  }

  private static Duration journalThreshold(DoctorContext ctx) {
    return Duration.ofMillis(Math.max(0, ctx.config().getLong(CoreConfig.TXJOURNAL_THRESHOLD_MS)));
  }

  /**
   * ADR-0025: a physical standby is read-only, so dictionary builds must run on the primary named
   * by {@code cdc.dictionary.database.url}. The URL is never echoed: a thin URL can carry a
   * password.
   */
  static Rule standbyDictionarySource() {
    return rule(
        "DOC-24",
        ctx -> {
          boolean builds = ctx.config().getLong(CoreConfig.DICTIONARY_BUILD_INTERVAL_MS) > 0;
          String url = ctx.config().getString(CoreConfig.DICTIONARY_DATABASE_URL);
          boolean set = url != null && !url.isBlank();
          String role = ctx.database().databaseRole();
          boolean standby = "PHYSICAL STANDBY".equals(role == null ? "" : role.trim());
          if (standby && builds && !set) {
            return List.of(
                Finding.blocking(
                    "DOC-24",
                    "The captured database is a physical standby, which is read-only: dictionary"
                        + " builds cannot run on it, so rows written before a DDL the connector has"
                        + " not mined yet cannot be decoded (CDC-6001). Set"
                        + " cdc.dictionary.database.url to the primary, or switch builds off with"
                        + " cdc.dictionary.build.interval.ms=0.",
                    null));
          }
          if (set && builds) {
            return List.of(
                Finding.info(
                    "DOC-24",
                    "Dictionary builds run on the database named by cdc.dictionary.database.url"
                        + (standby
                            ? ", which must be the primary"
                            : " rather than the captured one")
                        + "; the doctor does not check that database."));
          }
          return List.of();
        });
  }

  /** DOC-10's need: the journal threshold plus the planned maximum downtime, in whole hours. */
  private static Duration requiredRetention(DoctorContext ctx) {
    return Sizing.roundUpHours(ctx.maxDowntime().plus(journalThreshold(ctx)));
  }

  /**
   * ADR-0024: RDS deletes archived redo after {@code archivelog retention hours}, 0 by default, so
   * a stopped connector finds nothing to resume from beyond the online logs (CDC-2002).
   */
  static Rule rdsArchiveRetention() {
    return rule(
        "DOC-23",
        ctx -> {
          if (ctx.catalog().platform() != sh.oso.connect.oracle.core.topology.Platform.RDS) {
            return List.of();
          }
          long need = requiredRetention(ctx).toHours();
          String fix =
              PlatformSql.archiveRetention(sh.oso.connect.oracle.core.topology.Platform.RDS, need);
          String value = ctx.catalog().rdsConfiguration("archivelog retention hours");
          if (value == null) {
            return List.of(
                Finding.info(
                    "DOC-23",
                    "Amazon RDS: the archived redo retention could not be read"
                        + " (rdsadmin.rds_configuration). Check it as the master user with"
                        + " rdsadmin.rdsadmin_util.show_configuration; the connector needs at least"
                        + " "
                        + need
                        + " hours."));
          }
          long hours;
          try {
            hours = Long.parseLong(value.trim());
          } catch (NumberFormatException e) {
            hours = 0;
          }
          if (hours <= 0) {
            return List.of(
                Finding.blocking(
                    "DOC-23",
                    "Amazon RDS keeps no archived redo (archivelog retention hours is "
                        + value.trim()
                        + "): logs are deleted soon after they are archived, so a connector that"
                        + " stops for longer than the online logs last cannot resume (CDC-2002).",
                    fix));
          }
          if (hours < need) {
            return List.of(
                Finding.warning(
                    "DOC-23",
                    "Amazon RDS keeps archived redo for "
                        + hours
                        + " hours, but the connector needs "
                        + need
                        + " (cdc.txjournal.threshold.ms plus a planned maximum downtime of "
                        + Sizing.duration(ctx.maxDowntime())
                        + ").",
                    fix));
          }
          return List.of();
        });
  }

  static Rule switchRate() {
    return rule(
        "DOC-9",
        ctx -> {
          int dest = ctx.archiveDestId();
          if (dest < 0) {
            return List.of(); // DOC-12 reports the missing destination
          }
          Instant now = ctx.clock().instant();
          Instant from = now.minus(Duration.ofDays(7));
          Sizing.Result r =
              Sizing.compute(
                  ctx.catalog().archiveHistory(from, dest),
                  ctx.catalog().onlineLogGroups(),
                  from,
                  now,
                  ctx.maxDowntime(),
                  journalThreshold(ctx));
          List<Finding> out = new ArrayList<>();
          for (Sizing.ThreadFigures f : r.threads()) {
            if (f.peakSwitchesPerHour() > Sizing.MAX_SWITCHES_PER_HOUR) {
              long mib = f.recommendedLogBytes() / (1024 * 1024);
              out.add(
                  Finding.warning(
                      "DOC-9",
                      "Redo thread "
                          + f.thread()
                          + " switched logs "
                          + f.peakSwitchesPerHour()
                          + " times in the hour from "
                          + f.peakHour()
                          + ", above "
                          + Sizing.MAX_SWITCHES_PER_HOUR
                          + " per hour; every mining step then spans more logs. Online logs of "
                          + Sizing.bytes(f.recommendedLogBytes())
                          + " (now "
                          + Sizing.bytes(f.currentLogBytes())
                          + ") keep that hour at about "
                          + Sizing.TARGET_SWITCHES_PER_HOUR
                          + " switches.",
                      PlatformSql.addLogfile(ctx.catalog().platform(), f.thread(), mib)));
            }
          }
          return out;
        });
  }

  static Rule retention() {
    return rule(
        "DOC-10",
        ctx -> {
          int dest = ctx.archiveDestId();
          if (dest < 0) {
            return List.of();
          }
          Instant now = ctx.clock().instant();
          List<Integer> threads =
              ctx.catalog().threads().stream()
                  .filter(ThreadInfo::enabled)
                  .map(ThreadInfo::thread)
                  .toList();
          Sizing.Reach reach =
              Sizing.reach(ctx.catalog().archiveHistory(Instant.EPOCH, dest), threads, now);
          Duration required = Sizing.roundUpHours(ctx.maxDowntime().plus(journalThreshold(ctx)));
          String need =
              Sizing.duration(required)
                  + " (cdc.txjournal.threshold.ms of "
                  + Sizing.duration(journalThreshold(ctx))
                  + " plus a planned maximum downtime of "
                  + Sizing.duration(ctx.maxDowntime())
                  + ")";
          if (reach == null) {
            return List.of(
                Finding.info(
                    "DOC-10",
                    "No archived log is present at destination "
                        + dest
                        + "; retention cannot be measured. Keep archived logs for at least "
                        + need
                        + "."));
          }
          if (!reach.purgeSeen()) {
            return List.of(
                Finding.info(
                    "DOC-10",
                    "No archived log has been deleted at destination "
                        + dest
                        + " yet, so retention cannot be measured; redo reaches back "
                        + Sizing.duration(reach.reach())
                        + ". Keep archived logs for at least "
                        + need
                        + "."));
          }
          if (reach.reach().compareTo(required) >= 0) {
            return List.of();
          }
          long hours = required.toHours();
          return List.of(
              Finding.warning(
                  "DOC-10",
                  "Archived redo reaches back "
                      + Sizing.duration(reach.reach())
                      + " but the connector needs "
                      + need
                      + ". A connector stopped for longer finds its redo purged (CDC-2002) and"
                      + " needs oracle-cdc-admin resnapshot.",
                  PlatformSql.archiveRetention(ctx.catalog().platform(), hours)));
        });
  }

  static Rule undoRetention() {
    return rule(
        "DOC-11",
        ctx -> {
          String v = ctx.catalog().parameter("undo_retention");
          long seconds;
          try {
            seconds = v == null ? -1 : Long.parseLong(v.trim());
          } catch (NumberFormatException e) {
            seconds = -1;
          }
          if (seconds < 0) {
            return List.of(
                Finding.info("DOC-11", "UNDO_RETENTION could not be read from V$PARAMETER."));
          }
          if (seconds >= EXPECTED_CHUNK_READ.toSeconds()) {
            return List.of();
          }
          return List.of(
              Finding.warning(
                  "DOC-11",
                  "UNDO_RETENTION is "
                      + seconds
                      + " s, below the "
                      + EXPECTED_CHUNK_READ.toSeconds()
                      + " s a snapshot chunk read may take; chunk reads then fail with"
                      + " ORA-01555 and are retried at half the size, down to CDC-8001.",
                  PlatformSql.setParameter(ctx.catalog().platform(), "undo_retention", "900")));
        });
  }

  static Rule rac() {
    return rule(
        "DOC-13",
        ctx -> {
          List<ThreadInfo> threads = ctx.catalog().threads();
          List<Finding> out = new ArrayList<>();
          long enabled = threads.stream().filter(ThreadInfo::enabled).count();
          if (enabled > 1) {
            // ADR-0023: the task refuses this shape at start with CDC-5001
            out.add(
                Finding.blocking(
                    "DOC-13",
                    "RAC with "
                        + enabled
                        + " enabled redo threads ("
                        + sh.oso.connect.oracle.core.topology.TopologyGuard.describe(threads)
                        + "). This release captures a single redo thread: with one cursor the"
                        + " connector would skip or repeat whole threads, so the task refuses to"
                        + " start (CDC-5001). RAC capture is planned for Phase 2.",
                    null));
          } else if (threads.size() > 1) {
            out.add(
                Finding.info(
                    "DOC-13",
                    "RAC with "
                        + threads.size()
                        + " redo threads ("
                        + sh.oso.connect.oracle.core.topology.TopologyGuard.describe(threads)
                        + "). The connector requires the logs of every enabled thread and does"
                        + " not wait for a disabled one. This release is qualified for a single"
                        + " redo thread; RAC capture is planned for Phase 2."));
          }
          if (ctx.config().getBoolean(CoreConfig.DATABASE_FAN_ENABLED)) {
            out.add(
                Finding.info(
                    "DOC-13",
                    "cdc.database.fan.enabled is set, but this build does not subscribe to FAN"
                        + " events; instance failures surface as connection errors and the"
                        + " connector reconnects."));
          }
          return out;
        });
  }

  static Rule fixedObjectStatistics() {
    return rule(
        "DOC-16",
        ctx -> {
          int n = ctx.catalog().fixedTablesWithStatistics();
          if (n < 0) {
            return List.of(
                Finding.info(
                    "DOC-16",
                    "Fixed-object statistics could not be checked: DBA_TAB_STATISTICS is not"
                        + " readable."));
          }
          return n > 0
              ? List.of()
              : List.of(
                  Finding.warning(
                      "DOC-16",
                      "No fixed-object statistics: queries on V$LOGMNR_CONTENTS, V$ARCHIVED_LOG"
                          + " and the other fixed views are planned without them, which slows"
                          + " mining steps.",
                      "EXEC DBMS_STATS.GATHER_FIXED_OBJECTS_STATS;"));
        });
  }

  /**
   * DOC-21: LogMiner reads every container whose redo lies in the mined range, captured or not, so
   * a closed pluggable database stops mining with ORA-16331 until it opens; one without a saved
   * state stays closed after a restart.
   */
  static Rule pdbsOpen() {
    return rule(
        "DOC-21",
        ctx -> {
          List<PdbState> pdbs = ctx.catalog().pdbStates();
          if (pdbs == null) {
            return List.of(
                Finding.info(
                    "DOC-21",
                    "The pluggable databases' open modes could not be checked: V$PDBS or"
                        + " DBA_PDB_SAVED_STATES is not readable."));
          }
          List<Finding> out = new ArrayList<>();
          for (PdbState p : pdbs) {
            if (!p.open()) {
              out.add(
                  Finding.warning(
                      "DOC-21",
                      "Pluggable database "
                          + p.name()
                          + " is "
                          + p.openMode()
                          + ": LogMiner reads every container whose redo it mines, captured or"
                          + " not, so mining waits (ORA-16331) whenever redo of "
                          + p.name()
                          + " is in range until it opens.",
                      "ALTER PLUGGABLE DATABASE "
                          + p.name()
                          + " OPEN; ALTER PLUGGABLE DATABASE "
                          + p.name()
                          + " SAVE STATE;"));
            } else if (!p.savedState()) {
              out.add(
                  new Finding(
                      "DOC-21",
                      Severity.INFO,
                      "Pluggable database "
                          + p.name()
                          + " has no saved state: after a restart it stays closed until opened by"
                          + " hand, and mining waits for it (ORA-16331).",
                      "ALTER PLUGGABLE DATABASE " + p.name() + " SAVE STATE;"));
            }
          }
          return out;
        });
  }

  static final String BOOTSTRAP = "cdc.kafka.bootstrap.servers";

  static Rule kafkaTopics() {
    return rule(
        "DOC-17",
        ctx -> {
          KafkaFacts kafka = ctx.kafka();
          if (kafka == null) {
            return List.of(
                Finding.info(
                    "DOC-17",
                    "Internal topics not checked: no broker access. Set "
                        + BOOTSTRAP
                        + " in the connector configuration or pass --bootstrap-servers."));
          }
          boolean own = ctx.property(BOOTSTRAP, null) != null;
          List<InternalTopic> used = new ArrayList<>();
          for (InternalTopic t : ctx.internalTopics()) {
            // without broker access of its own the connector neither journals nor keeps versions
            if (own || !t.compacted()) {
              used.add(t);
            }
          }
          Map<String, KafkaFacts.TopicFacts> facts =
              kafka.topics(used.stream().map(InternalTopic::name).toList());
          List<Finding> out = new ArrayList<>();
          KafkaFacts.CreateRights canCreate = KafkaFacts.CreateRights.UNKNOWN;
          boolean asked = false;
          for (InternalTopic t : used) {
            KafkaFacts.TopicFacts f = facts.get(t.name());
            String policy = t.compacted() ? "compact" : "delete";
            String create =
                "-- kafka-topics.sh --bootstrap-server <brokers> --create --topic "
                    + t.name()
                    + " --partitions 1 --config cleanup.policy="
                    + policy;
            if (f == null) {
              if (!own) {
                out.add(
                    Finding.info(
                        "DOC-17",
                        t.name()
                            + " ("
                            + t.role()
                            + ") does not exist; the worker's topic creation or the broker's"
                            + " auto-creation makes it on the first write, if either is"
                            + " enabled."));
                continue;
              }
              if (!asked) {
                canCreate = kafka.canCreateTopics();
                asked = true;
              }
              if (canCreate == KafkaFacts.CreateRights.ALLOWED) {
                out.add(
                    Finding.info(
                        "DOC-17",
                        t.name()
                            + " ("
                            + t.role()
                            + ") does not exist yet; the connector creates it at start with"
                            + " cleanup.policy="
                            + policy
                            + "."));
              } else if (canCreate == KafkaFacts.CreateRights.DENIED) {
                out.add(
                    Finding.blocking(
                        "DOC-17",
                        t.name()
                            + " ("
                            + t.role()
                            + ") does not exist and the connector's Kafka principal may not"
                            + " create topics.",
                        create));
              } else {
                out.add(
                    Finding.warning(
                        "DOC-17",
                        t.name()
                            + " ("
                            + t.role()
                            + ") does not exist, and the cluster does not say whether the"
                            + " connector's Kafka principal may create it.",
                        create));
              }
              continue;
            }
            String alter =
                "-- kafka-configs.sh --bootstrap-server <brokers> --alter --entity-type topics"
                    + " --entity-name "
                    + t.name()
                    + " --add-config cleanup.policy="
                    + policy;
            String loss =
                "txjournal".equals(t.role())
                    ? "a deleted journal chunk stops the connector (CDC-4002)"
                    : "deleted schema versions leave older rows without their layout";
            if (t.compacted() && !f.compacted()) {
              out.add(
                  Finding.blocking(
                      "DOC-17",
                      t.name()
                          + " ("
                          + t.role()
                          + ") must be compacted but has cleanup.policy="
                          + f.cleanupPolicy()
                          + "; "
                          + loss
                          + ".",
                      alter));
            } else if (t.compacted() && f.deletes() && f.retentionMs() >= 0) {
              out.add(
                  Finding.blocking(
                      "DOC-17",
                      t.name()
                          + " ("
                          + t.role()
                          + ") has cleanup.policy="
                          + f.cleanupPolicy()
                          + " with retention.ms="
                          + f.retentionMs()
                          + ", so old records are deleted; "
                          + loss
                          + ".",
                      alter));
            } else if (!t.compacted() && !f.deletes()) {
              out.add(
                  Finding.warning(
                      "DOC-17",
                      t.name()
                          + " ("
                          + t.role()
                          + ") is compacted; it holds events, not state, and expects"
                          + " cleanup.policy=delete.",
                      alter));
            }
          }
          return out;
        });
  }

  static Rule exactlyOnce() {
    return rule(
        "DOC-18",
        ctx -> {
          boolean required =
              "required".equalsIgnoreCase(ctx.property("exactly.once.support", "requested"));
          boolean boundary =
              "connector".equalsIgnoreCase(ctx.property("transaction.boundary", "poll"));
          if (!required && !boundary) {
            return List.of();
          }
          List<Finding> out = new ArrayList<>();
          long producerTimeout =
              number(ctx.property("producer.override.transaction.timeout.ms", "60000"), 60000);
          long batchMs = number(ctx.property("cdc.eos.batch.max.ms", "500"), 500);
          if (batchMs >= producerTimeout) {
            out.add(
                Finding.blocking(
                    "DOC-18",
                    "cdc.eos.batch.max.ms ("
                        + batchMs
                        + " ms) is not below the producer's transaction.timeout.ms ("
                        + producerTimeout
                        + " ms): a Kafka transaction can time out before the connector commits"
                        + " it.",
                    null));
          }
          KafkaFacts kafka = ctx.kafka();
          if (kafka == null) {
            out.add(
                Finding.info(
                    "DOC-18",
                    "The brokers' transaction.max.timeout.ms was not checked: no broker"
                        + " access."));
          } else {
            long max = kafka.transactionMaxTimeoutMs();
            if (max < 0) {
              out.add(
                  Finding.warning(
                      "DOC-18",
                      "The brokers' transaction.max.timeout.ms could not be read; check that it"
                          + " is at least the producer's transaction.timeout.ms ("
                          + producerTimeout
                          + " ms).",
                      null));
            } else if (producerTimeout > max) {
              out.add(
                  Finding.blocking(
                      "DOC-18",
                      "The producer's transaction.timeout.ms ("
                          + producerTimeout
                          + " ms) is above the brokers' transaction.max.timeout.ms ("
                          + max
                          + " ms): the worker's transactional producer fails to start"
                          + " (InvalidTxnTimeoutException).",
                      null));
            }
          }
          if (required) {
            WorkerFacts worker = ctx.worker();
            if (worker == null) {
              out.add(
                  Finding.info(
                      "DOC-18",
                      "Not checked whether the Connect workers run with"
                          + " exactly.once.source.support=enabled: pass --connect-url."));
            } else if (worker.exactlyOnceError() != null) {
              out.add(
                  Finding.blocking(
                      "DOC-18",
                      "The Connect worker rejects exactly.once.support=required: "
                          + worker.exactlyOnceError()
                          + " Set exactly.once.source.support=enabled on every worker"
                          + " (distributed mode only).",
                      null));
            }
          }
          return out;
        });
  }

  private static long number(String v, long dflt) {
    try {
      return Long.parseLong(v.trim());
    } catch (RuntimeException e) {
      return dflt;
    }
  }

  static Rule idleTimeout() {
    return rule(
        "DOC-19",
        ctx -> {
          String keepIdle = null;
          String extra = ctx.config().getString(CoreConfig.DATABASE_CONNECTION_PROPERTIES);
          if (extra != null) {
            for (String kv : extra.split(";")) {
              int eq = kv.indexOf('=');
              if (eq > 0 && kv.substring(0, eq).trim().equals("oracle.net.TCP_KEEPIDLE")) {
                keepIdle = kv.substring(eq + 1).trim();
              }
            }
          }
          long seconds = keepIdle == null ? -1 : number(keepIdle, -1);
          if (seconds > 0 && seconds < AZURE_LB_IDLE.toSeconds()) {
            return List.of();
          }
          String limits =
              "the default idle timeouts of common load balancers (AWS Network Load Balancer "
                  + AWS_NLB_IDLE.toSeconds()
                  + " s, Azure Load Balancer "
                  + AZURE_LB_IDLE.toMinutes()
                  + " minutes)";
          String now =
              seconds > 0
                  ? "oracle.net.TCP_KEEPIDLE is " + seconds + " s, not below " + limits + "."
                  : "The database connections use TCP keepalive with the operating system's"
                      + " idle time (two hours by default on Linux), longer than "
                      + limits
                      + ".";
          return List.of(
              Finding.info(
                  "DOC-19",
                  now
                      + " If a load balancer or firewall sits between the worker and the"
                      + " database, set oracle.net.TCP_KEEPIDLE below its limit in "
                      + CoreConfig.DATABASE_CONNECTION_PROPERTIES
                      + " (for example oracle.net.TCP_KEEPIDLE=120), or SQLNET.EXPIRE_TIME on"
                      + " the database server."));
        });
  }

  static Rule lagRecovery() {
    return rule(
        "DOC-20",
        ctx -> {
          DoctorCatalog cat = ctx.catalog();
          boolean priv = cat.canExecute("SYS", "DBMS_LOGMNR_D");
          boolean builds = ctx.config().getLong(CoreConfig.DICTIONARY_BUILD_INTERVAL_MS) > 0;
          String user = ctx.config().getString(CoreConfig.DATABASE_USER);
          String grant =
              PlatformSql.grantExecute(cat.platform(), "DBMS_LOGMNR_D", user, ctx.database().cdb());
          int dest = ctx.archiveDestId();
          Optional<DictionaryBuild> build = Optional.empty();
          String broken = null;
          if (dest >= 0) {
            LogInventory inventory = new LogInventory(cat, ctx.config().captureMode(), dest);
            long scn = cat.currentScn();
            build = inventory.dictionaryBuildBefore(scn);
            if (build.isPresent()) {
              long first = build.get().firstScn();
              long end =
                  ctx.config().captureMode() == CaptureMode.ONLINE
                      ? scn
                      : inventory.archiveOnlySafeEnd(first);
              try {
                inventory.forRange(first, Math.max(first, end));
              } catch (OracleCdcException e) {
                broken = e.code().code();
              }
            }
          }
          String usable =
              build.isPresent() && broken == null
                  ? "the newest dictionary build starts at SCN "
                      + build.get().firstScn()
                      + " (thread "
                      + build.get().thread()
                      + ") and every log from it on is present"
                  : null;
          String none =
              build.isEmpty()
                  ? "the archived logs hold no complete dictionary build"
                  : "the dictionary build at SCN "
                      + build.get().firstScn()
                      + " cannot be replayed because a later log is missing ("
                      + broken
                      + ")";
          String lag =
              " Rows written before a DDL the connector has not mined yet then stop it with"
                  + " CDC-6001.";
          if (usable != null && priv) {
            return List.of(
                Finding.info(
                    "DOC-20",
                    "Lag recovery is possible: "
                        + usable
                        + (builds
                            ? ", and the connector may write new builds (EXECUTE ON"
                                + " DBMS_LOGMNR_D)."
                            : "; builds are switched off (cdc.dictionary.build.interval.ms=0),"
                                + " so this build serves until its logs are purged.")));
          }
          if (usable != null) {
            return List.of(
                new Finding(
                    "DOC-20",
                    Severity.INFO,
                    "Lag recovery is possible for now: "
                        + usable
                        + ", but without EXECUTE ON DBMS_LOGMNR_D the connector cannot write new"
                        + " builds, so recovery ends when that build's logs are purged.",
                    grant));
          }
          if (priv && builds) {
            return List.of(
                Finding.info(
                    "DOC-20",
                    "Lag recovery is not possible yet: "
                        + none
                        + (build.isEmpty()
                            ? ". The connector writes a build at its next start and then on the"
                                + " cdc.dictionary.build.* schedule."
                            : ". The connector writes the next build on the"
                                + " cdc.dictionary.build.* schedule.")));
          }
          return List.of(
              Finding.warning(
                  "DOC-20",
                  "Lag recovery is not possible: "
                      + none
                      + (priv
                          ? ", and builds are switched off (cdc.dictionary.build.interval.ms=0)."
                          : ", and the mining user may not run DBMS_LOGMNR_D.BUILD.")
                      + lag,
                  priv ? null : grant));
        });
  }

  interface Body {
    List<Finding> run(DoctorContext ctx) throws SQLException;
  }

  private static Rule rule(String id, Body body) {
    return new Rule() {
      @Override
      public String id() {
        return id;
      }

      @Override
      public List<Finding> evaluate(DoctorContext ctx) throws SQLException {
        return body.run(ctx);
      }
    };
  }
}
