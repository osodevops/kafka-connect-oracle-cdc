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
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.config.CoreConfig.CaptureMode;
import sh.oso.connect.oracle.core.topology.ArchiveDestination;
import sh.oso.connect.oracle.core.topology.DatabaseInfo;

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
        roleAndOpenMode(),
        version());
  }

  public static List<Rule> all() {
    return fastMode();
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
                      "SHUTDOWN IMMEDIATE;\n"
                          + "STARTUP MOUNT;\n"
                          + "ALTER DATABASE ARCHIVELOG;\n"
                          + "ALTER DATABASE OPEN;"));
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
                        "ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;")));
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
                            + " logs primary key columns only: updates carry the key and changed"
                            + " columns, so before images are partial (source.partial = true).",
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
                    "GRANT SELECT ON "
                        + v.replace("GV$", "GV_$").replace("V$", "V_$").replace("GV__$", "GV_$")
                        + " TO "
                        + user
                        + all
                        + ";"));
          }
          if (inaccessible.contains("V$DATABASE")) {
            return out; // container checks need V$DATABASE; the grant above comes first
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
              } else if ("rowid".equals(ctx.keyMissing()) && t.rowMovement()) {
                out.add(
                    Finding.warning(
                        "DOC-7",
                        t.fqn()
                            + " is keyed by ROWID but has ROW MOVEMENT enabled; a moved row changes"
                            + " its key.",
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
                      "ALTER SYSTEM SET log_archive_dest_1 = 'LOCATION=/path/to/archive'"
                          + " SCOPE=BOTH;"));
        });
  }

  static Rule roleAndOpenMode() {
    return rule(
        "DOC-14",
        ctx -> {
          DatabaseInfo db = ctx.database();
          CaptureMode mode = ctx.config().captureMode();
          if (mode == CaptureMode.ONLINE && !(db.primary() && "READ WRITE".equals(db.openMode()))) {
            return List.of(
                Finding.blocking(
                    "DOC-14",
                    "cdc.capture.mode=online needs a PRIMARY database open READ WRITE; this one is "
                        + db.databaseRole()
                        + " "
                        + db.openMode()
                        + ". Use archive_only for a standby.",
                    null));
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
