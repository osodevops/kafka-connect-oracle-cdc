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

import java.util.Locale;
import sh.oso.connect.oracle.core.topology.Platform;

/**
 * ADR-0024: the fix text of a doctor finding in the form the database's operator can run. On Amazon
 * RDS the master user has no SYSDBA, ALTER SYSTEM or ALTER DATABASE, so settings change through
 * rdsadmin procedures, the AWS console or CLI, and SYS objects are granted with grant_sys_object.
 * Elsewhere the SQL a DBA runs as SYSDBA.
 */
final class PlatformSql {

  private PlatformSql() {}

  /** DOC-1. */
  static String enableArchivelog(Platform p) {
    if (p == Platform.RDS) {
      return "-- RDS runs in ARCHIVELOG mode only while automated backups are on:\n"
          + "-- aws rds modify-db-instance --db-instance-identifier <instance>"
          + " --backup-retention-period 1 --apply-immediately";
    }
    return "SHUTDOWN IMMEDIATE;\nSTARTUP MOUNT;\nALTER DATABASE ARCHIVELOG;\nALTER DATABASE OPEN;";
  }

  /** DOC-2. */
  static String addSupplementalLogging(Platform p) {
    if (p == Platform.RDS) {
      return "BEGIN\n"
          + "  rdsadmin.rdsadmin_util.alter_supplemental_logging(p_action => 'ADD');\n"
          + "END;\n/";
    }
    return "ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;";
  }

  /** DOC-4: SELECT on a fixed view, written as its V_$ synonym target. */
  static String grantSelect(Platform p, String view, String user, boolean cdb) {
    String object = view.replace("GV$", "GV_$").replace("V$", "V_$").replace("GV__$", "GV_$");
    return grant(p, object, "SELECT", user, cdb);
  }

  /** DOC-20 and friends: EXECUTE on a SYS package. */
  static String grantExecute(Platform p, String pkg, String user, boolean cdb) {
    return grant(p, pkg, "EXECUTE", user, cdb);
  }

  private static String grant(
      Platform p, String object, String privilege, String user, boolean cdb) {
    if (p == Platform.RDS) {
      String grantee =
          user.startsWith("\"") ? user.replace("\"", "") : user.toUpperCase(Locale.ROOT);
      return "BEGIN\n  rdsadmin.rdsadmin_util.grant_sys_object('"
          + object
          + "', '"
          + grantee
          + "', '"
          + privilege
          + "');\nEND;\n/";
    }
    return "GRANT "
        + privilege
        + " ON "
        + object
        + " TO "
        + user
        + (cdb ? " CONTAINER=ALL" : "")
        + ";";
  }

  /** DOC-11 and other initialisation parameters. */
  static String setParameter(Platform p, String name, String value) {
    if (p == Platform.RDS) {
      return "-- Set "
          + name
          + " in the DB parameter group of the instance:\n"
          + "-- aws rds modify-db-parameter-group --db-parameter-group-name <group>"
          + " --parameters ParameterName="
          + name
          + ",ParameterValue="
          + value
          + ",ApplyMethod=immediate";
    }
    return "ALTER SYSTEM SET " + name.toUpperCase(Locale.ROOT) + " = " + value + " SCOPE=BOTH;";
  }

  /** DOC-9: one more online log group of {@code mib} MiB on {@code thread}. */
  static String addLogfile(Platform p, int thread, long mib) {
    if (p == Platform.RDS) {
      return "-- Add groups of the recommended size, switch until the old groups are INACTIVE,"
          + " then drop them\n"
          + "-- with rdsadmin.rdsadmin_util.drop_logfile(grp => <group>).\n"
          + "EXEC rdsadmin.rdsadmin_util.add_logfile(p_size => '"
          + mib
          + "M');";
    }
    return "-- Add groups of the recommended size, switch until the old groups are INACTIVE, then"
        + " drop them.\n"
        + "ALTER DATABASE ADD LOGFILE THREAD "
        + thread
        + " SIZE "
        + mib
        + "M;";
  }

  /** DOC-12. */
  static String archiveDestination(Platform p) {
    if (p == Platform.RDS) {
      return "-- RDS manages the archive destination; it is valid while automated backups are on:\n"
          + "-- aws rds modify-db-instance --db-instance-identifier <instance>"
          + " --backup-retention-period 1 --apply-immediately";
    }
    return "ALTER SYSTEM SET log_archive_dest_1 = 'LOCATION=/path/to/archive' SCOPE=BOTH;";
  }

  /** DOC-10 and DOC-23: keep archived redo for at least {@code hours}. */
  static String archiveRetention(Platform p, long hours) {
    if (p == Platform.RDS) {
      return "BEGIN\n"
          + "  rdsadmin.rdsadmin_util.set_configuration(\n"
          + "    name  => 'archivelog retention hours',\n"
          + "    value => '"
          + hours
          + "');\n"
          + "END;\n/\nCOMMIT;";
    }
    return "-- In the job that deletes archived logs, keep at least "
        + hours
        + " hours, for example in RMAN:\n"
        + "-- DELETE ARCHIVELOG ALL COMPLETED BEFORE 'SYSDATE-"
        + hours
        + "/24';";
  }
}
