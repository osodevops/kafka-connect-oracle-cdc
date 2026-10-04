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
package sh.oso.connect.oracle.core.mining;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * Builds the V$LOGMNR_CONTENTS query. Three OR branches, always present (ADR-0001): row changes on
 * captured object ids, DDL by non-Oracle owners, transaction control rows. MISSING_SCN rows are
 * always fetched because they signal a gap. SCN bounds are the two binds. Identifiers and user
 * names are embedded as literals (an in-list of a thousand ids cannot be bound), quoted and split
 * into chunks of {@code inlistMax} so Oracle's in-list limit is never hit.
 */
public final class LogMinerQuery {

  /** Column order matches {@link JdbcLogMinerSession}'s row mapper. */
  public static final List<String> COLUMNS =
      List.of(
          "SCN",
          "START_SCN",
          "COMMIT_SCN",
          "TIMESTAMP",
          "COMMIT_TIMESTAMP",
          "THREAD#",
          "XIDUSN",
          "XIDSLT",
          "XIDSQN",
          "OPERATION",
          "OPERATION_CODE",
          "ROLLBACK",
          "STATUS",
          "INFO",
          "SEG_OWNER",
          "SEG_NAME",
          "TABLE_NAME",
          "USERNAME",
          "SESSION#",
          "SERIAL#",
          "CLIENT_ID",
          "ROW_ID",
          "RS_ID",
          "SSN",
          "CSF",
          "DATA_OBJ#",
          "DATA_OBJD#",
          "DATA_OBJV#",
          "SRC_CON_ID",
          "SRC_CON_NAME",
          "SRC_CON_DBID",
          "CON_ID",
          "SQL_REDO",
          "SQL_UNDO");

  /** Row changes on captured objects, plus UNSUPPORTED rows for them (CORE-MINE-10). */
  static final List<Integer> ROW_CODES = List.of(1, 2, 3, 9, 10, 11, 28, 255);

  static final int DDL_CODE = 5;
  static final List<Integer> TX_CODES = List.of(6, 7, 36);
  static final int MISSING_SCN_CODE = 34;

  /** Owners whose DDL is Oracle's own housekeeping, never a captured table. */
  static final List<String> ORACLE_MAINTAINED =
      List.of(
          "SYS",
          "SYSTEM",
          "AUDSYS",
          "XDB",
          "OUTLN",
          "DBSNMP",
          "MDSYS",
          "CTXSYS",
          "ORDSYS",
          "WMSYS",
          "LBACSYS",
          "OJVMSYS",
          "GSMADMIN_INTERNAL",
          "DVSYS",
          "APPQOSSYS",
          "DBSFWUSER",
          "GGSYS",
          "ANONYMOUS",
          "REMOTE_SCHEDULER_AGENT",
          "SYS$UMF",
          "DIP",
          "ORACLE_OCM",
          "XS$NULL",
          "ORDDATA",
          "OLAPSYS",
          "DVF",
          "PDBADMIN");

  private LogMinerQuery() {}

  public static String sql(MiningFilter f) {
    StringBuilder sb = new StringBuilder("SELECT ");
    sb.append(String.join(", ", COLUMNS));
    sb.append(" FROM V$LOGMNR_CONTENTS WHERE SCN >= ? AND SCN < ? AND (");
    // 1. row changes on captured objects
    sb.append("(OPERATION_CODE IN (").append(join(ROW_CODES)).append(") AND ");
    if (f.objectIds().isEmpty()) {
      sb.append("1 = 0");
    } else {
      sb.append('(');
      List<Long> ids = new ArrayList<>(new TreeSet<>(f.objectIds()));
      for (int i = 0; i < ids.size(); i += f.inlistMax()) {
        if (i > 0) {
          sb.append(" OR ");
        }
        sb.append("DATA_OBJ# IN (")
            .append(join(ids.subList(i, Math.min(ids.size(), i + f.inlistMax()))))
            .append(')');
      }
      sb.append(')');
    }
    sb.append(')');
    // 2. DDL by any non-Oracle owner: a CREATE TABLE that newly matches the include patterns must
    //    be seen even when no table is captured yet (SRC-SEL-4); DDL rows are rare
    sb.append(" OR (OPERATION_CODE = ")
        .append(DDL_CODE)
        .append(" AND (SEG_OWNER IS NULL OR SEG_OWNER NOT IN (")
        .append(quoted(ORACLE_MAINTAINED))
        .append(")))");
    // 3. transaction control, minus excluded users (dbz#24)
    sb.append(" OR (OPERATION_CODE IN (").append(join(TX_CODES)).append(')');
    if (!f.excludedUsers().isEmpty()) {
      sb.append(" AND (USERNAME IS NULL OR USERNAME NOT IN (")
          .append(quoted(f.excludedUsers()))
          .append("))");
    }
    sb.append(')');
    // 4. gaps
    sb.append(" OR OPERATION_CODE = ").append(MISSING_SCN_CODE);
    sb.append(')');
    return sb.toString();
  }

  private static String join(List<? extends Number> values) {
    StringBuilder sb = new StringBuilder();
    for (Number v : values) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append(v.longValue());
    }
    return sb.toString();
  }

  private static String quoted(Iterable<String> values) {
    StringBuilder sb = new StringBuilder();
    TreeSet<String> sorted = new TreeSet<>();
    values.forEach(sorted::add);
    for (String v : sorted) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append('\'').append(v.replace("'", "''")).append('\'');
    }
    return sb.toString();
  }
}
