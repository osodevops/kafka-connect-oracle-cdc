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
package sh.oso.connect.oracle.e2e.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.ReferenceDoc;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * P0-08: the literal form LogMiner uses in SQL_REDO for every 1.0 data type, which types make a row
 * UNSUPPORTED, how the mining session's NLS settings change the temporal literals, how CSF
 * continuation splits long statements, and how the UPDATE WHERE clause differs with ALL COLUMNS
 * versus PRIMARY KEY supplemental logging. One table per type isolates the unsupported ones. The
 * raw statements are dumped as the parser's T0 corpus under
 * oracle-cdc-core/src/test/resources/sqlredo-corpus/ (update mode only).
 */
@Tag("engine")
class SqlRedoShapesRefEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  /** declared type, representative literal, classic (part of the combined tables) */
  private static final String[][] TYPES = {
    {"CHAR(10)", "'abc'", "y"},
    {"NCHAR(10)", "N'nchár'", "y"},
    {"VARCHAR2(100)", "'plain ''quoted'' text'", "y"},
    {"NVARCHAR2(100)", "N'ünïcode ☃ text'", "y"},
    {"NUMBER", "1234567890.123456789", "y"},
    {"NUMBER(10,2)", "-42.50", "y"},
    {"FLOAT", "3.14159", "y"},
    {"BINARY_FLOAT", "1.5E10", "y"},
    {"BINARY_DOUBLE", "-2.5E-300", "y"},
    {"DATE", "TIMESTAMP '2026-02-28 13:45:59'", "y"},
    {"TIMESTAMP(6)", "TIMESTAMP '2026-03-29 01:30:00.123456'", "y"},
    {"TIMESTAMP(9)", "TIMESTAMP '2026-03-29 01:30:00.123456789'", "y"},
    {"TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP '2026-03-29 01:30:00.5 +05:30'", "y"},
    {"TIMESTAMP(6) WITH LOCAL TIME ZONE", "TIMESTAMP '2026-10-25 02:30:00.25'", "y"},
    {"INTERVAL YEAR(4) TO MONTH", "INTERVAL '12-3' YEAR(4) TO MONTH", "y"},
    {"INTERVAL DAY(5) TO SECOND(6)", "INTERVAL '5 04:03:02.123456' DAY(5) TO SECOND(6)", "y"},
    {"RAW(100)", "HEXTORAW('DEADBEEF00FF')", "y"},
    {"CLOB", "TO_CLOB('short clob')", "n"},
    {"NCLOB", "TO_NCLOB(N'short nclob')", "n"},
    {"BLOB", "HEXTORAW('CAFEBABE')", "n"},
    {"XMLTYPE", "XMLTYPE('<a><b>1</b></a>')", "n"},
    {"BOOLEAN", "TRUE", "n"},
    {"JSON", "JSON('{\"k\": [1, 2, {\"n\": null}]}')", "n"},
    {"VECTOR(3, FLOAT32)", "TO_VECTOR('[1.5, 2.5, 3.5]')", "n"},
  };

  @Test
  void literalShapesPerTypeAreRecorded() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection root = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      w.setAutoCommit(false);
      List<String> created = new ArrayList<>();
      StringBuilder classicCols = new StringBuilder();
      StringBuilder classicVals = new StringBuilder();
      try (Statement s = w.createStatement()) {
        for (int i = 0; i < TYPES.length; i++) {
          String t = "T" + i;
          try {
            s.execute("CREATE TABLE " + t + " (id NUMBER PRIMARY KEY, val " + TYPES[i][0] + ")");
            s.execute("ALTER TABLE " + t + " ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
            created.add(t);
          } catch (SQLException e) {
            created.add(null); // type not available on this version
          }
          if ("y".equals(TYPES[i][2])) {
            classicCols.append(", c").append(i).append(' ').append(TYPES[i][0]);
            classicVals.append(", ").append(TYPES[i][1]);
          }
        }
        s.execute("CREATE TABLE classic_all (id NUMBER PRIMARY KEY" + classicCols + ")");
        s.execute("ALTER TABLE classic_all ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        s.execute("CREATE TABLE classic_pk (id NUMBER PRIMARY KEY" + classicCols + ")");
        s.execute("ALTER TABLE classic_pk ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS");
        s.execute(
            "CREATE TABLE longrow (id NUMBER PRIMARY KEY, a VARCHAR2(4000), b VARCHAR2(4000), c"
                + " VARCHAR2(4000))");
        s.execute("ALTER TABLE longrow ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      }
      w.commit();
      OracleSql.archiveLogCurrent(db);
      long start = LogMinerHelper.currentScn(root);

      try (Statement s = w.createStatement()) {
        for (int i = 0; i < TYPES.length; i++) {
          if (created.get(i) == null) {
            continue;
          }
          s.execute("INSERT INTO " + created.get(i) + " VALUES (1, " + TYPES[i][1] + ")");
          s.execute("INSERT INTO " + created.get(i) + " (id) VALUES (2)");
          w.commit();
        }
        for (String t : List.of("classic_all", "classic_pk")) {
          s.execute("INSERT INTO " + t + " VALUES (1" + classicVals + ")");
          s.execute(
              "INSERT INTO "
                  + t
                  + " (id, c2, c4, c9) VALUES (3, '', 0, TO_DATE('1900-01-01', 'YYYY-MM-DD'))");
          w.commit();
          s.execute("UPDATE " + t + " SET c2 = 'changed', c4 = 1 WHERE id = 1");
          w.commit();
          s.execute("DELETE FROM " + t + " WHERE id = 3");
          w.commit();
        }
        s.execute(
            "INSERT INTO longrow VALUES (1, RPAD('a', 4000, 'a'), RPAD('b', 4000, 'b'), RPAD('c',"
                + " 4000, 'c'))");
        w.commit();
      }
      OracleSql.archiveLogCurrent(db);
      long end = LogMinerHelper.currentScn(root);

      String where =
          "SEG_OWNER = '" + schema + "' AND OPERATION NOT IN ('INTERNAL') ORDER BY SCN, RS_ID, SSN";
      LogMinerHelper.start(root, start, end);
      List<Map<String, String>> rows = LogMinerHelper.rows(root, where);
      LogMinerHelper.end(root);
      assertThat(rows).isNotEmpty();
      // second pass: temporal literals follow the mining session's NLS settings (PRD-00
      // CORE-CONN-4)
      try (Statement s = root.createStatement()) {
        s.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'");
        s.execute("ALTER SESSION SET NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9'");
        s.execute(
            "ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF9 TZH:TZM'");
        s.execute("ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,'");
      }
      LogMinerHelper.start(root, start, end);
      List<Map<String, String>> rowsNls = LogMinerHelper.rows(root, where);
      LogMinerHelper.end(root);

      List<List<String>> typeRows = new ArrayList<>();
      for (int i = 0; i < TYPES.length; i++) {
        String t = created.get(i);
        if (t == null) {
          typeRows.add(List.of(TYPES[i][0], "type not available on this version", "", ""));
          continue;
        }
        TreeSet<String> ops = new TreeSet<>();
        for (Map<String, String> r : rows) {
          if (t.equals(r.get("TABLE_NAME"))) {
            ops.add(r.get("OPERATION") + " status=" + r.get("STATUS"));
          }
        }
        typeRows.add(
            List.of(
                TYPES[i][0],
                String.join(", ", ops),
                valLiteral(rows, t, false),
                valLiteral(rowsNls, t, true)));
      }

      int csfCount = 0;
      int longest = 0;
      String allWhere = "(no UPDATE row)";
      String pkWhere = "(no UPDATE row)";
      for (Map<String, String> r : rows) {
        if ("1".equals(r.get("CSF"))) {
          csfCount++;
        }
      }
      for (String sql : joinCsf(rows)) {
        longest = Math.max(longest, sql.length());
        String norm = sql.replaceAll("\\s+", " ");
        if (norm.startsWith("update")
            && norm.contains("\"CLASSIC_ALL\"")
            && norm.contains("'changed'")) {
          allWhere = whereCoverage(norm);
        }
        if (norm.startsWith("update")
            && norm.contains("\"CLASSIC_PK\"")
            && norm.contains("'changed'")) {
          pkWhere = whereCoverage(norm);
        }
      }

      if (Boolean.getBoolean("reference.update")) {
        Path corpus =
            Path.of(System.getProperty("repo.root", ".."))
                .resolve("oracle-cdc-core/src/test/resources/sqlredo-corpus")
                .toAbsolutePath()
                .normalize();
        Files.createDirectories(corpus);
        Files.writeString(
            corpus.resolve("types-and-lobs.sql"),
            String.join("\n", joinCsf(rowsNls)).replace(schema, "T_SCHEMA") + "\n",
            StandardCharsets.UTF_8);
      }

      new ReferenceDoc(
              "sql-redo-shapes",
              "SQL_REDO literal forms per data type",
              "The literal LogMiner writes into SQL_REDO for every 1.0 data type and the 23ai types"
                  + " (PRD-00 CORE-DEC-1, CORE-DEC-5), one table per type so an unsupported type"
                  + " cannot hide the others, mined with `DICT_FROM_ONLINE_CATALOG`,"
                  + " `NO_ROWID_IN_STMT` and `NO_SQL_DELIMITER`. The third column replaces values"
                  + " by placeholders; the fourth shows the literal with its value under the"
                  + " connector's NLS settings. The raw statements form the parser corpus under"
                  + " `oracle-cdc-core/src/test/resources/sqlredo-corpus/`.")
          .section("Per type: rows LogMiner emits for an INSERT and the literal form of the value")
          .table(
              List.of(
                  "Declared type",
                  "Rows (OPERATION status)",
                  "Literal shape with default NLS",
                  "Literal with the connector's NLS settings"),
              typeRows)
          .paragraph(
              "The default session formats (`DD-MON-RR`, no fractional seconds) lose the time of"
                  + " day of a DATE and the precision of a TIMESTAMP, which is why PRD-00"
                  + " CORE-CONN-4 sets `NLS_DATE_FORMAT`, `NLS_TIMESTAMP_FORMAT`,"
                  + " `NLS_TIMESTAMP_TZ_FORMAT` and `NLS_NUMERIC_CHARACTERS` on every mining"
                  + " session.")
          .section(
              "Statement splitting and supplemental logging (combined tables of the classic types)")
          .bullet(
              "Rows with CSF = 1 (statement continues in the next row): "
                  + (csfCount > 0 ? "observed" : "none observed")
                  + "; longest reassembled statement over 4000 characters: "
                  + (longest > 4000 ? "yes" : "no")
                  + ".")
          .bullet("UPDATE of two columns with ALL COLUMNS logging, WHERE clause: " + allWhere + ".")
          .bullet(
              "UPDATE of two columns with PRIMARY KEY logging only, WHERE clause: "
                  + pkWhere
                  + " (PRD-00 CORE-DEC-4 partial before image).")
          .assertUpToDate();
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  /** The VAL literal of the typed INSERT on table t, as a shape or with its value. */
  private static String valLiteral(List<Map<String, String>> rows, String t, boolean withValue) {
    List<Map<String, String>> tRows = new ArrayList<>();
    for (Map<String, String> r : rows) {
      if (t.equals(r.get("TABLE_NAME"))) {
        tRows.add(r);
      }
    }
    String out = "(no INSERT row)";
    for (String sql : joinCsf(tRows)) {
      Map<String, String> vals = parseInsert(sql.replaceAll("\\s+", " "));
      if (vals.containsKey("VAL") && !"NULL".equalsIgnoreCase(vals.get("VAL").trim())) {
        out = withValue ? trim(vals.get("VAL")) : shape(vals.get("VAL"));
      }
    }
    return out;
  }

  private static String trim(String literal) {
    String l = literal.trim();
    return l.length() > 70 ? l.substring(0, 70) + "..." : l;
  }

  /** Describes which columns an UPDATE's WHERE clause names. */
  static String whereCoverage(String norm) {
    int w = norm.indexOf(" where ");
    if (w < 0) {
      return "no WHERE clause";
    }
    String where = norm.substring(w + 7);
    int cols = where.split(" and ").length;
    boolean hasId = where.contains("\"ID\" =");
    boolean hasUnchanged = where.contains("\"C0\" =") || where.contains("\"C9\" =");
    if (hasId && hasUnchanged) {
      return "all " + cols + " columns (key, changed and unchanged)";
    }
    if (hasId) {
      return cols
          + " columns: key"
          + (cols > 1 ? " plus the changed columns' old values" : " only");
    }
    return cols + " columns, no key";
  }

  /** Concatenates SQL_REDO across CSF continuation rows. */
  static List<String> joinCsf(List<Map<String, String>> rows) {
    List<String> out = new ArrayList<>();
    StringBuilder current = new StringBuilder();
    for (Map<String, String> r : rows) {
      String sql = r.get("SQL_REDO");
      if (sql != null) {
        current.append(sql);
      }
      if (!"1".equals(r.get("CSF"))) {
        if (current.length() > 0) {
          out.add(current.toString());
        }
        current.setLength(0);
      }
    }
    if (current.length() > 0) {
      out.add(current.toString());
    }
    return out;
  }

  private static final Pattern INSERT =
      Pattern.compile("insert into \"[^\"]+\"\\.\"[^\"]+\"\\((.*?)\\) values \\((.*)\\)$");

  /** Column name to literal text for a LogMiner INSERT statement. */
  static Map<String, String> parseInsert(String sql) {
    Map<String, String> out = new TreeMap<>();
    Matcher m = INSERT.matcher(sql);
    if (!m.find()) {
      return out;
    }
    List<String> cols = new ArrayList<>();
    for (String c : m.group(1).split(",")) {
      cols.add(c.trim().replace("\"", ""));
    }
    List<String> vals = splitTopLevel(m.group(2));
    for (int i = 0; i < Math.min(cols.size(), vals.size()); i++) {
      out.put(cols.get(i), vals.get(i).trim());
    }
    return out;
  }

  /** Splits on commas outside quotes and parentheses. */
  static List<String> splitTopLevel(String s) {
    List<String> out = new ArrayList<>();
    int depth = 0;
    boolean inQuote = false;
    StringBuilder cur = new StringBuilder();
    for (int i = 0; i < s.length(); i++) {
      char ch = s.charAt(i);
      if (ch == '\'') {
        inQuote = !inQuote;
      } else if (!inQuote && ch == '(') {
        depth++;
      } else if (!inQuote && ch == ')') {
        depth--;
      }
      if (ch == ',' && !inQuote && depth == 0) {
        out.add(cur.toString());
        cur.setLength(0);
      } else {
        cur.append(ch);
      }
    }
    out.add(cur.toString());
    return out;
  }

  /**
   * Replaces the value inside a literal with a placeholder, keeping the wrapping function and mask.
   */
  static String shape(String literal) {
    String l = literal.trim();
    if (l.equalsIgnoreCase("NULL")) {
      return "NULL";
    }
    Matcher fn =
        Pattern.compile(
                "^([A-Z_]+)\\('([^']*)'(?:\\s*,\\s*'([^']*)')?\\)$", Pattern.CASE_INSENSITIVE)
            .matcher(l);
    if (fn.find()) {
      String name = fn.group(1).toUpperCase();
      String mask = fn.group(3);
      return name + "('<value>'" + (mask != null ? ", '" + mask + "'" : "") + ")";
    }
    if (l.matches("^[A-Z_]+\\(\\)$")) {
      return l.toUpperCase();
    }
    if (l.startsWith("'") && l.endsWith("'")) {
      return "'<string>'";
    }
    if (l.matches("^-?[0-9.]+([Ee][-+]?[0-9]+)?$")) {
      return "<number>";
    }
    return l.replaceAll("'[^']*'", "'<value>'").replaceAll("[0-9]+", "<n>");
  }

  @SuppressWarnings("unused")
  private static List<String> names() {
    return Arrays.stream(TYPES).map(t -> t[0]).toList();
  }
}
