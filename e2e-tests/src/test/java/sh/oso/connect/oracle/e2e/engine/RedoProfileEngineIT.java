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
import static org.assertj.core.api.Assertions.within;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.e2e.support.Cli;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * PRD-05 {@code redo-profile} against Oracle Database Free: after a scripted workload on a captured
 * table and an uncaptured delete-and-reload job, the command reports the hourly archive rates from
 * V$ARCHIVED_LOG, both tables with their row counts from a LogMiner sample of exactly the logs the
 * workload wrote, the share of rows the connector would filter out, and the reload pattern.
 */
@Tag("engine")
class RedoProfileEngineIT {

  static final int CAPTURED_INSERTS = 2000;
  static final int CAPTURED_UPDATES = 500;
  static final int NOISE_ROWS = 5000;

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void theProfileNamesBothTablesTheirRatesAndTheUncapturedShare(@TempDir Path dir)
      throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try {
      long before;
      long after;
      // one connection for the whole workload: a connection per statement exhausts processes
      try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
          Connection root = db.capture(OracleTestDatabase.CDB_SERVICE)) {
        exec(
            w,
            "CREATE TABLE captured (id NUMBER(9) PRIMARY KEY, v VARCHAR2(40))",
            "ALTER TABLE captured ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
            "CREATE TABLE noise (id NUMBER(9) PRIMARY KEY, v VARCHAR2(40))",
            // the initial load of the reload job lies before the sampled logs
            "INSERT INTO noise SELECT LEVEL, 'load ' || LEVEL FROM dual CONNECT BY LEVEL <= "
                + NOISE_ROWS);
        OracleSql.archiveLogCurrent(db); // the workload starts in a fresh log
        before = OracleSql.maxArchivedSequence(root);
        exec(
            w,
            "INSERT INTO captured SELECT LEVEL, 'c ' || LEVEL FROM dual CONNECT BY LEVEL <= "
                + CAPTURED_INSERTS,
            "UPDATE captured SET v = 'u ' || id WHERE id <= " + CAPTURED_UPDATES,
            // the nightly reload: delete everything, load it again
            "DELETE FROM noise",
            "INSERT INTO noise SELECT LEVEL, 'reload ' || LEVEL FROM dual CONNECT BY LEVEL <= "
                + NOISE_ROWS);
        OracleSql.archiveLogCurrent(db);
        after = OracleSql.maxArchivedSequence(root);
      }
      int sampleLogs = (int) Math.max(1, after - before);

      Map<String, String> props = new HashMap<>();
      props.put(CoreConfig.DATABASE_URL, db.jdbcUrl(OracleTestDatabase.CDB_SERVICE));
      props.put(CoreConfig.DATABASE_USER, OracleTestDatabase.CAPTURE_USER);
      props.put(CoreConfig.DATABASE_PASSWORD, OracleTestDatabase.CAPTURE_PASSWORD);
      props.put(CoreConfig.DATABASE_PDBS, OracleTestDatabase.PDB1);
      props.put("cdc.tables.include", "FREEPDB1\\." + schema + "\\.CAPTURED");
      Path config = dir.resolve("connector.json");
      Files.writeString(
          config,
          new ObjectMapper().writeValueAsString(Map.of("name", "redo-profile", "config", props)));

      Cli.Result r =
          Cli.doctor(
              "redo-profile",
              "--config",
              config.toString(),
              "--window",
              "2h",
              "--sample-logs",
              Integer.toString(sampleLogs),
              "--top",
              "1000");
      assertThat(r.exit()).as(r.toString()).isZero();
      String out = r.out();

      // hourly rates: the two archives the suite forced are inside the window
      assertThat(out).contains("## Archive generation per hour");
      assertThat(out).doesNotContain("No log was archived in the window.");
      List<String[]> hours = rows(out, "^\\| \\d{4}-\\d{2}-\\d{2}T\\d{2}:00:00Z \\|");
      assertThat(hours).as("hourly rows").isNotEmpty();
      long logs = 0;
      for (String[] h : hours) {
        assertThat(h[1]).as("thread").isEqualTo("1");
        logs += Long.parseLong(h[2]);
        assertThat(h[3]).as("archived bytes").isNotBlank();
      }
      assertThat(logs).isGreaterThanOrEqualTo(2);

      // the sample covers exactly the workload's logs
      assertThat(out)
          .contains(" in " + sampleLogs + (sampleLogs == 1 ? " log" : " logs") + " (")
          .contains("with no table filter");

      String capturedName = "FREEPDB1." + schema + ".CAPTURED";
      String noiseName = "FREEPDB1." + schema + ".NOISE";
      String[] captured = table(out, capturedName);
      String[] noise = table(out, noiseName);
      // | Table | Captured | Rows | Share | Inserts | Updates | Deletes | DDL | Other | Pattern |
      assertThat(captured[1]).isEqualTo("yes");
      assertThat(Long.parseLong(captured[4])).isGreaterThanOrEqualTo(CAPTURED_INSERTS);
      assertThat(Long.parseLong(captured[5])).isGreaterThanOrEqualTo(CAPTURED_UPDATES);
      assertThat(captured[9]).isEmpty();
      assertThat(noise[1]).isEqualTo("no");
      assertThat(Long.parseLong(noise[4])).isGreaterThanOrEqualTo(NOISE_ROWS);
      assertThat(Long.parseLong(noise[6])).isGreaterThanOrEqualTo(NOISE_ROWS);
      assertThat(noise[9]).isEqualTo("delete and reload");

      // the uncaptured share, and the arithmetic behind it, from the report's own figures
      Matcher m =
          Pattern.compile(
                  "Of (\\d+) rows naming a table, (\\d+) belong to captured tables\\. The"
                      + " connector's mining query filters out the other ([0-9.]+) per cent")
              .matcher(out);
      assertThat(m.find()).as("uncaptured share in:\n%s", out).isTrue();
      long tableRows = Long.parseLong(m.group(1));
      long capturedRows = Long.parseLong(m.group(2));
      double share = Double.parseDouble(m.group(3));
      assertThat(capturedRows).isEqualTo(Long.parseLong(captured[2]));
      assertThat(tableRows)
          .isGreaterThanOrEqualTo(capturedRows + Long.parseLong(noise[2]))
          .isGreaterThan(capturedRows);
      assertThat(share).isCloseTo(100.0 * (tableRows - capturedRows) / tableRows, within(0.1));
      assertThat(share).isGreaterThan(50.0); // the reload job writes most of the redo

      // the reload is named: as the top redo source, or on its own line
      String first = rows(out, "^\\| FREEPDB1\\.|^\\| [A-Z0-9_$#]+\\.").get(0)[0];
      if (first.equals(noiseName)) {
        assertThat(out)
            .contains(
                "The top redo source, "
                    + noiseName
                    + ", is not captured and looks like a delete and reload job");
      } else {
        assertThat(out).contains(noiseName + " looks like a delete and reload job.");
      }
      System.out.println("redo-profile: " + sampleLogs + " logs sampled, uncaptured " + share);
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  /** The Markdown table row for {@code name}, cells trimmed, without the leading empty cell. */
  private static String[] table(String out, String name) {
    List<String[]> found = rows(out, "^\\| " + Pattern.quote(name) + " \\|");
    assertThat(found).as("row for %s in:\n%s", name, out).hasSize(1);
    return found.get(0);
  }

  private static List<String[]> rows(String out, String regex) {
    Pattern p = Pattern.compile(regex);
    List<String[]> rows = new ArrayList<>();
    for (String line : out.split("\n")) {
      if (p.matcher(line).find()) {
        String[] cells = line.split("\\|", -1);
        String[] trimmed = new String[cells.length - 2];
        for (int i = 1; i < cells.length - 1; i++) {
          trimmed[i - 1] = cells[i].trim();
        }
        rows.add(trimmed);
      }
    }
    return rows;
  }

  private static void exec(Connection c, String... statements) throws Exception {
    c.setAutoCommit(false);
    try (Statement s = c.createStatement()) {
      for (String q : statements) {
        s.execute(q);
        c.commit();
      }
    }
  }
}
