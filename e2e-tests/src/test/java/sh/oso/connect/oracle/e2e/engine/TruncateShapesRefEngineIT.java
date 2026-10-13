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

import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.ReferenceDoc;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/** ADR-0028: the DDL's transaction and redo order determine delivery and acknowledgement. */
@Tag("engine")
class TruncateShapesRefEngineIT {
  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void recordsTheTruncateDdlAndItsTransactionBetweenTheImplicitCommits() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection root = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection work = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      exec(
          work,
          "CREATE TABLE events (id NUMBER(10) PRIMARY KEY, v VARCHAR2(20))",
          "ALTER TABLE events ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      OracleSql.archiveLogCurrent(db);
      long start = LogMinerHelper.currentScn(root);
      work.setAutoCommit(false);
      exec(
          work,
          "INSERT INTO events VALUES (1, 'before')",
          "TRUNCATE TABLE events",
          "INSERT INTO events VALUES (2, 'after')");
      work.commit();
      OracleSql.archiveLogCurrent(db);
      long end = LogMinerHelper.currentScn(root);
      List<Map<String, String>> rows;
      LogMinerHelper.start(root, start, end);
      try {
        rows =
            LogMinerHelper.rows(
                root,
                "SEG_OWNER = '"
                    + schema
                    + "' OR USERNAME = '"
                    + schema
                    + "' OR OPERATION_CODE IN (6, 7, 36)");
      } finally {
        LogMinerHelper.end(root);
      }
      List<Map<String, String>> ddl =
          rows.stream()
              .filter(
                  r ->
                      r.get("SQL_REDO") != null
                          && r.get("SQL_REDO")
                              .toUpperCase(java.util.Locale.ROOT)
                              .startsWith("TRUNCATE TABLE")
                          && schema.equals(r.get("USERNAME")))
              .toList();
      assertThat(ddl).as("captured user's TRUNCATE DDL").hasSize(1);
      Map<String, String> truncate = ddl.get(0);
      assertThat(truncate.get("OPERATION_CODE")).isEqualTo("5");
      Map<String, String> before =
          rows.stream()
              .filter(
                  r ->
                      "1".equals(r.get("OPERATION_CODE"))
                          && schema.equals(r.get("SEG_OWNER"))
                          && r.get("SQL_REDO").contains("'before'"))
              .findFirst()
              .orElseThrow();
      Map<String, String> after =
          rows.stream()
              .filter(
                  r ->
                      "1".equals(r.get("OPERATION_CODE"))
                          && schema.equals(r.get("SEG_OWNER"))
                          && r.get("SQL_REDO").contains("'after'"))
              .findFirst()
              .orElseThrow();
      Map<String, Map<String, String>> scenarios = new LinkedHashMap<>();
      scenarios.put("Insert before TRUNCATE", before);
      scenarios.put("TRUNCATE", truncate);
      scenarios.put("Insert after TRUNCATE", after);
      Set<String> transactions = new LinkedHashSet<>();
      List<List<String>> shape = new ArrayList<>();
      for (Map.Entry<String, Map<String, String>> scenario : scenarios.entrySet()) {
        String xid = xid(scenario.getValue());
        transactions.add(xid);
        List<Map<String, String>> transaction =
            rows.stream().filter(r -> xid.equals(xid(r))).toList();
        assertThat(transaction.stream().anyMatch(r -> "7".equals(r.get("OPERATION_CODE"))))
            .as(scenario.getKey() + " has a COMMIT")
            .isTrue();
        shape.add(
            List.of(
                scenario.getKey(),
                transaction.stream()
                    .map(r -> r.get("OPERATION_CODE"))
                    .collect(java.util.stream.Collectors.joining(", ")),
                java.util.Objects.toString(scenario.getValue().get("SEG_OWNER"), "null")
                    .replace(schema, "SCHEMA"),
                java.util.Objects.toString(scenario.getValue().get("TABLE_NAME"), "null"),
                java.util.Objects.toString(scenario.getValue().get("STATUS"), "null")));
      }
      assertThat(transactions).hasSize(3);
      assertThat(Long.parseLong(before.get("SCN"))).isLessThan(Long.parseLong(truncate.get("SCN")));
      assertThat(Long.parseLong(truncate.get("SCN"))).isLessThan(Long.parseLong(after.get("SCN")));
      new ReferenceDoc(
              "truncate-shapes",
              "LogMiner TRUNCATE transaction shape",
              "The external LogMiner output for an uncommitted INSERT, TRUNCATE TABLE and a later"
                  + " INSERT. This specifies how an optional truncate record participates in commit"
                  + " order and offsets (ADR-0028).")
          .section("Transaction shapes")
          .table(
              List.of("Statement", "Operation codes in redo order", "Owner", "Table", "Status"),
              shape)
          .section("Facts and decisions")
          .bullet("TRUNCATE is a DDL row (operation code 5), in its own committed transaction.")
          .bullet(
              "The earlier uncommitted INSERT is implicitly committed before TRUNCATE; the later"
                  + " INSERT has a separate commit.")
          .bullet(
              "Buffer a logical TRUNCATE change at the DDL's redo position and publish it only when"
                  + " its COMMIT is delivered.")
          .bullet(
              "Append the logical operation after UNKNOWN so existing codec ordinals remain"
                  + " unchanged.")
          .bullet(
              "Keep emission off by default. Only an acknowledged final record may advance past a"
                  + " truncate.")
          .assertUpToDate();
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  private static String xid(Map<String, String> row) {
    return row.get("XIDUSN") + "." + row.get("XIDSLT") + "." + row.get("XIDSQN");
  }

  private static void exec(Connection connection, String... sql) throws Exception {
    try (Statement statement = connection.createStatement()) {
      for (String text : sql) statement.execute(text);
    }
  }
}
