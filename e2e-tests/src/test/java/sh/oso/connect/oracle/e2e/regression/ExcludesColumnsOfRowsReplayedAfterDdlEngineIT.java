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
package sh.oso.connect.oracle.e2e.regression;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.engine.LobAssembler;
import sh.oso.connect.oracle.core.jdbc.ConnectionRole;
import sh.oso.connect.oracle.core.jdbc.SessionInitializer;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.schema.ColumnFilter;
import sh.oso.connect.oracle.core.testkit.InMemorySchemaStore;
import sh.oso.connect.oracle.e2e.support.EngineDriver;
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * Invariant: column filters hold for rows LogMiner first returns with generic {@code COL n} names
 * because a DDL followed them (the lag case, ADR-0016): the rows are mined again with the
 * dictionary from the redo, decoded with the layout of their moment, and the excluded columns are
 * dropped by name, before and after the DDL, including a column the DDL adds that matches a
 * pattern. Debezium could not apply its column filter to such rows and stopped ("Failed to find
 * column COL 29"). The connector-level case is {@code ExcludedColumnsStayOutAcrossDdlConnectorIT}.
 *
 * @see <a href="https://github.com/debezium/dbz/issues/1599">dbz#1599</a>
 */
@Tag("engine")
@Tag("dbz-1599")
class ExcludesColumnsOfRowsReplayedAfterDdlEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void rowsWithGenericColumnNamesLoseTheirExcludedColumnsAfterReplay() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection meta = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      SessionInitializer.apply(meta, ConnectionRole.METADATA);
      exec(
          w,
          "CREATE TABLE lagc (id NUMBER PRIMARY KEY, ssn VARCHAR2(20), name VARCHAR2(20),"
              + " amount NUMBER)",
          "ALTER TABLE lagc ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      // a dictionary build in an archived log before the position, as the scheduler leaves one
      exec(meta, "BEGIN DBMS_LOGMNR_D.BUILD(OPTIONS => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS); END;");
      OracleSql.archiveLogCurrent(db);
      InMemorySchemaStore store = new InMemorySchemaStore();
      String include = "FREEPDB1\\." + schema + "\\.LAGC";
      ColumnFilter excluded =
          ColumnFilter.of(List.of("FREEPDB1\\." + schema + "\\.LAGC\\.SSN.*"), false);

      long resume;
      try (Connection mining = mining();
          EngineDriver first =
              new EngineDriver(
                  meta,
                  mining,
                  null,
                  include,
                  LogMinerHelper.currentScn(meta),
                  LobAssembler.Mode.SKIP,
                  store)) {
        first.engine.withColumnFilter(excluded);
        exec(w, "INSERT INTO lagc VALUES (0, 'SECRET-0', 'seen', 0)");
        first.runTo(LogMinerHelper.currentScn(meta));
        assertThat(first.committed).hasSize(1);
        assertThat(first.committed.get(0).events().get(0).after())
            .containsOnlyKeys("ID", "NAME", "AMOUNT");
        resume = first.engine.cursor().scn();
      }

      // while stopped: rows, then DDL that drops a column and adds one matching the pattern
      for (int i = 1; i <= 30; i++) {
        exec(w, "INSERT INTO lagc VALUES (" + i + ", 'SECRET-" + i + "', 'n" + i + "', " + i + ")");
      }
      exec(
          w,
          "ALTER TABLE lagc DROP COLUMN amount",
          "ALTER TABLE lagc ADD (ssn_hash VARCHAR2(64), note VARCHAR2(10))");
      for (int i = 31; i <= 60; i++) {
        exec(
            w,
            "INSERT INTO lagc VALUES ("
                + i
                + ", 'SECRET-"
                + i
                + "', 'n"
                + i
                + "', 'HASH-"
                + i
                + "', 'note"
                + i
                + "')");
      }

      try (Connection mining = mining();
          EngineDriver second =
              new EngineDriver(
                  meta, mining, null, include, resume, LobAssembler.Mode.SKIP, store)) {
        second.engine.withColumnFilter(excluded);
        second.runTo(LogMinerHelper.currentScn(meta));
        List<RowChange> rows = new ArrayList<>();
        second.committed.forEach(tx -> rows.addAll(tx.events()));
        assertThat(rows).hasSize(60);
        for (int i = 0; i < 30; i++) {
          assertThat(rows.get(i).after())
              .as("row %d, written before the DDL", i + 1)
              .containsOnlyKeys("ID", "NAME", "AMOUNT")
              .containsEntry("NAME", "n" + (i + 1));
        }
        for (int i = 30; i < 60; i++) {
          assertThat(rows.get(i).after())
              .as("row %d, written after the DDL", i + 1)
              .containsOnlyKeys("ID", "NAME", "NOTE")
              .containsEntry("ID", new BigDecimal(i + 1))
              .containsEntry("NOTE", "note" + (i + 1));
        }
        assertThat(rows)
            .allSatisfy(r -> assertThat(String.valueOf(r)).doesNotContain("SECRET", "HASH-"));
        assertThat(second.engine.metrics().lagReplays.get()).isPositive();
      }
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  private Connection mining() throws Exception {
    Connection c = db.capture(OracleTestDatabase.CDB_SERVICE);
    SessionInitializer.apply(c, ConnectionRole.MINING);
    return c;
  }

  private static void exec(Connection c, String... statements) throws Exception {
    try (Statement s = c.createStatement()) {
      for (String q : statements) {
        s.execute(q);
      }
    }
  }
}
