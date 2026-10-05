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
import sh.oso.connect.oracle.core.testkit.InMemorySchemaStore;
import sh.oso.connect.oracle.e2e.support.EngineDriver;
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * PRD-03 acceptance, lag case: stop, run 50 DML, a column drop and a column add, then 50 more DML;
 * restart; all 100 rows decode with the right layout. The first 50 come back from the online
 * catalog as STATUS 2 with generic names and are mined again with the redo dictionary (ADR-0008).
 */
@Tag("engine")
class LagCaseReplayEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void rowsWrittenBeforeDdlWhileStoppedDecodeWithTheirLayoutAfterARestart() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection meta = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      SessionInitializer.apply(meta, ConnectionRole.METADATA);
      exec(
          w,
          "CREATE TABLE lagt (id NUMBER PRIMARY KEY, name VARCHAR2(20), amount NUMBER)",
          "ALTER TABLE lagt ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      // a dictionary build in an archived log before the position, as the scheduler leaves one
      exec(meta, "BEGIN DBMS_LOGMNR_D.BUILD(OPTIONS => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS); END;");
      OracleSql.archiveLogCurrent(db);
      InMemorySchemaStore store = new InMemorySchemaStore(); // the schema topic across the restart
      String include = "FREEPDB1\\." + schema + "\\.LAGT";

      // run 1 sees the table and stores its version, then stops
      long resume;
      try (Connection mining = miningConnection();
          EngineDriver first =
              new EngineDriver(
                  meta,
                  mining,
                  null,
                  include,
                  LogMinerHelper.currentScn(meta),
                  LobAssembler.Mode.SKIP,
                  store)) {
        exec(w, "INSERT INTO lagt VALUES (0, 'seen', 0)");
        first.runTo(LogMinerHelper.currentScn(meta));
        assertThat(first.committed).hasSize(1);
        resume = first.engine.cursor().scn();
      }
      assertThat(
              store.versions(
                  new sh.oso.connect.oracle.core.model.TableId("FREEPDB1", schema, "LAGT")))
          .hasSize(1);

      // while stopped
      for (int i = 1; i <= 50; i++) {
        exec(w, "INSERT INTO lagt VALUES (" + i + ", 'n" + i + "', " + i + ")");
      }
      exec(w, "ALTER TABLE lagt DROP COLUMN name", "ALTER TABLE lagt ADD (extra VARCHAR2(10))");
      for (int i = 51; i <= 100; i++) {
        exec(w, "INSERT INTO lagt VALUES (" + i + ", " + i + ", 'e" + i + "')");
      }

      // run 2 from the stored position with the stored versions
      try (Connection mining = miningConnection();
          EngineDriver second =
              new EngineDriver(
                  meta, mining, null, include, resume, LobAssembler.Mode.SKIP, store)) {
        second.runTo(LogMinerHelper.currentScn(meta));
        List<RowChange> rows = new ArrayList<>();
        second.committed.forEach(tx -> rows.addAll(tx.events()));
        assertThat(rows).hasSize(100);
        for (int i = 0; i < 50; i++) {
          assertThat(rows.get(i).after())
              .as("row %d, written before the DDL", i + 1)
              .containsOnlyKeys("ID", "NAME", "AMOUNT")
              .containsEntry("ID", new BigDecimal(i + 1))
              .containsEntry("NAME", "n" + (i + 1));
          assertThat(rows.get(i).schemaVersion()).isEqualTo(1);
        }
        for (int i = 50; i < 100; i++) {
          assertThat(rows.get(i).after())
              .as("row %d, written after the DDL", i + 1)
              .containsOnlyKeys("ID", "AMOUNT", "EXTRA")
              .containsEntry("EXTRA", "e" + (i + 1));
          assertThat(rows.get(i).schemaVersion()).isEqualTo(2);
        }
        assertThat(second.engine.metrics().lagReplays.get()).isPositive();
        assertThat(second.schemaChanges).extracting(s -> s.columns().size()).containsExactly(3);
        System.out.println(
            "lag-case: 100 rows decoded, "
                + second.engine.metrics().lagReplays.get()
                + " steps mined with the redo dictionary");
      }
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  private Connection miningConnection() throws Exception {
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
