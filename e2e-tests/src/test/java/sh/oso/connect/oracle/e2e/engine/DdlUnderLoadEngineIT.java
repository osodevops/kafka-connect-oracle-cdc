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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.engine.LobAssembler;
import sh.oso.connect.oracle.core.jdbc.ConnectionRole;
import sh.oso.connect.oracle.core.jdbc.SessionInitializer;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.e2e.support.EngineDriver;
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * PRD-03 SCH-1 against Oracle: DDL on a captured table while the engine follows the redo (no lag),
 * each followed by rows that must decode with the layout of their moment: add column with a
 * default, rename column, widen, drop column, add constraint, truncate, comment and index, rename
 * table.
 */
@Tag("engine")
class DdlUnderLoadEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void rowsAroundEveryDdlDecodeWithTheLayoutOfTheirMoment() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection meta = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection mining = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection reselect = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      SessionInitializer.apply(mining, ConnectionRole.MINING);
      SessionInitializer.apply(meta, ConnectionRole.METADATA);
      exec(
          w,
          "CREATE TABLE ddlt (id NUMBER PRIMARY KEY, name VARCHAR2(20))",
          "ALTER TABLE ddlt ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      OracleSql.archiveLogCurrent(db);
      try (EngineDriver d =
          new EngineDriver(
              meta,
              mining,
              reselect,
              "FREEPDB1\\." + schema + "\\.DDLT.*",
              LogMinerHelper.currentScn(meta),
              LobAssembler.Mode.SKIP)) {
        String[][] steps = {
          {"INSERT INTO ddlt VALUES (1, 'one')"},
          {"ALTER TABLE ddlt ADD (extra VARCHAR2(10) DEFAULT 'd')"},
          {"INSERT INTO ddlt VALUES (2, 'two', 'x')", "UPDATE ddlt SET extra = 'u' WHERE id = 1"},
          {"ALTER TABLE ddlt RENAME COLUMN name TO title"},
          {"INSERT INTO ddlt (id, title) VALUES (3, 'three')"},
          {"ALTER TABLE ddlt MODIFY (title VARCHAR2(50))"},
          {"INSERT INTO ddlt (id, title) VALUES (4, 'a title longer than twenty characters')"},
          {"ALTER TABLE ddlt DROP COLUMN extra"},
          {"INSERT INTO ddlt VALUES (5, 'five')"},
          {"ALTER TABLE ddlt ADD CONSTRAINT ddlt_u UNIQUE (title)"},
          {"TRUNCATE TABLE ddlt"},
          {"INSERT INTO ddlt VALUES (6, 'six')"},
          {"COMMENT ON TABLE ddlt IS 'it''s commented'", "CREATE INDEX ddlt_i ON ddlt (title, id)"},
          {"ALTER TABLE ddlt RENAME TO ddlt2"},
          {"INSERT INTO ddlt2 VALUES (7, 'seven')"},
        };
        for (String[] step : steps) {
          exec(w, step);
          d.runTo(LogMinerHelper.currentScn(meta));
        }
        List<RowChange> rows = new ArrayList<>();
        d.committed.forEach(tx -> rows.addAll(tx.events()));
        assertThat(rows)
            .extracting(r -> r.op().name() + " " + r.table().table())
            .containsExactly(
                "INSERT DDLT",
                "INSERT DDLT",
                "UPDATE DDLT",
                "INSERT DDLT",
                "INSERT DDLT",
                "INSERT DDLT",
                "INSERT DDLT",
                "INSERT DDLT2");
        assertThat(rows.get(0).after()).containsOnlyKeys("ID", "NAME").containsEntry("NAME", "one");
        assertThat(rows.get(1).after()).containsEntry("NAME", "two").containsEntry("EXTRA", "x");
        assertThat(rows.get(2).after()).containsEntry("EXTRA", "u").containsEntry("NAME", "one");
        assertThat(rows.get(3).after()).containsEntry("TITLE", "three").doesNotContainKey("NAME");
        assertThat(rows.get(4).after())
            .containsEntry("TITLE", "a title longer than twenty characters");
        assertThat(rows.get(5).after()).containsOnlyKeys("ID", "TITLE");
        assertThat(rows.get(6).after()).containsEntry("ID", new BigDecimal(6));
        assertThat(rows.get(7).after()).containsEntry("TITLE", "seven");
        // a version per structural change; the comment and the index add none
        assertThat(d.schemaChanges)
            .extracting(s -> s == null ? "removed" : s.columns().size() + " columns")
            .containsExactly("3 columns", "3 columns", "3 columns", "2 columns", "removed");
      }
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  private static void exec(Connection c, String... sql) throws SQLException {
    try (Statement s = c.createStatement()) {
      for (String q : sql) {
        s.execute(q);
      }
    }
  }
}
