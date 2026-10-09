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
package sh.oso.connect.oracle.e2e.qual;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.config.CoreConfig.CaptureMode;
import sh.oso.connect.oracle.core.engine.LobAssembler;
import sh.oso.connect.oracle.core.jdbc.ConnectionRole;
import sh.oso.connect.oracle.core.jdbc.SessionInitializer;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.testkit.InMemorySchemaStore;
import sh.oso.connect.oracle.e2e.support.EngineDriver;
import sh.oso.connect.oracle.e2e.support.Evidence;
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;
import sh.oso.connect.oracle.e2e.support.TestDatabase;

/**
 * Qualification (T4): the engine, wired as the connector wires it, captures inserts, updates and
 * deletes and the rows after a DDL on the target, in online mode and in archive-only mode (where a
 * change is mined once the log holding it is archived).
 */
@Tag("qual")
class CaptureQualIT {

  private final TestDatabase db = TestDatabase.get();

  @Test
  void onlineModeCapturesDmlAndTheRowsAfterADdl() throws Exception {
    capture("online", CaptureMode.ONLINE);
  }

  @Test
  void archiveOnlyModeCapturesOnceTheLogIsArchived() throws Exception {
    capture("archive-only", CaptureMode.ARCHIVE_ONLY);
  }

  private void capture(String caseName, CaptureMode mode) throws Exception {
    Evidence ev =
        Evidence.of(getClass(), caseName, "DML and DDL captured in " + caseName + " mode")
            .param("target", db.describe());
    String schema = SchemaFixtures.nameFor(getClass());
    try {
      db.recreateSchema(schema);
      try (Connection meta = db.capture();
          Connection mining = db.capture();
          Connection reselect = db.capture();
          Connection w = db.workload(schema)) {
        SessionInitializer.apply(mining, ConnectionRole.MINING);
        SessionInitializer.apply(meta, ConnectionRole.METADATA);
        exec(
            w,
            "CREATE TABLE q (id NUMBER PRIMARY KEY, name VARCHAR2(40))",
            "ALTER TABLE q ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        db.archiveLogCurrent();
        try (EngineDriver d =
            new EngineDriver(
                meta,
                mining,
                reselect,
                db.include(schema, "Q"),
                LogMinerHelper.currentScn(meta),
                LobAssembler.Mode.SKIP,
                new InMemorySchemaStore(),
                EngineDriver.Options.defaults().withPdbs(db.pdbs()).withCaptureMode(mode))) {
          exec(w, "INSERT INTO q VALUES (1, 'one')", "INSERT INTO q VALUES (2, 'two')");
          exec(w, "UPDATE q SET name = 'uno' WHERE id = 1");
          exec(w, "DELETE FROM q WHERE id = 2");
          exec(w, "ALTER TABLE q ADD (note VARCHAR2(20))");
          exec(w, "INSERT INTO q VALUES (3, 'three', 'after ddl')");
          long end = LogMinerHelper.currentScn(meta);
          if (mode == CaptureMode.ARCHIVE_ONLY) {
            d.runTo(end);
            assertThat(d.committed)
                .as("nothing is mined before the log holding it is archived")
                .isEmpty();
            db.archiveLogCurrent();
          }
          d.runTo(end);
          List<RowChange> rows = new ArrayList<>();
          d.committed.forEach(tx -> rows.addAll(tx.events()));
          ev.count("rows", rows.size()).count("transactions", d.committed.size());
          assertThat(rows)
              .extracting(r -> r.op().name())
              .containsExactly("INSERT", "INSERT", "UPDATE", "DELETE", "INSERT");
          assertThat(rows.get(2).after()).containsEntry("NAME", "uno");
          assertThat(rows.get(3).before()).containsEntry("NAME", "two");
          assertThat(rows.get(4).after())
              .containsEntry("NAME", "three")
              .containsEntry("NOTE", "after ddl");
        }
      }
    } catch (Throwable t) {
      ev.failed(t);
      throw t;
    } finally {
      ev.write();
      db.dropSchema(schema);
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
