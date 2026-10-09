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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.engine.LobAssembler;
import sh.oso.connect.oracle.core.errors.DictionaryUnavailableException;
import sh.oso.connect.oracle.core.jdbc.ConnectionRole;
import sh.oso.connect.oracle.core.jdbc.SessionInitializer;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.testkit.InMemorySchemaStore;
import sh.oso.connect.oracle.e2e.support.EngineDriver;
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * ADR-0027: the engine mines a PDB from inside it, as a local user, in range mode (no ADD_LOGFILE,
 * which a PDB refuses): inserts, updates, deletes and the rows after a DDL, as Amazon RDS with the
 * CDB architecture and Autonomous Database need. The local user's password is generated per run.
 */
@Tag("engine")
class PdbLocalCaptureEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  private static final String MINER = "T_PDBLOCALCAPTURE";

  @Test
  void aLocalUserInsideThePdbCapturesDmlAndTheRowsAfterADdl() throws Exception {
    run(
        (d, w, meta) -> {
          exec(w, "INSERT INTO pc VALUES (1, 'one')", "INSERT INTO pc VALUES (2, 'two')");
          exec(w, "UPDATE pc SET name = 'uno' WHERE id = 1");
          exec(w, "DELETE FROM pc WHERE id = 2");
          d.runTo(LogMinerHelper.currentScn(meta));
          exec(w, "ALTER TABLE pc ADD (note VARCHAR2(20))");
          exec(w, "INSERT INTO pc VALUES (3, 'three', 'after ddl')");
          d.runTo(LogMinerHelper.currentScn(meta));
          List<RowChange> rows = new ArrayList<>();
          d.committed.forEach(tx -> rows.addAll(tx.events()));
          assertThat(rows)
              .extracting(r -> r.op().name())
              .containsExactly("INSERT", "INSERT", "UPDATE", "DELETE", "INSERT");
          assertThat(rows.get(0).table().pdb()).isEqualTo(OracleTestDatabase.PDB1);
          assertThat(rows.get(2).after()).containsEntry("NAME", "uno");
          assertThat(rows.get(4).after()).containsEntry("NOTE", "after ddl");
        });
  }

  /**
   * Rows written before a DDL the engine has not mined yet need a dictionary from the redo, which
   * LogMiner cannot use inside a PDB: the task stops with CDC-6001 and range-mode advice, and
   * nothing is delivered from a guessed schema.
   */
  @Test
  void rowsWrittenBeforeAnUnminedDdlStopTheTaskInRangeMode() throws Exception {
    run(
        (d, w, meta) -> {
          exec(w, "INSERT INTO pc VALUES (1, 'one')");
          exec(w, "ALTER TABLE pc ADD (note VARCHAR2(20))");
          exec(w, "INSERT INTO pc VALUES (2, 'two', 'after ddl')");
          long end = LogMinerHelper.currentScn(meta);
          assertThatThrownBy(() -> d.runTo(end))
              .isInstanceOf(DictionaryUnavailableException.class)
              .hasMessageContaining("CDC-6001")
              .hasMessageContaining("range mode");
          assertThat(d.committed).as("nothing delivered from a guessed schema").isEmpty();
        });
  }

  @FunctionalInterface
  private interface Scenario {
    void run(EngineDriver d, Connection workload, Connection meta) throws Exception;
  }

  private void run(Scenario scenario) throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    String minerPassword = "M" + UUID.randomUUID().toString().replace("-", "").substring(0, 20);
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    createLocalMiner(minerPassword);
    try (Connection meta = db.connect(OracleTestDatabase.PDB1, MINER, minerPassword);
        Connection mining = db.connect(OracleTestDatabase.PDB1, MINER, minerPassword);
        Connection reselect = db.connect(OracleTestDatabase.PDB1, MINER, minerPassword);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      SessionInitializer.apply(mining, ConnectionRole.MINING);
      SessionInitializer.apply(meta, ConnectionRole.METADATA);
      exec(
          w,
          "CREATE TABLE pc (id NUMBER PRIMARY KEY, name VARCHAR2(40))",
          "ALTER TABLE pc ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      try (EngineDriver d =
          new EngineDriver(
              meta,
              mining,
              reselect,
              OracleTestDatabase.PDB1 + "\\." + schema + "\\.PC",
              LogMinerHelper.currentScn(meta),
              LobAssembler.Mode.SKIP,
              new InMemorySchemaStore(),
              EngineDriver.Options.defaults().withRangeMode(true))) {
        scenario.run(d, w, meta);
      }
    } finally {
      dropLocalMiner();
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  private void createLocalMiner(String password) throws SQLException {
    try (Connection sys = db.sysdbaInPdb(OracleTestDatabase.PDB1);
        Statement st = sys.createStatement()) {
      try {
        st.execute("DROP USER " + MINER + " CASCADE");
      } catch (SQLException ignore) {
        // first run
      }
      st.execute("CREATE USER " + MINER + " IDENTIFIED BY \"" + password + "\"");
      st.execute(
          "GRANT CREATE SESSION, LOGMINING, SELECT ANY TRANSACTION, SELECT ANY DICTIONARY,"
              + " SELECT ANY TABLE, FLASHBACK ANY TABLE, EXECUTE_CATALOG_ROLE TO "
              + MINER);
      st.execute("GRANT EXECUTE ON DBMS_LOGMNR TO " + MINER);
    }
  }

  private void dropLocalMiner() {
    try (Connection sys = db.sysdbaInPdb(OracleTestDatabase.PDB1);
        Statement st = sys.createStatement()) {
      st.execute("DROP USER " + MINER + " CASCADE");
    } catch (SQLException ignore) {
      // a session may still hold it; the next run drops it first
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
