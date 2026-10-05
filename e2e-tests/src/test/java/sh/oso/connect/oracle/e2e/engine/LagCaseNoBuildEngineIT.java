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
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.engine.LobAssembler;
import sh.oso.connect.oracle.core.errors.DictionaryUnavailableException;
import sh.oso.connect.oracle.core.jdbc.ConnectionRole;
import sh.oso.connect.oracle.core.jdbc.SessionInitializer;
import sh.oso.connect.oracle.core.testkit.InMemorySchemaStore;
import sh.oso.connect.oracle.e2e.support.EngineDriver;
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * PRD-03 acceptance: without a usable dictionary build the lag case stops with CDC-6001 and
 * guidance, and publishes nothing for the rows it cannot decode. Every earlier build is made
 * unusable by hiding the newest archived log, as when the logs since the last build were deleted.
 */
@Tag("engine")
class LagCaseNoBuildEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void theLagCaseWithoutAUsableBuildStopsWithCdc6001() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection meta = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      SessionInitializer.apply(meta, ConnectionRole.METADATA);
      exec(
          w,
          "CREATE TABLE nobuild (id NUMBER PRIMARY KEY, name VARCHAR2(20))",
          "ALTER TABLE nobuild ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      OracleSql.archiveLogCurrent(db);
      String newest;
      try (Statement s = meta.createStatement();
          ResultSet rs =
              s.executeQuery(
                  "SELECT name FROM v$archived_log WHERE dest_id = 1 AND name IS NOT NULL AND"
                      + " deleted = 'NO' ORDER BY sequence# DESC FETCH FIRST 1 ROW ONLY")) {
        rs.next();
        newest = rs.getString(1);
      }
      OracleSql.archiveLogCurrent(db);
      InMemorySchemaStore store = new InMemorySchemaStore();
      String include = "FREEPDB1\\." + schema + "\\.NOBUILD";
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
        exec(w, "INSERT INTO nobuild VALUES (0, 'seen')");
        first.runTo(LogMinerHelper.currentScn(meta));
        assertThat(first.committed).hasSize(1);
        resume = first.engine.cursor().scn();
      }
      exec(w, "INSERT INTO nobuild VALUES (1, 'lost?')", "ALTER TABLE nobuild DROP COLUMN name");
      OracleSql.hideArchivedLog(db, newest); // every build before it is now unreadable
      try (Connection mining = miningConnection();
          EngineDriver second =
              new EngineDriver(
                  meta, mining, null, include, resume, LobAssembler.Mode.SKIP, store)) {
        long end = LogMinerHelper.currentScn(meta);
        assertThatThrownBy(() -> second.runTo(end))
            .isInstanceOf(DictionaryUnavailableException.class)
            .hasMessageContaining("CDC-6001")
            .hasMessageContaining("DBMS_LOGMNR_D");
        assertThat(second.committed).isEmpty();
        System.out.println("lag-case without a build: stopped with CDC-6001");
      }
    } finally {
      OracleSql.restoreHiddenLogs(db);
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
