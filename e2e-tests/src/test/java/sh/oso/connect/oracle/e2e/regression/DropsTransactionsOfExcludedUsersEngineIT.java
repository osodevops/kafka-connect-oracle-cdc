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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
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
 * Invariant: with {@code cdc.users.exclude}, an excluded user's transactions never open in the
 * buffer: its START, COMMIT and ROLLBACK rows and its changes to captured tables are all dropped in
 * the mining query, so nothing of it is published and no transaction of it is left open to hold the
 * resume position back. The use case is a replication user writing to the captured tables. Debezium
 * filtered the changes but not the START, COMMIT and ROLLBACK rows; this connector had the opposite
 * gap until P1-26 (changes kept, transaction control dropped). The query shape is also checked at
 * T0 by {@code LogMinerQueryTest}.
 *
 * @see <a href="https://github.com/debezium/dbz/issues/24">dbz#24</a>
 */
@Tag("engine")
@Tag("dbz-24")
class DropsTransactionsOfExcludedUsersEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void anExcludedUsersWritesToACapturedTableOpenNothing() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    String replicator = schema.substring(0, Math.min(schema.length(), 28)) + "_R";
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, replicator);
    try (Connection meta = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection mining = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Connection r = db.connect(OracleTestDatabase.PDB1, replicator, replicator)) {
      SessionInitializer.apply(meta, ConnectionRole.METADATA);
      SessionInitializer.apply(mining, ConnectionRole.MINING);
      exec(
          w,
          "CREATE TABLE shared (id NUMBER PRIMARY KEY, who VARCHAR2(20))",
          "ALTER TABLE shared ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
          "GRANT SELECT, INSERT, UPDATE, DELETE ON shared TO " + replicator);
      OracleSql.archiveLogCurrent(db);
      long start = LogMinerHelper.currentScn(meta);
      w.setAutoCommit(false);
      r.setAutoCommit(false);
      String table = schema + ".shared";
      exec(w, "INSERT INTO " + table + " VALUES (1, 'app')");
      w.commit();
      exec(r, "INSERT INTO " + table + " VALUES (2, 'replicated')");
      r.commit();
      exec(r, "INSERT INTO " + table + " VALUES (3, 'replicated')");
      r.rollback();
      exec(r, "INSERT INTO " + table + " VALUES (4, 'replicated')");
      exec(w, "INSERT INTO " + table + " VALUES (5, 'app')");
      exec(r, "UPDATE " + table + " SET who = 'replicated' WHERE id = 1");
      w.commit();
      r.commit();
      exec(w, "INSERT INTO " + table + " VALUES (6, 'app')");
      w.commit();
      long end = LogMinerHelper.currentScn(meta);

      try (EngineDriver d =
          new EngineDriver(
                  meta,
                  mining,
                  null,
                  "FREEPDB1\\." + schema + "\\.SHARED",
                  start,
                  LobAssembler.Mode.SKIP)
              .excludeUsers(Set.of(replicator))) {
        d.runTo(end);
        List<String> rows = new ArrayList<>();
        for (CommittedTransaction tx : d.committed) {
          assertThat(tx.username()).isNotEqualTo(replicator);
          for (RowChange c : tx.events()) {
            rows.add(((BigDecimal) c.after().get("ID")).intValue() + ":" + c.after().get("WHO"));
          }
        }
        assertThat(rows).containsExactly("1:app", "5:app", "6:app");
        assertThat(d.engine.metrics().buffer.openTransactions())
            .as("no transaction of the excluded user is left open")
            .isZero();
        assertThat(d.engine.metrics().buffer.oldestOpenScn()).isLessThanOrEqualTo(0);
      }
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, replicator);
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
