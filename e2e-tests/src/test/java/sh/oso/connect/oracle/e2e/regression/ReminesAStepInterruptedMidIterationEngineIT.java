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
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.engine.LobAssembler;
import sh.oso.connect.oracle.core.jdbc.ConnectionRole;
import sh.oso.connect.oracle.core.jdbc.SessionInitializer;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.testkit.FaultyJdbc;
import sh.oso.connect.oracle.e2e.support.EngineDriver;
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * Invariant: when a real mining session's result set fails part way with ORA-00310, the step is
 * mined again from the same cursor and every committed row is delivered exactly once, a transaction
 * spanning the failure included. The error is injected into the mining connection (FaultyJdbc,
 * CORE-TEST-2) at a {@code ResultSet.next()} in the middle of the step, as the reported bulk load
 * met it when a log switched under the iteration.
 *
 * @see <a href="https://github.com/debezium/dbz/issues/2504">dbz#2504</a>
 */
@Tag("engine")
@Tag("dbz-2504")
class ReminesAStepInterruptedMidIterationEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void ora00310PartWayThroughAStepLosesAndRepeatsNoRow() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection meta = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection mining = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Connection open = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      SessionInitializer.apply(meta, ConnectionRole.METADATA);
      SessionInitializer.apply(mining, ConnectionRole.MINING);
      exec(
          w,
          "CREATE TABLE bulk (id NUMBER PRIMARY KEY, v VARCHAR2(30))",
          "ALTER TABLE bulk ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      OracleSql.archiveLogCurrent(db);
      long start = LogMinerHelper.currentScn(meta);
      // a long transaction around the batches, then twenty batches of ten rows
      open.setAutoCommit(false);
      exec(open, "INSERT INTO bulk VALUES (0, 'long-first')");
      w.setAutoCommit(false);
      int id = 1;
      for (int batch = 0; batch < 20; batch++) {
        try (Statement s = w.createStatement()) {
          for (int i = 0; i < 10; i++) {
            s.execute("INSERT INTO bulk VALUES (" + id++ + ", 'batch-" + batch + "')");
          }
        }
        w.commit();
      }
      exec(open, "INSERT INTO bulk VALUES (100000, 'long-last')");
      open.commit();
      long end = LogMinerHelper.currentScn(meta);

      FaultyJdbc faults = new FaultyJdbc();
      FaultyJdbc.Fault fault = faults.failNext(310, 60);
      try (EngineDriver d =
          new EngineDriver(
              meta,
              faults.wrap(mining),
              null,
              "FREEPDB1\\." + schema + "\\.BULK",
              start,
              LobAssembler.Mode.SKIP)) {
        d.runTo(end);
        assertThat(fault.fired()).as("the injected ORA-00310").isEqualTo(1);
        assertThat(d.engine.metrics().stepRetries.get()).isPositive();
        assertThat(d.committed).hasSize(21);
        Map<BigDecimal, Integer> seen = new TreeMap<>();
        for (CommittedTransaction tx : d.committed) {
          for (RowChange c : tx.events()) {
            seen.merge((BigDecimal) c.after().get("ID"), 1, Integer::sum);
          }
        }
        assertThat(seen).hasSize(202).allSatisfy((k, n) -> assertThat(n).as("row %s", k).isOne());
        assertThat(seen).containsKeys(BigDecimal.ZERO, new BigDecimal(200), new BigDecimal(100000));
        System.out.println(
            "dbz-2504: ORA-00310 injected at ResultSet.next() 60, "
                + d.engine.metrics().stepRetries.get()
                + " step retried, 202 rows delivered once");
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
