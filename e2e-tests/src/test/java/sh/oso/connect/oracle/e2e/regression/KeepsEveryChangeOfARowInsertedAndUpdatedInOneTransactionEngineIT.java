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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.engine.LobAssembler;
import sh.oso.connect.oracle.core.jdbc.ConnectionRole;
import sh.oso.connect.oracle.core.jdbc.SessionInitializer;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.e2e.support.EngineDriver;
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * Invariant: a transaction that updates an existing row, inserts a new row and then updates the new
 * row (setting long VARCHAR2 columns to NULL) delivers all three changes, in order, also when the
 * table has a LOB column and the insert is assembled with its LOB rows (ADR-0015): the assembler
 * may fold only a locator update into its insert, never a real one.
 *
 * <p>The issue lost the insert and the update of the new row on tables with extended VARCHAR2
 * columns ({@code MAX_STRING_SIZE=EXTENDED}, stored as LOBs). The Oracle Database Free test image
 * keeps the standard size, so this suite uses VARCHAR2(4000) and a CLOB; the extended variant is
 * pending for the T4 lab.
 *
 * @see <a href="https://github.com/debezium/dbz/issues/2184">dbz#2184</a>
 */
@Tag("engine")
@Tag("dbz-2184")
class KeepsEveryChangeOfARowInsertedAndUpdatedInOneTransactionEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void anInsertAndTheUpdatesAroundItAreAllDelivered() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection meta = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection mining = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      SessionInitializer.apply(meta, ConnectionRole.METADATA);
      SessionInitializer.apply(mining, ConnectionRole.MINING);
      exec(
          w,
          "CREATE TABLE wide (id NUMBER PRIMARY KEY, a VARCHAR2(4000), b VARCHAR2(4000), c CLOB)",
          "ALTER TABLE wide ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
          "INSERT INTO wide (id, a, b) VALUES (1, 'existing', 'existing')");
      OracleSql.archiveLogCurrent(db);
      long start = LogMinerHelper.currentScn(meta);
      w.setAutoCommit(false);
      // as reported: an update of an existing row, the insert, then the new row's columns nulled
      exec(
          w,
          "UPDATE wide SET a = 'touched' WHERE id = 1",
          "INSERT INTO wide VALUES (2, RPAD('x', 4000, 'x'), RPAD('y', 4000, 'y'), EMPTY_CLOB())",
          "UPDATE wide SET a = NULL, b = NULL WHERE id = 2");
      w.commit();
      // the same with a LOB value written by the insert, and an update that changes nothing
      exec(
          w,
          "INSERT INTO wide VALUES (3, RPAD('p', 4000, 'p'), 'q', TO_CLOB('small'))",
          "UPDATE wide SET b = b WHERE id = 3",
          "UPDATE wide SET b = NULL WHERE id = 3",
          "INSERT INTO wide (id, a) VALUES (4, 'four')",
          "UPDATE wide SET a = NULL WHERE id = 4");
      w.commit();
      long end = LogMinerHelper.currentScn(meta);

      try (EngineDriver d =
          new EngineDriver(
              meta,
              mining,
              null,
              "FREEPDB1\\." + schema + "\\.WIDE",
              start,
              LobAssembler.Mode.INLINE)) {
        d.runTo(end);
        assertThat(d.committed).hasSize(2);
        List<RowChange> first = d.committed.get(0).events();
        assertThat(first)
            .extracting(c -> c.op() + " " + c.after().get("ID"))
            .containsExactly("UPDATE 1", "INSERT 2", "UPDATE 2");
        assertThat(first.get(1).after().get("A")).isEqualTo("x".repeat(4000));
        assertThat(first.get(2).after()).containsEntry("A", null).containsEntry("B", null);
        List<RowChange> second = d.committed.get(1).events();
        assertThat(second)
            .as("only the update that changed nothing may fold into its insert")
            .extracting(c -> c.op() + " " + c.after().get("ID"))
            .contains("INSERT 3", "UPDATE 3", "INSERT 4", "UPDATE 4");
        Map<BigDecimal, Map<String, Object>> state = new TreeMap<>();
        state.put(BigDecimal.ONE, new HashMap<>(Map.of("ID", BigDecimal.ONE)));
        state.get(BigDecimal.ONE).put("A", "existing");
        state.get(BigDecimal.ONE).put("B", "existing");
        for (CommittedTransaction tx : d.committed) {
          for (RowChange c : tx.events()) {
            BigDecimal id =
                (BigDecimal) (c.op() == Operation.DELETE ? c.before() : c.after()).get("ID");
            state.computeIfAbsent(id, k -> new HashMap<>()).putAll(c.after());
          }
        }
        try (Statement s = w.createStatement();
            ResultSet rs = s.executeQuery("SELECT id, a, b FROM wide ORDER BY id")) {
          int rows = 0;
          while (rs.next()) {
            rows++;
            Map<String, Object> got = state.get(rs.getBigDecimal(1));
            assertThat(got).as("row %s", rs.getBigDecimal(1)).isNotNull();
            assertThat(got.get("A")).as("A of %s", rs.getBigDecimal(1)).isEqualTo(rs.getString(2));
            assertThat(got.get("B")).as("B of %s", rs.getBigDecimal(1)).isEqualTo(rs.getString(3));
          }
          assertThat(rows).isEqualTo(4);
        }
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
