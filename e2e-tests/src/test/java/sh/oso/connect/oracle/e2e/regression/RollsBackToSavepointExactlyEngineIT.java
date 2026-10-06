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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
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
 * Invariant: a rollback to a savepoint removes exactly the changes made after the savepoint, for
 * inserts, updates, deletes, key changes and LOB writes, however the redo is cut into mining steps:
 * mined in one step or in many small ones (each its own LogMiner window, so undo rows arrive in a
 * later window than the changes they reverse), the published changes applied in order equal the
 * table.
 *
 * <p>Debezium 3.7 fixed four savepoint bugs: a rollback applied again in a later mining session
 * that dropped the original insert (dbz#1914), and three with LOB columns, where a rolled-back LOB
 * update was merged into an earlier insert (dbz#1422, dbz#1917) or never undone because its undo
 * row had another ROWID (dbz#1735). The buffer-level LOB cases are {@code LobUndoBufferTest};
 * {@code LobModesEngineIT} covers more LOB shapes.
 *
 * @see <a href="https://github.com/debezium/dbz/issues/1914">dbz#1914</a>
 * @see <a href="https://github.com/debezium/dbz/issues/1422">dbz#1422</a>
 * @see <a href="https://github.com/debezium/dbz/issues/1735">dbz#1735</a>
 * @see <a href="https://github.com/debezium/dbz/issues/1917">dbz#1917</a>
 */
@Tag("engine")
@Tag("dbz-1914")
class RollsBackToSavepointExactlyEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void partialRollbacksOfInsertsUpdatesDeletesAndKeyChangesInAnyStepCut() throws Exception {
    String[][] transactions = {
      {
        "INSERT INTO sp VALUES (1, 'one', 1)",
        "SAVEPOINT s",
        "INSERT INTO sp VALUES (2, 'gone', 2)",
        "ROLLBACK TO SAVEPOINT s"
      },
      {
        "UPDATE sp SET v = 'u1' WHERE id = 1",
        "SAVEPOINT s",
        "UPDATE sp SET v = 'gone' WHERE id = 1",
        "ROLLBACK TO SAVEPOINT s",
        "UPDATE sp SET n = 5 WHERE id = 1"
      },
      {
        "SAVEPOINT s",
        "DELETE FROM sp WHERE id = 1",
        "ROLLBACK TO SAVEPOINT s",
        "INSERT INTO sp VALUES (3, 'three', 3)"
      },
      {
        "INSERT INTO sp VALUES (4, 'four', 4)",
        "SAVEPOINT a",
        "UPDATE sp SET v = 'x' WHERE id = 4",
        "SAVEPOINT b",
        "DELETE FROM sp WHERE id = 4",
        "ROLLBACK TO SAVEPOINT b",
        "UPDATE sp SET n = 44 WHERE id = 4",
        "ROLLBACK TO SAVEPOINT a"
      },
      {
        "INSERT INTO sp VALUES (5, 'five', 5)",
        "SAVEPOINT s",
        "UPDATE sp SET id = 6 WHERE id = 5",
        "ROLLBACK TO SAVEPOINT s",
        "UPDATE sp SET v = 'five!' WHERE id = 5"
      },
      {
        "SAVEPOINT s",
        "UPDATE sp SET id = 7 WHERE id = 3",
        "UPDATE sp SET v = 'moved' WHERE id = 7",
        "ROLLBACK TO SAVEPOINT s",
        "UPDATE sp SET id = 8 WHERE id = 3"
      },
      {
        "SAVEPOINT s",
        "INSERT INTO sp VALUES (9, 'nine', 9)",
        "DELETE FROM sp WHERE id = 9",
        "INSERT INTO sp VALUES (9, 'nine again', 9)",
        "ROLLBACK TO SAVEPOINT s",
        "INSERT INTO sp VALUES (10, 'ten', 10)"
      },
    };
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection meta = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      SessionInitializer.apply(meta, ConnectionRole.METADATA);
      exec(
          w,
          "CREATE TABLE sp (id NUMBER PRIMARY KEY, v VARCHAR2(20), n NUMBER)",
          "ALTER TABLE sp ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      OracleSql.archiveLogCurrent(db);
      long start = LogMinerHelper.currentScn(meta);
      w.setAutoCommit(false);
      for (String[] tx : transactions) {
        exec(w, tx);
        w.commit();
      }
      long end = LogMinerHelper.currentScn(meta);
      Map<BigDecimal, Map<String, Object>> table = table(w, "SELECT id, v, n FROM sp");
      String include = "FREEPDB1\\." + schema + "\\.SP";

      // one step over the whole window
      Map<BigDecimal, Map<String, Object>> whole = new TreeMap<>();
      try (Connection mining = mining();
          EngineDriver d =
              new EngineDriver(meta, mining, null, include, start, LobAssembler.Mode.SKIP)) {
        d.runTo(end);
        assertThat(d.committed).hasSize(transactions.length);
        fold(d.committed, whole);
      }
      assertSameRows("mined in one step", whole, table);

      // many small steps, each its own LogMiner window
      Map<BigDecimal, Map<String, Object>> cut = new TreeMap<>();
      try (Connection mining = mining();
          EngineDriver d =
              new EngineDriver(meta, mining, null, include, start, LobAssembler.Mode.SKIP)) {
        long stride = Math.max(1, (end - start) / 40);
        for (long to = start + stride; to < end; to += stride) {
          d.safeEndCap = to;
          d.runTo(to);
        }
        d.safeEndCap = Long.MAX_VALUE;
        d.runTo(end);
        assertThat(d.engine.metrics().steps.get()).isGreaterThan(transactions.length);
        fold(d.committed, cut);
      }
      assertSameRows("mined in many steps", cut, table);
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  @Test
  @Tag("dbz-1422")
  @Tag("dbz-1735")
  @Tag("dbz-1917")
  void partialRollbacksOfLobWritesKeepTheChangesBeforeTheSavepoint() throws Exception {
    String big = "a".repeat(9000) + "z";
    String other = "b".repeat(11000) + "y";
    // LogMiner reads no object whose owner name is longer than 30 characters (DOC-6)
    String base = SchemaFixtures.nameFor(getClass());
    String schema = base.substring(0, Math.min(base.length(), 29)) + "L";
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection meta = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      SessionInitializer.apply(meta, ConnectionRole.METADATA);
      exec(
          w,
          "CREATE TABLE docs (id NUMBER PRIMARY KEY, name VARCHAR2(20), c CLOB)",
          "ALTER TABLE docs ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
          "INSERT INTO docs VALUES (2, 'two', TO_CLOB('existing'))");
      OracleSql.archiveLogCurrent(db);
      long start = LogMinerHelper.currentScn(meta);
      w.setAutoCommit(false);
      // dbz#1422, dbz#1917: an insert, then a LOB update of the same row rolled back
      bind(w, "INSERT INTO docs VALUES (1, 'one', ?)", big);
      exec(w, "SAVEPOINT s");
      bind(w, "UPDATE docs SET c = ? WHERE id = 1", other);
      exec(w, "ROLLBACK TO SAVEPOINT s");
      w.commit();
      // dbz#1735: a LOB update of an existing row rolled back, an earlier update of it kept
      exec(w, "UPDATE docs SET name = 'kept' WHERE id = 2", "SAVEPOINT s");
      bind(w, "UPDATE docs SET c = ? WHERE id = 2", big);
      exec(w, "ROLLBACK TO SAVEPOINT s");
      w.commit();
      // an insert with an out-of-row LOB rolled back, a later insert kept
      exec(w, "SAVEPOINT s");
      bind(w, "INSERT INTO docs VALUES (3, 'three', ?)", other);
      exec(w, "ROLLBACK TO SAVEPOINT s", "INSERT INTO docs (id, name) VALUES (4, 'four')");
      w.commit();
      long end = LogMinerHelper.currentScn(meta);
      Map<BigDecimal, Map<String, Object>> table = table(w, "SELECT id, name, c FROM docs");
      Map<BigDecimal, Map<String, Object>> state = new TreeMap<>();
      state.put(new BigDecimal(2), new LinkedHashMap<>(table.get(new BigDecimal(2))));
      state.get(new BigDecimal(2)).put("NAME", "two");
      try (Connection mining = mining();
          EngineDriver d =
              new EngineDriver(
                  meta,
                  mining,
                  null,
                  "FREEPDB1\\." + schema + "\\.DOCS",
                  start,
                  LobAssembler.Mode.INLINE)) {
        d.runTo(end);
        assertThat(d.committed).hasSize(3);
        fold(d.committed, state);
      }
      assertSameRows("with LOB columns", state, table);
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  /**
   * The same keys, and per row the table's values; a column the changes never carried counts as
   * equal only when the table holds NULL there (an insert that left it out).
   */
  static void assertSameRows(
      String label,
      Map<BigDecimal, Map<String, Object>> state,
      Map<BigDecimal, Map<String, Object>> table) {
    assertThat(state.keySet()).as(label + ": keys").isEqualTo(table.keySet());
    for (Map.Entry<BigDecimal, Map<String, Object>> row : table.entrySet()) {
      Map<String, Object> got = state.get(row.getKey());
      for (Map.Entry<String, Object> col : row.getValue().entrySet()) {
        Object want = col.getValue();
        if (want == null && !got.containsKey(col.getKey())) {
          continue;
        }
        Object have = got.get(col.getKey());
        if (want instanceof BigDecimal x && have instanceof BigDecimal y) {
          assertThat(y.compareTo(x)).as("%s: %s of %s", label, col.getKey(), row.getKey()).isZero();
        } else {
          assertThat(have).as("%s: %s of %s", label, col.getKey(), row.getKey()).isEqualTo(want);
        }
      }
    }
  }

  private Connection mining() throws SQLException {
    Connection c = db.capture(OracleTestDatabase.CDB_SERVICE);
    SessionInitializer.apply(c, ConnectionRole.MINING);
    return c;
  }

  /**
   * Applies changes in order, by key; a LOB column missing from an after image keeps its previous
   * value (unavailable, ADR-0015).
   */
  static void fold(
      List<CommittedTransaction> committed, Map<BigDecimal, Map<String, Object>> rows) {
    for (CommittedTransaction tx : committed) {
      for (RowChange c : tx.events()) {
        if (c.op() == Operation.DELETE) {
          rows.remove((BigDecimal) c.before().get("ID"));
          continue;
        }
        if (c.before() != null && c.before().get("ID") != null) {
          BigDecimal old = (BigDecimal) c.before().get("ID");
          if (!old.equals(c.after().get("ID"))) {
            Map<String, Object> moved = rows.remove(old);
            rows.put(
                (BigDecimal) c.after().get("ID"), moved == null ? new LinkedHashMap<>() : moved);
          }
        }
        rows.computeIfAbsent((BigDecimal) c.after().get("ID"), k -> new LinkedHashMap<>())
            .putAll(c.after());
      }
    }
  }

  static Map<BigDecimal, Map<String, Object>> table(Connection w, String sql) throws SQLException {
    Map<BigDecimal, Map<String, Object>> out = new TreeMap<>();
    try (Statement s = w.createStatement();
        ResultSet rs = s.executeQuery(sql)) {
      int n = rs.getMetaData().getColumnCount();
      while (rs.next()) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 1; i <= n; i++) {
          Object v = rs.getObject(i);
          if (v instanceof java.sql.Clob clob) {
            v = clob.getSubString(1, (int) clob.length());
          }
          row.put(rs.getMetaData().getColumnName(i), v);
        }
        out.put(rs.getBigDecimal(1), row);
      }
    }
    return out;
  }

  private static void bind(Connection w, String sql, String value) throws SQLException {
    try (PreparedStatement ps = w.prepareStatement(sql)) {
      ps.setString(1, value);
      ps.executeUpdate();
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
