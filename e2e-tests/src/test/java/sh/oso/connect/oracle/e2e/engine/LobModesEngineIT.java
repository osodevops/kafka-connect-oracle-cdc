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
import sh.oso.connect.oracle.core.engine.CaptureEngine;
import sh.oso.connect.oracle.core.engine.LobAssembler;
import sh.oso.connect.oracle.core.jdbc.ConnectionRole;
import sh.oso.connect.oracle.core.jdbc.SessionInitializer;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * CORE-DEC-6, CORE-DEC-7 and CORE-TX-2 for LOBs against Oracle (ADR-0015): the real engine mines a
 * scripted workload of LOB statements, savepoint rollbacks included, and the changes it publishes,
 * applied in order with an unavailable value keeping the previous one, must equal the table.
 */
@Tag("engine")
class LobModesEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  static final String BIG = "a".repeat(9000) + "z";
  static final String BIG2 = "b".repeat(11000) + "y";
  static final String WIDE = "it's été € ".repeat(800) + "😀 end";

  interface Step {
    void run(Connection w) throws SQLException;
  }

  @Test
  void inlineAssemblyMatchesTheTableThroughSavepointsAndStatementFailures() throws Exception {
    Map<Integer, Map<String, Object>> state =
        capture(
            LobAssembler.Mode.INLINE,
            List.of(
                w ->
                    exec(
                        w,
                        "INSERT INTO docs VALUES (1, 'one', 'small', N'né', HEXTORAW('DEADBEEF'))"),
                w -> insert(w, 2, BIG, "n".repeat(5000), bytes(9000)),
                w -> insert(w, 3, BIG, null, null),
                w -> bind(w, "UPDATE docs SET c = ? WHERE id = 2", BIG2),
                w -> bind(w, "UPDATE docs SET name = 'both', c = ? WHERE id = 3", BIG),
                w -> nclob(w, 3, "é€'".repeat(2000)),
                w -> bind(w, "UPDATE docs SET c = ? WHERE id = 1", WIDE),
                w -> bind(w, "UPDATE docs SET c = ? WHERE id IN (2, 3)", BIG2),
                w -> exec(w, "UPDATE docs SET c = 'short now' WHERE id = 2"),
                w -> exec(w, "UPDATE docs SET c = EMPTY_CLOB() WHERE id = 3"),
                w -> exec(w, "UPDATE docs SET nc = NULL WHERE id = 1"),
                // savepoint rollbacks: the rolled-back writes must not show
                w -> {
                  exec(w, "UPDATE docs SET name = 'kept' WHERE id = 2", "SAVEPOINT s");
                  bind(w, "UPDATE docs SET c = ? WHERE id = 2", BIG);
                  exec(w, "ROLLBACK TO SAVEPOINT s");
                },
                w -> {
                  exec(w, "SAVEPOINT s");
                  bind(w, "UPDATE docs SET name = 'x', c = ? WHERE id = 3", BIG2);
                  exec(w, "ROLLBACK TO SAVEPOINT s", "UPDATE docs SET name = 'y' WHERE id = 3");
                },
                w -> {
                  exec(w, "SAVEPOINT s");
                  insert(w, 4, BIG, "m".repeat(5000), bytes(9000));
                  exec(w, "ROLLBACK TO SAVEPOINT s");
                  exec(w, "INSERT INTO docs (id, name) VALUES (5, 'five')");
                },
                w -> {
                  exec(w, "SAVEPOINT s");
                  bind(w, "UPDATE docs SET c = ? WHERE id IN (2, 3)", BIG);
                  exec(w, "ROLLBACK TO SAVEPOINT s", "UPDATE docs SET name = 'z' WHERE id = 2");
                },
                w -> {
                  bind(w, "UPDATE docs SET c = ? WHERE id = 1", BIG);
                  exec(w, "SAVEPOINT s");
                  bind(w, "UPDATE docs SET c = ? WHERE id = 1", BIG2);
                  exec(w, "ROLLBACK TO SAVEPOINT s");
                },
                w -> {
                  exec(w, "UPDATE docs SET name = 'before failure' WHERE id = 5");
                  try {
                    insert(w, 2, BIG, null, null); // ORA-00001: the statement is rolled back
                  } catch (SQLException expected) {
                    assertThat(expected.getErrorCode()).isEqualTo(1);
                  }
                },
                w -> exec(w, "DELETE FROM docs WHERE id = 3")));
    assertThat(state).isEqualTo(table());
  }

  @Test
  void reselectFetchesWhatTheRedoCannotCarry() throws Exception {
    String append =
        "DECLARE l CLOB; BEGIN SELECT c INTO l FROM docs WHERE id = %d FOR UPDATE;"
            + " DBMS_LOB.WRITEAPPEND(l, 5, 'hello'); END;";
    Map<Integer, Map<String, Object>> state =
        capture(
            LobAssembler.Mode.RESELECT,
            List.of(
                w -> insert(w, 1, BIG, "n".repeat(5000), bytes(9000)),
                w -> insert(w, 2, BIG, null, null),
                w -> exec(w, String.format(append, 1)),
                w ->
                    exec(
                        w,
                        "DECLARE l CLOB; n INTEGER := 10; BEGIN SELECT c INTO l FROM docs WHERE id"
                            + " = 2 FOR UPDATE; DBMS_LOB.ERASE(l, n, 3); DBMS_LOB.TRIM(l, 7000);"
                            + " END;"),
                w -> exec(w, "UPDATE docs SET name = 'renamed' WHERE id = 1"),
                // an erase alone: its row (OPERATION_CODE 29) is the only sign of the change
                w ->
                    exec(
                        w,
                        "DECLARE l CLOB; n INTEGER := 4; BEGIN SELECT c INTO l FROM docs WHERE id"
                            + " = 1 FOR UPDATE; DBMS_LOB.ERASE(l, n, 2); END;"),
                w ->
                    exec(
                        w,
                        "DECLARE l BLOB; BEGIN SELECT b INTO l FROM docs WHERE id = 1 FOR UPDATE;"
                            + " DBMS_LOB.WRITE(l, 4, 100, HEXTORAW('CAFEBABE')); END;"),
                w -> {
                  exec(w, String.format(append, 2), "SAVEPOINT s");
                  exec(w, String.format(append, 2), "ROLLBACK TO SAVEPOINT s");
                }));
    assertThat(state).isEqualTo(table());
  }

  @Test
  void skipModePublishesEveryStatementWithoutLobValues() throws Exception {
    Map<Integer, Map<String, Object>> state =
        capture(
            LobAssembler.Mode.SKIP,
            List.of(
                w -> insert(w, 1, BIG, "n".repeat(5000), bytes(9000)),
                w -> bind(w, "UPDATE docs SET name = 'both', c = ? WHERE id = 1", BIG2),
                w -> {
                  exec(w, "SAVEPOINT s");
                  bind(w, "UPDATE docs SET name = 'gone', c = ? WHERE id = 1", BIG);
                  exec(w, "ROLLBACK TO SAVEPOINT s", "UPDATE docs SET name = 'after' WHERE id = 1");
                }));
    Map<Integer, Map<String, Object>> expected = table();
    for (Map<String, Object> row : expected.values()) {
      row.keySet().removeAll(List.of("C", "NC", "B"));
    }
    for (Map<String, Object> row : state.values()) {
      row.keySet().removeAll(List.of("C", "NC", "B"));
    }
    assertThat(state).isEqualTo(expected);
  }

  private String schema;

  /** Runs the steps, one transaction each, mines them and folds the published changes per row. */
  private Map<Integer, Map<String, Object>> capture(LobAssembler.Mode mode, List<Step> steps)
      throws Exception {
    schema = SchemaFixtures.nameFor(getClass()) + mode.name().charAt(0);
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection meta = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection mining = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection reselect = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      SessionInitializer.apply(mining, ConnectionRole.MINING);
      SessionInitializer.apply(meta, ConnectionRole.METADATA);
      try (Statement s = w.createStatement()) {
        s.execute(
            "CREATE TABLE docs (id NUMBER PRIMARY KEY, name VARCHAR2(50), c CLOB, nc NCLOB, b"
                + " BLOB)");
        s.execute("ALTER TABLE docs ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      }
      // a flashback query within the SCN-to-time granularity of a DDL raises ORA-01466, which
      // reselect reports as unavailable; real tables are not created seconds before their data
      Thread.sleep(3500);
      OracleSql.archiveLogCurrent(db);
      long startScn = LogMinerHelper.currentScn(meta);
      w.setAutoCommit(false);
      for (Step step : steps) {
        step.run(w);
        w.commit();
      }
      OracleSql.archiveLogCurrent(db);
      long end = LogMinerHelper.currentScn(meta);

      List<CommittedTransaction> committed =
          mine(meta, mining, reselect, "FREEPDB1\\." + schema + "\\.DOCS", startScn, end, mode);
      assertThat(committed).as("one transaction per step").hasSize(steps.size());
      Map<Integer, Map<String, Object>> state = new TreeMap<>();
      for (CommittedTransaction tx : committed) {
        for (RowChange c : tx.events()) {
          apply(state, c, mode);
        }
      }
      return state;
    }
  }

  /** Mines [startScn, endScn] with the real engine and returns the committed transactions. */
  static List<CommittedTransaction> mine(
      Connection meta,
      Connection mining,
      Connection reselect,
      String include,
      long startScn,
      long end,
      LobAssembler.Mode mode)
      throws Exception {
    return mine(meta, mining, reselect, include, startScn, end, mode, e -> {}, e -> {});
  }

  /** As above, with hooks to configure the engine before the run and inspect it after. */
  static List<CommittedTransaction> mine(
      Connection meta,
      Connection mining,
      Connection reselect,
      String include,
      long startScn,
      long end,
      LobAssembler.Mode mode,
      java.util.function.Consumer<CaptureEngine> configure,
      java.util.function.Consumer<CaptureEngine> inspect)
      throws Exception {
    sh.oso.connect.oracle.e2e.support.EngineDriver d =
        new sh.oso.connect.oracle.e2e.support.EngineDriver(
            meta, mining, reselect, include, startScn, mode);
    configure.accept(d.engine);
    d.runTo(end);
    inspect.accept(d.engine);
    d.close();
    return d.committed;
  }

  /** Applies a change; a LOB column missing from the after image keeps its previous value. */
  private static void apply(
      Map<Integer, Map<String, Object>> state, RowChange c, LobAssembler.Mode mode) {
    if (c.op() == Operation.DELETE) {
      state.remove(((BigDecimal) c.before().get("ID")).intValue());
      return;
    }
    int id = ((BigDecimal) c.after().get("ID")).intValue();
    if (c.op() == Operation.INSERT && mode != LobAssembler.Mode.SKIP) {
      assertThat(c.after()).as("an INSERT carries every LOB value").containsKeys("C", "NC", "B");
    }
    Map<String, Object> row = state.computeIfAbsent(id, k -> new LinkedHashMap<>());
    for (Map.Entry<String, Object> e : c.after().entrySet()) {
      row.put(e.getKey(), normal(e.getValue()));
    }
  }

  private Map<Integer, Map<String, Object>> table() throws SQLException {
    Map<Integer, Map<String, Object>> out = new TreeMap<>();
    try (Connection r = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Statement s = r.createStatement();
        ResultSet rs = s.executeQuery("SELECT id, name, c, nc, b FROM docs")) {
      while (rs.next()) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("ID", rs.getBigDecimal(1).intValue());
        row.put("NAME", rs.getString(2));
        row.put("C", rs.getString(3));
        row.put("NC", rs.getNString(4));
        row.put("B", normal(rs.getBytes(5)));
        out.put(rs.getBigDecimal(1).intValue(), row);
      }
    }
    return out;
  }

  /** Comparable values: numbers as int, bytes as a hex string, EMPTY_CLOB as the empty string. */
  private static Object normal(Object v) {
    if (v instanceof BigDecimal d) {
      return d.intValue();
    }
    if (v instanceof byte[] b) {
      return b.length == 0 ? null : java.util.HexFormat.of().formatHex(b);
    }
    if ("".equals(v)) {
      return null; // Oracle returns NULL for an empty LOB through getString
    }
    return v;
  }

  private static byte[] bytes(int n) {
    byte[] b = new byte[n];
    for (int i = 0; i < n; i++) {
      b[i] = (byte) i;
    }
    return b;
  }

  private static void insert(Connection w, int id, String c, String nc, byte[] b)
      throws SQLException {
    try (PreparedStatement ps = w.prepareStatement("INSERT INTO docs VALUES (?, ?, ?, ?, ?)")) {
      ps.setInt(1, id);
      ps.setString(2, "row" + id);
      ps.setString(3, c);
      ps.setNString(4, nc);
      ps.setBytes(5, b);
      ps.executeUpdate();
    }
  }

  private static void nclob(Connection w, int id, String value) throws SQLException {
    try (PreparedStatement ps = w.prepareStatement("UPDATE docs SET nc = ? WHERE id = ?")) {
      ps.setNString(1, value);
      ps.setInt(2, id);
      ps.executeUpdate();
    }
  }

  private static void bind(Connection w, String sql, String value) throws SQLException {
    try (PreparedStatement ps = w.prepareStatement(sql)) {
      ps.setString(1, value);
      ps.executeUpdate();
    }
  }

  private static void exec(Connection w, String... sql) throws SQLException {
    try (Statement s = w.createStatement()) {
      for (String q : sql) {
        s.execute(q);
      }
    }
  }
}
