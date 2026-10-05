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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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
 * Invariant: DDL on one captured table while other sessions keep committing changes to another, and
 * a transaction stays open across every DDL, loses no row and decodes every row with the layout of
 * its moment. Each DDL ends a mining step and re-resolves the object ids (ADR-0001), so this is the
 * scenario of {@code DdlUnderLoadEngineIT} with concurrent DML and an open transaction added.
 *
 * <p>No Debezium issue number: the testing strategy first attributed this scenario to dbz#2184,
 * whose report (missing events with extended VARCHAR2 columns) involves no DDL; that issue has its
 * own suite, {@code KeepsEveryChangeOfARowInsertedAndUpdatedInOneTransactionEngineIT}.
 */
@Tag("engine")
@Tag("ddl-under-dml")
class DeliversEveryRowAroundDdlUnderConcurrentDmlEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void noRowIsLostWhileDdlRunsBetweenConcurrentTransactions() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    AtomicBoolean stop = new AtomicBoolean();
    AtomicReference<Throwable> writerFailure = new AtomicReference<>();
    ConcurrentLinkedQueue<Integer> written = new ConcurrentLinkedQueue<>();
    Thread writer = null;
    try (Connection meta = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection mining = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Connection open = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      SessionInitializer.apply(meta, ConnectionRole.METADATA);
      SessionInitializer.apply(mining, ConnectionRole.MINING);
      exec(
          w,
          "CREATE TABLE ddlt (id NUMBER PRIMARY KEY, name VARCHAR2(20))",
          "ALTER TABLE ddlt ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
          "CREATE TABLE load (id NUMBER PRIMARY KEY, v VARCHAR2(20))",
          "ALTER TABLE load ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      OracleSql.archiveLogCurrent(db);
      try (EngineDriver d =
          new EngineDriver(
              meta,
              mining,
              null,
              "FREEPDB1\\." + schema + "\\.(DDLT.*|LOAD)",
              LogMinerHelper.currentScn(meta),
              LobAssembler.Mode.SKIP)) {
        open.setAutoCommit(false);
        exec(open, "INSERT INTO load VALUES (1000000, 'open-first')");
        writer =
            new Thread(
                () -> {
                  // one connection for the whole loop
                  try (Connection c = db.connect(OracleTestDatabase.PDB1, schema, schema);
                      Statement s = c.createStatement()) {
                    c.setAutoCommit(false);
                    for (int i = 1; !stop.get() && i < 100000; i++) {
                      s.execute("INSERT INTO load VALUES (" + i + ", 'w" + i + "')");
                      c.commit();
                      written.add(i);
                      Thread.sleep(5);
                    }
                  } catch (Throwable t) {
                    writerFailure.set(t);
                  }
                },
                "dml-writer");
        writer.start();
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
          {"COMMENT ON TABLE ddlt IS 'commented'", "CREATE INDEX ddlt_i ON ddlt (title, id)"},
          {"ALTER TABLE ddlt RENAME TO ddlt2"},
          {"INSERT INTO ddlt2 VALUES (7, 'seven')"},
        };
        for (String[] step : steps) {
          exec(w, step);
          d.runTo(LogMinerHelper.currentScn(meta));
        }
        stop.set(true);
        writer.join(60_000);
        assertThat(writerFailure.get()).as("the concurrent writer").isNull();
        exec(open, "INSERT INTO load VALUES (1000001, 'open-last')");
        open.commit();
        d.runTo(LogMinerHelper.currentScn(meta));

        List<RowChange> ddlRows = new ArrayList<>();
        Map<BigDecimal, Integer> loadRows = new TreeMap<>();
        CommittedTransaction openTx = null;
        for (CommittedTransaction tx : d.committed) {
          for (RowChange c : tx.events()) {
            if (c.table().table().equals("LOAD")) {
              loadRows.merge((BigDecimal) c.after().get("ID"), 1, Integer::sum);
              if (c.after().get("ID").equals(new BigDecimal(1000000))) {
                openTx = tx;
              }
            } else {
              ddlRows.add(c);
            }
          }
        }
        assertThat(ddlRows)
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
        assertThat(ddlRows.get(1).after()).containsEntry("EXTRA", "x");
        assertThat(ddlRows.get(3).after()).containsEntry("TITLE", "three");
        assertThat(ddlRows.get(5).after()).containsOnlyKeys("ID", "TITLE");
        assertThat(ddlRows.get(7).after()).containsEntry("TITLE", "seven");
        Set<BigDecimal> expected = new TreeSet<>();
        written.forEach(i -> expected.add(new BigDecimal(i)));
        expected.add(new BigDecimal(1000000));
        expected.add(new BigDecimal(1000001));
        assertThat(loadRows.keySet()).as("every committed row of LOAD").isEqualTo(expected);
        assertThat(loadRows).allSatisfy((k, n) -> assertThat(n).as("row %s", k).isOne());
        assertThat(openTx).as("the transaction open across every DDL").isNotNull();
        assertThat(openTx.events())
            .extracting(c -> c.after().get("V"))
            .containsExactly("open-first", "open-last");
        System.out.println(
            "ddl-under-dml: "
                + steps.length
                + " steps, "
                + written.size()
                + " concurrent transactions, open transaction delivered whole");
      }
    } finally {
      stop.set(true);
      if (writer != null) {
        writer.join(60_000);
      }
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
