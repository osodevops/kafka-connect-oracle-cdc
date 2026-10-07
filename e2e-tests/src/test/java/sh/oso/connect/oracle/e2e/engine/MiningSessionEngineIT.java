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
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.config.CoreConfig.CaptureMode;
import sh.oso.connect.oracle.core.errors.ErrorCode;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.logs.LogInventory;
import sh.oso.connect.oracle.core.mining.DictionaryMode;
import sh.oso.connect.oracle.core.mining.JdbcLogMinerSession;
import sh.oso.connect.oracle.core.mining.JdbcObjectCatalog;
import sh.oso.connect.oracle.core.mining.MiningFilter;
import sh.oso.connect.oracle.core.mining.ObjectIdResolver;
import sh.oso.connect.oracle.core.mining.ResolvedObjects;
import sh.oso.connect.oracle.core.mining.event.EventCursor;
import sh.oso.connect.oracle.core.mining.event.LogMinerEventSource;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.topology.JdbcCatalogSource;
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * P1-01: a scripted workload mined in SCN order through the real session, with the object-id
 * pushdown verified in V$SQL (PRD-00 CORE-MINE-2 "verified by query plan logging"), excluded users
 * dropped server-side, DDL seen, ORA-01291 typed as a step retry.
 */
@Tag("engine")
class MiningSessionEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void minesOnlyCapturedObjectsInRedoOrderWithServerSidePushdown() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    String excluded = schema + "X";
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, excluded);
    try (Connection mining = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection meta = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Connection x = db.connect(OracleTestDatabase.PDB1, excluded, excluded)) {
      w.setAutoCommit(false);
      x.setAutoCommit(false);
      try (Statement s = w.createStatement();
          Statement sx = x.createStatement()) {
        s.execute("CREATE TABLE captured (id NUMBER PRIMARY KEY, name VARCHAR2(50))");
        s.execute("ALTER TABLE captured ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        s.execute("CREATE TABLE noise (id NUMBER PRIMARY KEY, name VARCHAR2(50))");
        s.execute("ALTER TABLE noise ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        sx.execute("CREATE TABLE theirs (id NUMBER PRIMARY KEY)");
      }
      w.commit();
      x.commit();
      OracleSql.archiveLogCurrent(db);
      long startScn = LogMinerHelper.currentScn(meta);

      try (Statement s = w.createStatement();
          Statement sx = x.createStatement()) {
        s.execute("INSERT INTO captured VALUES (1, 'one')");
        s.execute("INSERT INTO noise VALUES (1, 'noise')");
        s.execute("INSERT INTO captured VALUES (2, 'two')");
        s.execute("UPDATE captured SET name = 'two!' WHERE id = 2");
        w.commit();
        s.execute("INSERT INTO captured VALUES (3, 'rolled back')");
        w.rollback();
        sx.execute("INSERT INTO theirs VALUES (1)");
        x.commit();
        s.execute("ALTER TABLE captured ADD (c2 NUMBER)");
        s.execute("DELETE FROM captured WHERE id = 1");
        w.commit();
      }
      OracleSql.archiveLogCurrent(db);
      long endScn = LogMinerHelper.currentScn(meta);

      JdbcCatalogSource catalog = new JdbcCatalogSource(meta);
      LogInventory inventory = new LogInventory(catalog, CaptureMode.ONLINE, 1);
      ResolvedObjects objects =
          new ObjectIdResolver(
                  new JdbcObjectCatalog(meta),
                  List.of("FREEPDB1\\." + schema + "\\.CAPTURED"),
                  List.of(),
                  List.of("FREEPDB1"),
                  false)
              .resolve();
      assertThat(objects.tables()).containsExactly(new TableId("FREEPDB1", schema, "CAPTURED"));
      assertThat(objects.owners()).containsExactly(schema);
      MiningFilter filter = objects.filter(Set.of(excluded), 1000);

      JdbcLogMinerSession session = new JdbcLogMinerSession(mining, 1000, Duration.ofMinutes(5));
      List<MiningEvent> events = new ArrayList<>();
      try (LogMinerEventSource source =
          new LogMinerEventSource(
              inventory, session, objects, filter, DictionaryMode.ONLINE_CATALOG)) {
        // two adjacent steps: the second START replaces the range without END (CORE-MINE-7)
        long mid = (startScn + endScn) / 2;
        for (long[] step : new long[][] {{startScn, mid}, {mid, endScn}}) {
          try (EventCursor c =
              source.open(sh.oso.connect.oracle.core.mining.step.StepCursor.at(step[0]), step[1])) {
            while (c.next()) {
              events.add(c.event());
            }
          }
        }
        assertThat(session.pgaUsedBytes()).isPositive();

        // ORA-01291: a range no added log covers is a step retry, never silent
        assertThatThrownBy(() -> session.start(1, 2, DictionaryMode.ONLINE_CATALOG))
            .isInstanceOfSatisfying(
                SQLException.class,
                e -> {
                  assertThat(e.getErrorCode()).isEqualTo(1291);
                  assertThat(new OraErrorClassifier().classify(e))
                      .isEqualTo(ErrorCode.MINING_STEP_RETRY);
                });
      }

      // redo order; a failure names both events, so a row LogMiner returns with an unusual
      // address (an all-zero RS_ID was seen once on a CI runner) is identified
      for (int i = 1; i < events.size(); i++) {
        assertThat(events.get(i).id())
            .as("event %d %s after %s", i, events.get(i), events.get(i - 1))
            .isGreaterThanOrEqualTo(events.get(i - 1).id());
      }
      List<MiningEvent.Dml> dml =
          events.stream()
              .filter(e -> e instanceof MiningEvent.Dml)
              .map(e -> (MiningEvent.Dml) e)
              .toList();
      assertThat(dml).isNotEmpty();
      assertThat(dml).extracting(d -> d.table().table()).containsOnly("CAPTURED");
      // the rolled-back insert is undone by a ROLLBACK=1 delete before the ROLLBACK row
      assertThat(dml)
          .extracting(MiningEvent.Dml::op, MiningEvent.Dml::undo)
          .containsExactly(
              org.assertj.core.groups.Tuple.tuple(Operation.INSERT, false),
              org.assertj.core.groups.Tuple.tuple(Operation.INSERT, false),
              org.assertj.core.groups.Tuple.tuple(Operation.UPDATE, false),
              org.assertj.core.groups.Tuple.tuple(Operation.INSERT, false),
              org.assertj.core.groups.Tuple.tuple(Operation.DELETE, true),
              org.assertj.core.groups.Tuple.tuple(Operation.DELETE, false));
      Set<TxKey> committed = new HashSet<>();
      Set<TxKey> rolledBack = new HashSet<>();
      Set<String> startUsers = new HashSet<>();
      for (MiningEvent e : events) {
        if (e instanceof MiningEvent.Commit c) {
          committed.add(c.tx());
        } else if (e instanceof MiningEvent.Rollback r) {
          rolledBack.add(r.tx());
        } else if (e instanceof MiningEvent.TxStart s) {
          startUsers.add(s.username());
        }
      }
      assertThat(committed).contains(dml.get(0).tx(), dml.get(5).tx());
      assertThat(rolledBack).contains(dml.get(3).tx());
      assertThat(dml.get(4).tx()).isEqualTo(dml.get(3).tx());
      assertThat(startUsers).doesNotContain(excluded);
      assertThat(events)
          .filteredOn(e -> e instanceof MiningEvent.Ddl)
          .map(e -> (MiningEvent.Ddl) e)
          .anySatisfy(
              d -> {
                assertThat(d.owner()).isEqualTo(schema);
                assertThat(d.objectName()).isEqualTo("CAPTURED");
                assertThat(d.sql()).containsIgnoringCase("alter table");
                assertThat(d.pdb()).isEqualTo("FREEPDB1");
              });
      assertThat(events).noneMatch(e -> e instanceof MiningEvent.MissingScn);

      // the object-id filter reached the server (CORE-MINE-2)
      try (Statement s = meta.createStatement();
          ResultSet rs =
              s.executeQuery(
                  "SELECT COUNT(*) FROM v$sql WHERE sql_text LIKE 'SELECT SCN, START_SCN%' AND"
                      + " DBMS_LOB.INSTR(sql_fulltext, 'DATA_OBJ# IN (') > 0 AND"
                      + " DBMS_LOB.INSTR(sql_fulltext, 'V$LOGMNR_CONTENTS') > 0")) {
        assertThat(rs.next()).isTrue();
        assertThat(rs.getInt(1)).isPositive();
      }
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, excluded);
    }
  }
}
