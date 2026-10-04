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
package sh.oso.connect.oracle.core.mining.event;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.mining.LogMinerRow;
import sh.oso.connect.oracle.core.mining.ObjectKey;
import sh.oso.connect.oracle.core.mining.RowCursor;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;

class LogMinerRowAdapterTest {

  private static final TableId ORDERS = new TableId("FREEPDB1", "APP", "ORDERS");

  private static LogMinerRow row(
      long scn, int code, boolean rollback, long dataObj, String table, String sql, boolean csf) {
    return new LogMinerRow(
        scn,
        900,
        0,
        Instant.EPOCH,
        null,
        1,
        new Xid(7, 1, 42),
        Operation.fromCode(code).name(),
        code,
        rollback,
        0,
        null,
        table == null ? null : "APP",
        table,
        table,
        "APP",
        10,
        1,
        null,
        "AAAR",
        " 0x000001.00000010.0010 ",
        0,
        csf,
        dataObj,
        dataObj,
        1,
        3,
        "FREEPDB1",
        999,
        1,
        sql,
        null);
  }

  @Test
  void mapsEveryOperationKind() {
    LogMinerRowAdapter a = new LogMinerRowAdapter(Map.of(new ObjectKey(3, 1001L), ORDERS));
    assertThat(a.accept(row(1, 6, false, 0, null, null, false)))
        .isInstanceOfSatisfying(
            MiningEvent.TxStart.class,
            s -> assertThat(s.tx()).isEqualTo(new TxKey(3, new Xid(7, 1, 42))));
    assertThat(a.accept(row(2, 1, false, 1001, "ORDERS", "insert into ...", false)))
        .isInstanceOfSatisfying(
            MiningEvent.Dml.class,
            d -> {
              assertThat(d.op()).isEqualTo(Operation.INSERT);
              assertThat(d.table()).isEqualTo(ORDERS);
              assertThat(d.undo()).isFalse();
            });
    assertThat(a.accept(row(3, 2, true, 1001, "ORDERS", "delete ...", false)))
        .isInstanceOfSatisfying(MiningEvent.Dml.class, d -> assertThat(d.undo()).isTrue());
    assertThat(a.accept(row(4, 10, false, 1001, "ORDERS", null, false)))
        .isInstanceOfSatisfying(
            MiningEvent.Dml.class, d -> assertThat(d.op()).isEqualTo(Operation.LOB_WRITE));
    assertThat(a.accept(row(5, 5, false, 1001, "ORDERS", "alter table ...", false)))
        .isInstanceOfSatisfying(
            MiningEvent.Ddl.class,
            d -> {
              assertThat(d.pdb()).isEqualTo("FREEPDB1");
              assertThat(d.owner()).isEqualTo("APP");
            });
    assertThat(a.accept(row(6, 7, false, 0, null, null, false)))
        .isInstanceOf(MiningEvent.Commit.class);
    assertThat(a.accept(row(7, 36, false, 0, null, null, false)))
        .isInstanceOf(MiningEvent.Rollback.class);
    assertThat(a.accept(row(8, 255, false, 1001, "ORDERS", null, false)))
        .isInstanceOfSatisfying(
            MiningEvent.Unsupported.class, u -> assertThat(u.table()).isEqualTo(ORDERS));
    assertThat(a.accept(row(9, 34, false, 0, null, null, false)))
        .isInstanceOf(MiningEvent.MissingScn.class);
    assertThat(a.accept(row(10, 25, false, 1001, "ORDERS", null, false)))
        .isInstanceOfSatisfying(
            MiningEvent.Other.class,
            o -> assertThat(o.op()).isEqualTo(Operation.SELECT_FOR_UPDATE));
  }

  @Test
  void namesDroppedSegmentsThroughTheResolvedIdMap() {
    LogMinerRowAdapter a = new LogMinerRowAdapter(Map.of(new ObjectKey(3, 1001L), ORDERS));
    MiningEvent.Dml named = (MiningEvent.Dml) a.accept(row(1, 1, false, 1001, null, "x", false));
    assertThat(named.table()).isEqualTo(ORDERS);
    MiningEvent.Dml unnamed = (MiningEvent.Dml) a.accept(row(2, 1, false, 7777, null, "x", false));
    assertThat(unnamed.table()).isEqualTo(new TableId("FREEPDB1", "", "OBJ# 7777"));
  }

  @Test
  void theSameObjectIdInAnotherContainerIsNotTheCapturedTable() {
    LogMinerRowAdapter a = new LogMinerRowAdapter(Map.of(new ObjectKey(3, 1001L), ORDERS));
    // the row says container 3: resolved
    assertThat(((MiningEvent.Dml) a.accept(row(1, 1, false, 1001, "ORDERS", "x", false))).table())
        .isEqualTo(ORDERS);
    // the same id from the root (an AWR table) must not be named as the PDB table
    LogMinerRow root =
        new LogMinerRow(
            2,
            900,
            0,
            Instant.EPOCH,
            null,
            1,
            new Xid(7, 1, 42),
            "INSERT",
            1,
            false,
            0,
            null,
            "SYS",
            "WRH$_X",
            "WRH$_X",
            "SYS",
            10,
            1,
            null,
            "AAAR",
            " 0x01 ",
            0,
            false,
            1001,
            1001,
            1,
            1,
            "CDB$ROOT",
            999,
            1,
            "insert ...",
            null);
    assertThat(((MiningEvent.Dml) a.accept(root)).table())
        .isEqualTo(new TableId(null, "SYS", "WRH$_X"));
  }

  @Test
  void joinsCsfContinuationRowsIntoOneEvent() throws SQLException {
    LogMinerRowAdapter a = new LogMinerRowAdapter(Map.of());
    assertThat(a.accept(row(1, 1, false, 1, "T", "insert into T values ('aaaa", true))).isNull();
    assertThat(a.hasPending()).isTrue();
    assertThat(a.accept(row(1, 1, false, 1, "T", "bbbb", true))).isNull();
    MiningEvent e = a.accept(row(1, 1, false, 1, "T", "cccc')", false));
    assertThat(((MiningEvent.Dml) e).sqlRedo()).isEqualTo("insert into T values ('aaaabbbbcccc')");
    assertThat(a.hasPending()).isFalse();

    List<LogMinerRow> rows =
        List.of(
            row(1, 6, false, 0, null, null, false),
            row(2, 1, false, 1, "T", "part1", true),
            row(2, 1, false, 1, "T", "part2", false),
            row(3, 7, false, 0, null, null, false));
    List<MiningEvent> out = new ArrayList<>();
    try (EventCursor c = a.adapt(cursor(rows))) {
      while (c.next()) {
        out.add(c.event());
      }
    }
    assertThat(out).hasSize(3);
    assertThat(((MiningEvent.Dml) out.get(1)).sqlRedo()).isEqualTo("part1part2");
  }

  private static RowCursor cursor(List<LogMinerRow> rows) {
    return new RowCursor() {
      int i = -1;

      public boolean next() {
        return ++i < rows.size();
      }

      public LogMinerRow row() {
        return rows.get(i);
      }

      public void cancel() {}

      public void close() {}
    };
  }
}
