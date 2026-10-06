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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.engine.CaptureEngine;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.position.PositionCodec;

/**
 * <a href="https://github.com/debezium/dbz/issues/2779">dbz#2779</a>, at the engine (ADR-0019): a
 * transaction already open when a connector first starts is delivered whole, every transaction that
 * ended before the start stays out (its rows are not even decoded), and a restart before the first
 * acknowledged commit applies the same rule. {@code
 * KeepsTransactionsOpenAtSnapshotStartAcrossRestartConnectorIT} proves it against Oracle.
 */
@Tag("dbz-2779")
class MinesTransactionsOpenAtTheFirstStartWholeTest {

  final ScriptedEngine s = new ScriptedEngine();
  TxKey open;
  TxKey before;
  TxKey closing;
  TxKey later;
  long openStart;
  long openScn;
  long startScn;

  /**
   * {@code open} starts first and is still open at the start; {@code before} commits before the
   * open-transaction query with a row the decoder cannot read; {@code closing} is listed by the
   * query but commits before the start SCN is read; {@code later} begins between the query and the
   * start SCN. Then the connector starts.
   */
  void script() {
    open = s.fake.tx(1, 1, 1);
    before = s.fake.tx(1, 2, 2);
    closing = s.fake.tx(1, 3, 3);
    later = s.fake.tx(1, 4, 4);
    openStart = s.fake.nextScn();
    s.fake.start(open, "APP");
    s.fake.insert(open, ScriptedEngine.ORDERS, ScriptedEngine.insert(1, "early"));
    s.fake.start(before, "APP");
    s.fake.insert(before, ScriptedEngine.ORDERS, "this is not SQL the decoder can read");
    s.fake.start(closing, "APP");
    s.fake.insert(closing, ScriptedEngine.ORDERS, ScriptedEngine.insert(301, "closing"));
    s.fake.commit(before);
    s.fake.insert(open, ScriptedEngine.ORDERS, ScriptedEngine.insert(2, "early"));
    openScn = s.fake.nextScn(); // GV$TRANSACTION is read now: open and closing are listed
    s.fake.commit(closing);
    s.fake.start(later, "APP");
    s.fake.insert(later, ScriptedEngine.ORDERS, ScriptedEngine.insert(201, "later"));
    startScn = s.fake.nextScn(); // the start SCN
    s.fake.insert(open, ScriptedEngine.ORDERS, ScriptedEngine.insert(3, "late"));
    s.fake.insert(later, ScriptedEngine.ORDERS, ScriptedEngine.insert(202, "later"));
    s.fake.commit(later);
    s.fake.commit(open);
  }

  Position firstStart() {
    return Position.firstStart(
        startScn, openScn, openStart, List.of(open, closing), new DatabaseIdentity(1, 1));
  }

  @Test
  void aTransactionOpenAtTheFirstStartIsDeliveredWhole() throws Exception {
    script();
    Position start = firstStart();
    assertThat(start.resumeScn())
        .as("mined from the open transaction's start")
        .isEqualTo(openStart);
    CaptureEngine e = s.engine(20, start);
    s.safeEnd = s.fake.nextScn() + 1;
    s.runUntilIdle(e);
    assertThat(s.committed).extracting(CommittedTransaction::key).containsExactly(later, open);
    assertThat(ids(s.committed.get(0))).containsExactly(201, 202);
    assertThat(ids(s.committed.get(1))).containsExactly(1, 2, 3);
  }

  @Test
  void theOldStartAtTheStartScnLostTheEarlyChanges() throws Exception {
    script();
    CaptureEngine e = s.engine(20, Position.initial(startScn, new DatabaseIdentity(1, 1)));
    s.safeEnd = s.fake.nextScn() + 1;
    s.runUntilIdle(e);
    assertThat(s.committed).extracting(CommittedTransaction::key).containsExactly(later, open);
    assertThat(ids(s.committed.get(1))).as("the defect this suite guards").containsExactly(3);
  }

  @Test
  void aRestartBeforeTheFirstCommitAppliesTheSameRule() throws Exception {
    script();
    // the offset a heartbeat wrote before any commit was acknowledged, read back
    Map<String, Object> stored = PositionCodec.write(firstStart());
    Position restarted = PositionCodec.read(stored);
    assertThat(restarted.startFloorScn()).isEqualTo(startScn);
    assertThat(restarted.startOpenTransactions())
        .isEqualTo(Set.of(open.toString(), closing.toString()));
    CaptureEngine e = s.engine(20, restarted);
    s.safeEnd = s.fake.nextScn() + 1;
    s.runUntilIdle(e);
    assertThat(s.committed).extracting(CommittedTransaction::key).containsExactly(later, open);
    assertThat(ids(s.committed.get(1))).containsExactly(1, 2, 3);
  }

  @Test
  void anAcknowledgedCommitEndsTheFirstStartRule() {
    script();
    Position p = firstStart();
    Position committed = p.withCommit(startScn + 5, 1, open, 1);
    assertThat(committed.startFloorScn()).isZero();
    assertThat(committed.extras()).doesNotContainKeys(Position.START_OPEN_XIDS);
  }

  private static List<Integer> ids(CommittedTransaction t) {
    List<Integer> out = new ArrayList<>();
    for (RowChange ch : t.events()) {
      out.add(((BigDecimal) ch.after().get("ID")).intValue());
    }
    return out;
  }
}
