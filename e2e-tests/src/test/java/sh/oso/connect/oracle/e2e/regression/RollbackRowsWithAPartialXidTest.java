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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.engine.CaptureEngine;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;

/**
 * On GitHub runners (7 October 2026) LogMiner gave rows written by a rollback, the ROLLBACK row and
 * the undo rows of a rollback to a savepoint, the transaction sequence 0xFFFFFFFF instead of the
 * real one. Matched by the full XID, the undo rows reached no transaction and the transaction
 * committed with changes the database had rolled back: the log switch storm suite found 35 rows in
 * Kafka the database did not have, 7 stale values and 1 missing row.
 */
@Tag("partial-xid")
class RollbackRowsWithAPartialXidTest {

  static final long PARTIAL = 0xFFFFFFFFL;

  static TxKey partialOf(TxKey tx) {
    return new TxKey(tx.srcConId(), new Xid(tx.xid().usn(), tx.xid().slot(), PARTIAL));
  }

  @Test
  void aSavepointUndoRowWithAPartialXidRemovesTheChangeItUndoes() throws Exception {
    ScriptedEngine s = new ScriptedEngine();
    TxKey a = s.fake.tx(5, 0, 614);
    s.fake
        .start(a, "APP")
        .dmlWithRowId(
            a, Operation.INSERT, ScriptedEngine.ORDERS, ScriptedEngine.insert(1, "kept"), "R1")
        .dmlWithRowId(
            a, Operation.INSERT, ScriptedEngine.ORDERS, ScriptedEngine.insert(2, "undone"), "R2")
        .undo(partialOf(a), Operation.DELETE, ScriptedEngine.ORDERS, "delete", "R2")
        .commit(a);
    s.safeEnd = s.fake.nextScn() + 1;
    s.runUntilIdle(s.engine(3));

    assertThat(s.committed).extracting(CommittedTransaction::key).containsExactly(a);
    assertThat(s.committed.get(0).events())
        .extracting(RowChange::rowId)
        .as("the insert rolled back to the savepoint is not published")
        .containsExactly("R1");
  }

  @Test
  void aSavepointUndoRowWithAPartialXidInALaterStepRemovesTheChangeItUndoes() throws Exception {
    ScriptedEngine s = new ScriptedEngine();
    TxKey a = s.fake.tx(5, 0, 614);
    s.fake
        .start(a, "APP")
        .dmlWithRowId(
            a, Operation.INSERT, ScriptedEngine.ORDERS, ScriptedEngine.insert(1, "kept"), "R1")
        .dmlWithRowId(
            a, Operation.INSERT, ScriptedEngine.ORDERS, ScriptedEngine.insert(2, "undone"), "R2");
    s.safeEnd = s.fake.nextScn();
    CaptureEngine e = s.engine(3);
    s.runUntilIdle(e);

    s.fake.undo(partialOf(a), Operation.DELETE, ScriptedEngine.ORDERS, "delete", "R2").commit(a);
    s.safeEnd = s.fake.nextScn() + 1;
    s.runUntilIdle(e);

    assertThat(s.committed.get(0).events()).extracting(RowChange::rowId).containsExactly("R1");
  }

  @Test
  void aRollbackRowWithAPartialXidEndsItsTransaction() throws Exception {
    ScriptedEngine s = new ScriptedEngine();
    TxKey b = s.fake.tx(9, 6, 77);
    TxKey a = s.fake.tx(1, 1, 1);
    s.fake
        .start(b, "APP")
        .dmlWithRowId(
            b, Operation.INSERT, ScriptedEngine.ORDERS, ScriptedEngine.insert(3, "gone"), "R3")
        .undo(partialOf(b), Operation.DELETE, ScriptedEngine.ORDERS, "delete", "R3")
        .zeroRsId()
        .rollback(partialOf(b))
        .start(a, "APP")
        .insert(a, ScriptedEngine.ORDERS, ScriptedEngine.insert(1, "one"))
        .commit(a);
    s.safeEnd = s.fake.nextScn() + 1;
    CaptureEngine e = s.engine(3);
    s.runUntilIdle(e);

    assertThat(s.committed).extracting(CommittedTransaction::key).containsExactly(a);
    assertThat(e.metrics().buffer.openTransactions()).isZero();
  }

  @Test
  void aPartialXidRowWhoseSlotHasNoOpenTransactionIsDropped() throws Exception {
    ScriptedEngine s = new ScriptedEngine();
    TxKey a = s.fake.tx(1, 1, 1);
    TxKey other = s.fake.tx(7, 7, 7);
    s.fake
        .undo(partialOf(other), Operation.DELETE, ScriptedEngine.ORDERS, "delete", "R9")
        .rollback(partialOf(other))
        .start(a, "APP")
        .insert(a, ScriptedEngine.ORDERS, ScriptedEngine.insert(1, "one"))
        .commit(a);
    s.safeEnd = s.fake.nextScn() + 1;
    CaptureEngine e = s.engine(3);
    s.runUntilIdle(e);

    assertThat(s.committed).extracting(CommittedTransaction::key).containsExactly(a);
    assertThat(e.metrics().buffer.openTransactions()).isZero();
  }
}
