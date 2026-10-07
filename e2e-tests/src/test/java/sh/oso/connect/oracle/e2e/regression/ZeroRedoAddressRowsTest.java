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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.engine.CaptureEngine;
import sh.oso.connect.oracle.core.errors.OracleCdcException;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;

/**
 * LogMiner returned the ROLLBACK row of a rolled-back transaction with an all-zero RS_ID on a
 * GitHub runner (7 October 2026). An all-zero RS_ID is no redo byte address: such a row sorts
 * before every real one, so the address cursor (ADR-0014) dropped it after a step boundary and, as
 * the only row of a first step, made it the cursor and re-read the log from its start.
 */
@Tag("zero-rs-id")
class ZeroRedoAddressRowsTest {

  @Test
  void aZeroAddressRollbackAfterAStepBoundaryStillEndsItsTransaction() throws Exception {
    ScriptedEngine s = new ScriptedEngine();
    TxKey rolledBack = s.fake.tx(2, 2, 2);
    TxKey a = s.fake.tx(1, 1, 1);
    s.fake
        .start(rolledBack, "APP")
        .insert(rolledBack, ScriptedEngine.ORDERS, ScriptedEngine.insert(3, "rolled back"));
    s.safeEnd = s.fake.nextScn();
    CaptureEngine e = s.engine(3);
    s.runUntilIdle(e);
    assertThat(e.metrics().buffer.openTransactions()).isEqualTo(1);

    // the next step starts from the insert's redo byte address; the ROLLBACK has none
    s.fake
        .zeroRsId()
        .rollback(rolledBack)
        .start(a, "APP")
        .insert(a, ScriptedEngine.ORDERS, ScriptedEngine.insert(1, "one"))
        .commit(a);
    s.safeEnd = s.fake.nextScn() + 1;
    s.runUntilIdle(e);

    assertThat(s.committed).extracting(CommittedTransaction::key).containsExactly(a);
    assertThat(e.metrics().buffer.openTransactions())
        .as("the rolled-back transaction is closed by its ROLLBACK, not left to the orphan check")
        .isZero();
  }

  @Test
  void aZeroAddressRowNeverBecomesTheResumePoint() throws Exception {
    ScriptedEngine s = new ScriptedEngine();
    TxKey before = s.fake.tx(1, 1, 1);
    TxKey rolledBack = s.fake.tx(2, 2, 2);
    TxKey a = s.fake.tx(3, 3, 3);
    // a transaction committed before the start position, in the same log
    s.fake
        .start(before, "APP")
        .insert(before, ScriptedEngine.ORDERS, ScriptedEngine.insert(9, "before"))
        .commit(before);
    long startScn = s.fake.nextScn();
    // the first step holds nothing but a ROLLBACK without an address
    s.fake.zeroRsId().rollback(rolledBack);
    s.safeEnd = s.fake.nextScn();
    CaptureEngine e = s.engine(3, Position.initial(startScn, new DatabaseIdentity(1, 1)));
    s.runUntilIdle(e);

    s.fake
        .start(a, "APP")
        .insert(a, ScriptedEngine.ORDERS, ScriptedEngine.insert(1, "one"))
        .commit(a);
    s.safeEnd = s.fake.nextScn() + 1;
    s.runUntilIdle(e);

    assertThat(s.committed)
        .extracting(CommittedTransaction::key)
        .as("the transaction committed before the start is not read again from the log start")
        .containsExactly(a);
  }

  @Test
  void aZeroAddressOnADataRowStopsTheTask() throws Exception {
    ScriptedEngine s = new ScriptedEngine();
    TxKey a = s.fake.tx(1, 1, 1);
    s.fake
        .start(a, "APP")
        .zeroRsId()
        .insert(a, ScriptedEngine.ORDERS, ScriptedEngine.insert(1, "one"))
        .commit(a);
    s.safeEnd = s.fake.nextScn() + 1;
    CaptureEngine e = s.engine(3);

    assertThatThrownBy(() -> s.runUntilIdle(e))
        .isInstanceOf(OracleCdcException.class)
        .hasMessageContaining("all-zero RS_ID");
    assertThat(s.committed).isEmpty();
  }
}
