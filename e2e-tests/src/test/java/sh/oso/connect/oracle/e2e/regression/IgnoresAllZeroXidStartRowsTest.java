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
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;

/**
 * Invariant: a START row whose transaction id is all zeros (LogMiner writes them for some internal
 * operations) opens no transaction, so it never holds the resume position back and never counts as
 * open work. Debezium created a phantom transaction from it that pinned the offset for ever. The
 * buffer-level case is {@code HeapTransactionBufferTest}; this one runs the whole engine.
 *
 * @see <a href="https://github.com/debezium/dbz/issues/2683">dbz#2683</a>
 */
@Tag("dbz-2683")
class IgnoresAllZeroXidStartRowsTest {

  @Test
  void zeroXidStartRowsNeitherOpenATransactionNorPinTheResumePoint() throws Exception {
    ScriptedEngine s = new ScriptedEngine();
    TxKey zero = new TxKey(3, Xid.ZERO);
    TxKey a = s.fake.tx(1, 1, 1);
    s.fake.start(zero, "SYS");
    long zeroStart = s.fake.nextScn() - 1;
    s.fake
        .start(a, "APP")
        .insert(a, ScriptedEngine.ORDERS, ScriptedEngine.insert(1, "one"))
        .commit(a)
        .start(zero, "SYS")
        .start(zero, "SYS");
    s.safeEnd = s.fake.nextScn() + 1;
    CaptureEngine e = s.engine(3);
    s.runUntilIdle(e);
    assertThat(s.committed).extracting(CommittedTransaction::key).containsExactly(a);
    assertThat(e.metrics().buffer.openTransactions()).isZero();
    assertThat(e.metrics().buffer.ignoredZeroXidStarts()).isEqualTo(3);
    assertThat(s.resume.scn())
        .as("the resume point follows the cursor, not the zero-XID START at %s", zeroStart)
        .isEqualTo(s.minedTo)
        .isGreaterThan(zeroStart);
  }
}
