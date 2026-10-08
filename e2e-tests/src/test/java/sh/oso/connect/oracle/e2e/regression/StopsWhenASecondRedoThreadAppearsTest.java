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

import java.util.Set;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.engine.CaptureEngine;
import sh.oso.connect.oracle.core.errors.TopologyException;
import sh.oso.connect.oracle.core.model.TxKey;

/**
 * The redo byte address cursor (ADR-0014) and the commit order are per thread, but nothing told a
 * RAC database apart from a single instance (found by code review, 8 October 2026): with two
 * threads the one cursor would skip or repeat whole threads without stopping. Until the per-thread
 * position exists, a second enabled thread is refused at start (ADR-0023), and a row from a thread
 * the start did not qualify stops the task before anything of it is buffered.
 */
@Tag("rac-single-thread")
class StopsWhenASecondRedoThreadAppearsTest {

  @Test
  void aRowFromAnUnqualifiedThreadStopsTheTaskBeforeItIsBuffered() throws Exception {
    ScriptedEngine s = new ScriptedEngine();
    TxKey a = s.fake.tx(1, 1, 1);
    TxKey b = s.fake.tx(5, 1, 1);
    s.fake
        .start(a, "APP")
        .insert(a, ScriptedEngine.ORDERS, ScriptedEngine.insert(1, "one"))
        .commit(a);
    // the second instance's thread comes up after the start qualified thread 1 alone
    s.fake
        .onThread(2)
        .start(b, "APP")
        .insert(b, ScriptedEngine.ORDERS, ScriptedEngine.insert(2, "two"))
        .commit(b);
    s.safeEnd = s.fake.nextScn();
    CaptureEngine e = s.engine(3).withExpectedThreads(Set.of(1));

    assertThatThrownBy(() -> s.runUntilIdle(e))
        .isInstanceOf(TopologyException.class)
        .hasMessageContaining("CDC-5001")
        .hasMessageContaining("thread 2");
    assertThat(s.committed)
        .as(
            "the thread-1 transaction mined before the foreign row is delivered; nothing of thread"
                + " 2")
        .extracting(CommittedTransaction::key)
        .containsExactly(a);
    assertThat(s.minedTo)
        .as(
            "the step that carried the foreign row was never acknowledged, so a restart re-mines"
                + " it")
        .isZero();
  }

  @Test
  void rowsOfTheQualifiedThreadAreDeliveredAsBefore() throws Exception {
    ScriptedEngine s = new ScriptedEngine();
    TxKey a = s.fake.tx(1, 1, 1);
    s.fake
        .start(a, "APP")
        .insert(a, ScriptedEngine.ORDERS, ScriptedEngine.insert(1, "one"))
        .commit(a);
    s.safeEnd = s.fake.nextScn();
    CaptureEngine e = s.engine(3).withExpectedThreads(Set.of(1));
    s.runUntilIdle(e);
    assertThat(s.committed).extracting(CommittedTransaction::key).containsExactly(a);
  }
}
