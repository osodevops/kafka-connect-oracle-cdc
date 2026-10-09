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
package sh.oso.connect.oracle.core.mining.step;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.model.RedoRecordId;

/** ADR-0026: a step cursor holds one mark per redo thread and compares within a thread only. */
class StepCursorTest {

  static RedoRecordId rba(long scn, long block) {
    return new RedoRecordId(scn, String.format(" 0x%06x.%08x.%04x ", 1, block, 0), 0);
  }

  @Test
  void aResumedCursorHasAThreadLessMarkThatAppliesToEveryThread() {
    StepCursor c = StepCursor.resume(rba(100, 5));
    assertThat(c.marks()).containsOnlyKeys(StepCursor.ANY_THREAD);
    assertThat(c.inclusive()).isTrue();
    assertThat(c.rbaThreads()).containsExactly(StepCursor.ANY_THREAD);
    assertThat(c.alreadyApplied(1, rba(99, 4))).isTrue();
    assertThat(c.alreadyApplied(1, rba(100, 5))).as("inclusive: re-read").isFalse();
    assertThat(c.alreadyApplied(2, rba(99, 4))).isTrue();
  }

  @Test
  void aMarkOnARealThreadSupersedesTheThreadLessOne() {
    Map<Integer, RedoRecordId> m = new TreeMap<>();
    m.put(StepCursor.ANY_THREAD, rba(100, 5));
    m.put(1, rba(120, 9));
    StepCursor c = StepCursor.resume(rba(100, 5)).after(130, m);
    assertThat(c.marks()).containsOnlyKeys(1);
    assertThat(c.lastApplied()).isEqualTo(rba(120, 9));
  }

  @Test
  void theSameAddressOnTwoThreadsIsTwoRecords() {
    Map<Integer, RedoRecordId> m = new TreeMap<>();
    m.put(1, rba(120, 9));
    m.put(2, rba(110, 3));
    StepCursor c = StepCursor.at(0).after(130, m);
    assertThat(c.alreadyApplied(1, rba(118, 9))).isTrue();
    assertThat(c.alreadyApplied(2, rba(125, 9)))
        .as("thread 2's block 9 comes after its own mark, whatever thread 1 reached")
        .isFalse();
    assertThat(c.alreadyApplied(2, rba(105, 3))).isTrue();
    assertThat(c.alreadyApplied(3, rba(1, 1))).as("a thread without a mark").isFalse();
    assertThat(c.rbaThreads()).containsExactly(1, 2);
  }

  @Test
  void aCursorOverTwoThreadsNamesNoSingleLastRecord() {
    StepCursor c = StepCursor.at(0).after(130, Map.of(1, rba(120, 9), 2, rba(110, 3)));
    assertThatThrownBy(c::lastApplied)
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("ADR-0026");
  }

  @Test
  void marksWithoutARedoByteAddressSelectByScnOnly() {
    StepCursor c = StepCursor.of(200, 1, new RedoRecordId(190, null, 0));
    assertThat(c.hasRba()).isFalse();
    assertThat(c.rbaThreads()).isEmpty();
    assertThat(StepCursor.of(200, 1, null)).isEqualTo(StepCursor.at(200));
  }
}
