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
import static sh.oso.connect.oracle.e2e.regression.ResumesEachThreadFromItsOwnPositionTest.names;
import static sh.oso.connect.oracle.e2e.regression.ResumesEachThreadFromItsOwnPositionTest.row;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.engine.CaptureEngine;
import sh.oso.connect.oracle.core.errors.ErrorCode;
import sh.oso.connect.oracle.core.errors.ResumeFloorViolationException;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.testkit.FakeLogMiner;
import sh.oso.connect.oracle.delivery.RecordQueueSink;

/**
 * ADR-0026 decision 4: every record the connector emits carries a position, and a restart from a
 * position re-reads each redo thread from that thread's resume address and skips that thread's
 * commits at or before its acknowledged commit. A commit handed to the sink below what an earlier
 * record's position already covers on its own thread would be lost by a restart from that record:
 * skipped as acknowledged, or never read again. Each thread's redo is returned in redo order, so
 * this cannot happen while LogMiner and the step cursor behave; if it does, the task stops with
 * CDC-7003 before the commit is emitted, instead of dropping it on the next restart. Found by
 * design review of the per-thread position (9 October 2026), before RAC is qualified.
 *
 * <p>The comparison is by redo byte address within the thread, never by SCN: a late-bound commit
 * has an SCN below records already mined and is not below any floor ({@code
 * MinesALateCommitOfASlowThreadTest}).
 */
@Tag("rac-resume-floor")
class StopsWhenACommitArrivesBelowTheAcknowledgedFloorTest {

  static final Position START =
      Position.initial(1000, ResumesEachThreadFromItsOwnPositionTest.IDENTITY);

  /**
   * Thread 2 holds O open from address 2, then commits W at address 12; the source then returns
   * transaction V of thread 2 with addresses 5 to 7: above thread 2's resume address (O's first
   * change) but below its last emitted commit, which a restart would skip.
   */
  @Test
  void aCommitBelowItsThreadsLastHandedCommitStopsTheTask() throws Exception {
    ScriptedEngine s = new ScriptedEngine();
    FakeLogMiner f = s.fake.perThreadRba();
    TxKey a = f.tx(1, 1, 1);
    TxKey o = f.tx(2, 1, 1);
    TxKey w = f.tx(2, 2, 1);
    TxKey v = f.tx(2, 3, 1);
    f.onThread(1).start(a, "APP").insert(a, ScriptedEngine.ORDERS, row(1, "a1")).commit(a);
    f.onThread(2).start(o, "APP").insert(o, ScriptedEngine.ORDERS, row(100, "o1"));
    f.rbaAt(10).start(w, "APP").insert(w, ScriptedEngine.ORDERS, row(101, "w1")).commit(w);
    f.rbaAt(5).start(v, "APP").insert(v, ScriptedEngine.ORDERS, row(102, "v1")).commit(v);
    s.safeEnd = f.nextScn();

    List<SourceRecord> queued = new ArrayList<>();
    assertThatThrownBy(() -> run(s, queued))
        .isInstanceOfSatisfying(
            ResumeFloorViolationException.class,
            ex -> assertThat(ex.code()).isEqualTo(ErrorCode.RESUME_FLOOR_VIOLATION))
        .hasMessageContaining("the last commit of thread 2");
    assertThat(names(queued)).as("nothing of V is emitted").containsExactly("a1", "w1");
  }

  /**
   * Thread 2 commits W at address 12 with nothing open, so its resume address is W's commit; the
   * source then returns transaction V of thread 2 from address 5, which a restart from W's offset
   * would never read again.
   */
  @Test
  void aRowBelowItsThreadsHandedResumeAddressStopsTheTask() throws Exception {
    ScriptedEngine s = new ScriptedEngine();
    FakeLogMiner f = s.fake.perThreadRba();
    TxKey a = f.tx(1, 1, 1);
    TxKey w = f.tx(2, 2, 1);
    TxKey v = f.tx(2, 3, 1);
    f.onThread(1).start(a, "APP").insert(a, ScriptedEngine.ORDERS, row(1, "a1")).commit(a);
    f.onThread(2).rbaAt(10);
    f.start(w, "APP").insert(w, ScriptedEngine.ORDERS, row(101, "w1")).commit(w);
    f.rbaAt(5).start(v, "APP").insert(v, ScriptedEngine.ORDERS, row(102, "v1")).commit(v);
    s.safeEnd = f.nextScn();

    List<SourceRecord> queued = new ArrayList<>();
    assertThatThrownBy(() -> run(s, queued))
        .isInstanceOfSatisfying(
            ResumeFloorViolationException.class,
            ex -> assertThat(ex.code()).isEqualTo(ErrorCode.RESUME_FLOOR_VIOLATION))
        .hasMessageContaining("TxStart")
        .hasMessageContaining("on thread 2, below");
    assertThat(names(queued)).as("nothing of V is emitted").containsExactly("a1", "w1");
  }

  /** Runs a qualified two-thread task from the start; what it queued is left in {@code queued}. */
  static void run(ScriptedEngine s, List<SourceRecord> queued) throws Exception {
    RecordQueueSink[] sink = new RecordQueueSink[1];
    CaptureEngine e =
        s.engine(
                3,
                START,
                registry -> sink[0] = ResumesEachThreadFromItsOwnPositionTest.sink(registry, START))
            .withExpectedThreads(Set.of(1, 2));
    try {
      s.runUntilIdle(e);
    } finally {
      for (SourceRecord r : sink[0].drain(Integer.MAX_VALUE, 0)) {
        if (r.topic().endsWith("ORDERS")) {
          queued.add(r);
        }
      }
    }
  }
}
