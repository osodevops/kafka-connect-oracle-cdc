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
import static sh.oso.connect.oracle.e2e.regression.ResumesEachThreadFromItsOwnPositionTest.name;
import static sh.oso.connect.oracle.e2e.regression.ResumesEachThreadFromItsOwnPositionTest.names;
import static sh.oso.connect.oracle.e2e.regression.ResumesEachThreadFromItsOwnPositionTest.row;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.engine.CaptureEngine;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.position.PositionCodec;
import sh.oso.connect.oracle.core.testkit.FakeLogMiner;
import sh.oso.connect.oracle.delivery.RecordQueueSink;

/**
 * ADR-0026, ADR-0014: a slow RAC instance writes its redo late. A commit of its thread can reach
 * the redo after a step has mined past the commit's SCN, with an SCN below records of the same
 * thread already mined (a private strand bound late) and below the lowest resume SCN of the offsets
 * already handed to Kafka. The thread's own redo byte address places it after everything mined on
 * that thread, so it is mined by the next step and after a restart from any acknowledged record,
 * exactly once, and it is no CDC-7003: a floor compared by SCN would have stopped a healthy task.
 * Found by design review of the per-thread position (9 October 2026), before RAC is qualified.
 */
@Tag("rac-late-commit")
class MinesALateCommitOfASlowThreadTest {

  static final Position START = StopsWhenACommitArrivesBelowTheAcknowledgedFloorTest.START;

  /** Thread 2 commits W at SCN 1010; thread 1 commits A and B around it. */
  static void beforeTheStep(FakeLogMiner f) {
    f.perThreadRba();
    TxKey a = f.tx(1, 1, 1);
    TxKey b = f.tx(1, 2, 1);
    TxKey w = f.tx(2, 1, 1);
    f.onThread(1).start(a, "APP").insert(a, ScriptedEngine.ORDERS, row(1, "a1")).commit(a);
    f.onThread(2).start(w, "APP").insert(w, ScriptedEngine.ORDERS, row(101, "w1"));
    f.late(1010).commit(w); // SCNs 1000 to 1004 so far; W commits at 1010
    f.onThread(1).start(b, "APP").insert(b, ScriptedEngine.ORDERS, row(2, "b1")).commit(b);
  }

  /**
   * After a step mined to 1012, thread 2's redo grows by L, whose changes and commit carry SCNs
   * 1005 and 1006: below the step's end, below W's commit on the same thread, and below 1007, the
   * lowest resume SCN of the offsets already handed over (thread 1's, at B's commit).
   */
  static void afterTheStep(FakeLogMiner f) {
    TxKey l = f.tx(2, 2, 1);
    f.onThread(2).late(1005).start(l, "APP").late(1005);
    f.insert(l, ScriptedEngine.ORDERS, row(102, "l1")).late(1006).commit(l);
  }

  @Test
  void aSlowThreadsCommitBelowTheMinedEndIsDeliveredOnceAcrossRestarts() throws Exception {
    ScriptedEngine s = new ScriptedEngine();
    beforeTheStep(s.fake);
    s.safeEnd = 1012;
    RecordQueueSink[] sink = new RecordQueueSink[1];
    CaptureEngine e =
        s.engine(
                3,
                START,
                registry -> sink[0] = ResumesEachThreadFromItsOwnPositionTest.sink(registry, START))
            .withExpectedThreads(Set.of(1, 2));
    s.runUntilIdle(e);
    List<SourceRecord> first = orders(sink[0]);
    assertThat(names(first)).containsExactly("a1", "w1", "b1");
    Position handed = PositionCodec.read(first.get(first.size() - 1).sourceOffset());
    assertThat(handed.resumeScn()).as("the lowest resume SCN handed so far").isEqualTo(1007);

    afterTheStep(s.fake);
    s.safeEnd = 1020;
    s.runUntilIdle(e); // a CDC-7003 here would stop the task
    List<SourceRecord> second = orders(sink[0]);
    assertThat(names(second)).as("mined by the next step").containsExactly("l1");
    assertThat(s.fake.events().get(s.fake.events().size() - 1).scn())
        .as("L committed below the SCN floor of the offsets already handed to Kafka")
        .isLessThan(handed.resumeScn());

    List<String> all = new ArrayList<>(names(first));
    all.addAll(names(second));
    for (int acked = 1; acked <= first.size(); acked++) {
      SourceRecord last = first.get(acked - 1);
      List<String> delivered = new ArrayList<>(names(first.subList(0, acked)));
      delivered.addAll(
          names(
              ResumesEachThreadFromItsOwnPositionTest.run(
                  s, PositionCodec.read(last.sourceOffset()))));
      assertThat(delivered)
          .as(
              "Connect acknowledged up to %s (offset %s), then the task restarted",
              name(last), last)
          .containsExactlyInAnyOrderElementsOf(all);
    }
  }

  static List<SourceRecord> orders(RecordQueueSink sink) throws InterruptedException {
    List<SourceRecord> out = new ArrayList<>();
    for (SourceRecord r : sink.drain(Integer.MAX_VALUE, 0)) {
      if (r.topic().endsWith("ORDERS")) {
        out.add(r);
      }
    }
    return out;
  }
}
