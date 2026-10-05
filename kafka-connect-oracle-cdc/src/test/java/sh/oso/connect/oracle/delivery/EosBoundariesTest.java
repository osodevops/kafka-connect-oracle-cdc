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
package sh.oso.connect.oracle.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.TransactionContext;
import org.junit.jupiter.api.Test;

class EosBoundariesTest {

  int requests;
  final TransactionContext ctx =
      new TransactionContext() {
        public void commitTransaction() {
          requests++;
        }

        public void commitTransaction(SourceRecord r) {
          throw new AssertionError("commits are requested per batch");
        }

        public void abortTransaction() {}

        public void abortTransaction(SourceRecord r) {}
      };

  static RecordQueueSink.Queued q(String name, boolean boundary, boolean force, long bytes) {
    return new RecordQueueSink.Queued(
        new SourceRecord(null, null, name, null, null), boundary, force, bytes);
  }

  static List<String> names(List<SourceRecord> rs) {
    return rs.stream().map(SourceRecord::topic).toList();
  }

  @Test
  void aBatchStopsAtTheBoundaryThatEndsTheTransactionAndTheRestIsHeld() {
    EosBoundaries b = new EosBoundaries(3, 1000, 10_000);
    List<SourceRecord> out =
        b.next(
            List.of(
                q("a1", false, false, 10),
                q("a2", false, false, 10),
                q("a3", true, false, 10),
                q("b1", false, false, 10),
                q("b2", true, false, 10)),
            ctx,
            0,
            false);
    assertThat(names(out)).containsExactly("a1", "a2", "a3");
    assertThat(requests).isEqualTo(1);
    assertThat(b.holding()).isTrue();
    // the held records come first; the queue is empty and the batch ends at a boundary
    out = b.next(List.of(q("c1", true, false, 10)), ctx, 1, true);
    assertThat(names(out)).containsExactly("b1", "b2", "c1");
    assertThat(requests).isEqualTo(2);
    assertThat(b.holding()).isFalse();
    // a batch ending inside a transaction never commits, even with an empty queue
    out = b.next(List.of(q("d1", false, false, 10)), ctx, 2, true);
    assertThat(names(out)).containsExactly("d1");
    assertThat(requests).isEqualTo(2);
    b.next(List.of(q("d2", true, false, 10)), ctx, 3, true);
    assertThat(requests).isEqualTo(3);
    assertThat(b.commits()).isEqualTo(3);
  }

  @Test
  void bytesTimeAndForcedSplitsEndATransaction() {
    EosBoundaries bytes = new EosBoundaries(1000, 25, 10_000);
    List<SourceRecord> out =
        bytes.next(
            List.of(q("a", true, false, 10), q("b", true, false, 20), q("c", true, false, 1)),
            ctx,
            0,
            false);
    assertThat(names(out)).containsExactly("a", "b");
    EosBoundaries time = new EosBoundaries(1000, 1000, 500);
    assertThat(names(time.next(List.of(q("a", true, false, 1)), ctx, 0, false))).hasSize(1);
    int before = requests;
    time.next(List.of(q("b", true, false, 1)), ctx, 499, false);
    assertThat(requests).isEqualTo(before);
    time.next(List.of(q("c", true, false, 1)), ctx, 500, false);
    assertThat(requests).isEqualTo(before + 1);
    EosBoundaries split = new EosBoundaries(1000, 1000, 10_000);
    out =
        split.next(
            List.of(q("x1", true, true, 1), q("x2", false, false, 1), q("x3", true, false, 1)),
            ctx,
            0,
            false);
    assertThat(names(out)).containsExactly("x1");
    assertThat(split.holding()).isTrue();
  }
}
