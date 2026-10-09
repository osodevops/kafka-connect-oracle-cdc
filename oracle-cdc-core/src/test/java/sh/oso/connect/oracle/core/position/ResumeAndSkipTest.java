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
package sh.oso.connect.oracle.core.position;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;

class ResumeAndSkipTest {

  /** A redo byte address that orders like (scn, sqn) within a thread. */
  static String rba(int thread, long scn, long sqn) {
    return String.format(" 0x%06x.%08x.%04x ", thread, scn, sqn);
  }

  static CommittedTransaction tx(long commitScn, int thread, long sqn, int events) {
    TxKey k = new TxKey(3, new Xid(1, 1, sqn));
    List<RowChange> changes =
        java.util.stream.IntStream.range(0, events)
            .mapToObj(
                i ->
                    new RowChange(
                        sh.oso.connect.oracle.core.testkit.FakeLogMiner.DEFAULT_TABLE,
                        sh.oso.connect.oracle.core.model.Operation.INSERT,
                        null,
                        java.util.Map.of("I", i),
                        false,
                        "R" + i,
                        new RedoRecordId(
                            commitScn - 10 + i, rba(thread, commitScn - 10 + i, sqn), i),
                        k,
                        Instant.EPOCH))
            .toList();
    return new CommittedTransaction(
        k,
        new RedoRecordId(commitScn - 10, rba(thread, commitScn - 10, sqn), 0),
        null,
        new RedoRecordId(commitScn, rba(thread, commitScn, sqn), 0),
        Instant.EPOCH,
        thread,
        "APP",
        null,
        changes);
  }

  @Test
  void resumeScnIsTheLowerOfSafeEndAndOldestOpenTransaction() {
    assertThat(ResumeCalculator.resumeScn(1000, Optional.empty())).isEqualTo(1000);
    assertThat(ResumeCalculator.resumeScn(1000, Optional.of(new RedoRecordId(900, "0x0", 0))))
        .isEqualTo(900);
    assertThat(ResumeCalculator.resumeScn(1000, Optional.of(new RedoRecordId(1200, "0x0", 0))))
        .isEqualTo(1000);
    Position p = Position.initial(950, new DatabaseIdentity(1, 1));
    assertThat(ResumeCalculator.advance(p, 1000, Optional.empty()).resumeScn()).isEqualTo(1000);
    assertThat(ResumeCalculator.advance(p, 1000, Optional.of(new RedoRecordId(900, "0x0", 0))))
        .as("never moves backwards")
        .isSameAs(p);
  }

  @Test
  void skipRuleFollowsCommitOrder() {
    Position fresh = Position.initial(1, new DatabaseIdentity(1, 1));
    assertThat(SkipRule.eventsToSkip(fresh, tx(100, 1, 1, 3))).isZero();
    CommittedTransaction acked = tx(100, 1, 5, 4);
    Position p = fresh.withCommit(100, 1, acked.key(), 2);
    assertThat(SkipRule.eventsToSkip(p, tx(90, 2, 9, 3))).as("earlier commit").isEqualTo(3);
    assertThat(SkipRule.eventsToSkip(p, tx(100, 1, 4, 3))).as("same scn, lower key").isEqualTo(3);
    assertThat(SkipRule.eventsToSkip(p, acked)).as("the acknowledged one").isEqualTo(2);
    assertThat(SkipRule.eventsToSkip(p, tx(100, 1, 6, 3))).as("same scn, higher key").isZero();
    assertThat(SkipRule.eventsToSkip(p, tx(100, 2, 1, 3))).as("same scn, higher thread").isZero();
    assertThat(SkipRule.eventsToSkip(p, tx(101, 1, 1, 3))).isZero();
    assertThat(SkipRule.eventsToSkip(fresh.withCommit(100, 1, acked.key(), 99), acked))
        .as("event index never exceeds size")
        .isEqualTo(4);
    assertThat(CommitOrder.COMPARATOR.compare(tx(100, 1, 1, 1), tx(100, 1, 2, 1))).isNegative();
    assertThat(CommitOrder.COMPARATOR.compare(tx(100, 2, 1, 1), tx(100, 1, 2, 1))).isPositive();
    // ADR-0014: with redo byte addresses on both sides the order is the redo order, so a commit
    // written later with a lower SCN (a late-bound private strand) is never skipped
    Position rba = fresh.withCommit(acked.commitId(), 1, acked.key(), 2);
    assertThat(rba.lastCommitRsId()).isEqualTo(acked.commitId().rsId());
    assertThat(SkipRule.eventsToSkip(rba, acked)).isEqualTo(2);
    assertThat(SkipRule.eventsToSkip(rba, tx(90, 1, 9, 3))).as("earlier redo").isEqualTo(3);
    CommittedTransaction lateLowScn =
        new CommittedTransaction(
            new sh.oso.connect.oracle.core.model.TxKey(
                3, new sh.oso.connect.oracle.core.model.Xid(1, 1, 77)),
            new RedoRecordId(80, rba(1, 100, 9), 0),
            null,
            new RedoRecordId(95, rba(1, 100, 9), 0),
            Instant.EPOCH,
            1,
            "APP",
            null,
            tx(95, 1, 77, 2).events());
    assertThat(SkipRule.eventsToSkip(rba, lateLowScn))
        .as("lower commit SCN but written after the acknowledged commit: replayed")
        .isZero();
  }

  // ADR-0026: per-thread marks

  static ThreadMark mark(CommittedTransaction t) {
    return new ThreadMark(t.firstCaptured(), t.commitId(), t.key());
  }

  /** Thread 1 acknowledged a, thread 2 acknowledged b (the globally last, 2 of its 3 events). */
  static Position perThread(CommittedTransaction a, CommittedTransaction b) {
    java.util.SortedMap<Integer, ThreadMark> marks = new java.util.TreeMap<>();
    marks.put(1, mark(a));
    marks.put(2, mark(b));
    return Position.initial(a.firstCaptured().scn(), new DatabaseIdentity(1, 1))
        .withCommit(b.commitId(), 2, b.key(), 2)
        .withThreads(marks);
  }

  @Test
  void aLaterCommitOfAnotherThreadIsNotSkippedBecauseOfTheGlobalLastCommit() {
    CommittedTransaction a = tx(200, 1, 1, 3);
    CommittedTransaction b = tx(300, 2, 2, 3);
    CommittedTransaction later = tx(250, 1, 3, 2); // thread 1, after a in thread 1's redo
    Position p = perThread(a, b);
    assertThat(SkipRule.eventsToSkip(p, later))
        .as("one cursor would order thread 1 below thread 2 and skip it whole")
        .isZero();
    Position legacy =
        Position.initial(190, new DatabaseIdentity(1, 1)).withCommit(b.commitId(), 2, b.key(), 2);
    assertThat(SkipRule.eventsToSkip(legacy, later))
        .as("the single-thread rule, kept for single-thread positions")
        .isEqualTo(later.size());
  }

  @Test
  void aCommitAtOrBeforeItsThreadsMarkIsSkippedWhole() {
    CommittedTransaction a = tx(200, 1, 1, 3);
    CommittedTransaction b = tx(300, 2, 2, 3);
    Position p = perThread(a, b);
    assertThat(SkipRule.eventsToSkip(p, a)).isEqualTo(a.size());
    assertThat(SkipRule.eventsToSkip(p, tx(150, 1, 4, 2))).isEqualTo(2);
  }

  @Test
  void theGloballyLastCommitResumesAtItsEventIndex() {
    CommittedTransaction a = tx(200, 1, 1, 3);
    CommittedTransaction b = tx(300, 2, 2, 3);
    assertThat(SkipRule.eventsToSkip(perThread(a, b), b)).isEqualTo(2);
  }

  @Test
  void aThreadWithoutAnAcknowledgedCommitSkipsNothing() {
    CommittedTransaction a = tx(200, 1, 1, 3);
    CommittedTransaction b = tx(300, 2, 2, 3);
    assertThat(SkipRule.eventsToSkip(perThread(a, b), tx(100, 3, 5, 2))).isZero();
  }
}
