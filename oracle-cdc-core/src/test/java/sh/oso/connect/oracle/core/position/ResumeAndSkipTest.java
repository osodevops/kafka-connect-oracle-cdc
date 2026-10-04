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
                        new RedoRecordId(commitScn - 10 + i, "0x0", 0),
                        k,
                        Instant.EPOCH))
            .toList();
    return new CommittedTransaction(
        k,
        new RedoRecordId(commitScn - 10, "0x0", 0),
        null,
        new RedoRecordId(commitScn, "0x0", 0),
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
  }
}
