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

import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.errors.OracleCdcCorruptionException;
import sh.oso.connect.oracle.core.errors.OracleCdcPurgedException;
import sh.oso.connect.oracle.core.errors.TransientDatabaseException;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.testkit.FakeLogMiner;

class StepRunnerTest {

  private static final TableId T = FakeLogMiner.DEFAULT_TABLE;

  private static FakeLogMiner script() {
    FakeLogMiner f = new FakeLogMiner().startAt(1000);
    TxKey tx = f.tx(1, 1, 1);
    f.start(tx, "APP").insert(tx, T, "i1").update(tx, T, "u1").insert(tx, T, "i2").commit(tx);
    return f; // events at scn 1000..1004
  }

  private static StepRunner runner() {
    return new StepRunner(new OraErrorClassifier(), new DdlStepCut(Set.of("APP")));
  }

  @Test
  void completeStepStagesEveryEventAndMovesTheCursorToTheEnd() {
    StepOutcome o = runner().run(script(), StepCursor.at(1000), 1010);
    assertThat(o.kind()).isEqualTo(StepOutcome.Kind.COMPLETE);
    assertThat(o.events()).hasSize(5);
    assertThat(o.next().scn()).isEqualTo(1010);
    assertThat(o.next().lastApplied())
        .as("the cursor is the redo byte address of the last row (ADR-0014)")
        .isEqualTo(o.events().get(o.events().size() - 1).id());
    assertThat(o.next().inclusive()).isFalse();
    assertThat(o.rowsSeen()).isEqualTo(5);
    assertThat(o.applies()).isTrue();
  }

  @Test
  void stepRetryAtFirstMiddleOrLastRowLeavesNothingAppliedAndTheSameRangeIsReMined() {
    for (int at : new int[] {0, 2, 4}) {
      FakeLogMiner f = script();
      f.faultAt(
          at, new SQLException("ORA-00310: archived log contains sequence 7", "72000", 310), false);
      StepRunner r = runner();
      StepOutcome o = r.run(f, StepCursor.at(1000), 1010);
      assertThat(o.kind()).as("fault at " + at).isEqualTo(StepOutcome.Kind.RETRY);
      assertThat(o.events()).isEmpty();
      assertThat(o.next()).isEqualTo(StepCursor.at(1000));
      assertThat(o.cause()).hasMessageContaining("ORA-00310");
      StepOutcome again = r.run(f, o.next(), 1010);
      assertThat(again.kind()).isEqualTo(StepOutcome.Kind.COMPLETE);
      assertThat(again.events()).hasSize(5);
    }
  }

  @Test
  void timeoutsAreReportedAsTimeoutsNotRetries() {
    FakeLogMiner f = script();
    f.faultAt(1, new SQLTimeoutException("query timed out"), false);
    StepOutcome o = runner().run(f, StepCursor.at(1000), 1010);
    assertThat(o.kind()).isEqualTo(StepOutcome.Kind.TIMEOUT);
    assertThat(o.events()).isEmpty();
    FakeLogMiner g = script();
    g.faultAt(
        1,
        new SQLException("ORA-01013: user requested cancel of current operation", "72000", 1013),
        false);
    assertThat(runner().run(g, StepCursor.at(1000), 1010).kind())
        .isEqualTo(StepOutcome.Kind.TIMEOUT);
  }

  @Test
  void ddlThatChangesObjectIdsCutsTheStepAfterTheDdlAndTheCursorSkipsItNextTime() {
    FakeLogMiner f = new FakeLogMiner().startAt(1000);
    TxKey tx = f.tx(1, 1, 2);
    TxKey ddlTx = f.tx(1, 1, 3);
    f.start(tx, "APP")
        .insert(tx, T, "i1")
        .ddl(
            ddlTx,
            new TableId("FREEPDB1", "APP", "EVENTS"),
            5555,
            "ALTER TABLE events ADD PARTITION p9 VALUES LESS THAN (9)")
        .insert(tx, T, "i2")
        .commit(tx);
    StepRunner r = runner();
    StepOutcome cut = r.run(f, StepCursor.at(1000), 1010);
    assertThat(cut.kind()).isEqualTo(StepOutcome.Kind.CUT);
    assertThat(cut.events()).hasSize(3);
    assertThat(cut.events().get(2)).isInstanceOf(MiningEvent.Ddl.class);
    assertThat(cut.next().scn()).isEqualTo(1002);
    assertThat(cut.next().lastApplied()).isEqualTo(cut.events().get(2).id());
    StepOutcome rest = r.run(f, cut.next(), 1010);
    assertThat(rest.kind()).isEqualTo(StepOutcome.Kind.COMPLETE);
    assertThat(rest.events()).hasSize(2);
    assertThat(rest.events().get(0)).isInstanceOf(MiningEvent.Dml.class);
    assertThat(rest.rowsSeen())
        .as("the DDL row is not re-read: the cursor is its redo byte address")
        .isEqualTo(2);
  }

  @Test
  void ddlOnOtherOwnersOrWithoutIdChangesDoesNotCut() {
    FakeLogMiner f = new FakeLogMiner().startAt(1000);
    TxKey tx = f.tx(1, 1, 4);
    f.ddl(
            tx,
            new TableId("FREEPDB1", "HR", "EMP"),
            1,
            "ALTER TABLE emp ADD PARTITION p1 VALUES LESS THAN (1)")
        .ddl(tx, T, 2, "TRUNCATE TABLE orders")
        .ddl(tx, T, 3, "ALTER TABLE orders MOVE")
        .ddl(tx, T, 4, "ALTER TABLE orders ADD (c NUMBER)")
        .commit(tx);
    StepOutcome o = runner().run(f, StepCursor.at(1000), 1010);
    assertThat(o.kind()).isEqualTo(StepOutcome.Kind.COMPLETE);
    assertThat(o.events()).hasSize(5);
    assertThat(DdlStepCut.changesObjectIds("create table x (a number)")).isTrue();
    assertThat(DdlStepCut.changesObjectIds("DROP TABLE x PURGE")).isTrue();
    assertThat(
            DdlStepCut.changesObjectIds(
                "alter table x split partition p1 at (5) into (partition p1a, partition p1b)"))
        .isTrue();
    assertThat(DdlStepCut.changesObjectIds("alter table x exchange partition p1 with table y"))
        .isTrue();
    assertThat(
            DdlStepCut.changesObjectIds("alter table x merge partitions p1, p2 into partition p12"))
        .isTrue();
    assertThat(DdlStepCut.changesObjectIds("alter table x rename to y")).isTrue();
    assertThat(DdlStepCut.changesObjectIds("alter table x shrink space")).isFalse();
    assertThat(DdlStepCut.changesObjectIds("alter table x truncate partition p1")).isFalse();
    assertThat(DdlStepCut.changesObjectIds(null)).isFalse();
  }

  @Test
  void missingScnAndNonRetriableErrorsStopTheTask() {
    FakeLogMiner f = new FakeLogMiner().startAt(1000);
    TxKey tx = f.tx(1, 1, 5);
    f.insert(tx, T, "i1").missingScn().commit(tx);
    assertThatThrownBy(() -> runner().run(f, StepCursor.at(1000), 1010))
        .isInstanceOf(OracleCdcCorruptionException.class)
        .hasMessageContaining("MISSING_SCN");
    FakeLogMiner purged = script();
    purged.faultAt(0, new SQLException("ORA-01284: file x cannot be opened", "72000", 1284), false);
    assertThatThrownBy(() -> runner().run(purged, StepCursor.at(1000), 1010))
        .isInstanceOf(OracleCdcPurgedException.class);
    FakeLogMiner dropped = script();
    dropped.faultAt(
        3,
        new SQLException("ORA-03113: end-of-file on communication channel", "72000", 3113),
        false);
    assertThatThrownBy(() -> runner().run(dropped, StepCursor.at(1000), 1010))
        .isInstanceOf(TransientDatabaseException.class);
    assertThatThrownBy(
            () ->
                new StepOutcome(
                    StepOutcome.Kind.RETRY,
                    List.of(script().events().get(0)),
                    StepCursor.at(1),
                    1,
                    null))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void sessionRecyclerIsDueAfterMaxAge() {
    sh.oso.connect.oracle.core.testkit.FixedClock clock =
        new sh.oso.connect.oracle.core.testkit.FixedClock(0);
    SessionRecycler r =
        new SessionRecycler(
            java.time.Duration.ofHours(1), () -> java.time.Instant.ofEpochMilli(clock.getAsLong()));
    assertThat(r.due()).isFalse();
    clock.advance(java.time.Duration.ofMinutes(61));
    assertThat(r.due()).isTrue();
    r.recycled();
    assertThat(r.due()).isFalse();
    assertThat(r.recycles()).isEqualTo(1);
  }

  /**
   * ADR-0026: RS_ID starts with the thread's own log sequence, so thread 1 being further on in its
   * redo says nothing about thread 2. With one cursor for both, thread 2's later rows sort below
   * thread 1's mark and are skipped without a trace; with a mark per thread they are mined.
   */
  @Test
  void aSlowThreadsRowsAreNotHiddenByAnotherThreadsAddress() {
    FakeLogMiner f = new FakeLogMiner().startAt(1000).perThreadRba();
    TxKey a = f.tx(1, 1, 1);
    f.start(a, "APP");
    for (int i = 0; i < 6; i++) {
      f.insert(a, T, "a" + i);
    }
    f.commit(a); // thread 1 at block 8
    TxKey b = f.onThread(2).tx(2, 1, 1);
    f.start(b, "APP").insert(b, T, "b0").commit(b); // thread 2 at block 3
    StepOutcome first = runner().run(f, StepCursor.at(1000), 1100);
    assertThat(first.events()).hasSize(11);
    assertThat(first.next().marks()).containsOnlyKeys(1, 2);

    TxKey c = f.tx(2, 2, 1);
    f.start(c, "APP").insert(c, T, "c0").commit(c); // thread 2, blocks 4 to 6
    StepOutcome second = runner().run(f, first.next(), 1200);
    assertThat(second.events())
        .as("thread 2's blocks 4 to 6 sort below thread 1's block 8 but are new")
        .hasSize(3)
        .allSatisfy(e -> assertThat(e.thread()).isEqualTo(2));
    assertThat(second.next().marks().get(1)).isEqualTo(first.next().marks().get(1));
  }

  @Test
  void aThreadLessResumeMarkIsReplacedByTheThreadsOwnMark() {
    FakeLogMiner f = script();
    StepOutcome o = runner().run(f, StepCursor.resume(f.events().get(1).id()), 1010);
    assertThat(o.events()).hasSize(4).first().isEqualTo(f.events().get(1));
    assertThat(o.next().marks()).containsOnlyKeys(1);
    assertThat(o.next().lastApplied()).isEqualTo(f.events().get(4).id());
  }
}
