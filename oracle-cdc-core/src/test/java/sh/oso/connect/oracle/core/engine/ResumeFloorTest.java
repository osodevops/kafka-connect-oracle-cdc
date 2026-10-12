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
package sh.oso.connect.oracle.core.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.List;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.errors.ErrorCode;
import sh.oso.connect.oracle.core.errors.ResumeFloorViolationException;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.position.ResumePoint;

/** ADR-0026 decision 4: the CDC-7003 floor compares redo byte addresses within a thread only. */
@org.junit.jupiter.api.Tag("rac-resume-floor")
class ResumeFloorTest {

  static final TxKey TX = new TxKey(3, new Xid(1, 1, 1));

  static RedoRecordId at(long scn, long rba) {
    return new RedoRecordId(scn, String.format(" 0x%06x.%08x.%04x ", 1, rba, 0), 0);
  }

  static MiningEvent row(int thread, RedoRecordId id) {
    return new MiningEvent.Commit(TX, id, thread, Instant.EPOCH);
  }

  static CommittedTransaction commit(int thread, RedoRecordId id) {
    return new CommittedTransaction(TX, id, id, id, Instant.EPOCH, thread, "APP", null, List.of());
  }

  static ResumeFloor fresh() {
    return ResumeFloor.from(Position.initial(1000, new DatabaseIdentity(1, 1)));
  }

  @Test
  void aRowAfterItsThreadsFloorPassesWhateverItsScn() {
    ResumeFloor f = fresh();
    f.handed(ResumePoint.single(1, at(2000, 10)));
    // late bound: an SCN far below the floor's, an address after it
    assertThatCode(() -> f.checkRow(row(1, at(1500, 11)))).doesNotThrowAnyException();
    assertThatCode(() -> f.checkRow(row(1, at(2000, 10))))
        .as("the resume record itself is read again by a restart")
        .doesNotThrowAnyException();
    assertThatThrownBy(() -> f.checkRow(row(1, at(2500, 9))))
        .as("an address below it, whatever the SCN")
        .isInstanceOfSatisfying(
            ResumeFloorViolationException.class,
            e -> assertThat(e.code()).isEqualTo(ErrorCode.RESUME_FLOOR_VIOLATION))
        .hasMessageContaining("on thread 1, below");
  }

  @Test
  void floorsAreKeptPerThreadAndNeverCompareAcrossThreads() {
    ResumeFloor f = fresh();
    TreeMap<Integer, RedoRecordId> by = new TreeMap<>();
    by.put(1, at(2000, 50));
    by.put(2, at(1900, 5));
    f.handed(ResumePoint.of(by));
    assertThatCode(() -> f.checkRow(row(2, at(1950, 6)))).doesNotThrowAnyException();
    assertThatThrownBy(() -> f.checkRow(row(1, at(2100, 49))))
        .isInstanceOf(ResumeFloorViolationException.class);
    assertThatCode(() -> f.checkRow(row(3, at(1, 1))))
        .as("a thread with no floor")
        .doesNotThrowAnyException();
    f.handed(ResumePoint.single(at(1000, 1)));
    assertThatCode(() -> f.checkRow(row(2, at(1950, 6))))
        .as("a thread-less point after per-thread floors changes nothing")
        .doesNotThrowAnyException();
  }

  @Test
  void aCommitAtOrBeforeItsThreadsLastHandedCommitStops() {
    ResumeFloor f = fresh();
    f.handed(commit(2, at(1010, 12)));
    assertThatCode(() -> f.checkCommit(commit(1, at(1000, 3))))
        .as("another thread's commit")
        .doesNotThrowAnyException();
    assertThatCode(() -> f.checkCommit(commit(2, at(1005, 13)))).doesNotThrowAnyException();
    assertThatThrownBy(() -> f.checkCommit(commit(2, at(1020, 12))))
        .as("handed in this run already")
        .isInstanceOf(ResumeFloorViolationException.class);
    assertThatThrownBy(() -> f.checkCommit(commit(2, at(1020, 7))))
        .isInstanceOf(ResumeFloorViolationException.class)
        .hasMessageContaining("the last commit of thread 2");
  }

  @Test
  void theStartPositionsCommitMayResumeAtItsEventIndex() {
    Position start =
        Position.initial(1000, new DatabaseIdentity(1, 1))
            .withResume(at(1010, 12))
            .withCommit(at(1010, 12), 1, TX, 1);
    ResumeFloor f = ResumeFloor.from(start);
    assertThatCode(() -> f.checkCommit(commit(1, at(1010, 12))))
        .as("the acknowledged commit, re-mined and resumed part way")
        .doesNotThrowAnyException();
    assertThatThrownBy(() -> f.checkCommit(commit(1, at(1009, 11))))
        .isInstanceOf(ResumeFloorViolationException.class);
    assertThatCode(() -> f.checkRow(row(1, at(1010, 12))))
        .as("the resume record, read again inclusive")
        .doesNotThrowAnyException();
    assertThatThrownBy(() -> f.checkRow(row(1, at(1010, 11))))
        .isInstanceOf(ResumeFloorViolationException.class);
  }

  @Test
  void recordsWithoutARedoByteAddressAreNotCompared() {
    ResumeFloor f = fresh();
    f.handed(ResumePoint.single(1, at(2000, 10)));
    f.handed(commit(1, at(2000, 10)));
    RedoRecordId zero = new RedoRecordId(1500, " 0x000000.00000000.0000 ", 0);
    assertThatCode(() -> f.checkRow(row(1, zero))).doesNotThrowAnyException();
    assertThatCode(() -> f.checkCommit(commit(1, zero))).doesNotThrowAnyException();
    assertThatCode(() -> f.checkRow(row(1, new RedoRecordId(1, null, 0))))
        .doesNotThrowAnyException();
  }
}
