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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static sh.oso.connect.oracle.core.position.ResumePointTest.rba;
import static sh.oso.connect.oracle.core.position.ResumePointTest.threads;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;

/** ADR-0026: a position takes a resume point per redo thread; one thread changes nothing. */
class PerThreadPositionTest {

  static final DatabaseIdentity IDENT = new DatabaseIdentity(42, 7);
  static final TxKey K1 = new TxKey(0, Xid.parse("0001.002.00000003"));
  static final TxKey K2 = new TxKey(0, Xid.parse("0004.005.00000006"));

  /** Single-thread positions of every shape a running task writes. */
  static List<Position> singleThread() {
    Position initial = Position.initial(100, IDENT);
    Position firstStart =
        Position.firstStart(100, 90, 80, List.of(K2), IDENT).withExtra("signal_offset", 4L);
    return List.of(
        initial,
        initial.withCommit(rba(120, 12), 1, K1, 2),
        initial.withCommit(130, 1, K1, 1), // SCN only, as before ADR-0014
        firstStart,
        firstStart.withCommit(rba(140, 15), 1, K1, 3).withReleased(List.of("0:9.9.9")));
  }

  @Test
  void aResumePointOfOneThreadWritesTheOffsetOfBefore() {
    List<RedoRecordId> points =
        List.of(rba(150, 20), new RedoRecordId(150, null, 0), rba(95, 3), rba(95, 0));
    for (Position p : singleThread()) {
      for (RedoRecordId point : points) {
        Map<String, Object> before = PositionCodec.write(p.withResume(point));
        assertThat(before).doesNotContainKey("threads");
        for (ResumePoint same :
            List.of(
                ResumePoint.single(point),
                ResumePoint.single(1, point),
                ResumePoint.of(threads(3, point)))) {
          assertThat(PositionCodec.write(p.withResume(same)))
              .as("%s resumed at %s", p, same)
              .isEqualTo(before);
        }
        // the sink now applies the resume point before the commit: the same fields either way
        assertThat(
                PositionCodec.write(
                    p.withResume(ResumePoint.single(1, point)).withCommit(rba(160, 21), 1, K2, 1)))
            .isEqualTo(PositionCodec.write(p.withCommit(rba(160, 21), 1, K2, 1).withResume(point)));
      }
    }
  }

  @Test
  void severalThreadsWriteAMarkPerThreadThatAgreesWithTheLegacyKeys() {
    Position single = Position.initial(100, IDENT).withCommit(rba(120, 12), 1, K1, 2);
    Position p = single.withResume(ResumePoint.of(threads(1, rba(125, 13), 2, rba(110, 4))));

    assertThat(p.perThread()).isTrue();
    assertThat(p.resumeScn()).isEqualTo(110);
    assertThat(p.resumeRsId()).as("no address places two threads").isNull();
    assertThat(p.threads())
        .containsExactly(
            Map.entry(1, new ThreadMark(rba(125, 13), rba(120, 12), K1)),
            Map.entry(2, new ThreadMark(rba(110, 4), null, null)));
    assertThat(p.lastCommitId()).isEqualTo(rba(120, 12));
    assertThat(p.eventIndex()).isEqualTo(2);
    assertThat(PositionCodec.agrees(p.threads(), p.withThreads(null))).isTrue();

    Map<String, Object> written = PositionCodec.write(p);
    assertThat(written).containsKey("threads").containsEntry("resume_rs_id", null);
    Position back = PositionCodec.read(written);
    assertThat(back.perThread()).isTrue();
    assertThat(back.threads()).isEqualTo(p.threads());
    assertThat(back.resume().byThread())
        .containsExactly(Map.entry(1, rba(125, 13)), Map.entry(2, rba(110, 4)));
  }

  @Test
  void aCommitOnAPerThreadPositionMovesItsOwnThreadsMark() {
    Position p =
        Position.initial(100, IDENT)
            .withCommit(rba(120, 12), 1, K1, 2)
            .withResume(ResumePoint.of(threads(1, rba(125, 13), 2, rba(110, 4))))
            .withCommit(rba(130, 7), 2, K2, 1);

    assertThat(p.threads().get(1)).isEqualTo(new ThreadMark(rba(125, 13), rba(120, 12), K1));
    assertThat(p.threads().get(2)).isEqualTo(new ThreadMark(rba(110, 4), rba(130, 7), K2));
    assertThat(p.lastCommitThread()).isEqualTo(2);
    assertThat(p.lastCommitKey()).isEqualTo(K2);
    assertThat(PositionCodec.read(PositionCodec.write(p)).threads()).isEqualTo(p.threads());

    // the next resume point keeps both threads' commits
    Position next = p.withResume(ResumePoint.of(threads(1, rba(140, 16), 2, rba(140, 8))));
    assertThat(next.threads())
        .containsExactly(
            Map.entry(1, new ThreadMark(rba(140, 16), rba(120, 12), K1)),
            Map.entry(2, new ThreadMark(rba(140, 8), rba(130, 7), K2)));
    assertThat(next.resumeScn()).isEqualTo(140);
    assertThat(PositionCodec.read(PositionCodec.write(next)).perThread()).isTrue();
  }

  @Test
  void aThreadMissingFromThePointKeepsItsMark() {
    Position p =
        Position.initial(100, IDENT)
            .withResume(ResumePoint.of(threads(1, rba(125, 13), 2, rba(110, 4), 3, rba(105, 2))));
    Position next = p.withResume(ResumePoint.of(threads(1, rba(150, 20), 2, rba(150, 9))));
    assertThat(next.threads().get(3)).isEqualTo(new ThreadMark(rba(105, 2), null, null));
    assertThat(next.resumeScn()).as("the kept mark is still the lowest").isEqualTo(105);
    assertThat(PositionCodec.read(PositionCodec.write(next)).perThread()).isTrue();

    // a single-thread point moves its thread of a per-thread position and keeps the block
    Position one = next.withResume(ResumePoint.single(2, rba(160, 10)));
    assertThat(one.threads()).hasSize(3);
    assertThat(one.threads().get(2).resume()).isEqualTo(rba(160, 10));
    assertThat(PositionCodec.read(PositionCodec.write(one)).perThread()).isTrue();
  }

  @Test
  void theLastCommitsThreadAlwaysHasAMark() {
    // the legacy last commit is on thread 3, which the point does not name yet
    Position p =
        Position.initial(100, IDENT)
            .withCommit(rba(120, 12), 3, K1, 1)
            .withResume(ResumePoint.of(threads(1, rba(125, 13), 2, rba(110, 4))));
    assertThat(p.threads().get(3))
        .as("read again by SCN from the lowest resume point, its commit kept")
        .isEqualTo(new ThreadMark(new RedoRecordId(110, null, 0), rba(120, 12), K1));
    assertThat(PositionCodec.read(PositionCodec.write(p)).perThread()).isTrue();
  }

  @Test
  void aPerThreadPositionRefusesAPointThatNamesNoThread() {
    Position p =
        Position.initial(100, IDENT)
            .withResume(ResumePoint.of(threads(1, rba(125, 13), 2, rba(110, 4))));
    assertThatThrownBy(() -> p.withResume(ResumePoint.single(rba(130, 14))))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void eachThreadIsReadAgainFromItsOwnMark() {
    Position p =
        Position.initial(100, IDENT)
            .withResume(ResumePoint.of(threads(1, rba(125, 13), 2, rba(110, 4))));
    assertThat(p.resumePointFor(1)).isEqualTo(rba(125, 13));
    assertThat(p.resumePointFor(2)).isEqualTo(rba(110, 4));
    assertThat(p.resumePointFor(5)).as("a thread without a mark").isEqualTo(p.resumePoint());
    Position single = Position.initial(100, IDENT).withResume(rba(125, 13));
    assertThat(single.resumePointFor(2)).isEqualTo(rba(125, 13));
    assertThat(single.resume()).isEqualTo(ResumePoint.single(rba(125, 13)));
  }
}
