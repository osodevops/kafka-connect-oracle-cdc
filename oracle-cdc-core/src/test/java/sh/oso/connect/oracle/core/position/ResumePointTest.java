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

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.model.RedoRecordId;

/** ADR-0026: the resume point per redo thread and its legacy form. */
class ResumePointTest {

  static RedoRecordId rba(long scn, long block) {
    return new RedoRecordId(scn, String.format(" 0x%06x.%08x.%04x ", 1, block, 0), 0);
  }

  static SortedMap<Integer, RedoRecordId> threads(Object... threadAndId) {
    SortedMap<Integer, RedoRecordId> m = new TreeMap<>();
    for (int i = 0; i < threadAndId.length; i += 2) {
      m.put((Integer) threadAndId[i], (RedoRecordId) threadAndId[i + 1]);
    }
    return m;
  }

  @Test
  void aSingleThreadKeepsItsPointAsTheLegacyPoint() {
    ResumePoint none = ResumePoint.single(rba(100, 7));
    assertThat(none.legacy()).isEqualTo(rba(100, 7));
    assertThat(none.byThread()).isEmpty();
    assertThat(none.perThread()).isFalse();

    ResumePoint one = ResumePoint.of(threads(2, rba(100, 7)));
    assertThat(one.legacy()).isEqualTo(rba(100, 7));
    assertThat(one.byThread()).containsExactly(Map.entry(2, rba(100, 7)));
    assertThat(one.perThread()).isFalse();
  }

  @Test
  void severalThreadsGiveTheLowestScnWithoutARedoByteAddress() {
    ResumePoint two = ResumePoint.of(threads(1, rba(120, 30), 2, rba(110, 4)));
    assertThat(two.perThread()).isTrue();
    assertThat(two.legacy()).isEqualTo(new RedoRecordId(110, null, 0));
    assertThat(two.legacy().hasRba()).isFalse();
    assertThatThrownBy(() -> ResumePoint.of(new TreeMap<>()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void withEarlierOnOneThreadLowersTheLegacyPointAsBefore() {
    // the rule RecordQueueSink applied before ADR-0026
    RedoRecordId candidate = rba(130, 20);
    RedoRecordId first = rba(125, 12);
    RedoRecordId before = ResumePoint.earlier(candidate, first);
    assertThat(before).isEqualTo(new RedoRecordId(125, rba(0, 12).rsId(), 0));

    assertThat(ResumePoint.single(candidate).withEarlier(1, first))
        .isEqualTo(ResumePoint.single(before));
    ResumePoint one = ResumePoint.single(1, candidate).withEarlier(1, first);
    assertThat(one.legacy()).isEqualTo(before);
    assertThat(one.byThread()).containsExactly(Map.entry(1, before));
    // a later first record changes nothing but the SCN floor rule
    assertThat(ResumePoint.single(1, candidate).withEarlier(1, rba(140, 25)).legacy())
        .isEqualTo(candidate);
  }

  @Test
  void withEarlierLowersOnlyTheTransactionsOwnThread() {
    // thread 2 is the quiet instance: its addresses are far below thread 1's
    ResumePoint candidate = ResumePoint.of(threads(1, rba(200, 90), 2, rba(200, 6)));
    ResumePoint during = candidate.withEarlier(1, rba(150, 40));
    assertThat(during.byThread())
        .containsExactly(Map.entry(1, rba(150, 40)), Map.entry(2, rba(200, 6)));
    assertThat(during.legacy()).isEqualTo(new RedoRecordId(150, null, 0));

    // an open transaction on the quiet thread does not move the busy thread's point
    ResumePoint quiet = candidate.withEarlier(2, rba(190, 5));
    assertThat(quiet.byThread())
        .containsExactly(Map.entry(1, rba(200, 90)), Map.entry(2, rba(190, 5)));
    assertThat(quiet.legacy()).isEqualTo(new RedoRecordId(190, null, 0));
  }
}
