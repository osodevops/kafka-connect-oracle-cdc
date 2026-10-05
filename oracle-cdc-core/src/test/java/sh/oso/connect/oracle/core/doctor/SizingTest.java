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
package sh.oso.connect.oracle.core.doctor;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class SizingTest {

  static final Instant NOW = Instant.parse("2026-10-05T12:00:00Z");
  static final long MIB = 1024L * 1024;

  private static ArchiveStat log(int thread, long seq, Instant next, long bytes) {
    return new ArchiveStat(
        thread,
        seq,
        seq * 100,
        seq * 100 + 100,
        next.minus(Duration.ofMinutes(10)),
        next,
        bytes,
        false);
  }

  @Test
  void figuresPerThreadAndRecommendations() {
    List<ArchiveStat> h = new ArrayList<>();
    // thread 1: 8 logs of 256 MiB in the hour from 08:00 two days ago, 2 logs later
    Instant peak = NOW.minus(Duration.ofDays(2)).minus(Duration.ofHours(4));
    for (int i = 0; i < 8; i++) {
      h.add(log(1, 10 + i, peak.plus(Duration.ofMinutes(5 + i * 6)), 256 * MIB));
    }
    h.add(log(1, 30, NOW.minus(Duration.ofHours(3)), 256 * MIB));
    h.add(log(1, 31, NOW.minus(Duration.ofHours(1)), 256 * MIB));
    // thread 2: one log, outside the window
    h.add(log(2, 5, NOW.minus(Duration.ofDays(9)), 64 * MIB));
    h.add(log(2, 6, NOW.minus(Duration.ofHours(2)), 64 * MIB));
    List<OnlineLogGroup> groups =
        List.of(
            new OnlineLogGroup(1, 1, 256 * MIB, "CURRENT"),
            new OnlineLogGroup(2, 2, 1024 * MIB, "CURRENT"),
            new OnlineLogGroup(3, 3, 64 * MIB, "UNUSED"));
    Sizing.Result r =
        Sizing.compute(
            h,
            groups,
            NOW.minus(Duration.ofDays(7)),
            NOW,
            Duration.ofHours(24),
            Duration.ofMinutes(5));
    assertThat(r.threads()).extracting(Sizing.ThreadFigures::thread).containsExactly(1, 2, 3);
    Sizing.ThreadFigures t1 = r.threads().get(0);
    assertThat(t1.logs()).isEqualTo(10);
    assertThat(t1.peakSwitchesPerHour()).isEqualTo(8);
    assertThat(t1.peakHour()).isEqualTo(Instant.parse("2026-10-03T08:00:00Z"));
    assertThat(t1.peakHourBytes()).isEqualTo(8 * 256 * MIB);
    assertThat(t1.recommendedLogBytes()).isEqualTo(512 * MIB);
    Sizing.ThreadFigures t2 = r.threads().get(1);
    assertThat(t2.logs()).isEqualTo(1);
    assertThat(t2.recommendedLogBytes()).isEqualTo(1024 * MIB); // never below the current size
    assertThat(r.threads().get(2).logs()).isZero();
    assertThat(r.peakHourBytes()).isEqualTo(8 * 256 * MIB);
    assertThat(r.busiestDayBytes()).isEqualTo(8 * 256 * MIB);
    assertThat(r.requiredRetention()).isEqualTo(Duration.ofHours(25));
    assertThat(r.archiveSpaceBytes()).isEqualTo(Math.round(8 * 256 * MIB * (25 / 24.0)));
    assertThat(r.observed()).isGreaterThan(Duration.ofDays(2));

    Sizing.Result shortRetention =
        Sizing.compute(
            h, groups, NOW.minus(Duration.ofDays(7)), NOW, Duration.ofHours(2), Duration.ZERO);
    assertThat(shortRetention.requiredRetention()).isEqualTo(Duration.ofHours(2));
    assertThat(shortRetention.archiveSpaceBytes()).isEqualTo(2 * 8 * 256 * MIB);
  }

  @Test
  void anEmptyHistoryStillReportsTheOnlineLogs() {
    Sizing.Result r =
        Sizing.compute(
            List.of(),
            List.of(new OnlineLogGroup(1, 1, 200 * MIB, "CURRENT")),
            NOW.minus(Duration.ofDays(7)),
            NOW,
            Duration.ofHours(1),
            Duration.ofMinutes(30));
    assertThat(r.threads())
        .singleElement()
        .satisfies(
            t -> {
              assertThat(t.logs()).isZero();
              assertThat(t.peakHour()).isNull();
              assertThat(t.recommendedLogBytes()).isEqualTo(200 * MIB);
            });
    assertThat(r.requiredRetention()).isEqualTo(Duration.ofHours(2));
    assertThat(r.bytesPerDay()).isZero();
  }

  @Test
  void reachIsTheNewestOfEachThreadsOldestPresentLog() {
    List<ArchiveStat> h = new ArrayList<>();
    h.add(
        new ArchiveStat(
            1,
            1,
            0,
            10,
            NOW.minus(Duration.ofHours(30)),
            NOW.minus(Duration.ofHours(29)),
            1,
            true));
    h.add(
        new ArchiveStat(
            1,
            2,
            10,
            20,
            NOW.minus(Duration.ofHours(20)),
            NOW.minus(Duration.ofHours(19)),
            1,
            false));
    h.add(
        new ArchiveStat(
            2,
            1,
            0,
            15,
            NOW.minus(Duration.ofHours(10)),
            NOW.minus(Duration.ofHours(9)),
            1,
            false));
    Sizing.Reach r = Sizing.reach(h, List.of(1, 2), NOW);
    assertThat(r.purgeSeen()).isTrue();
    assertThat(r.reach()).isEqualTo(Duration.ofHours(10));
    assertThat(Sizing.reach(h, List.of(1), NOW).reach()).isEqualTo(Duration.ofHours(20));
    assertThat(Sizing.reach(List.of(), List.of(1), NOW)).isNull();
  }

  @Test
  void humanFigures() {
    assertThat(Sizing.bytes(512)).isEqualTo("0 KiB");
    assertThat(Sizing.bytes(300 * 1024)).isEqualTo("300 KiB");
    assertThat(Sizing.bytes(200 * MIB)).isEqualTo("200 MiB");
    assertThat(Sizing.bytes(2048 * MIB)).isEqualTo("2 GiB");
    assertThat(Sizing.bytes(1536 * MIB)).isEqualTo("1.5 GiB");
    assertThat(Sizing.duration(Duration.ofMinutes(45))).isEqualTo("45 min");
    assertThat(Sizing.duration(Duration.ofMinutes(90))).isEqualTo("1 h 30 min");
    assertThat(Sizing.duration(Duration.ofHours(26))).isEqualTo("26 h");
    assertThat(Sizing.roundUp(0)).isZero();
    assertThat(Sizing.roundUpHours(Duration.ofHours(3))).isEqualTo(Duration.ofHours(3));
  }
}
