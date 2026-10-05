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
import java.util.List;
import org.junit.jupiter.api.Test;

class RedoProfileTest {

  static final Instant NOW = Instant.parse("2026-10-05T12:00:00Z");
  static final long MIB = 1024L * 1024;

  private static RedoProfile.SampleRow row(
      String owner, String table, String op, long rows, long truncates) {
    return new RedoProfile.SampleRow("FREEPDB1", owner, table, op, rows, truncates);
  }

  /** A truncate-and-reload job on a table that is not captured, next to normal OLTP traffic. */
  static final List<RedoProfile.SampleRow> SAMPLE =
      List.of(
          row("STAGE", "PRICES", "DDL", 2, 2),
          row("STAGE", "PRICES", "INSERT", 50_000, 0),
          row("APP", "ORDERS", "INSERT", 3_000, 0),
          row("APP", "ORDERS", "UPDATE", 2_000, 0),
          row("APP", "ORDERS", "DELETE", 100, 0),
          row("APP", "ORDERS", "LOB_WRITE", 50, 0),
          row("ETL", "SWAP", "DELETE", 4_000, 0),
          row("ETL", "SWAP", "DIRECT INSERT", 4_200, 0),
          row(null, null, "START", 900, 0),
          row(null, null, "COMMIT", 900, 0),
          row(null, null, "INTERNAL", 30, 0),
          new RedoProfile.SampleRow("FREEPDB1", "APP", null, "UNSUPPORTED", 5, 0));

  @Test
  void sampleFlagsReloadsAndEstimatesTheDiscardedShare() {
    RedoProfile.Sample s =
        RedoProfile.sample(
            SAMPLE, true, List.of("FREEPDB1\\.APP\\..*"), List.of(), 100, 200, 2, 512 * MIB);
    assertThat(s.totalRows()).isEqualTo(65_187);
    assertThat(s.controlRows()).isEqualTo(1_800);
    assertThat(s.unattributedRows()).isEqualTo(35);
    assertThat(s.tables())
        .extracting(RedoProfile.TableProfile::name)
        .containsExactly("FREEPDB1.STAGE.PRICES", "FREEPDB1.ETL.SWAP", "FREEPDB1.APP.ORDERS");
    RedoProfile.TableProfile prices = s.tables().get(0);
    assertThat(prices.captured()).isFalse();
    assertThat(prices.truncates()).isEqualTo(2);
    assertThat(prices.pattern()).isEqualTo("truncate and reload");
    assertThat(s.tables().get(1).pattern()).isEqualTo("delete and reload");
    RedoProfile.TableProfile orders = s.tables().get(2);
    assertThat(orders.captured()).isTrue();
    assertThat(orders.other()).isEqualTo(50);
    assertThat(orders.pattern()).isNull();
    assertThat(s.capturedRows()).isEqualTo(5_150);
    assertThat(s.tableRows()).isEqualTo(63_352);
    assertThat(s.discardShare()).isBetween(0.918, 0.919);

    String md =
        RedoProfile.toMarkdown(
            NOW.minus(Duration.ofHours(2)),
            NOW,
            List.of(new RedoProfile.HourRate(NOW.minus(Duration.ofHours(1)), 1, 4, 800 * MIB)),
            s,
            2);
    assertThat(md)
        .contains("| 2026-10-05T11:00:00Z | 1 | 4 | 800 MiB |")
        .contains("| FREEPDB1.STAGE.PRICES | no | 50002 | 78.9 per cent |")
        .doesNotContain("| FREEPDB1.APP.ORDERS |")
        .contains("filters out the other 91.9 per cent")
        .contains(
            "The top redo source, FREEPDB1.STAGE.PRICES, is not captured and looks like a"
                + " truncate and reload job")
        .contains("FREEPDB1.ETL.SWAP looks like a delete and reload job.");
  }

  @Test
  void nonCdbNamesAndExcludes() {
    RedoProfile.Sample s =
        RedoProfile.sample(
            List.of(
                new RedoProfile.SampleRow("ORCL", "APP", "A", "INSERT", 10, 0),
                new RedoProfile.SampleRow("ORCL", "APP", "B", "UPDATE", 10, 0)),
            false,
            List.of(),
            List.of("APP\\.B"),
            1,
            2,
            1,
            MIB);
    assertThat(s.tables())
        .extracting(RedoProfile.TableProfile::name, RedoProfile.TableProfile::captured)
        .containsExactlyInAnyOrder(
            org.assertj.core.groups.Tuple.tuple("APP.A", true),
            org.assertj.core.groups.Tuple.tuple("APP.B", false));
    assertThat(s.discardShare()).isEqualTo(0.5);
    assertThat(
            RedoProfile.sample(List.of(), false, List.of(), List.of(), 1, 2, 1, MIB).discardShare())
        .isZero();
  }

  @Test
  void hourlyRatesAndSampleRange() {
    List<ArchiveStat> h =
        List.of(
            new ArchiveStat(1, 7, 100, 200, null, NOW.minus(Duration.ofMinutes(150)), 10, false),
            new ArchiveStat(1, 8, 200, 300, null, NOW.minus(Duration.ofMinutes(50)), 20, false),
            new ArchiveStat(2, 3, 150, 320, null, NOW.minus(Duration.ofMinutes(40)), 30, false),
            new ArchiveStat(1, 9, 300, 400, null, NOW.minus(Duration.ofMinutes(10)), 40, false),
            new ArchiveStat(1, 6, 0, 100, null, NOW.minus(Duration.ofDays(1)), 5, true),
            new ArchiveStat(1, 10, 400, 500, null, null, 5, false));
    assertThat(RedoProfile.hourly(h, NOW.minus(Duration.ofHours(2)), NOW))
        .containsExactly(
            new RedoProfile.HourRate(Instant.parse("2026-10-05T11:00:00Z"), 1, 2, 60),
            new RedoProfile.HourRate(Instant.parse("2026-10-05T11:00:00Z"), 2, 1, 30));
    assertThat(RedoProfile.sampleRange(h, 1)).containsExactly(300, 399);
    assertThat(RedoProfile.sampleRange(h, 3)).containsExactly(150, 399);
    assertThat(RedoProfile.sampleRange(List.of(), 3)).isNull();
    assertThat(RedoProfile.toMarkdown(NOW, NOW, List.of(), null, 5))
        .contains("No log was archived")
        .contains("No archived log is present to sample");
  }
}
