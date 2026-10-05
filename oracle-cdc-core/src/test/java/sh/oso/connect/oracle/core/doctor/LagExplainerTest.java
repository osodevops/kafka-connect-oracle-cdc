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

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.doctor.LagExplainer.Cause;

/** The three scripted bottlenecks of PRD-05 section 7, plus replays and a healthy task. */
class LagExplainerTest {

  static final Instant T0 = Instant.parse("2026-10-05T12:00:00Z");
  static final LagExplainer.Settings SETTINGS = new LagExplainer.Settings(8000, 1000);

  private static Map<String, Long> base() {
    Map<String, Long> m = new HashMap<>();
    m.put("Steps", 100L);
    m.put("RowsMined", 10_000L);
    m.put("EventsApplied", 9_000L);
    m.put("StepTimeouts", 0L);
    m.put("StepRetries", 0L);
    m.put("TransactionsCommitted", 500L);
    m.put("LagReplays", 0L);
    m.put("LastStepMillis", 300L);
    m.put("WindowLogs", 1L);
    m.put("MinedToScn", 5_000L);
    m.put("SafeEndScn", 5_010L);
    m.put("ScnLag", 10L);
    m.put("OpenTransactions", 2L);
    m.put("BufferedEvents", 20L);
    m.put("BufferHeapBytes", 4096L);
    m.put("BufferMemoryMaxBytes", 1L << 30);
    m.put("OldestOpenScn", 4_990L);
    m.put("SpilledTransactions", 0L);
    m.put("SpilledBytes", 0L);
    m.put("JournaledTransactions", 0L);
    m.put("QueueDepth", 10L);
    m.put("MillisBehindSource", 400L);
    return m;
  }

  private static MetricsSample at(
      int seconds, Map<String, Long> values, List<MetricsSample.OpenTransaction> largest) {
    return new MetricsSample(T0.plusSeconds(seconds), values, largest);
  }

  private static Map<String, Long> with(Map<String, Long> m, Object... kv) {
    Map<String, Long> out = new HashMap<>(m);
    for (int i = 0; i < kv.length; i += 2) {
      out.put((String) kv[i], ((Number) kv[i + 1]).longValue());
    }
    return out;
  }

  @Test
  void aHealthyTaskKeepsUp() {
    LagExplainer.Explanation e =
        LagExplainer.explain(
            at(0, base(), List.of()),
            at(10, with(base(), "Steps", 110, "TransactionsCommitted", 600), List.of()),
            SETTINGS);
    assertThat(e.top().cause()).isEqualTo(Cause.NONE);
    assertThat(e.top().text()).contains("keeps up");
    assertThat(e.breakdown()).containsEntry("Commits", "10.0 transactions per second queued");
    assertThat(LagExplainer.toMarkdown(e)).contains("| Record queue | 10 of 8000 records");
  }

  @Test
  void slowKafkaShowsAsAFullQueue() {
    Map<String, Long> full = with(base(), "QueueDepth", 7_900, "MillisBehindSource", 45_000);
    LagExplainer.Explanation e =
        LagExplainer.explain(
            at(0, full, List.of()),
            at(10, with(full, "QueueDepth", 8_000, "Steps", 101), List.of()),
            SETTINGS);
    assertThat(e.top().cause()).isEqualTo(Cause.KAFKA);
    assertThat(e.top().text()).contains("8000 of 8000").contains("Kafka Connect");
    assertThat(e.causes()).extracting(LagExplainer.Diagnosis::cause).doesNotContain(Cause.MINING);
  }

  @Test
  void slowMiningShowsAsLongOrTimedOutSteps() {
    Map<String, Long> slow = with(base(), "LastStepMillis", 9_000, "WindowLogs", 6);
    LagExplainer.Explanation e =
        LagExplainer.explain(
            at(0, with(slow, "MillisBehindSource", 30_000), List.of()),
            at(
                30,
                with(slow, "MillisBehindSource", 60_000, "StepTimeouts", 2, "ScnLag", 50_000),
                List.of()),
            SETTINGS);
    assertThat(e.top().cause()).isEqualTo(Cause.MINING);
    assertThat(e.top().text())
        .contains("9000 ms over 6 logs")
        .contains("2 steps hit cdc.mining.query.timeout.ms")
        .contains("redo-profile");
    LagExplainer.Explanation noTimeouts =
        LagExplainer.explain(at(0, slow, List.of()), at(30, slow, List.of()), SETTINGS);
    assertThat(noTimeouts.top().cause()).isEqualTo(Cause.MINING);
    assertThat(noTimeouts.top().score()).isLessThan(e.top().score());
  }

  @Test
  void aLargeTransactionIsNamed() {
    MetricsSample.OpenTransaction big =
        new MetricsSample.OpenTransaction(
            "5.12.900", "BATCH", 600_000, 4_000, 2_500_000, 300L << 20, 0, false);
    MetricsSample.OpenTransaction small =
        new MetricsSample.OpenTransaction("5.1.1", "APP", 200, 4_990, 3, 512, 0, false);
    Map<String, Long> m =
        with(
            base(),
            "BufferedEvents",
            2_500_003,
            "BufferHeapBytes",
            300L << 20,
            "MillisBehindSource",
            600_000);
    LagExplainer.Explanation e =
        LagExplainer.explain(
            at(0, m, List.of(big, small)), at(10, m, List.of(big, small)), SETTINGS);
    assertThat(e.top().cause()).isEqualTo(Cause.LARGE_TRANSACTION);
    assertThat(e.top().text())
        .contains("Transaction 5.12.900 of user BATCH")
        .contains("600 s with 2500000 changes")
        .contains("300 MiB on heap");

    // without LargestTransactions (Prometheus) the buffer figures still point at it
    Map<String, Long> spilled = with(m, "SpilledBytes", 64L << 20, "OpenTransactions", 1);
    LagExplainer.Explanation p =
        LagExplainer.explain(at(0, spilled, List.of()), at(10, spilled, List.of()), SETTINGS);
    assertThat(p.top().cause()).isEqualTo(Cause.LARGE_TRANSACTION);
    assertThat(p.top().text()).contains("64 MiB spilled").contains("LargestTransactions");

    LagExplainer.Explanation smallOnly =
        LagExplainer.explain(
            at(0, base(), List.of(small)), at(10, base(), List.of(small)), SETTINGS);
    assertThat(smallOnly.top().cause()).isEqualTo(Cause.NONE);
  }

  @Test
  void replaysAndUnclearLag() {
    Map<String, Long> lagging = with(base(), "MillisBehindSource", 20_000);
    LagExplainer.Explanation replay =
        LagExplainer.explain(
            at(0, lagging, List.of()), at(10, with(lagging, "LagReplays", 3), List.of()), SETTINGS);
    assertThat(replay.top().cause()).isEqualTo(Cause.LAG_REPLAY);
    assertThat(replay.top().text()).startsWith("3 steps were mined a second time");

    LagExplainer.Explanation unclear =
        LagExplainer.explain(at(0, lagging, List.of()), at(10, lagging, List.of()), SETTINGS);
    assertThat(unclear.top().cause()).isEqualTo(Cause.UNCLEAR);
    assertThat(unclear.top().text()).contains("20000 ms behind");

    Map<String, Long> missing = new HashMap<>();
    LagExplainer.Explanation empty =
        LagExplainer.explain(at(0, missing, List.of()), at(1, missing, List.of()), SETTINGS);
    assertThat(empty.breakdown()).containsEntry("Behind source", "unknown");
    assertThat(at(0, missing, List.of()).get("Steps")).isEqualTo(-1);
  }

  @Test
  void otherContributorsAreListed() {
    Map<String, Long> both =
        with(base(), "QueueDepth", 8_000, "MillisBehindSource", 90_000, "SpilledBytes", 1L << 20);
    LagExplainer.Explanation e =
        LagExplainer.explain(
            at(0, both, List.of()), at(10, with(both, "LagReplays", 1), List.of()), SETTINGS);
    assertThat(e.top().cause()).isEqualTo(Cause.KAFKA);
    assertThat(LagExplainer.toMarkdown(e)).contains("Other contributors:").contains("spilled");
  }
}
