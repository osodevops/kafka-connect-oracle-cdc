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
package sh.oso.connect.oracle.bench.soak;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The exporter's text, the columns taken from it and the summary aggregates. */
class MetricsSamplerTest {

  /** What the JMX exporter 1.x agent serves for one task with prefix cdc, abridged. */
  static final String SAMPLE =
      String.join(
          "\n",
          "# HELP jvm_memory_used_bytes Used bytes of a given JVM memory area.",
          "# TYPE jvm_memory_used_bytes gauge",
          "jvm_memory_used_bytes{area=\"heap\"} 2.68435456E8",
          "jvm_memory_used_bytes{area=\"nonheap\"} 9.1E7",
          "# HELP oracle_cdc_millis_behind_source Time between the commit of the last"
              + " transaction queued and its queueing, in milliseconds.",
          "# TYPE oracle_cdc_millis_behind_source gauge",
          "oracle_cdc_millis_behind_source{server=\"cdc\",} 250.0",
          "oracle_cdc_millis_behind_source{server=\"other\",} 99999.0",
          "oracle_cdc_scn_lag{server=\"cdc\",} 12.0",
          "oracle_cdc_queue_depth{server=\"cdc\",} 3.0",
          "oracle_cdc_open_transactions{server=\"cdc\",} NaN",
          "oracle_cdc_transactions_committed_total{server=\"cdc\",} 1500.0 1759740000000",
          "oracle_cdc_reconnects_total{server=\"cdc\",} 0.0",
          "kafka_connect_label_test{note=\"a \\\"quoted\\\", value\",server=\"cdc\"} 7",
          "this line is not a sample",
          "");

  @TempDir Path dir;

  @Test
  void parsesSampleLinesAndSkipsCommentsAndNoise() {
    PrometheusText t = PrometheusText.parse(SAMPLE);

    assertThat(t.samples())
        .extracting(PrometheusText.Sample::name)
        .contains("jvm_memory_used_bytes");
    assertThat(t.max("jvm_memory_used_bytes", l -> "heap".equals(l.get("area"))))
        .hasValue(268_435_456d);
    assertThat(t.max("oracle_cdc_millis_behind_source", l -> "cdc".equals(l.get("server"))))
        .hasValue(250d);
    PrometheusText.Sample quoted =
        t.samples().stream()
            .filter(s -> s.name().equals("kafka_connect_label_test"))
            .findFirst()
            .orElseThrow();
    assertThat(quoted.labels()).containsEntry("note", "a \"quoted\", value");
    assertThat(quoted.value()).isEqualTo(7d);
    assertThat(t.samples()).noneMatch(s -> s.name().equals("this"));
    assertThat(PrometheusText.parseLine("x{a=\"unterminated} 1")).isNull();
    assertThat(PrometheusText.parseLine("x 1e3"))
        .extracting(PrometheusText.Sample::value)
        .isEqualTo(1000d);
    assertThat(PrometheusText.number("+Inf")).isInfinite();
  }

  @Test
  void readsTheChosenSeriesForThePrefixAndLeavesMissingOnesOut() {
    Map<String, Double> v = MetricsSampler.read(PrometheusText.parse(SAMPLE), "cdc");

    assertThat(v)
        .containsEntry("heap_used_bytes", 268_435_456d)
        .containsEntry("millis_behind_source", 250d) // not the other connector's 99999
        .containsEntry("scn_lag", 12d)
        .containsEntry("queue_depth", 3d)
        .containsEntry("connector_transactions_committed", 1500d)
        .containsEntry("reconnects", 0d);
    // NaN is no value; series the endpoint does not publish are absent, not zero
    assertThat(v)
        .doesNotContainKeys(
            "open_transactions",
            "buffered_events",
            "buffer_heap_bytes",
            "spilled_bytes",
            "step_retries");
  }

  @Test
  void theOlderExporterHeapNameIsReadToo() {
    Map<String, Double> v =
        MetricsSampler.read(
            PrometheusText.parse("jvm_memory_bytes_used{area=\"heap\",} 1.0E8\n"), "cdc");
    assertThat(v).containsOnlyKeys("heap_used_bytes").containsEntry("heap_used_bytes", 1.0e8);
  }

  @Test
  void writesOneCsvRowPerSampleWithEmptyCellsForMissingSeries() throws Exception {
    SoakTest.FakeClock clock = new SoakTest.FakeClock();
    AtomicLong committed = new AtomicLong(10);
    Path csv = dir.resolve("metrics.csv");
    List<String> logged = new ArrayList<>();
    String[] text = {SAMPLE};
    MetricsSampler s =
        new MetricsSampler(
            () -> {
              if (text[0] == null) {
                throw new java.io.IOException("connection refused");
              }
              return text[0];
            },
            "cdc",
            committed::get,
            () -> 2,
            csv,
            clock,
            logged::add,
            60_000);
    Files.writeString(csv, MetricsSampler.HEADER + "\n");
    s.phase("workload");
    s.sampleOnce();
    clock.now += 60_000;
    committed.set(25);
    text[0] = null; // the worker is restarting
    s.sampleOnce();
    s.sampleOnce();

    List<String> lines = Files.readAllLines(csv);
    assertThat(lines).hasSize(4);
    assertThat(lines.get(0))
        .startsWith("timestamp,elapsed_seconds,phase,heap_used_bytes,millis_behind_source")
        .endsWith("workload_committed,workload_rolled_back");
    String[] first = lines.get(1).split(",", -1);
    String[] header = lines.get(0).split(",", -1);
    assertThat(first).hasSameSizeAs(header);
    assertThat(cell(header, first, "phase")).isEqualTo("workload");
    assertThat(cell(header, first, "heap_used_bytes")).isEqualTo("268435456");
    assertThat(cell(header, first, "millis_behind_source")).isEqualTo("250");
    assertThat(cell(header, first, "open_transactions")).isEmpty();
    assertThat(cell(header, first, "workload_committed")).isEqualTo("10");
    String[] second = lines.get(2).split(",", -1);
    assertThat(cell(header, second, "elapsed_seconds")).isEqualTo("60");
    assertThat(cell(header, second, "heap_used_bytes")).isEmpty();
    assertThat(cell(header, second, "workload_committed")).isEqualTo("25");
    assertThat(s.summary()).containsEntry("samples", 3).containsEntry("scrapeFailures", 2);
    assertThat(logged).hasSize(1); // the first failure only
  }

  @Test
  void summarisesHeapInTheFirstAndLastHourAndTheMaxima() {
    SoakTest.FakeClock clock = new SoakTest.FakeClock();
    long[] heap = {0};
    long[] lag = {0};
    MetricsSampler s =
        new MetricsSampler(
            () ->
                "jvm_memory_used_bytes{area=\"heap\"} "
                    + heap[0]
                    + "\noracle_cdc_millis_behind_source{server=\"cdc\"} "
                    + lag[0]
                    + "\n",
            "cdc",
            () -> 0,
            () -> 0,
            dir.resolve("m.csv"),
            clock,
            m -> {},
            60_000);
    // three hours, a sample a minute: heap 100 MB in hour one, 300 MB in hour three, a 500 MB
    // spike in hour two; lag peaks at 9 s once
    for (int minute = 0; minute < 180; minute++) {
      heap[0] = minute < 60 ? 100_000_000 : minute < 120 ? 200_000_000 : 300_000_000;
      if (minute == 90) {
        heap[0] = 500_000_000;
        lag[0] = 9_000;
      } else {
        lag[0] = 100;
      }
      s.sampleOnce();
      clock.now += 60_000;
    }
    Map<String, Object> m = s.summary();
    assertThat(m)
        .containsEntry("samples", 180)
        .containsEntry("heapFirstHourMeanBytes", 100_000_000L)
        .containsEntry("heapFirstHourMaxBytes", 100_000_000L)
        .containsEntry("heapLastHourMeanBytes", 300_000_000L)
        .containsEntry("heapMaxBytes", 500_000_000L)
        .containsEntry("millisBehindSourceMax", 9_000L);
    assertThat(m.get("scnLagMax")).isNull(); // never published: not sampled, not zero
  }

  @Test
  void withoutAMetricsUrlOnlyTheWorkloadCountersAreSampled() throws Exception {
    SoakTest.FakeClock clock = new SoakTest.FakeClock();
    Path csv = dir.resolve("w.csv");
    MetricsSampler s = new MetricsSampler(null, "cdc", () -> 7, () -> 1, csv, clock, m -> {}, 1000);
    s.sampleOnce();
    String row = Files.readAllLines(csv).get(0);
    assertThat(row).endsWith(",7,1");
    Map<String, Object> m = s.summary();
    assertThat(m).containsEntry("scrapeFailures", 0);
    assertThat(m.get("heapMaxBytes")).isNull();
  }

  private static String cell(String[] header, String[] row, String column) {
    for (int i = 0; i < header.length; i++) {
      if (header[i].equals(column)) {
        return row[i];
      }
    }
    throw new AssertionError("no column " + column);
  }
}
