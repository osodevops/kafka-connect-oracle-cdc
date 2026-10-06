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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

/**
 * Samples the Connect worker every minute into {@code metrics.csv}: worker heap, the connector's
 * lag and queue gauges from the JMX exporter (ops/jmx-exporter, names from the generated metrics
 * reference), and the workload's committed and rolled-back counters. A series the endpoint does not
 * publish leaves its cell empty; a failed scrape leaves every metric cell empty and is counted.
 * Without a metrics URL only the workload counters are sampled.
 */
public final class MetricsSampler implements Soak.Sampler {

  /** Fetches the exporter's text; null scraper means no metrics URL was given. */
  public interface Scraper {
    String scrape() throws Exception;
  }

  /** A column read from a scrape. */
  private record Metric(String column, List<String> names, boolean connector, boolean sum) {}

  /**
   * Heap: {@code jvm_memory_used_bytes} is the JMX exporter 1.x name, {@code jvm_memory_bytes_used}
   * the 0.x one; both carry {@code area="heap"}. Connector series carry {@code server=<topic
   * prefix>}: gauges take the largest series, counters the sum.
   */
  private static final List<Metric> METRICS =
      List.of(
          new Metric(
              "heap_used_bytes",
              List.of("jvm_memory_used_bytes", "jvm_memory_bytes_used"),
              false,
              true),
          new Metric(
              "millis_behind_source", List.of("oracle_cdc_millis_behind_source"), true, false),
          new Metric("scn_lag", List.of("oracle_cdc_scn_lag"), true, false),
          new Metric("queue_depth", List.of("oracle_cdc_queue_depth"), true, false),
          new Metric("buffered_events", List.of("oracle_cdc_buffered_events"), true, false),
          new Metric("buffer_heap_bytes", List.of("oracle_cdc_buffer_heap_bytes"), true, false),
          new Metric("open_transactions", List.of("oracle_cdc_open_transactions"), true, false),
          new Metric("spilled_bytes", List.of("oracle_cdc_spilled_bytes"), true, false),
          new Metric(
              "connector_transactions_committed",
              List.of("oracle_cdc_transactions_committed_total"),
              true,
              true),
          new Metric("reconnects", List.of("oracle_cdc_reconnects_total"), true, true),
          new Metric("step_retries", List.of("oracle_cdc_step_retries_total"), true, true));

  static final String HEADER;

  static {
    StringBuilder h = new StringBuilder("timestamp,elapsed_seconds,phase");
    for (Metric m : METRICS) {
      h.append(',').append(m.column());
    }
    h.append(",workload_committed,workload_rolled_back");
    HEADER = h.toString();
  }

  /** One row, kept for the summary aggregates. */
  record Row(long at, String phase, Map<String, Double> values) {}

  private final Scraper scraper;
  private final String server;
  private final LongSupplier committed;
  private final LongSupplier rolledBack;
  private final java.nio.file.Path csv;
  private final Soak.Clock clock;
  private final java.util.function.Consumer<String> log;
  private final long intervalMillis;
  private final List<Row> rows = new ArrayList<>();
  private volatile String phase = "start";
  private long firstAt = -1;
  private int scrapeFailures;
  private boolean failureLogged;
  private ScheduledExecutorService scheduler;

  public MetricsSampler(
      Scraper scraper,
      String server,
      LongSupplier committed,
      LongSupplier rolledBack,
      java.nio.file.Path csv,
      Soak.Clock clock,
      java.util.function.Consumer<String> log,
      long intervalMillis) {
    this.scraper = scraper;
    this.server = server;
    this.committed = committed;
    this.rolledBack = rolledBack;
    this.csv = csv;
    this.clock = clock;
    this.log = log;
    this.intervalMillis = intervalMillis;
  }

  @Override
  public synchronized void start() {
    SoakOutput.append(csv, HEADER + "\n");
    scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "soak-metrics");
              t.setDaemon(true);
              return t;
            });
    scheduler.scheduleAtFixedRate(this::sampleQuietly, 0, intervalMillis, TimeUnit.MILLISECONDS);
  }

  @Override
  public void phase(String p) {
    this.phase = p;
  }

  @Override
  public void stop() {
    ScheduledExecutorService s;
    synchronized (this) {
      s = scheduler;
      scheduler = null;
    }
    if (s != null) {
      s.shutdownNow();
      try {
        s.awaitTermination(30, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      sampleQuietly(); // the state at the end
    }
  }

  private void sampleQuietly() {
    try {
      sampleOnce();
    } catch (RuntimeException e) {
      synchronized (this) {
        scrapeFailures++;
      }
    }
  }

  /** Takes one sample and appends its row. */
  public synchronized void sampleOnce() {
    long now = clock.nowMillis();
    if (firstAt < 0) {
      firstAt = now;
    }
    Map<String, Double> values = new LinkedHashMap<>();
    if (scraper != null) {
      try {
        values.putAll(read(PrometheusText.parse(scraper.scrape()), server));
      } catch (Exception e) {
        scrapeFailures++;
        if (!failureLogged) {
          failureLogged = true;
          log.accept(
              "metrics scrape failed ("
                  + e.getClass().getSimpleName()
                  + "); the row keeps the workload counters only. Further failures are counted"
                  + " in the summary");
        }
      }
    }
    String ph = phase;
    rows.add(new Row(now, ph, values));
    StringBuilder line =
        new StringBuilder()
            .append(SoakSummary.iso(now))
            .append(',')
            .append((now - firstAt) / 1000)
            .append(',')
            .append(ph);
    for (Metric m : METRICS) {
      Double v = values.get(m.column());
      line.append(',').append(v == null ? "" : cell(v));
    }
    line.append(',').append(committed.getAsLong()).append(',').append(rolledBack.getAsLong());
    SoakOutput.append(csv, line.append('\n').toString());
  }

  /** The columns a scrape provides; a series that is absent is left out. */
  static Map<String, Double> read(PrometheusText text, String server) {
    Map<String, Double> out = new LinkedHashMap<>();
    for (Metric m : METRICS) {
      Predicate<Map<String, String>> labels =
          m.connector()
              ? l -> server == null || server.equals(l.get("server"))
              : l -> "heap".equals(l.get("area"));
      for (String name : m.names()) {
        OptionalDouble v = m.sum() ? text.sum(name, labels) : text.max(name, labels);
        if (v.isPresent()) {
          out.put(m.column(), v.getAsDouble());
          break;
        }
      }
    }
    return out;
  }

  private static String cell(double v) {
    return v == Math.rint(v) && !Double.isInfinite(v)
        ? Long.toString((long) v)
        : Double.toString(v);
  }

  /** Aggregates for the summary: heap in the first and last hour, maxima of lag and queue. */
  @Override
  public synchronized Map<String, Object> summary() {
    Map<String, Object> s = new LinkedHashMap<>();
    s.put("samples", rows.size());
    s.put("scrapeFailures", scrapeFailures);
    if (rows.isEmpty()) {
      return s;
    }
    long first = rows.get(0).at();
    long last = rows.get(rows.size() - 1).at();
    List<Double> firstHour = new ArrayList<>();
    List<Double> lastHour = new ArrayList<>();
    for (Row r : rows) {
      Double h = r.values().get("heap_used_bytes");
      if (h == null) {
        continue;
      }
      if (r.at() < first + 3_600_000) {
        firstHour.add(h);
      }
      if (r.at() > last - 3_600_000) {
        lastHour.add(h);
      }
    }
    s.put("heapFirstHourMeanBytes", mean(firstHour));
    s.put("heapFirstHourMaxBytes", max(firstHour));
    s.put("heapLastHourMeanBytes", mean(lastHour));
    s.put("heapLastHourMaxBytes", max(lastHour));
    s.put("heapMaxBytes", maxOf("heap_used_bytes"));
    s.put("millisBehindSourceMax", maxOf("millis_behind_source"));
    s.put("scnLagMax", maxOf("scn_lag"));
    s.put("queueDepthMax", maxOf("queue_depth"));
    s.put("bufferHeapBytesMax", maxOf("buffer_heap_bytes"));
    s.put("spilledBytesMax", maxOf("spilled_bytes"));
    return s;
  }

  synchronized List<Row> rows() {
    return List.copyOf(rows);
  }

  private Long maxOf(String column) {
    Double m = null;
    for (Row r : rows) {
      Double v = r.values().get(column);
      if (v != null && (m == null || v > m)) {
        m = v;
      }
    }
    return m == null ? null : Math.round(m);
  }

  private static Long mean(List<Double> v) {
    return v.isEmpty()
        ? null
        : Math.round(v.stream().mapToDouble(Double::doubleValue).average().orElse(0));
  }

  private static Long max(List<Double> v) {
    return v.isEmpty()
        ? null
        : Math.round(v.stream().mapToDouble(Double::doubleValue).max().orElse(0));
  }
}
