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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import sh.oso.connect.oracle.bench.check.CheckReport;

/**
 * What a soak did: plan, configuration (never a password), segments, every check with its catch-up
 * time and evidence file, workload counters and metric aggregates. Written as {@code summary.json}
 * and {@code summary.md}; the figures are internal and are not published.
 */
public final class SoakSummary {

  public static final String REPORT = "oracle-cdc-soak";
  public static final int SCHEMA_VERSION = 1;

  /** Printed at the top of both files (CLAUDE.md: never publish database benchmark figures). */
  public static final String NOTICE =
      "Internal figures for the release gate. Do not publish them: Oracle's development licence"
          + " terms forbid disclosing database benchmark results without Oracle's consent, so OSO"
          + " publishes the harness and the method, never the figures.";

  /** One check point. */
  public record Check(
      int index,
      String kind,
      long atMillis,
      double elapsedHours,
      long scn,
      long catchUpMillis,
      long checkMillis,
      CheckReport.Verdict verdict,
      long checkScn,
      long recordsConsumed,
      List<String> failures,
      String inconclusiveReason,
      String evidence,
      String sha256,
      long harnessHeapPeakBytes) {

    String failuresText() {
      return failures.isEmpty() ? "" : String.join("; ", failures);
    }
  }

  /** One stretch of running workload between two pauses. */
  public record Segment(
      int index, long startMillis, long endMillis, long committed, long rolledBack) {}

  private Map<String, Object> configuration = new LinkedHashMap<>();
  private Map<String, Object> spec = new LinkedHashMap<>();
  private Soak.Plan plan;
  private long startedAt;
  private long endedAt;
  private long workloadStartedAt = -1;
  private Soak.Outcome outcome = Soak.Outcome.ERROR;
  private String reason = "the soak did not finish";
  private String scnMethod;
  private Long resetScn;
  private Long resetCatchUpMillis;
  private final List<Segment> segments = new ArrayList<>();
  private final List<Check> checks = new ArrayList<>();
  private long committed;
  private long rolledBack;
  private Map<String, Object> metrics = Map.of();
  private int skippedCheckPoints;
  private int suspensions;
  private long suspendedMillis;
  private int probeErrors;
  private long harnessMaxHeapBytes = Runtime.getRuntime().maxMemory();

  public synchronized void configuration(Map<String, Object> c) {
    this.configuration = new LinkedHashMap<>(c);
  }

  public synchronized void spec(Map<String, Object> s) {
    this.spec = new LinkedHashMap<>(s);
  }

  synchronized void plan(Soak.Plan p) {
    this.plan = p;
  }

  synchronized void startedAt(long millis) {
    this.startedAt = millis;
  }

  synchronized void endedAt(long millis) {
    this.endedAt = millis;
  }

  synchronized void workloadStartedAt(long millis) {
    this.workloadStartedAt = millis;
  }

  synchronized void outcome(Soak.Outcome o, String why) {
    this.outcome = o;
    this.reason = why;
  }

  public synchronized Soak.Outcome outcome() {
    return outcome;
  }

  public synchronized String reason() {
    return reason;
  }

  synchronized void scnMethod(String m) {
    this.scnMethod = m;
  }

  synchronized void reset(long scn, long catchUpMillis) {
    this.resetScn = scn;
    this.resetCatchUpMillis = catchUpMillis;
  }

  synchronized void segment(long start, long end, long committedDelta, long rolledBackDelta) {
    segments.add(new Segment(segments.size() + 1, start, end, committedDelta, rolledBackDelta));
  }

  synchronized void check(Check c) {
    checks.add(c);
  }

  public synchronized List<Check> checks() {
    return List.copyOf(checks);
  }

  public synchronized List<Segment> segments() {
    return List.copyOf(segments);
  }

  synchronized void workload(long committedTotal, long rolledBackTotal) {
    this.committed = committedTotal;
    this.rolledBack = rolledBackTotal;
  }

  synchronized void metrics(Map<String, Object> m) {
    this.metrics = m == null ? Map.of() : new LinkedHashMap<>(m);
  }

  synchronized void skippedCheckPoint() {
    skippedCheckPoints++;
  }

  synchronized void suspended(long gapMillis) {
    suspensions++;
    suspendedMillis += gapMillis;
  }

  synchronized void probeError() {
    probeErrors++;
  }

  public synchronized int suspensions() {
    return suspensions;
  }

  synchronized void harnessMaxHeapBytes(long bytes) {
    this.harnessMaxHeapBytes = bytes;
  }

  /** The summary as an ordered document. */
  public synchronized Map<String, Object> document() {
    Map<String, Object> d = new LinkedHashMap<>();
    d.put("report", REPORT);
    d.put("schemaVersion", SCHEMA_VERSION);
    d.put("notice", NOTICE);
    d.put("outcome", outcome.name());
    d.put("reason", reason);
    d.put("startedAt", iso(startedAt));
    d.put("endedAt", iso(endedAt));
    d.put("durationSeconds", Math.max(0, endedAt - startedAt) / 1000);
    d.put("workloadStartedAt", workloadStartedAt < 0 ? null : iso(workloadStartedAt));
    if (plan != null) {
      Map<String, Object> p = new LinkedHashMap<>();
      p.put("hours", plan.total().toMillis() / 3_600_000d);
      p.put("checkEveryHours", plan.checkEvery().toMillis() / 3_600_000d);
      p.put("catchUpTimeoutMinutes", plan.catchUpTimeout().toMinutes());
      d.put("plan", p);
    }
    d.put("configuration", configuration);
    d.put("spec", spec);
    d.put("scnMethod", scnMethod);
    Map<String, Object> r = new LinkedHashMap<>();
    r.put("scn", resetScn);
    r.put("catchUpSeconds", resetCatchUpMillis == null ? null : resetCatchUpMillis / 1000d);
    d.put("reset", r);
    List<Map<String, Object>> segs = new ArrayList<>();
    for (Segment s : segments) {
      Map<String, Object> m = new LinkedHashMap<>();
      m.put("index", s.index());
      m.put("startedAt", iso(s.startMillis()));
      m.put("endedAt", iso(s.endMillis()));
      m.put("seconds", (s.endMillis() - s.startMillis()) / 1000);
      m.put("committed", s.committed());
      m.put("rolledBack", s.rolledBack());
      segs.add(m);
    }
    d.put("segments", segs);
    List<Map<String, Object>> cs = new ArrayList<>();
    for (Check c : checks) {
      Map<String, Object> m = new LinkedHashMap<>();
      m.put("index", c.index());
      m.put("kind", c.kind());
      m.put("at", iso(c.atMillis()));
      m.put("elapsedHours", Double.valueOf(round(c.elapsedHours(), 3)));
      m.put("scn", c.scn());
      m.put("catchUpSeconds", c.catchUpMillis() / 1000d);
      m.put("checkSeconds", c.checkMillis() / 1000d);
      m.put("verdict", c.verdict().name());
      m.put("checkScn", c.checkScn());
      m.put("recordsConsumed", c.recordsConsumed());
      m.put("failures", c.failures());
      m.put("inconclusiveReason", c.inconclusiveReason());
      m.put("evidence", c.evidence());
      m.put("sha256", c.sha256());
      m.put("harnessHeapPeakBytes", c.harnessHeapPeakBytes());
      cs.add(m);
    }
    d.put("checks", cs);
    Map<String, Object> w = new LinkedHashMap<>();
    w.put("committed", committed);
    w.put("rolledBack", rolledBack);
    d.put("workload", w);
    d.put("metrics", metrics);
    Map<String, Object> h = new LinkedHashMap<>();
    h.put("skippedCheckPoints", skippedCheckPoints);
    h.put("suspectedHostSleeps", suspensions);
    h.put("suspectedHostSleepSeconds", suspendedMillis / 1000);
    h.put("connectProbeErrors", probeErrors);
    h.put("maxHeapBytes", harnessMaxHeapBytes);
    d.put("harness", h);
    return d;
  }

  public synchronized String toJson() {
    try {
      return new ObjectMapper()
              .enable(SerializationFeature.INDENT_OUTPUT)
              .writeValueAsString(document())
          + "\n";
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  /** A readable rendering for the release notes' internal evidence folder. */
  public synchronized String toMarkdown() {
    StringBuilder b = new StringBuilder();
    b.append("# Soak summary\n\n").append(NOTICE).append("\n\n");
    b.append("| Item | Value |\n|---|---|\n");
    row(b, "Outcome", outcome.name());
    if (reason != null) {
      row(b, "Reason", reason);
    }
    row(b, "Started", iso(startedAt));
    row(b, "Ended", iso(endedAt));
    row(b, "Duration", round((endedAt - startedAt) / 3_600_000d, 2) + " hours");
    if (plan != null) {
      row(
          b,
          "Plan",
          round(plan.total().toMillis() / 3_600_000d, 3)
              + " hours, a check every "
              + round(plan.checkEvery().toMillis() / 3_600_000d, 3)
              + " hours, catch-up timeout "
              + plan.catchUpTimeout().toMinutes()
              + " minutes");
    }
    row(b, "SCN read with", scnMethod == null ? "not read" : scnMethod);
    row(
        b,
        "Reset",
        resetScn == null
            ? "not reached"
            : "SCN " + resetScn + ", passed after " + resetCatchUpMillis / 1000 + " s");
    row(b, "Workload", committed + " committed, " + rolledBack + " rolled back");
    row(
        b,
        "Suspected host sleep",
        suspensions == 0
            ? "none"
            : (suspensions == 1 ? "once" : suspensions + " times")
                + ", "
                + suspendedMillis / 60_000
                + " minutes in total");
    row(b, "Skipped check points", Integer.toString(skippedCheckPoints));
    b.append("\n## Checks\n\n");
    if (checks.isEmpty()) {
      b.append("No check ran.\n");
    } else {
      b.append(
          "| Check | Kind | Hours | SCN | Catch-up s | Check s | Records | Verdict | Evidence |\n");
      b.append("|---|---|---|---|---|---|---|---|---|\n");
      for (Check c : checks) {
        b.append("| ")
            .append(c.index())
            .append(" | ")
            .append(c.kind())
            .append(" | ")
            .append(round(c.elapsedHours(), 2))
            .append(" | ")
            .append(c.scn())
            .append(" | ")
            .append(round(c.catchUpMillis() / 1000d, 1))
            .append(" | ")
            .append(round(c.checkMillis() / 1000d, 1))
            .append(" | ")
            .append(c.recordsConsumed())
            .append(" | ")
            .append(c.verdict())
            .append(" | ")
            .append(c.evidence())
            .append(" |\n");
      }
      for (Check c : checks) {
        if (!c.failures().isEmpty()) {
          b.append("\nCheck ").append(c.index()).append(" failures: ").append(c.failuresText());
          b.append('\n');
        }
        if (c.inconclusiveReason() != null) {
          b.append("\nCheck ")
              .append(c.index())
              .append(" inconclusive: ")
              .append(c.inconclusiveReason())
              .append('\n');
        }
      }
    }
    b.append("\n## Segments\n\n");
    if (segments.isEmpty()) {
      b.append("The workload did not run.\n");
    } else {
      b.append("| Segment | Started | Hours | Committed | Rolled back |\n");
      b.append("|---|---|---|---|---|\n");
      for (Segment s : segments) {
        b.append("| ")
            .append(s.index())
            .append(" | ")
            .append(iso(s.startMillis()))
            .append(" | ")
            .append(round((s.endMillis() - s.startMillis()) / 3_600_000d, 2))
            .append(" | ")
            .append(s.committed())
            .append(" | ")
            .append(s.rolledBack())
            .append(" |\n");
      }
    }
    b.append("\n## Metrics\n\n");
    b.append("- Samples: ")
        .append(metrics.getOrDefault("samples", 0))
        .append(", scrape failures: ")
        .append(metrics.getOrDefault("scrapeFailures", 0))
        .append('\n');
    b.append("- Worker heap used in the first hour: ")
        .append(
            meanAndMax(metrics.get("heapFirstHourMeanBytes"), metrics.get("heapFirstHourMaxBytes")))
        .append('\n');
    b.append("- Worker heap used in the last hour: ")
        .append(
            meanAndMax(metrics.get("heapLastHourMeanBytes"), metrics.get("heapLastHourMaxBytes")))
        .append('\n');
    b.append("- Worker heap used, maximum: ").append(mib(metrics.get("heapMaxBytes"))).append('\n');
    b.append("- Lag behind source (`oracle_cdc_millis_behind_source`), maximum: ")
        .append(plain(metrics.get("millisBehindSourceMax"), " ms"))
        .append('\n');
    b.append("- SCN lag (`oracle_cdc_scn_lag`), maximum: ")
        .append(plain(metrics.get("scnLagMax"), ""))
        .append('\n');
    b.append("- Queue depth (`oracle_cdc_queue_depth`), maximum: ")
        .append(plain(metrics.get("queueDepthMax"), ""))
        .append('\n');
    b.append("\nPer-minute samples are in `metrics.csv`, the log in `soak.log`.\n");
    return b.toString();
  }

  private static void row(StringBuilder b, String k, String v) {
    b.append("| ").append(k).append(" | ").append(v.replace("|", "/")).append(" |\n");
  }

  private static String meanAndMax(Object mean, Object max) {
    if (!(mean instanceof Number) || !(max instanceof Number)) {
      return "not sampled";
    }
    return mib(mean) + " mean, " + mib(max) + " maximum";
  }

  private static String mib(Object bytes) {
    if (!(bytes instanceof Number n)) {
      return "not sampled";
    }
    return round(n.doubleValue() / (1024d * 1024d), 1) + " MiB";
  }

  private static String plain(Object v, String unit) {
    if (!(v instanceof Number n)) {
      return "not sampled";
    }
    double d = n.doubleValue();
    return (d == Math.rint(d) ? Long.toString((long) d) : round(d, 2)) + unit;
  }

  static String iso(long millis) {
    return Instant.ofEpochMilli(millis).toString();
  }

  static String round(double v, int places) {
    if (Double.isNaN(v) || Double.isInfinite(v)) {
      return Double.toString(v);
    }
    return BigDecimal.valueOf(v)
        .setScale(places, RoundingMode.HALF_UP)
        .stripTrailingZeros()
        .toPlainString()
        .toLowerCase(Locale.ROOT);
  }
}
