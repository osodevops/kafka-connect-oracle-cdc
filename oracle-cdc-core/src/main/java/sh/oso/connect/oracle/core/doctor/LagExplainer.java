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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * PRD-05 {@code explain-lag}: compares two metric readings of a task and names the most likely
 * bottleneck in plain English. The task's metrics give the mining step time as one figure (query,
 * fetch and decode together), the buffer, the record queue between the engine and Kafka Connect,
 * and the commit-to-queue delay, so the causes are told apart by their signatures: a full queue
 * (Kafka side), slow or timed-out steps (mining), one transaction holding most of the buffer, and
 * steps replayed with a redo dictionary.
 */
public final class LagExplainer {

  /** Below this many milliseconds behind the source the task counts as keeping up. */
  public static final long LAG_MS = 10_000;

  /** A queue at least this full in both readings means Kafka Connect is the bottleneck. */
  public static final double QUEUE_FULL = 0.8;

  /** An open transaction with at least this many changes counts as large. */
  public static final long LARGE_EVENTS = 100_000;

  public enum Cause {
    NONE,
    KAFKA,
    MINING,
    LARGE_TRANSACTION,
    LAG_REPLAY,
    UNCLEAR
  }

  /** A candidate cause, its weight and the sentence that explains it. */
  public record Diagnosis(Cause cause, double score, String text) {}

  /** The record queue's capacity and the configured mining latency target. */
  public record Settings(int queueCapacity, long targetLatencyMs) {}

  /** Ranked causes (the first is the answer) and the figures behind them. */
  public record Explanation(List<Diagnosis> causes, Map<String, String> breakdown) {
    public Diagnosis top() {
      return causes.get(0);
    }
  }

  private LagExplainer() {}

  public static Explanation explain(MetricsSample before, MetricsSample after, Settings s) {
    double seconds = Math.max(0.001, Duration.between(before.at(), after.at()).toMillis() / 1000.0);
    long behind = after.get("MillisBehindSource");
    long steps = delta(before, after, "Steps");
    long rows = delta(before, after, "RowsMined");
    long commits = delta(before, after, "TransactionsCommitted");
    long timeouts = delta(before, after, "StepTimeouts");
    long replays = delta(before, after, "LagReplays");
    long stepMs = after.get("LastStepMillis");
    long logs = after.get("WindowLogs");
    long capacity = Math.max(1, s.queueCapacity());
    double fillBefore = before.get("QueueDepth") / (double) capacity;
    double fillAfter = after.get("QueueDepth") / (double) capacity;
    long scnLag = after.get("ScnLag");
    long scnGrowth = scnLag - before.get("ScnLag");

    Map<String, String> breakdown = new LinkedHashMap<>();
    breakdown.put("Behind source", behind < 0 ? "unknown" : behind + " ms");
    breakdown.put(
        "SCN lag",
        scnLag
            + " ("
            + (scnGrowth >= 0 ? "+" : "")
            + scnGrowth
            + " over "
            + String.format(Locale.ROOT, "%.0f", seconds)
            + " s)");
    breakdown.put(
        "Mining",
        "last step "
            + stepMs
            + " ms over "
            + logs
            + " logs (target "
            + s.targetLatencyMs()
            + " ms); "
            + steps
            + " steps, "
            + rate(rows, seconds)
            + " rows mined per second, "
            + timeouts
            + " timeouts in the interval");
    breakdown.put(
        "Buffer",
        after.get("OpenTransactions")
            + " open transactions, "
            + after.get("BufferedEvents")
            + " changes, "
            + Sizing.bytes(Math.max(0, after.get("BufferHeapBytes")))
            + " on heap, "
            + Sizing.bytes(Math.max(0, after.get("SpilledBytes")))
            + " spilled, "
            + after.get("JournaledTransactions")
            + " journaled");
    breakdown.put(
        "Record queue",
        after.get("QueueDepth") + " of " + capacity + " records waiting for Connect");
    breakdown.put("Commits", rate(commits, seconds) + " transactions per second queued");

    List<Diagnosis> causes = new ArrayList<>();
    boolean queueFull = Math.min(fillBefore, fillAfter) >= QUEUE_FULL;
    if (queueFull) {
      causes.add(
          new Diagnosis(
              Cause.KAFKA,
              1 + fillAfter,
              "The record queue is full ("
                  + after.get("QueueDepth")
                  + " of "
                  + capacity
                  + " records): Kafka Connect takes records more slowly than the connector"
                  + " produces them, so mining waits for room. Look at the producer and the"
                  + " brokers: produce latency, under-replicated partitions, and producer.override"
                  + " settings such as batch.size, linger.ms and compression.type."));
    }
    Diagnosis large = largeTransaction(after);
    if (large != null) {
      causes.add(large);
    }
    if (timeouts > 0 || (!queueFull && stepMs >= Math.max(5_000, 2 * s.targetLatencyMs()))) {
      double score =
          timeouts > 0 ? 1.4 : Math.min(1.3, Math.max(0.6, stepMs / (4.0 * s.targetLatencyMs())));
      causes.add(
          new Diagnosis(
              Cause.MINING,
              score,
              "Mining is the bottleneck: the last step took "
                  + stepMs
                  + " ms over "
                  + logs
                  + " logs against a target of "
                  + s.targetLatencyMs()
                  + " ms"
                  + (timeouts > 0
                      ? ", and " + timeouts + " steps hit cdc.mining.query.timeout.ms"
                      : "")
                  + ". LogMiner reads all redo in the window, captured or not, so a busy"
                  + " database slows every step. Run oracle-cdc-doctor redo-profile to see which"
                  + " tables write the redo, and check fixed-object statistics (DOC-16)."));
    }
    if (replays > 0) {
      causes.add(
          new Diagnosis(
              Cause.LAG_REPLAY,
              1.1,
              replays
                  + " steps were mined a second time with a dictionary from the redo, because"
                  + " rows predate a DDL the connector had not mined yet (the lag case). Each"
                  + " replay reads the redo since the last dictionary build; this ends once"
                  + " mining passes the DDL."));
    }
    boolean lagging =
        behind >= LAG_MS || timeouts > 0 || queueFull || (scnGrowth > 0 && steps == 0);
    if (causes.isEmpty()) {
      causes.add(
          lagging
              ? new Diagnosis(
                  Cause.UNCLEAR,
                  0.1,
                  "The connector is "
                      + behind
                      + " ms behind the source, but no single cause stands out: steps take "
                      + stepMs
                      + " ms, the queue holds "
                      + after.get("QueueDepth")
                      + " of "
                      + capacity
                      + " records and "
                      + after.get("OpenTransactions")
                      + " transactions are open. Sample again over a longer interval.")
              : new Diagnosis(
                  Cause.NONE,
                  0,
                  "The connector keeps up: "
                      + Math.max(0, behind)
                      + " ms behind the source, steps of "
                      + stepMs
                      + " ms and room in the record queue."));
    }
    causes.sort(Comparator.comparingDouble(Diagnosis::score).reversed());
    return new Explanation(List.copyOf(causes), breakdown);
  }

  private static Diagnosis largeTransaction(MetricsSample after) {
    long budget = after.get("BufferMemoryMaxBytes");
    MetricsSample.OpenTransaction big =
        after.largest().stream()
            .max(
                Comparator.comparingLong(MetricsSample.OpenTransaction::bytes)
                    .thenComparingLong(MetricsSample.OpenTransaction::events))
            .orElse(null);
    if (big != null) {
      boolean spilled = big.spilledBytes() > 0;
      boolean heavy = budget > 0 && big.bytes() >= budget / 4;
      if (!spilled && !heavy && big.events() < LARGE_EVENTS) {
        return null;
      }
      return new Diagnosis(
          Cause.LARGE_TRANSACTION,
          spilled || heavy ? 1.5 : 1.0,
          "Transaction "
              + big.xid()
              + (big.username() == null ? "" : " of user " + big.username())
              + " has been open "
              + big.ageMillis() / 1000
              + " s with "
              + big.events()
              + " changes ("
              + Sizing.bytes(big.heapBytes())
              + " on heap, "
              + Sizing.bytes(big.spilledBytes())
              + " spilled"
              + (big.journaled() ? ", journaled" : "")
              + "). Its records are published only when it commits and, until it is journaled,"
              + " it holds the resume position back; the lag clears after its commit. Splitting"
              + " the job into smaller commits avoids it.");
    }
    long events = after.get("BufferedEvents");
    long open = after.get("OpenTransactions");
    long spilled = after.get("SpilledBytes");
    if (spilled > 0 || (events >= LARGE_EVENTS && open > 0 && open <= 3)) {
      return new Diagnosis(
          Cause.LARGE_TRANSACTION,
          spilled > 0 ? 1.2 : 0.9,
          open
              + " open transactions hold "
              + events
              + " changes"
              + (spilled > 0 ? " and " + Sizing.bytes(spilled) + " spilled to disk" : "")
              + ". Their records are published only at commit, so a large transaction looks"
              + " like lag until it commits. Read LargestTransactions over JMX to name it.");
    }
    return null;
  }

  private static long delta(MetricsSample before, MetricsSample after, String attr) {
    long a = after.get(attr);
    long b = before.get(attr);
    return a < 0 || b < 0 ? 0 : Math.max(0, a - b);
  }

  private static String rate(long n, double seconds) {
    return String.format(Locale.ROOT, "%.1f", n / seconds);
  }

  /** The Markdown report. */
  public static String toMarkdown(Explanation e) {
    StringBuilder sb = new StringBuilder("# oracle-cdc-doctor explain-lag\n\n");
    sb.append(e.top().text()).append("\n\n| Area | Reading |\n|---|---|\n");
    for (Map.Entry<String, String> b : e.breakdown().entrySet()) {
      sb.append("| ").append(b.getKey()).append(" | ").append(b.getValue()).append(" |\n");
    }
    if (e.causes().size() > 1) {
      sb.append("\nOther contributors:\n\n");
      for (Diagnosis d : e.causes().subList(1, e.causes().size())) {
        sb.append("- ").append(d.text()).append('\n');
      }
    }
    return sb.toString();
  }
}
