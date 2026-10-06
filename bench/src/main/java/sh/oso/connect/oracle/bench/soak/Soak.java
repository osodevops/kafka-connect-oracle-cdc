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

import java.time.Duration;
import java.util.List;
import java.util.OptionalLong;
import java.util.function.LongSupplier;
import sh.oso.connect.oracle.bench.check.CheckReport;

/**
 * The T3 soak (testing strategy section 1): a paced workload against an already running connector
 * for many hours, with the correctness oracle at fixed check points.
 *
 * <p>Flow: refuse to start when the table topics already hold records (the oracle reads them from
 * the beginning); reset the tables and the ledger; wait until the connector's committed position
 * has passed the reset, so streaming captures the new tables before the first row; start the
 * workload open-ended. At each check point: pause the workload until every session is parked (no
 * transaction open), read the database SCN, wait until the committed {@code resume_scn} is past it
 * (the catch-up time), run the oracle, write its evidence, resume. The first FAIL stops the soak
 * with its evidence kept; INCONCLUSIVE is recorded and the soak goes on. A catch-up that does not
 * finish within the timeout stops the soak as LAG: a finding, not a correctness verdict. The final
 * check runs once the workload has stopped at the end.
 *
 * <p>Every collaborator is a small interface so the schedule can be tested with fakes and a fake
 * clock.
 */
public final class Soak {

  /** How the soak ends; the exit code of {@code bench soak}. */
  public enum Outcome {
    PASS(0),
    FAIL(1),
    INCONCLUSIVE(3),
    LAG(4),
    ERROR(5);

    public final int exitCode;

    Outcome(int exitCode) {
      this.exitCode = exitCode;
    }
  }

  /** The workload under the soak's control. */
  public interface Workload {
    /** Drops and recreates the tables and the ledger. */
    void reset() throws Exception;

    /** Starts the open-ended run in the background. */
    void start();

    /** Every session parks before its next transaction. */
    void pause();

    /** True once every running session is parked, false when the timeout passes first. */
    boolean awaitParked(long timeoutMillis) throws InterruptedException;

    void resume();

    /** Ends the run: each session finishes its transaction; waits until the run has returned. */
    void stop() throws Exception;

    long committed();

    long rolledBack();

    /** The error that ended the run before the soak stopped it, or null. */
    Throwable failure();
  }

  /** The database's current SCN, read with what the workload user may run. */
  public interface ScnSource {
    long currentScn() throws Exception;

    /** How the last SCN was read, for the summary. */
    String method();
  }

  /** The connector under test, through the Kafka Connect REST API. */
  public interface Connector {
    /** The lowest committed {@code resume_scn} over the connector's offsets; empty when none. */
    OptionalLong resumeScn() throws Exception;

    /** A short description when the connector or a task is FAILED, else null. */
    String failure() throws Exception;
  }

  /** One run of the correctness oracle. */
  public interface Checks {
    CheckReport run() throws Exception;
  }

  /** Refuses a start that would make the first check meaningless. */
  public interface Preflight {
    /** Reasons not to start; empty when the soak may start. */
    List<String> problems() throws Exception;
  }

  /** The background metrics sampler. */
  public interface Sampler {
    void start();

    /** What the soak is doing now, recorded with every sample. */
    void phase(String phase);

    void stop();

    /** Aggregates for the summary. */
    java.util.Map<String, Object> summary();
  }

  /** Wall-clock time and sleeping, faked in tests. */
  public interface Clock {
    long nowMillis();

    void sleep(long millis) throws InterruptedException;

    Clock SYSTEM =
        new Clock() {
          @Override
          public long nowMillis() {
            return System.currentTimeMillis();
          }

          @Override
          public void sleep(long millis) throws InterruptedException {
            Thread.sleep(millis);
          }
        };
  }

  /**
   * Durations of a soak. {@code total} runs from the workload start to the final check; check
   * points fall at every multiple of {@code checkEvery} before it.
   */
  public record Plan(
      Duration total,
      Duration checkEvery,
      Duration catchUpTimeout,
      Duration pollInterval,
      Duration parkTimeout,
      Duration settle,
      Duration supervise) {

    public Plan {
      if (total.isNegative() || total.isZero() || checkEvery.isNegative() || checkEvery.isZero()) {
        throw new IllegalArgumentException("the soak and its check interval must be positive");
      }
    }

    /**
     * The defaults of {@code bench soak}: poll the offsets every two seconds, supervise every
     * minute.
     */
    public static Plan of(double hours, double checkEveryHours, Duration catchUpTimeout) {
      return new Plan(
          hoursToDuration(hours),
          hoursToDuration(checkEveryHours),
          catchUpTimeout,
          Duration.ofSeconds(2),
          Duration.ofMinutes(5),
          Duration.ofSeconds(10),
          Duration.ofMinutes(1));
    }

    static Duration hoursToDuration(double hours) {
      return Duration.ofMillis(Math.round(hours * 3_600_000d));
    }
  }

  /** A wall-clock jump this much longer than a requested sleep counts as the host sleeping. */
  static final long SUSPEND_SLACK_MILLIS = 60_000;

  private final Plan plan;
  private final Workload workload;
  private final ScnSource scn;
  private final Connector connector;
  private final Checks checks;
  private final Preflight preflight;
  private final Sampler sampler;
  private final Clock clock;
  private final SoakOutput out;
  private final LongSupplier harnessHeapPeak;
  private final SoakSummary summary;

  // written by the soak's thread; read by onSignal on a shutdown hook's thread
  private volatile boolean started;
  private volatile boolean stopped;
  private volatile boolean finished;
  private volatile long segmentStart = -1;
  private volatile long segmentCommitted;
  private volatile long segmentRolledBack;

  public Soak(
      Plan plan,
      Workload workload,
      ScnSource scn,
      Connector connector,
      Checks checks,
      Preflight preflight,
      Sampler sampler,
      Clock clock,
      SoakOutput out,
      LongSupplier harnessHeapPeak,
      SoakSummary summary) {
    this.plan = plan;
    this.workload = workload;
    this.scn = scn;
    this.connector = connector;
    this.checks = checks;
    this.preflight = preflight;
    this.sampler = sampler;
    this.clock = clock;
    this.out = out;
    this.harnessHeapPeak = harnessHeapPeak;
    this.summary = summary;
  }

  /** Runs the soak to its end or its first stop; the summary is written either way. */
  public SoakSummary run() {
    summary.startedAt(clock.nowMillis());
    summary.plan(plan);
    sampler.start();
    try {
      phases();
    } catch (Abort a) {
      summary.outcome(a.outcome, a.reason);
      out.log(a.outcome + ": " + a.reason);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      summary.outcome(Outcome.ERROR, "interrupted");
      out.log("ERROR: interrupted");
    } catch (Exception e) {
      String reason = "unexpected error: " + out.describe(e);
      summary.outcome(Outcome.ERROR, reason);
      out.log("ERROR: " + reason);
    } finally {
      if (started && !stopped) {
        try {
          workload.stop();
          closeSegment();
        } catch (Exception e) {
          out.log("the workload did not stop cleanly: " + out.describe(e));
        }
      }
      sampler.stop();
      summary.endedAt(clock.nowMillis());
      summary.workload(workload.committed(), workload.rolledBack());
      summary.metrics(sampler.summary());
      out.writeSummary(summary);
      finished = true;
      out.log(
          "soak "
              + summary.outcome()
              + (summary.reason() == null ? "" : " (" + summary.reason() + ")")
              + "; summary in "
              + out.dir());
    }
    return summary;
  }

  /**
   * For a shutdown hook (Ctrl-C during a long run): records the early stop and writes the summary
   * with the checks so far. Does nothing once the soak has finished.
   */
  public void onSignal() {
    if (finished) {
      return;
    }
    summary.outcome(Outcome.ERROR, "stopped by a signal before the end; the checks so far stand");
    summary.endedAt(clock.nowMillis());
    summary.workload(workload.committed(), workload.rolledBack());
    summary.metrics(sampler.summary());
    out.writeSummary(summary);
    out.log("stopped by a signal; summary in " + out.dir());
  }

  private void phases() throws Exception {
    List<String> problems;
    try {
      problems = preflight.problems();
    } catch (Exception e) {
      throw new Abort(
          Outcome.ERROR, "could not read the table topics before the start: " + out.describe(e));
    }
    if (!problems.isEmpty()) {
      throw new Abort(Outcome.ERROR, "refusing to start: " + String.join("; ", problems));
    }
    out.log("resetting the tables and the ledger");
    sampler.phase("reset");
    workload.reset();
    clock.sleep(plan.settle().toMillis());
    long resetScn = scn.currentScn();
    summary.scnMethod(scn.method());
    out.log("tables reset; waiting for the connector to pass SCN " + resetScn);
    long resetCatchUp = awaitCatchUp(resetScn, "the table reset");
    summary.reset(resetScn, resetCatchUp);

    long start = clock.nowMillis();
    long end = start + plan.total().toMillis();
    long every = plan.checkEvery().toMillis();
    summary.workloadStartedAt(start);
    workload.start();
    started = true;
    openSegment();
    out.log(
        "workload started; checks every "
            + hours(every)
            + " h, final check at "
            + hours(end - start)
            + " h");
    int point = 1;
    int index = 1;
    while (true) {
      long next = start + point * every;
      superviseUntil(Math.min(next, end));
      if (next >= end) {
        break;
      }
      sampler.phase("check");
      workload.pause();
      if (!workload.awaitParked(plan.parkTimeout().toMillis())) {
        rethrowWorkloadFailure();
        throw new Abort(
            Outcome.ERROR,
            "the sessions did not park within " + plan.parkTimeout().toSeconds() + " s");
      }
      rethrowWorkloadFailure();
      closeSegment();
      SoakSummary.Check c = check(index++, "periodic", start);
      if (c.verdict() == CheckReport.Verdict.FAIL) {
        throw new Abort(Outcome.FAIL, "check " + c.index() + " failed: " + c.failuresText());
      }
      workload.resume();
      sampler.phase("workload");
      openSegment();
      long now = clock.nowMillis();
      point++;
      while (start + point * every <= now && start + point * every < end) {
        out.log(
            "check point at "
                + hours(point * every)
                + " h passed during the previous check; skipped");
        summary.skippedCheckPoint();
        point++;
      }
    }

    sampler.phase("final-check");
    workload.stop();
    stopped = true;
    closeSegment();
    out.log("workload stopped after " + hours(clock.nowMillis() - start) + " h; final check");
    SoakSummary.Check last = check(index, "final", start);
    if (last.verdict() == CheckReport.Verdict.FAIL) {
      throw new Abort(Outcome.FAIL, "final check failed: " + last.failuresText());
    }
    boolean inconclusive =
        summary.checks().stream().anyMatch(c -> c.verdict() == CheckReport.Verdict.INCONCLUSIVE);
    summary.outcome(
        inconclusive ? Outcome.INCONCLUSIVE : Outcome.PASS,
        inconclusive ? "at least one check was inconclusive; read its evidence" : null);
  }

  /** Sleeps until {@code target} in slices, stopping on a workload or connector failure. */
  private void superviseUntil(long target) throws Exception {
    long now = clock.nowMillis();
    while (now < target) {
      long want = Math.min(plan.supervise().toMillis(), target - now);
      clock.sleep(want);
      long after = clock.nowMillis();
      if (after - now > want + SUSPEND_SLACK_MILLIS) {
        long gap = after - now - want;
        summary.suspended(gap);
        out.log(
            "the clock jumped "
                + (gap / 1000)
                + " s past a "
                + (want / 1000)
                + " s sleep: the host was probably asleep; figures around it are unreliable");
      }
      now = after;
      rethrowWorkloadFailure();
      String failed;
      try {
        failed = connector.failure();
      } catch (Exception e) {
        summary.probeError();
        failed = null; // the REST API was briefly unreachable; the next check judges the lag
      }
      if (failed != null) {
        throw new Abort(Outcome.ERROR, "the connector stopped: " + failed);
      }
    }
  }

  private SoakSummary.Check check(int index, String kind, long workloadStart) throws Exception {
    long at = clock.nowMillis();
    long checkScn = scn.currentScn();
    out.log(
        "check " + index + " (" + kind + "): waiting for the connector to pass SCN " + checkScn);
    long catchUp = awaitCatchUp(checkScn, "check " + index);
    out.log("check " + index + ": caught up in " + (catchUp / 1000) + " s; running the oracle");
    long t0 = clock.nowMillis();
    CheckReport report;
    try {
      report = checks.run();
    } catch (Exception e) {
      report = new CheckReport();
      report.inconclusive("the check could not run: " + out.describe(e));
    }
    long took = clock.nowMillis() - t0;
    String file = out.writeCheck(index, report);
    SoakSummary.Check c =
        new SoakSummary.Check(
            index,
            kind,
            at,
            (at - workloadStart) / 3_600_000d,
            checkScn,
            catchUp,
            took,
            report.verdict(),
            report.checkScn(),
            report.recordsConsumed(),
            List.copyOf(report.failures()),
            report.inconclusiveReason() == null ? null : out.scrub(report.inconclusiveReason()),
            file,
            report.sha256(),
            harnessHeapPeak.getAsLong());
    summary.check(c);
    out.log(
        "check "
            + index
            + ": "
            + c.verdict()
            + " after "
            + (took / 1000)
            + " s, "
            + c.recordsConsumed()
            + " records"
            + (c.failures().isEmpty() ? "" : "; " + c.failuresText())
            + (c.inconclusiveReason() == null ? "" : "; " + c.inconclusiveReason())
            + "; evidence "
            + file);
    return c;
  }

  /**
   * Polls the committed offsets until {@code resume_scn} is past {@code target}: every transaction
   * committed at or before it has been delivered and acknowledged (on a quiet database the
   * connector's heartbeats move the offset). Returns the wait in milliseconds.
   */
  private long awaitCatchUp(long target, String what) throws Exception {
    long t0 = clock.nowMillis();
    long deadline = t0 + plan.catchUpTimeout().toMillis();
    Long seen = null;
    String lastError = null;
    while (true) {
      String failed = null;
      try {
        failed = connector.failure();
        OptionalLong r = connector.resumeScn();
        if (r.isPresent()) {
          seen = r.getAsLong();
          if (seen > target) {
            return clock.nowMillis() - t0;
          }
        }
      } catch (Exception e) {
        lastError = out.describe(e);
      }
      if (failed != null) {
        throw new Abort(
            Outcome.ERROR, "the connector stopped while waiting for " + what + ": " + failed);
      }
      long now = clock.nowMillis();
      if (now >= deadline) {
        throw new Abort(
            Outcome.LAG,
            "the committed resume_scn ("
                + (seen == null ? "none" : seen)
                + ") did not pass SCN "
                + target
                + " for "
                + what
                + " within "
                + plan.catchUpTimeout().toMinutes()
                + " min"
                + (lastError == null ? "" : "; last error: " + lastError)
                + ". Lag is a finding, not a correctness verdict: read the worker log and the"
                + " lag metrics");
      }
      clock.sleep(Math.min(plan.pollInterval().toMillis(), deadline - now));
    }
  }

  private void rethrowWorkloadFailure() throws Abort {
    Throwable f = workload.failure();
    if (f != null) {
      throw new Abort(Outcome.ERROR, "the workload stopped: " + out.describe(f));
    }
  }

  private void openSegment() {
    segmentStart = clock.nowMillis();
    segmentCommitted = workload.committed();
    segmentRolledBack = workload.rolledBack();
  }

  private void closeSegment() {
    if (segmentStart < 0) {
      return;
    }
    summary.segment(
        segmentStart,
        clock.nowMillis(),
        workload.committed() - segmentCommitted,
        workload.rolledBack() - segmentRolledBack);
    segmentStart = -1;
  }

  private static String hours(long millis) {
    return SoakSummary.round(millis / 3_600_000d, 3);
  }

  /** Ends the soak with an outcome. */
  private static final class Abort extends Exception {
    private static final long serialVersionUID = 1L;
    final Outcome outcome;
    final String reason;

    Abort(Outcome outcome, String reason) {
      super(reason, null, false, false);
      this.outcome = outcome;
      this.reason = reason;
    }
  }
}
