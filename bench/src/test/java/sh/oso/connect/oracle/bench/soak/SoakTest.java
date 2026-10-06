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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.LongFunction;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import sh.oso.connect.oracle.bench.check.CheckReport;

/** The soak's schedule and stop rules, with fakes and a fake clock: no database, no Kafka. */
class SoakTest {

  static final long HOUR = 3_600_000;
  static final long MINUTE = 60_000;
  static final String PASSWORD = "S3cr3t-Soak-Pw-9f2";

  @TempDir Path dir;

  /** Wall clock in milliseconds from an arbitrary epoch; sleeping advances it. */
  static final class FakeClock implements Soak.Clock {
    long now = 1_800_000_000_000L;
    long jumpAt = -1;
    long jumpBy;

    @Override
    public long nowMillis() {
      return now;
    }

    @Override
    public void sleep(long millis) {
      now += millis;
      if (jumpAt >= 0 && now >= jumpAt) {
        now += jumpBy;
        jumpAt = -1;
      }
    }
  }

  /** Records the calls; commits one transaction per running second. */
  static final class FakeWorkload implements Soak.Workload {
    final FakeClock clock;
    final List<String> events = new ArrayList<>();
    boolean running;
    boolean paused;
    long runningMillis;
    long since;
    long failAt = -1;

    FakeWorkload(FakeClock clock) {
      this.clock = clock;
    }

    @Override
    public void reset() {
      events.add("reset");
    }

    @Override
    public void start() {
      events.add("start");
      running = true;
      since = clock.now;
    }

    @Override
    public void pause() {
      events.add("pause");
      accrue();
      paused = true;
    }

    @Override
    public boolean awaitParked(long timeoutMillis) {
      return true;
    }

    @Override
    public void resume() {
      events.add("resume");
      paused = false;
      since = clock.now;
    }

    @Override
    public void stop() {
      events.add("stop");
      accrue();
      running = false;
    }

    private void accrue() {
      if (running && !paused) {
        runningMillis += clock.now - since;
        since = clock.now;
      }
    }

    @Override
    public long committed() {
      return (runningMillis + (running && !paused ? clock.now - since : 0)) / 1000;
    }

    @Override
    public long rolledBack() {
      return committed() / 20;
    }

    @Override
    public Throwable failure() {
      return failAt >= 0 && clock.now >= failAt
          ? new SQLException("ORA-03113: end-of-file on communication channel")
          : null;
    }
  }

  /** The database SCN grows by one a second. */
  static final class FakeScn implements Soak.ScnSource {
    final FakeClock clock;
    final List<Long> reads = new ArrayList<>();
    String throwWith;

    FakeScn(FakeClock clock) {
      this.clock = clock;
    }

    long at(long millis) {
      return 1_000 + (millis - 1_800_000_000_000L) / 1000;
    }

    @Override
    public long currentScn() throws SQLException {
      if (throwWith != null) {
        throw new SQLException(throwWith);
      }
      long s = at(clock.now);
      reads.add(s);
      return s;
    }

    @Override
    public String method() {
      return "fake";
    }
  }

  /** The committed position trails the database by {@code lag}; it can stall from a time on. */
  static final class FakeConnector implements Soak.Connector {
    final FakeClock clock;
    final FakeScn scn;
    LongFunction<Long> lagAt = t -> 40_000L;
    long stallFrom = Long.MAX_VALUE;
    long failedFrom = Long.MAX_VALUE;
    String throwWith;
    int throwTimes;

    FakeConnector(FakeClock clock, FakeScn scn) {
      this.clock = clock;
      this.scn = scn;
    }

    @Override
    public OptionalLong resumeScn() throws Exception {
      if (throwTimes > 0) {
        throwTimes--;
        throw new java.io.IOException(throwWith);
      }
      long t = Math.min(clock.now, stallFrom);
      return OptionalLong.of(scn.at(t - lagAt.apply(t)));
    }

    @Override
    public String failure() {
      return clock.now >= failedFrom ? "task 0 FAILED with CDC-2001" : null;
    }
  }

  /** Returns the queued verdicts in turn, then PASS. */
  static final class FakeChecks implements Soak.Checks {
    final FakeClock clock;
    final FakeWorkload workload;
    final Deque<Object> results = new ArrayDeque<>();
    final List<Long> ranAt = new ArrayList<>();
    final List<Boolean> workloadQuietAtRun = new ArrayList<>();
    long takes = 10 * MINUTE;

    FakeChecks(FakeClock clock, FakeWorkload workload) {
      this.clock = clock;
      this.workload = workload;
    }

    @Override
    public CheckReport run() throws Exception {
      ranAt.add(clock.now);
      workloadQuietAtRun.add(workload.paused || !workload.running);
      clock.now += takes;
      Object next = results.isEmpty() ? CheckReport.Verdict.PASS : results.poll();
      if (next instanceof Exception e) {
        throw e;
      }
      CheckReport r = new CheckReport();
      r.recordsConsumed(1234);
      r.checkScn(99);
      if (next == CheckReport.Verdict.FAIL) {
        r.fail("3 committed transactions in the ledger are not in Kafka");
      } else if (next == CheckReport.Verdict.INCONCLUSIVE) {
        r.inconclusive("AS OF SCN 99 is no longer available (ORA-01555); increase undo retention");
      }
      return r;
    }
  }

  static final class FakeSampler implements Soak.Sampler {
    final List<String> phases = new ArrayList<>();
    boolean started;
    boolean stopped;

    @Override
    public void start() {
      started = true;
    }

    @Override
    public void phase(String phase) {
      phases.add(phase);
    }

    @Override
    public void stop() {
      stopped = true;
    }

    @Override
    public Map<String, Object> summary() {
      return Map.of("samples", 0);
    }
  }

  final FakeClock clock = new FakeClock();
  final FakeWorkload workload = new FakeWorkload(clock);
  final FakeScn scn = new FakeScn(clock);
  final FakeConnector connector = new FakeConnector(clock, scn);
  final FakeChecks checks = new FakeChecks(clock, workload);
  final FakeSampler sampler = new FakeSampler();
  final ByteArrayOutputStream console = new ByteArrayOutputStream();
  List<String> preflightProblems = List.of();

  SoakSummary run(double hours, double every) {
    return run(hours, every, Duration.ofMinutes(30));
  }

  SoakSummary run(double hours, double every, Duration catchUp) {
    SoakOutput out =
        new SoakOutput(
            dir,
            new PrintStream(console, true, StandardCharsets.UTF_8),
            new Redactor().add(PASSWORD),
            clock);
    Soak soak =
        new Soak(
            Soak.Plan.of(hours, every, catchUp),
            workload,
            scn,
            connector,
            checks,
            () -> preflightProblems,
            sampler,
            clock,
            out,
            () -> 512L * 1024 * 1024,
            new SoakSummary());
    return soak.run();
  }

  JsonNode summaryJson() throws Exception {
    return new ObjectMapper().readTree(dir.resolve(SoakOutput.SUMMARY_JSON).toFile());
  }

  @Test
  void checksRunAtEveryIntervalFromTheStartAndOnceMoreAtTheEnd() throws Exception {
    SoakSummary s = run(24, 6);

    assertThat(s.outcome()).isEqualTo(Soak.Outcome.PASS);
    assertThat(s.outcome().exitCode).isZero();
    assertThat(s.checks())
        .extracting(SoakSummary.Check::kind)
        .containsExactly("periodic", "periodic", "periodic", "final");
    // check points are multiples of the interval from the workload start, however long a check
    // takes (ten minutes here)
    assertThat(s.checks())
        .extracting(c -> SoakSummary.round(c.elapsedHours(), 2))
        .containsExactly("6", "12", "18", "24");
    assertThat(workload.events)
        .containsExactly(
            "reset", "start", "pause", "resume", "pause", "resume", "pause", "resume", "stop");
    assertThat(checks.workloadQuietAtRun).containsOnly(true);
    assertThat(s.segments()).hasSize(4);
    for (int i = 1; i <= 4; i++) {
      assertThat(dir.resolve(String.format("check-%03d.json", i))).exists();
    }
    assertThat(dir.resolve(SoakOutput.SUMMARY_MD)).exists();
    JsonNode j = summaryJson();
    assertThat(j.path("outcome").asText()).isEqualTo("PASS");
    assertThat(j.path("checks")).hasSize(4);
    assertThat(j.path("checks").get(0).path("evidence").asText()).isEqualTo("check-001.json");
    assertThat(j.path("checks").get(0).path("sha256").asText()).hasSize(64);
    assertThat(j.path("notice").asText()).contains("Do not publish");
    assertThat(Files.readString(dir.resolve(SoakOutput.SUMMARY_MD))).contains("Do not publish");
    assertThat(sampler.started).isTrue();
    assertThat(sampler.stopped).isTrue();
    assertThat(sampler.phases).startsWith("reset").contains("check", "workload", "final-check");
  }

  @Test
  void theTablesAreResetAndPassedByTheConnectorBeforeTheWorkloadStarts() throws Exception {
    long t0 = clock.now;
    connector.lagAt = t -> 20_000L;
    SoakSummary s = run(1, 6);

    JsonNode reset = summaryJson().path("reset");
    // ten seconds of settle, then twenty seconds of lag plus at most a poll and an SCN tick
    assertThat(reset.path("catchUpSeconds").asDouble()).isBetween(20.0, 24.0);
    assertThat(reset.path("scn").asLong()).isEqualTo(scn.at(t0 + 10_000));
    assertThat(workload.events.subList(0, 2)).containsExactly("reset", "start");
    assertThat(s.checks()).hasSize(1);
    assertThat(s.checks().get(0).kind()).isEqualTo("final");
  }

  @Test
  void catchUpTimeIsTheWaitForTheCommittedPositionToPassTheCheckScn() {
    connector.lagAt = t -> 45_000L;
    SoakSummary s = run(12, 6);

    assertThat(s.checks()).hasSize(2);
    for (SoakSummary.Check c : s.checks()) {
      assertThat(c.catchUpMillis()).isBetween(45_000L, 48_000L);
      assertThat(c.scn()).isPositive();
    }
  }

  @Test
  void aCatchUpBeyondTheTimeoutStopsTheSoakAsLagAndKeepsTheEvidence() throws Exception {
    // the connector stops moving seven hours in: the first check passes, the second waits
    connector.stallFrom = clock.now + 7 * HOUR;
    SoakSummary s = run(24, 6, Duration.ofMinutes(30));

    assertThat(s.outcome()).isEqualTo(Soak.Outcome.LAG);
    assertThat(s.outcome().exitCode).isEqualTo(4);
    assertThat(s.reason())
        .contains("did not pass SCN")
        .contains("within 30 min")
        .contains("not a correctness verdict");
    assertThat(s.checks()).hasSize(1);
    assertThat(dir.resolve("check-001.json")).exists();
    assertThat(dir.resolve("check-002.json")).doesNotExist();
    assertThat(workload.events.get(workload.events.size() - 1)).isEqualTo("stop");
    assertThat(summaryJson().path("outcome").asText()).isEqualTo("LAG");
  }

  @Test
  void theFirstFailStopsTheSoakWithItsEvidenceKept() throws Exception {
    checks.results.add(CheckReport.Verdict.PASS);
    checks.results.add(CheckReport.Verdict.FAIL);
    SoakSummary s = run(24, 6);

    assertThat(s.outcome()).isEqualTo(Soak.Outcome.FAIL);
    assertThat(s.outcome().exitCode).isEqualTo(1);
    assertThat(s.reason()).contains("check 2 failed").contains("not in Kafka");
    assertThat(s.checks())
        .extracting(SoakSummary.Check::verdict)
        .containsExactly(CheckReport.Verdict.PASS, CheckReport.Verdict.FAIL);
    JsonNode evidence = new ObjectMapper().readTree(dir.resolve("check-002.json").toFile());
    assertThat(evidence.path("verdict").asText()).isEqualTo("FAIL");
    assertThat(dir.resolve("check-003.json")).doesNotExist();
    // stopped while paused for the check; never resumed after the FAIL
    assertThat(workload.events)
        .containsExactly("reset", "start", "pause", "resume", "pause", "stop");
    assertThat(Files.readString(dir.resolve(SoakOutput.SUMMARY_MD))).contains("FAIL");
  }

  @Test
  void anInconclusiveCheckIsRecordedAndTheSoakGoesOn() {
    checks.results.add(CheckReport.Verdict.INCONCLUSIVE);
    SoakSummary s = run(24, 6);

    assertThat(s.checks())
        .extracting(SoakSummary.Check::verdict)
        .containsExactly(
            CheckReport.Verdict.INCONCLUSIVE,
            CheckReport.Verdict.PASS,
            CheckReport.Verdict.PASS,
            CheckReport.Verdict.PASS);
    assertThat(s.outcome()).isEqualTo(Soak.Outcome.INCONCLUSIVE);
    assertThat(s.outcome().exitCode).isEqualTo(3);
    assertThat(s.checks().get(0).inconclusiveReason()).contains("ORA-01555");
  }

  @Test
  void aCheckThatCannotRunIsInconclusiveNotFatal() {
    checks.results.add(new java.io.IOException("Kafka unreachable"));
    SoakSummary s = run(12, 6);

    assertThat(s.checks()).hasSize(2);
    assertThat(s.checks().get(0).verdict()).isEqualTo(CheckReport.Verdict.INCONCLUSIVE);
    assertThat(s.checks().get(0).inconclusiveReason())
        .contains("the check could not run")
        .contains("Kafka unreachable");
    assertThat(dir.resolve("check-001.json")).exists();
    assertThat(s.outcome()).isEqualTo(Soak.Outcome.INCONCLUSIVE);
  }

  @Test
  void theFinalCheckRunsAfterTheWorkloadHasStopped() {
    SoakSummary s = run(2, 6);

    assertThat(workload.events).containsExactly("reset", "start", "stop");
    assertThat(s.checks()).hasSize(1);
    assertThat(s.checks().get(0).kind()).isEqualTo("final");
    assertThat(SoakSummary.round(s.checks().get(0).elapsedHours(), 2)).isEqualTo("2");
    assertThat(checks.workloadQuietAtRun).containsExactly(true);
    assertThat(workload.running).isFalse();
    assertThat(s.outcome()).isEqualTo(Soak.Outcome.PASS);
  }

  @Test
  void aCheckPointThatPassesDuringALongCheckIsSkipped() throws Exception {
    checks.takes = 70 * MINUTE;
    SoakSummary s = run(4, 1);

    // 1 h, then 2 h passes during the first check, then 3 h, then the final check
    assertThat(s.checks())
        .extracting(c -> (int) Math.floor(c.elapsedHours()))
        .containsExactly(1, 3, 4);
    assertThat(summaryJson().path("harness").path("skippedCheckPoints").asInt()).isEqualTo(1);
  }

  @Test
  void topicsThatAlreadyHoldRecordsRefuseTheStart() throws Exception {
    preflightProblems = List.of("topic cdc.FREEPDB1.WORKLOAD.WL_T1 already holds 12 offsets");
    SoakSummary s = run(24, 6);

    assertThat(s.outcome()).isEqualTo(Soak.Outcome.ERROR);
    assertThat(s.outcome().exitCode).isEqualTo(5);
    assertThat(s.reason()).contains("refusing to start").contains("already holds 12 offsets");
    assertThat(workload.events).isEmpty();
    assertThat(dir.resolve(SoakOutput.SUMMARY_JSON)).exists();
  }

  @Test
  void anUnreachableKafkaAtTheStartIsSaidPlainly() throws Exception {
    SoakOutput out =
        new SoakOutput(
            dir, new PrintStream(console, true, StandardCharsets.UTF_8), new Redactor(), clock);
    SoakSummary s =
        new Soak(
                Soak.Plan.of(1, 1, Duration.ofMinutes(1)),
                workload,
                scn,
                connector,
                checks,
                () -> {
                  throw new java.util.concurrent.TimeoutException("Timed out waiting for a node");
                },
                sampler,
                clock,
                out,
                () -> 0L,
                new SoakSummary())
            .run();

    assertThat(s.outcome()).isEqualTo(Soak.Outcome.ERROR);
    assertThat(s.reason()).startsWith("could not read the table topics before the start");
    assertThat(workload.events).isEmpty();
  }

  @Test
  void aFailedTaskStopsTheSoakBetweenChecks() {
    connector.failedFrom = clock.now + 3 * HOUR;
    SoakSummary s = run(24, 6);

    assertThat(s.outcome()).isEqualTo(Soak.Outcome.ERROR);
    assertThat(s.reason()).contains("the connector stopped").contains("CDC-2001");
    assertThat(s.checks()).isEmpty();
    assertThat(workload.events.get(workload.events.size() - 1)).isEqualTo("stop");
  }

  @Test
  void aWorkloadErrorStopsTheSoak() {
    workload.failAt = clock.now + 8 * HOUR;
    SoakSummary s = run(24, 6);

    assertThat(s.outcome()).isEqualTo(Soak.Outcome.ERROR);
    assertThat(s.reason()).contains("the workload stopped").contains("ORA-03113");
    assertThat(s.checks()).hasSize(1);
  }

  @Test
  void aClockJumpPastASleepIsRecordedAsSuspectedHostSleep() throws Exception {
    clock.jumpAt = clock.now + 2 * HOUR;
    clock.jumpBy = 20 * MINUTE;
    SoakSummary s = run(6, 6);

    assertThat(s.suspensions()).isEqualTo(1);
    JsonNode h = summaryJson().path("harness");
    assertThat(h.path("suspectedHostSleeps").asInt()).isEqualTo(1);
    assertThat(h.path("suspectedHostSleepSeconds").asLong()).isEqualTo(20 * 60);
    assertThat(console.toString(StandardCharsets.UTF_8)).contains("host was probably asleep");
  }

  @Test
  void transientRestErrorsDuringCatchUpAreRetried() {
    connector.throwWith = "Connection refused";
    connector.throwTimes = 5;
    SoakSummary s = run(1, 6);

    assertThat(s.outcome()).isEqualTo(Soak.Outcome.PASS);
  }

  @Test
  void aSignalBeforeTheEndStillWritesTheSummary() throws Exception {
    SoakOutput out =
        new SoakOutput(
            dir, new PrintStream(console, true, StandardCharsets.UTF_8), new Redactor(), clock);
    Soak soak =
        new Soak(
            Soak.Plan.of(1, 1, Duration.ofMinutes(1)),
            workload,
            scn,
            connector,
            checks,
            List::of,
            sampler,
            clock,
            out,
            () -> 0L,
            new SoakSummary());
    soak.onSignal();

    assertThat(summaryJson().path("outcome").asText()).isEqualTo("ERROR");
    assertThat(summaryJson().path("reason").asText()).contains("stopped by a signal");
  }

  @Test
  void noPasswordReachesAnyOutput() throws Exception {
    // foreign text carrying the password: a check error, a REST error during catch-up
    checks.results.add(new SQLException("ORA-01017: invalid credential " + PASSWORD));
    checks.results.add(CheckReport.Verdict.FAIL);
    connector.throwWith = "refused for workload/" + PASSWORD;
    connector.throwTimes = 3;
    run(24, 6);

    // and a fatal SCN error in a second run in the same directory's sibling
    Path other = dir.resolve("second");
    FakeScn bad = new FakeScn(clock);
    bad.throwWith = "jdbc:oracle:thin:workload/" + PASSWORD + "@//db:1521/FREEPDB1 refused";
    new Soak(
            Soak.Plan.of(1, 1, Duration.ofMinutes(1)),
            new FakeWorkload(clock),
            bad,
            connector,
            checks,
            List::of,
            new FakeSampler(),
            clock,
            new SoakOutput(
                other,
                new PrintStream(console, true, StandardCharsets.UTF_8),
                new Redactor().add(PASSWORD),
                clock),
            () -> 0L,
            new SoakSummary())
        .run();

    List<Path> files;
    try (Stream<Path> w = Files.walk(dir)) {
      files = w.filter(Files::isRegularFile).toList();
    }
    assertThat(files).isNotEmpty();
    for (Path f : files) {
      assertThat(Files.readString(f)).as(f.toString()).doesNotContain(PASSWORD);
    }
    String out = console.toString(StandardCharsets.UTF_8);
    assertThat(out).doesNotContain(PASSWORD).contains(Redactor.MASK);
    assertThat(Files.readString(other.resolve(SoakOutput.SUMMARY_JSON)))
        .contains("jdbc:oracle:thin:***@");
  }
}
