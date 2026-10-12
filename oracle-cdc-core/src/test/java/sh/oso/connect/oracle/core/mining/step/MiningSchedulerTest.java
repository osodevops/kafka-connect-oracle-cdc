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
package sh.oso.connect.oracle.core.mining.step;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.errors.MiningStalledException;
import sh.oso.connect.oracle.core.logs.RedoLog;
import sh.oso.connect.oracle.core.testkit.FakeCatalog;

class MiningSchedulerTest {

  private static List<RedoLog> logs(int count) {
    // logs of 100 SCNs starting at 1000: boundaries 1100, 1200, ...
    return new FakeCatalog().archivedRun(1, 1, count, 1000, 100).archived;
  }

  @Test
  void insideTheCurrentLogMinesToTheSafeEnd() {
    MiningScheduler s = new MiningScheduler(Duration.ofSeconds(2), 8);
    StepPlan p = s.plan(1000, 1050, logs(1));
    assertThat(p.endScn()).isEqualTo(1050);
    assertThat(p.caughtUp()).isTrue();
    assertThat(p.logsAvailable()).isZero();
  }

  @Test
  void windowDoublesOnFastStepsUpToTheMaximumAndHalvesOnTimeout() {
    MiningScheduler s = new MiningScheduler(Duration.ofSeconds(2), 8);
    List<RedoLog> ten = logs(10);
    StepPlan p1 = s.plan(1000, 2000, ten);
    assertThat(p1.windowLogs()).isEqualTo(1);
    assertThat(p1.endScn()).isEqualTo(1100);
    assertThat(p1.caughtUp()).isFalse();
    assertThat(p1.logsAvailable()).isEqualTo(10);
    s.stepCompleted(Duration.ofMillis(500));
    assertThat(s.plan(1100, 2000, ten).endScn()).isEqualTo(1300);
    s.stepCompleted(Duration.ofMillis(500));
    assertThat(s.plan(1300, 2000, ten).endScn()).isEqualTo(1700);
    s.stepCompleted(Duration.ofMillis(500));
    assertThat(s.windowLogs()).isEqualTo(8);
    StepPlan wide = s.plan(1700, 2000, ten);
    assertThat(wide.endScn()).isEqualTo(2000);
    assertThat(wide.caughtUp()).isTrue();
    assertThat(wide.reason()).contains("covers");
    s.stepCompleted(Duration.ofMillis(1));
    assertThat(s.windowLogs()).as("capped").isEqualTo(8);
    s.stepCompleted(Duration.ofSeconds(5));
    assertThat(s.windowLogs()).as("slow step does not widen").isEqualTo(8);
    s.stepTimedOut(wide);
    assertThat(s.windowLogs()).isEqualTo(4);
    s.stepTimedOut(wide);
    s.stepTimedOut(wide);
    assertThat(s.windowLogs()).isEqualTo(1);
    assertThat(s.doublings()).isEqualTo(3);
    assertThat(s.halvings()).isEqualTo(3);
    assertThat(s.plans()).isEqualTo(4);
  }

  @Test
  void threeTimeoutsAtOneLogIsAStallAndACompletionResetsTheCount() {
    MiningScheduler s = new MiningScheduler(Duration.ofSeconds(2), 8);
    StepPlan p = s.plan(1000, 1100, logs(1));
    s.stepTimedOut(p);
    s.stepTimedOut(p);
    s.stepCompleted(Duration.ofSeconds(3));
    s.stepTimedOut(p);
    s.stepTimedOut(p);
    assertThatThrownBy(() -> s.stepTimedOut(p))
        .isInstanceOf(MiningStalledException.class)
        .hasMessageContaining("CDC-4003");
    assertThatThrownBy(() -> s.plan(1000, 1000, logs(1)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new MiningScheduler(Duration.ofSeconds(1), 0))
        .isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * ADR-0026: thread 1 switches every 100 SCNs, thread 2 every 250. A window of N logs holds N logs
   * of the thread that switches most and no more than N of any thread. Counting both threads'
   * boundaries together ended a four-log window at 1300, three logs of thread 1.
   */
  @org.junit.jupiter.api.Tag("rac-window")
  @Test
  void aWindowOfNLogsHoldsAtMostNLogsOfAnyThread() {
    List<RedoLog> two = new java.util.ArrayList<>(logs(10)); // thread 1: 1100, 1200, ... 1900
    two.addAll(new FakeCatalog().archivedRun(2, 1, 4, 1000, 250).archived); // 1250, 1500, 1750
    MiningScheduler s = new MiningScheduler(Duration.ofSeconds(2), 8);
    StepPlan one = s.plan(1000, 2000, two);
    assertThat(one.endScn()).as("one log on the busier thread").isEqualTo(1100);
    assertThat(one.logsAvailable()).as("logs on the thread with most").isEqualTo(10);
    s.stepCompleted(Duration.ofMillis(1));
    s.stepCompleted(Duration.ofMillis(1));
    StepPlan four = s.plan(1000, 2000, two);
    assertThat(four.windowLogs()).isEqualTo(4);
    assertThat(four.endScn())
        .as("thread 1's fourth boundary; thread 2 has two logs to there")
        .isEqualTo(1400);
    s.stepCompleted(Duration.ofMillis(1));
    StepPlan eight = s.plan(1000, 2000, two);
    assertThat(eight.endScn()).isEqualTo(1800);
    assertThat(eight.caughtUp()).isFalse();
    // thread 2 is idle in a long current log: it never lengthens the step
    List<RedoLog> idle = new java.util.ArrayList<>(logs(10));
    idle.add(new FakeCatalog().onlineCurrent(2, 7, 900).online.get(0));
    MiningScheduler t = new MiningScheduler(Duration.ofSeconds(2), 8);
    assertThat(t.plan(1000, 2000, idle).endScn()).isEqualTo(1100);
    // the busy thread alone: the boundaries of before
    assertThat(new MiningScheduler(Duration.ofSeconds(2), 8).plan(1000, 2000, logs(10)).endScn())
        .isEqualTo(1100);
  }

  @Test
  void aThreadWithFewerBoundariesThanTheWindowDoesNotEndTheStep() {
    List<RedoLog> two = new java.util.ArrayList<>(logs(3)); // thread 1: 1100, 1200, 1300 (to 1300)
    two.addAll(new FakeCatalog().archivedRun(2, 1, 1, 1000, 150).archived); // 1150
    MiningScheduler s = new MiningScheduler(Duration.ofSeconds(2), 8);
    s.stepCompleted(Duration.ofMillis(1)); // a window of two logs
    StepPlan p = s.plan(1000, 1400, two);
    assertThat(p.endScn()).as("thread 1's second boundary").isEqualTo(1200);
    s.stepCompleted(Duration.ofMillis(1)); // four logs: more than any thread has
    StepPlan all = s.plan(1000, 1400, two);
    assertThat(all.endScn()).isEqualTo(1400);
    assertThat(all.caughtUp()).isTrue();
    assertThat(all.logsAvailable()).isEqualTo(4);
  }
}
