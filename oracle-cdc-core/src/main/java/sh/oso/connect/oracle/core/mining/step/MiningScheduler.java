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

import java.time.Duration;
import java.util.List;
import java.util.TreeSet;
import sh.oso.connect.oracle.core.errors.MiningStalledException;
import sh.oso.connect.oracle.core.logs.RedoLog;

/**
 * Chooses each step's end SCN (CORE-MINE-4, ADR-0005). The unit is the log, not the SCN: when the
 * pending range lies inside the current log the step runs to the safe end; otherwise it ends at a
 * log boundary, starting one log wide, doubling while steps finish within the target latency and
 * halving on a timeout (CORE-MINE-5). Three timeouts in a row at one log is a stall.
 */
public final class MiningScheduler {

  private final Duration targetLatency;
  private final int maxLogsPerStep;
  private int windowLogs = 1;
  private int consecutiveTimeoutsAtOne;
  private long plans;
  private long doublings;
  private long halvings;

  public MiningScheduler(Duration targetLatency, int maxLogsPerStep) {
    if (maxLogsPerStep < 1) {
      throw new IllegalArgumentException("maxLogsPerStep must be at least 1");
    }
    this.targetLatency = targetLatency;
    this.maxLogsPerStep = maxLogsPerStep;
  }

  /**
   * Plans the step from {@code startScn} towards {@code safeEndScn} given the logs that cover the
   * range. Boundaries are the NEXT_CHANGE# values strictly inside the range.
   */
  public StepPlan plan(long startScn, long safeEndScn, List<RedoLog> logsCoveringRange) {
    if (safeEndScn <= startScn) {
      throw new IllegalArgumentException("nothing to mine: " + startScn + " >= " + safeEndScn);
    }
    plans++;
    TreeSet<Long> boundaries = new TreeSet<>();
    for (RedoLog l : logsCoveringRange) {
      if (l.nextScn() > startScn && l.nextScn() < safeEndScn) {
        boundaries.add(l.nextScn());
      }
    }
    if (boundaries.isEmpty()) {
      return new StepPlan(startScn, safeEndScn, windowLogs, 0, true, "inside the current log");
    }
    if (windowLogs > boundaries.size()) {
      return new StepPlan(
          startScn, safeEndScn, windowLogs, boundaries.size() + 1, true, "window covers the range");
    }
    long end = boundaries.stream().skip(windowLogs - 1).findFirst().orElseThrow();
    return new StepPlan(
        startScn, end, windowLogs, boundaries.size() + 1, false, "log-count window");
  }

  /** A step finished: widen the window when it was quick. */
  public void stepCompleted(Duration elapsed) {
    consecutiveTimeoutsAtOne = 0;
    if (elapsed.compareTo(targetLatency) < 0 && windowLogs < maxLogsPerStep) {
      windowLogs = Math.min(maxLogsPerStep, windowLogs * 2);
      doublings++;
    }
  }

  /** A step timed out: halve the window; at one log, count towards a stall. */
  public void stepTimedOut(StepPlan plan) {
    halvings++;
    if (windowLogs > 1) {
      windowLogs = Math.max(1, windowLogs / 2);
      consecutiveTimeoutsAtOne = 0;
      return;
    }
    consecutiveTimeoutsAtOne++;
    if (consecutiveTimeoutsAtOne >= 3) {
      throw new MiningStalledException(
          "Mining "
              + plan.startScn()
              + ".."
              + plan.endScn()
              + " timed out three times at a single log",
          "Check the database for a long-running LogMiner session, redo volume in that range and"
              + " cdc.mining.query.timeout.ms; see V$SESSION_LONGOPS for the mining session.");
    }
  }

  public int windowLogs() {
    return windowLogs;
  }

  public long plans() {
    return plans;
  }

  public long doublings() {
    return doublings;
  }

  public long halvings() {
    return halvings;
  }
}
