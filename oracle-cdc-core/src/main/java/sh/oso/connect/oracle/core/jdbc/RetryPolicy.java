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
package sh.oso.connect.oracle.core.jdbc;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongSupplier;

/**
 * Exponential backoff with full jitter, bounded by a total budget (PRD-00 CORE-CONN-6). The clock
 * and sleeper are injectable so unit tests run in no time.
 */
public final class RetryPolicy {

  /** Puts the thread to sleep; replaceable in tests. */
  public interface Sleeper {
    void sleep(Duration d) throws InterruptedException;
  }

  private final Duration initial;
  private final Duration max;
  private final Duration budget;
  private final LongSupplier clockMillis;
  private final Sleeper sleeper;

  public RetryPolicy(Duration budget) {
    this(
        Duration.ofSeconds(1),
        Duration.ofSeconds(30),
        budget,
        System::currentTimeMillis,
        d -> Thread.sleep(d.toMillis()));
  }

  public RetryPolicy(
      Duration initial, Duration max, Duration budget, LongSupplier clockMillis, Sleeper sleeper) {
    this.initial = initial;
    this.max = max;
    this.budget = budget;
    this.clockMillis = clockMillis;
    this.sleeper = sleeper;
  }

  /** Delay before the given attempt (1-based), capped and jittered. */
  public Duration delayFor(int attempt) {
    long base = Math.min(max.toMillis(), initial.toMillis() << Math.min(attempt - 1, 20));
    long jitter = base <= 1 ? 0 : ThreadLocalRandom.current().nextLong(base);
    return Duration.ofMillis(Math.max(0, base / 2 + jitter / 2));
  }

  /** A retry loop state: tracks the budget from the first failure. */
  public final class Attempts {
    private final long startedAt = clockMillis.getAsLong();
    private int attempt;

    /** Waits before the next attempt, or returns false when the budget is spent. */
    public boolean backoff() throws InterruptedException {
      attempt++;
      long elapsed = clockMillis.getAsLong() - startedAt;
      Duration delay = delayFor(attempt);
      if (elapsed + delay.toMillis() > budget.toMillis()) {
        return false;
      }
      sleeper.sleep(delay);
      return true;
    }

    public int attempt() {
      return attempt;
    }
  }

  public Attempts begin() {
    return new Attempts();
  }

  public Duration budget() {
    return budget;
  }
}
