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
package sh.oso.connect.oracle.core.logs;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.function.LongSupplier;

/**
 * {@code cdc.rac.safety.lag.ms} (CORE-LOG-6, ADR-0026): on a database with more than one enabled
 * redo thread the online safe end is not the current SCN but the current SCN read at least the lag
 * ago. Each read of the current SCN is kept with the time it was read; the safe end is the newest
 * read that is old enough. An instance whose LGWR is behind has then had the lag to write its redo
 * below that SCN before a step's end passes it. This is defence in depth: the per-thread redo
 * address cursor is what mines such redo when it does arrive late (ADR-0026). With one enabled
 * thread the lag is zero and the safe end is the current SCN, as before.
 */
public final class SafetyLag {

  /** The lag on RAC when the key is left at -1. */
  public static final long RAC_DEFAULT_MS = 3000;

  /** The safe end while no read is old enough yet: nothing is mined. */
  public static final long NONE = 0;

  private final long lagMillis;
  private final LongSupplier clockMillis;

  /** Reads of the current SCN, oldest first, as {time, scn}; the first may be old enough. */
  private final ArrayDeque<long[]> samples = new ArrayDeque<>();

  /**
   * The lag for {@code configuredMs} (the key; -1 is the default) on a database with {@code
   * enabledThreads} enabled redo threads: zero with one thread whatever the key says, otherwise the
   * key, or 3000 ms when it is -1.
   */
  public static Duration effective(long configuredMs, int enabledThreads) {
    if (enabledThreads <= 1) {
      return Duration.ZERO;
    }
    return Duration.ofMillis(configuredMs < 0 ? RAC_DEFAULT_MS : configuredMs);
  }

  /** {@code clockMillis} is a monotonic clock in milliseconds. */
  public SafetyLag(Duration lag, LongSupplier clockMillis) {
    this.lagMillis = lag.toMillis();
    this.clockMillis = clockMillis;
  }

  public Duration lag() {
    return Duration.ofMillis(lagMillis);
  }

  /**
   * The safe end given the current SCN read now: the SCN itself without a lag, else the newest read
   * at least the lag old, or {@link #NONE} while there is none.
   */
  public synchronized long safeEnd(long currentScn) {
    if (lagMillis <= 0) {
      return currentScn;
    }
    long now = clockMillis.getAsLong();
    samples.addLast(new long[] {now, currentScn});
    long[] newestOldEnough = null;
    while (!samples.isEmpty() && now - samples.peekFirst()[0] >= lagMillis) {
      newestOldEnough = samples.pollFirst();
    }
    if (newestOldEnough == null) {
      return NONE;
    }
    samples.addFirst(newestOldEnough); // the answer until a newer read is old enough
    return newestOldEnough[1];
  }
}
