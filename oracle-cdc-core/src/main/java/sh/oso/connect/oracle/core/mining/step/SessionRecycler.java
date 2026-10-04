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
import java.time.Instant;
import java.util.function.Supplier;

/** CORE-MINE-7: the LogMiner session is restarted after {@code maxAge} to release PGA. */
public final class SessionRecycler {

  private final Duration maxAge;
  private final Supplier<Instant> clock;
  private Instant started;
  private long recycles;

  public SessionRecycler(Duration maxAge, Supplier<Instant> clock) {
    this.maxAge = maxAge;
    this.clock = clock;
    this.started = clock.get();
  }

  public boolean due() {
    return !clock.get().isBefore(started.plus(maxAge));
  }

  public void recycled() {
    started = clock.get();
    recycles++;
  }

  public long recycles() {
    return recycles;
  }
}
