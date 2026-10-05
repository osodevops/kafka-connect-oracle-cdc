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
package sh.oso.connect.oracle.core.buffer;

import java.time.Duration;

/**
 * When a transaction is journaled (CORE-TX-4): open longer than {@code maxAge}, or holding more
 * than {@code maxEvents} changes. Chunks are split at about {@code chunkMaxBytes}. A policy with no
 * sink never journals.
 *
 * @param maxAge age threshold measured from the first captured change, wall clock
 * @param maxEvents event threshold
 * @param chunkMaxBytes target chunk size in bytes
 */
public record JournalPolicy(Duration maxAge, long maxEvents, int chunkMaxBytes) {

  public static final JournalPolicy NEVER = new JournalPolicy(null, Long.MAX_VALUE, 1 << 19);

  public boolean enabled() {
    return maxAge != null || maxEvents < Long.MAX_VALUE;
  }

  boolean due(Duration age, long events) {
    return (maxAge != null && age.compareTo(maxAge) >= 0) || events >= maxEvents;
  }
}
