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

import sh.oso.connect.oracle.core.topology.ThreadInfo;

/**
 * CORE-LOG-3: a redo thread's state in V$THREAD differs from the inventory's previous read. {@code
 * state} and {@code previous} are {@code open}, {@code closed} or {@code disabled}; {@code
 * sequence} is the thread's current log sequence; {@code closedAtScn} is where a closed thread's
 * redo ends once its last log is archived, and null otherwise.
 */
public record ThreadStateChange(
    int thread, String previous, String state, long sequence, Long closedAtScn) {

  public static final String OPEN = "open";
  public static final String CLOSED = "closed";
  public static final String DISABLED = "disabled";

  /** The state a V$THREAD row shows: disabled, or else its STATUS (OPEN or CLOSED). */
  public static String of(ThreadInfo t) {
    if (!t.enabled()) {
      return DISABLED;
    }
    return "CLOSED".equals(t.status() == null ? "" : t.status().trim()) ? CLOSED : OPEN;
  }
}
