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

import java.util.List;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;

/**
 * Result of running one step. Only COMPLETE and CUT carry events to apply; RETRY and TIMEOUT carry
 * none, so a failed step can never leak a partial result (CORE-LOG-5).
 */
public record StepOutcome(
    Kind kind, List<MiningEvent> events, StepCursor next, int rowsSeen, Throwable cause) {

  public enum Kind {
    COMPLETE,
    CUT,
    RETRY,
    TIMEOUT
  }

  public StepOutcome {
    events = List.copyOf(events);
    if ((kind == Kind.RETRY || kind == Kind.TIMEOUT) && !events.isEmpty()) {
      throw new IllegalArgumentException("a failed step cannot carry events");
    }
  }

  public boolean applies() {
    return kind == Kind.COMPLETE || kind == Kind.CUT;
  }
}
