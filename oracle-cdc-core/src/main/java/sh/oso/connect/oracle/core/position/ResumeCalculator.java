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
package sh.oso.connect.oracle.core.position;

import java.util.Optional;
import sh.oso.connect.oracle.core.model.RedoRecordId;

/**
 * CORE-POS-2: {@code resume_scn} is the lower of the safe mined SCN and the first captured record
 * of the oldest open, non-journaled transaction. It never moves backwards within one task lifetime.
 */
public final class ResumeCalculator {

  private ResumeCalculator() {}

  public static long resumeScn(long safeMinedScn, Optional<RedoRecordId> oldestOpenNonJournaled) {
    long open = oldestOpenNonJournaled.map(RedoRecordId::scn).orElse(Long.MAX_VALUE);
    return Math.min(safeMinedScn, open);
  }

  /** Applies the rule to a position, refusing to move backwards. */
  public static Position advance(
      Position current, long safeMinedScn, Optional<RedoRecordId> oldestOpenNonJournaled) {
    long candidate = resumeScn(safeMinedScn, oldestOpenNonJournaled);
    return candidate > current.resumeScn() ? current.withResumeScn(candidate) : current;
  }
}
