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

  /**
   * CORE-POS-2 with the ADR-0014 cursor: the resume point is the earlier of the cursor and the
   * oldest record a restart must re-read (the first capture of an open non-journaled transaction,
   * the last journaled record of a journaled one), compared in redo order. Its SCN is the lower of
   * the two SCNs, so log selection and LogMiner's start bound never exclude it.
   */
  public static RedoRecordId resume(
      sh.oso.connect.oracle.core.mining.step.StepCursor cursor,
      Optional<RedoRecordId> oldestOpenNonJournaled) {
    RedoRecordId at = cursor.lastApplied();
    if (oldestOpenNonJournaled.isEmpty()) {
      return at == null
          ? new RedoRecordId(cursor.scn(), null, 0)
          : new RedoRecordId(cursor.scn(), at.rsId(), at.ssn());
    }
    RedoRecordId open = oldestOpenNonJournaled.get();
    long scn = Math.min(cursor.scn(), open.scn());
    if (at == null || !at.hasRba() || !open.hasRba()) {
      return new RedoRecordId(scn, open.hasRba() ? open.rsId() : null, open.ssn());
    }
    return open.compareTo(at) < 0
        ? new RedoRecordId(scn, open.rsId(), open.ssn())
        : new RedoRecordId(scn, at.rsId(), at.ssn());
  }

  /** The SCN form of the rule, for callers that only track SCNs. */
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
