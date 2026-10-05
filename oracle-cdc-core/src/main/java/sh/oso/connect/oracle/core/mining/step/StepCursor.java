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

import sh.oso.connect.oracle.core.model.RedoRecordId;

/**
 * Where the next step starts: an SCN for START_LOGMNR plus, after a DDL step cut, the last record
 * already applied at that SCN so the re-mined rows at the same SCN are not applied twice.
 */
public record StepCursor(long scn, RedoRecordId lastApplied, boolean inclusive) {

  /** A cursor at an SCN with no redo byte address: rows are selected by SCN only (legacy). */
  public static StepCursor at(long scn) {
    return new StepCursor(scn, null, false);
  }

  /**
   * A cursor from a position: re-read from the resume record inclusive, so the first row of an open
   * transaction is mined again; without a redo byte address it is an SCN cursor.
   */
  public static StepCursor resume(RedoRecordId point) {
    return point == null || !point.hasRba()
        ? at(point == null ? 0 : point.scn())
        : new StepCursor(point.scn(), point, true);
  }

  /** The cursor after applying {@code id}: later rows have a greater redo byte address. */
  public StepCursor after(long minedToScn, RedoRecordId id) {
    return new StepCursor(minedToScn, id, false);
  }

  public boolean hasRba() {
    return lastApplied != null && lastApplied.hasRba();
  }

  /** True when {@code id} was already applied under this cursor. */
  public boolean alreadyApplied(RedoRecordId id) {
    if (lastApplied == null) {
      return false;
    }
    int c = id.compareTo(lastApplied);
    return inclusive ? c < 0 : c <= 0;
  }
}
