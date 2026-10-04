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
public record StepCursor(long scn, RedoRecordId lastApplied) {

  public static StepCursor at(long scn) {
    return new StepCursor(scn, null);
  }

  /** True when {@code id} was already applied under this cursor. */
  public boolean alreadyApplied(RedoRecordId id) {
    return lastApplied != null && id.compareTo(lastApplied) <= 0;
  }
}
