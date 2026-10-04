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
package sh.oso.connect.oracle.core.model;

import java.util.Objects;

/**
 * Position of one redo record: SCN, RS_ID (redo byte address, fixed-width text) and SSN (sequence
 * within the record). Totally ordered in redo order; the unit of step cursors and of rollback
 * matching.
 */
public record RedoRecordId(long scn, String rsId, long ssn) implements Comparable<RedoRecordId> {

  public RedoRecordId {
    Objects.requireNonNull(rsId, "rsId");
  }

  @Override
  public int compareTo(RedoRecordId o) {
    int c = Long.compare(scn, o.scn);
    if (c == 0) {
      c = rsId.trim().compareTo(o.rsId.trim());
    }
    return c == 0 ? Long.compare(ssn, o.ssn) : c;
  }

  @Override
  public String toString() {
    return scn + "/" + rsId.trim() + "/" + ssn;
  }
}
