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

/**
 * Position of one redo record: SCN, RS_ID (redo byte address, fixed-width text) and SSN (sequence
 * within the record). Totally ordered in redo order; the unit of step cursors and of rollback
 * matching.
 */
public record RedoRecordId(long scn, String rsId, long ssn) implements Comparable<RedoRecordId> {

  public RedoRecordId {}

  /**
   * Redo order within a thread: the redo byte address (RS_ID, then SSN) first, because the log is
   * append-only in that order while SCNs can arrive out of order when a private redo strand is
   * bound late (ADR-0014). Ids without an RS_ID fall back to SCN order.
   */
  @Override
  public int compareTo(RedoRecordId o) {
    if (rsId != null && o.rsId != null) {
      int c = rsId.trim().compareTo(o.rsId.trim());
      if (c == 0) {
        c = Long.compare(ssn, o.ssn);
      }
      return c == 0 ? Long.compare(scn, o.scn) : c;
    }
    return Long.compare(scn, o.scn);
  }

  /** True when this record carries a redo byte address. */
  public boolean hasRba() {
    return rsId != null && !rsId.isBlank();
  }

  @Override
  public String toString() {
    return scn + "/" + (rsId == null ? "-" : rsId.trim()) + "/" + ssn;
  }
}
