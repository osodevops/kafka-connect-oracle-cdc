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

import java.util.Comparator;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.TxKey;

/**
 * The total order of commits across RAC threads and PDBs: (commit SCN, thread, key) (CORE-POS-4).
 */
public final class CommitOrder {

  /** Commit order of two transactions: redo order of their COMMIT rows within a thread. */
  public static final Comparator<CommittedTransaction> COMPARATOR =
      (a, b) -> compare(a.commitId(), a.thread(), a.key(), b.commitId(), b.thread(), b.key());

  private CommitOrder() {}

  /**
   * Compares two commits. With redo byte addresses on both sides the order is thread, then redo
   * byte address: the order the connector emits in, since the log is append-only in that order and
   * SCNs can be out of order when a private redo strand is bound late (ADR-0014). Without them
   * (positions written before ADR-0014) the order is commit SCN, thread, transaction key.
   */
  public static int compare(
      RedoRecordId a, int threadA, TxKey keyA, RedoRecordId b, int threadB, TxKey keyB) {
    if (a != null && b != null && a.hasRba() && b.hasRba()) {
      int c = Integer.compare(threadA, threadB);
      if (c != 0) {
        return c;
      }
      c = a.rsId().trim().compareTo(b.rsId().trim());
      if (c != 0) {
        return c;
      }
      return Long.compare(a.ssn(), b.ssn());
    }
    return compare(a == null ? 0 : a.scn(), threadA, keyA, b == null ? 0 : b.scn(), threadB, keyB);
  }

  /** SCN order with thread and key as tie breaks (the pre-ADR-0014 rule). */
  public static int compare(
      long scnA, int threadA, TxKey keyA, long scnB, int threadB, TxKey keyB) {
    int c = Long.compare(scnA, scnB);
    if (c == 0) {
      c = Integer.compare(threadA, threadB);
    }
    return c == 0 ? keyA.compareTo(keyB) : c;
  }
}
