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
import sh.oso.connect.oracle.core.model.TxKey;

/**
 * The total order of commits across RAC threads and PDBs: (commit SCN, thread, key) (CORE-POS-4).
 */
public final class CommitOrder {

  public static final Comparator<CommittedTransaction> COMPARATOR =
      Comparator.comparingLong(CommittedTransaction::commitScn)
          .thenComparingInt(CommittedTransaction::thread)
          .thenComparing(CommittedTransaction::key);

  private CommitOrder() {}

  public static int compare(
      long scnA, int threadA, TxKey keyA, long scnB, int threadB, TxKey keyB) {
    int c = Long.compare(scnA, scnB);
    if (c == 0) {
      c = Integer.compare(threadA, threadB);
    }
    return c == 0 ? keyA.compareTo(keyB) : c;
  }
}
