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
 * An Oracle transaction id as LogMiner exposes it: XIDUSN, XIDSLT, XIDSQN. Rendered {@code
 * usn.slot.sqn}, the form DBMS_TRANSACTION.LOCAL_TRANSACTION_ID returns and the bench ledger
 * stores.
 */
public record Xid(long usn, long slot, long sqn) implements Comparable<Xid> {

  public static final Xid ZERO = new Xid(0, 0, 0);

  public static Xid parse(String s) {
    String[] p = s.trim().split("\\.");
    if (p.length != 3) {
      throw new IllegalArgumentException("not a usn.slot.sqn transaction id: " + s);
    }
    return new Xid(Long.parseLong(p[0]), Long.parseLong(p[1]), Long.parseLong(p[2]));
  }

  /** All-zero XIDs appear on START rows of some internal operations and never own changes. */
  public boolean isZero() {
    return usn == 0 && slot == 0 && sqn == 0;
  }

  @Override
  public int compareTo(Xid o) {
    int c = Long.compare(usn, o.usn);
    if (c == 0) {
      c = Long.compare(slot, o.slot);
    }
    return c == 0 ? Long.compare(sqn, o.sqn) : c;
  }

  @Override
  public String toString() {
    return usn + "." + slot + "." + sqn;
  }
}
