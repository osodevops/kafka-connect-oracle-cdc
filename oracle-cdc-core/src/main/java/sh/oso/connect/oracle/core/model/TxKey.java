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
 * The buffer key for a transaction: source container id plus XID. Undo is local to each PDB, so the
 * same XID can be live in two PDBs inside one mining window (ADR-0002, reference/pdb-routing.md).
 */
public record TxKey(int srcConId, Xid xid) implements Comparable<TxKey> {

  public TxKey {
    Objects.requireNonNull(xid, "xid");
  }

  @Override
  public int compareTo(TxKey o) {
    int c = Integer.compare(srcConId, o.srcConId);
    return c == 0 ? xid.compareTo(o.xid) : c;
  }

  @Override
  public String toString() {
    return srcConId + ":" + xid;
  }
}
