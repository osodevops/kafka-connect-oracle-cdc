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
package sh.oso.connect.oracle.core.decode;

import java.util.List;
import java.util.Objects;
import sh.oso.connect.oracle.core.model.Operation;

/**
 * The structure of one reconstructed DML statement. INSERT carries its columns in {@code set};
 * UPDATE carries the assignments in {@code set} and the before image in {@code where}; DELETE
 * carries the before image in {@code where}. A {@code ROWID = '...'} predicate, which LogMiner adds
 * to undo rows even with NO_ROWID_IN_STMT, lands in {@code rowId} rather than in {@code where}.
 */
public record ParsedDml(
    Operation op,
    String owner,
    String table,
    List<ColumnValue> set,
    List<ColumnValue> where,
    String rowId) {

  public ParsedDml {
    Objects.requireNonNull(op, "op");
    Objects.requireNonNull(owner, "owner");
    Objects.requireNonNull(table, "table");
    set = List.copyOf(set);
    where = List.copyOf(where);
    if (op != Operation.INSERT && op != Operation.UPDATE && op != Operation.DELETE) {
      throw new IllegalArgumentException("not a DML operation: " + op);
    }
  }
}
