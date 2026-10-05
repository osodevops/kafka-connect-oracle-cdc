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

/**
 * The structure of one LOB_WRITE, LOB_TRIM or LOB_ERASE row (reference/lob-redo-shapes.md): a
 * PL/SQL block that selects the locator of {@code column} in the row named by {@code where} and
 * applies {@code ops} to it. LogMiner marks these rows STATUS 2 ("LOB sql_redo not re-executable")
 * although the block is complete.
 */
public record LobRedo(
    String owner, String table, String column, List<ColumnValue> where, List<Op> ops) {

  public LobRedo {
    Objects.requireNonNull(owner, "owner");
    Objects.requireNonNull(table, "table");
    Objects.requireNonNull(column, "column");
    where = List.copyOf(where);
    ops = List.copyOf(ops);
  }

  /** One DBMS_LOB call; offsets are one-based, amounts in characters (CLOB) or bytes (BLOB). */
  public sealed interface Op {}

  /** {@code dbms_lob.write(loc, amount, offset, buf)} with the buffer's literal. */
  public record Write(long amount, long offset, SqlLiteral data) implements Op {}

  /** {@code dbms_lob.trim(loc, length)}. */
  public record Trim(long length) implements Op {}

  /** {@code dbms_lob.erase(loc, amount, offset)}. */
  public record Erase(long amount, long offset) implements Op {}
}
