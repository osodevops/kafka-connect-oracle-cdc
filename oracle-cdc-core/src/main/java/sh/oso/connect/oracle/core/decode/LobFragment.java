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

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;

/**
 * A decoded LOB_WRITE, LOB_TRIM or LOB_ERASE row: the row it names by its non-LOB columns, the LOB
 * column, and the edits with their data already typed ({@code String} for CLOB and NCLOB, {@code
 * byte[]} for BLOB). Offsets are one-based as in DBMS_LOB.
 */
public record LobFragment(
    TableId table,
    String column,
    boolean binary,
    Map<String, Object> where,
    List<Edit> edits,
    String rowId,
    RedoRecordId id,
    TxKey tx,
    Instant timestamp) {

  public LobFragment {
    Objects.requireNonNull(table, "table");
    Objects.requireNonNull(column, "column");
    where = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(where));
    edits = List.copyOf(edits);
  }

  public sealed interface Edit {}

  /**
   * Writes {@code data} at {@code offset}, extending the LOB when it ends past the current end.
   * Text offsets and lengths count characters (code points), as DBMS_LOB does: a character outside
   * the Basic Multilingual Plane is one, not two (reference/lob-redo-shapes.md).
   */
  public record Write(long offset, Object data) implements Edit {
    public long length() {
      if (data instanceof byte[] b) {
        return b.length;
      }
      String s = (String) data;
      return s.codePointCount(0, s.length());
    }
  }

  public record Trim(long length) implements Edit {}

  public record Erase(long amount, long offset) implements Edit {}
}
