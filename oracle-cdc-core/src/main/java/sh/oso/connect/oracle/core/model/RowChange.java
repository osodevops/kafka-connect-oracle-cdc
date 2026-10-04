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

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * One decoded row change. {@code before} and {@code after} hold Java values keyed by column name in
 * table order; a null value is a SQL NULL, an absent column was not logged. {@code partial} is true
 * when supplemental logging gave less than the full before image (CORE-DEC-4).
 */
public record RowChange(
    TableId table,
    Operation op,
    Map<String, Object> before,
    Map<String, Object> after,
    boolean partial,
    String rowId,
    RedoRecordId id,
    TxKey tx,
    Instant timestamp) {

  public RowChange {
    Objects.requireNonNull(table, "table");
    Objects.requireNonNull(op, "op");
    before = before == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(before));
    after = after == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(after));
  }
}
