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
import java.util.Set;

/**
 * One decoded row change. {@code before} and {@code after} hold Java values keyed by column name in
 * table order; a null value is a SQL NULL, an absent column was not logged. {@code partial} is true
 * when supplemental logging gave less than the full before image (CORE-DEC-4). {@code sql} is the
 * statement text, present only when a record format publishes it, otherwise null.
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
    Instant timestamp,
    int schemaVersion,
    RedoText sql,
    Set<String> touchedLobs) {

  public RowChange {
    Objects.requireNonNull(table, "table");
    Objects.requireNonNull(op, "op");
    touchedLobs = touchedLobs == null ? Set.of() : Set.copyOf(touchedLobs);
    before = before == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(before));
    after = after == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(after));
  }

  /** A change without explicit LOB write tracking (payloads before separate LOB topics). */
  public RowChange(
      TableId table,
      Operation op,
      Map<String, Object> before,
      Map<String, Object> after,
      boolean partial,
      String rowId,
      RedoRecordId id,
      TxKey tx,
      Instant timestamp,
      int schemaVersion,
      RedoText sql) {
    this(table, op, before, after, partial, rowId, id, tx, timestamp, schemaVersion, sql, Set.of());
  }

  /** A change without its statement text. */
  public RowChange(
      TableId table,
      Operation op,
      Map<String, Object> before,
      Map<String, Object> after,
      boolean partial,
      String rowId,
      RedoRecordId id,
      TxKey tx,
      Instant timestamp,
      int schemaVersion) {
    this(table, op, before, after, partial, rowId, id, tx, timestamp, schemaVersion, null);
  }

  /** A change not tied to a schema version: it renders with the table's current one. */
  public RowChange(
      TableId table,
      Operation op,
      Map<String, Object> before,
      Map<String, Object> after,
      boolean partial,
      String rowId,
      RedoRecordId id,
      TxKey tx,
      Instant timestamp) {
    this(table, op, before, after, partial, rowId, id, tx, timestamp, 0, null);
  }

  /** This change with {@code sql} as its statement text. */
  public RowChange withSql(RedoText sql) {
    return new RowChange(
        table,
        op,
        before,
        after,
        partial,
        rowId,
        id,
        tx,
        timestamp,
        schemaVersion,
        sql,
        touchedLobs);
  }

  /** LOB columns explicitly written by this row or its assembled fragments, even if unavailable. */
  public RowChange withTouchedLobs(Set<String> columns) {
    return new RowChange(
        table, op, before, after, partial, rowId, id, tx, timestamp, schemaVersion, sql, columns);
  }
}
