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
package sh.oso.connect.oracle.core.schema;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import sh.oso.connect.oracle.core.errors.DecodeException;
import sh.oso.connect.oracle.core.model.TableId;

/**
 * Picks the record key for a table (SRC-TOP-2, SRC-TOP-3): a per-table override first, then the
 * primary key, then the first NOT NULL unique index, then the {@code cdc.key.missing} policy.
 */
public final class KeySelector {

  public enum MissingKeyPolicy {
    FAIL,
    ROWID,
    NONE;

    public static MissingKeyPolicy parse(String s) {
      return s == null ? FAIL : valueOf(s.trim().toUpperCase(Locale.ROOT));
    }
  }

  /** Dictionary facts about candidate keys. */
  public record Candidates(List<String> primaryKey, List<List<String>> notNullUniqueIndexes) {
    public Candidates {
      primaryKey = List.copyOf(primaryKey);
      notNullUniqueIndexes = List.copyOf(notNullUniqueIndexes);
    }
  }

  private final Map<String, List<String>> overrides;
  private final MissingKeyPolicy policy;

  /** Overrides keyed by upper-cased fully qualified table name. */
  public KeySelector(Map<String, List<String>> overrides, MissingKeyPolicy policy) {
    this.overrides = Map.copyOf(overrides);
    this.policy = policy;
  }

  public TableSchema select(TableSchema schema, Candidates c) {
    TableId t = schema.table();
    List<String> override = overrides.get(t.fqnUpper());
    if (override != null) {
      for (String col : override) {
        if (schema.column(col) == null) {
          throw new DecodeException(
              "Key override for " + t.fqn() + " names column " + col + " which does not exist",
              "Fix the cdc.key override for this table.");
        }
      }
      return schema.withKey(override, KeySource.OVERRIDE);
    }
    if (!c.primaryKey().isEmpty()) {
      return schema.withKey(c.primaryKey(), KeySource.PRIMARY_KEY);
    }
    if (!c.notNullUniqueIndexes().isEmpty()) {
      return schema.withKey(c.notNullUniqueIndexes().get(0), KeySource.UNIQUE_INDEX);
    }
    switch (policy) {
      case ROWID:
        return schema.withKey(List.of(), KeySource.ROWID);
      case NONE:
        return schema.withKey(List.of(), KeySource.NONE);
      default:
        throw new DecodeException(
            t.fqn() + " has no primary key or NOT NULL unique index and cdc.key.missing=fail",
            "Add a key, set cdc.key.missing to rowid or none, or exclude the table.");
    }
  }
}
