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

import sh.oso.connect.oracle.core.errors.DecodeException;

/** Stable column keys are required to delete or move a row's separate LOB state (ADR-0028). */
public final class LobTopicRequirements {
  private LobTopicRequirements() {}

  public static void requireKey(TableSchema schema) {
    boolean lobs = schema.columns().stream().anyMatch(c -> c.type().isLob());
    if (!lobs) return;
    if (schema.keyColumns().isEmpty()
        || schema.keySource() == KeySource.ROWID
        || schema.keySource() == KeySource.NONE) {
      throw new DecodeException(
          "LOB topic table " + schema.table().fqn() + " has no stable column key.",
          "Add a primary key or NOT NULL unique index, or supply cdc.key.columns; topic mode"
              + " refuses keyless LOB state.");
    }
    if (schema.columns().stream().anyMatch(c -> c.type() == OracleType.XMLTYPE)) {
      throw new DecodeException(
          "LOB topic table "
              + schema.table().fqn()
              + " contains XMLTYPE, whose bytes are not supported.",
          "Exclude the XMLTYPE column or choose a supported LOB type; topic mode never publishes"
              + " unavailable values.");
    }
  }
}
