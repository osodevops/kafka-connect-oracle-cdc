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
package sh.oso.connect.oracle.envelope.confluent;

/**
 * How the Confluent-compatible format names and fills its records (ADR-0028). Defaults are those of
 * Confluent's Oracle CDC Source connector, whose {@code output.*}, {@code key.template}, {@code
 * numeric.mapping}, {@code numeric.default.scale}, {@code oracle.date.mapping} and {@code
 * emit.tombstone.on.delete} properties the {@code cdc.output.confluent.*} keys mirror. An empty
 * field name leaves the field out.
 */
public record ConfluentSettings(
    String tableField,
    String scnField,
    String commitScnField,
    String opTypeField,
    String opTsField,
    String currentTsField,
    String rowIdField,
    String usernameField,
    String redoField,
    String undoField,
    String beforeStateField,
    String opRead,
    String opInsert,
    String opUpdate,
    String opDelete,
    String opTruncate,
    String keyTemplate,
    ConfluentTypes.NumericMapping numericMapping,
    int numericDefaultScale,
    ConfluentTypes.DateMapping dateMapping,
    boolean tombstonesOnDelete) {

  public static final String KEY_STRUCT_OR_VALUE = "${primaryKeyStructOrValue}";
  public static final String KEY_STRUCT = "${primaryKeyStruct}";
  public static final String KEY_CONCATENATED = "${primaryKey}";

  /** Confluent's defaults, with tombstones on delete as {@code tombstones} says. */
  public static ConfluentSettings defaults(boolean tombstones) {
    return new ConfluentSettings(
        "table",
        "scn",
        "",
        "op_type",
        "op_ts",
        "current_ts",
        "row_id",
        "username",
        "",
        "",
        "",
        "R",
        "I",
        "U",
        "D",
        "T",
        KEY_STRUCT_OR_VALUE,
        ConfluentTypes.NumericMapping.NONE,
        127,
        ConfluentTypes.DateMapping.DATE,
        tombstones);
  }
}
