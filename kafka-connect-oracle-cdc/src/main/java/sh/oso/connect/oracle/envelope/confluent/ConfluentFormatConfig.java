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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;

/**
 * The {@code cdc.output.confluent.*} keys (ADR-0028). Each mirrors a property of Confluent's Oracle
 * CDC Source connector with the same default: {@code output.table.name.field} is {@code
 * cdc.output.confluent.table.name.field}, {@code key.template} is {@code
 * cdc.output.confluent.key.template}, and so on, so a migration copies the values unchanged.
 */
public final class ConfluentFormatConfig {

  public static final String GROUP = "Confluent-compatible format";
  public static final String PREFIX = "cdc.output.confluent.";

  public static final String TABLE_NAME_FIELD = PREFIX + "table.name.field";
  public static final String SCN_FIELD = PREFIX + "scn.field";
  public static final String COMMIT_SCN_FIELD = PREFIX + "commit.scn.field";
  public static final String OP_TYPE_FIELD = PREFIX + "op.type.field";
  public static final String OP_TS_FIELD = PREFIX + "op.ts.field";
  public static final String CURRENT_TS_FIELD = PREFIX + "current.ts.field";
  public static final String ROW_ID_FIELD = PREFIX + "row.id.field";
  public static final String USERNAME_FIELD = PREFIX + "username.field";
  public static final String REDO_FIELD = PREFIX + "redo.field";
  public static final String UNDO_FIELD = PREFIX + "undo.field";
  public static final String BEFORE_STATE_FIELD = PREFIX + "before.state.field";
  public static final String OP_READ = PREFIX + "op.type.read.value";
  public static final String OP_INSERT = PREFIX + "op.type.insert.value";
  public static final String OP_UPDATE = PREFIX + "op.type.update.value";
  public static final String OP_DELETE = PREFIX + "op.type.delete.value";
  public static final String OP_TRUNCATE = PREFIX + "op.type.truncate.value";
  public static final String KEY_TEMPLATE = PREFIX + "key.template";
  public static final String NUMERIC_MAPPING = PREFIX + "numeric.mapping";
  public static final String NUMERIC_DEFAULT_SCALE = PREFIX + "numeric.default.scale";
  public static final String DATE_MAPPING = PREFIX + "date.mapping";

  private static final String PLACEMENT =
      " Empty leaves the field out. A name starting with header: puts the value in a record header"
          + " of that name instead; a dotted name such as meta.op_ts puts it in a nested struct.";

  private ConfluentFormatConfig() {}

  public static void define(ConfigDef def) {
    int n = 0;
    n =
        field(
            def,
            n,
            TABLE_NAME_FIELD,
            "table",
            "The fully qualified table name, PDB (or database), owner and table separated by dots."
                + " Confluent's output.table.name.field.",
            "Table name field");
    n =
        field(
            def,
            n,
            SCN_FIELD,
            "scn",
            "The SCN of the change, or the snapshot SCN for a snapshot row. Confluent's"
                + " output.scn.field.",
            "SCN field");
    n =
        field(
            def,
            n,
            COMMIT_SCN_FIELD,
            "",
            "The commit SCN of the change's transaction, or the snapshot SCN for a snapshot row."
                + " Confluent's output.commit.scn.field.",
            "Commit SCN field");
    n =
        field(
            def,
            n,
            OP_TYPE_FIELD,
            "op_type",
            "The operation, one of the op.type values. Confluent's output.op.type.field.",
            "Operation field");
    n =
        field(
            def,
            n,
            OP_TS_FIELD,
            "op_ts",
            "The time of the change in the redo, as milliseconds since the epoch; null for a"
                + " snapshot row. Confluent's output.op.ts.field.",
            "Operation time field");
    n =
        field(
            def,
            n,
            CURRENT_TS_FIELD,
            "current_ts",
            "The time the connector wrote the record, as milliseconds since the epoch. Confluent's"
                + " output.current.ts.field.",
            "Current time field");
    n =
        field(
            def,
            n,
            ROW_ID_FIELD,
            "row_id",
            "The ROWID of the changed row; null for a snapshot row. Confluent's"
                + " output.row.id.field.",
            "Row id field");
    n =
        field(
            def,
            n,
            USERNAME_FIELD,
            "username",
            "The database user who made the change; null for a snapshot row. Confluent's"
                + " output.username.field.",
            "Username field");
    n =
        field(
            def,
            n,
            REDO_FIELD,
            "",
            "The SQL_REDO text LogMiner gave for the change. Never filled for a table"
                + " cdc.columns.exclude could touch. Confluent's output.redo.field.",
            "Redo field");
    n =
        field(
            def,
            n,
            UNDO_FIELD,
            "",
            "The SQL_UNDO text LogMiner gave for the change. Never filled for a table"
                + " cdc.columns.exclude could touch. Confluent's output.undo.field.",
            "Undo field");
    def.define(
        BEFORE_STATE_FIELD,
        Type.STRING,
        "",
        Importance.LOW,
        "Name of a field that holds, in update records only, the row before the change as a struct"
            + " of the table's columns. Empty leaves it out. A plain name: neither header: nor a"
            + " dotted path. Confluent's output.before.state.field.",
        GROUP,
        ++n,
        Width.MEDIUM,
        "Before-state field");
    n = op(def, n, OP_READ, "R", "snapshot rows", "read");
    n = op(def, n, OP_INSERT, "I", "inserts", "insert");
    n = op(def, n, OP_UPDATE, "U", "updates", "update");
    n = op(def, n, OP_DELETE, "D", "deletes", "delete");
    n = op(def, n, OP_TRUNCATE, "T", "truncates", "truncate");
    def.define(
        KEY_TEMPLATE,
        Type.STRING,
        ConfluentSettings.KEY_STRUCT_OR_VALUE,
        ConfigDef.ValidString.in(
            ConfluentSettings.KEY_STRUCT_OR_VALUE,
            ConfluentSettings.KEY_STRUCT,
            ConfluentSettings.KEY_CONCATENATED),
        Importance.MEDIUM,
        "The record key. ${primaryKeyStructOrValue}: the key column's value for a one-column key,"
            + " otherwise a struct of the key columns. ${primaryKeyStruct}: always a struct."
            + " ${primaryKey}: the key values joined with underscores, as an optional string. A"
            + " table without a key has a null key. Confluent's key.template.",
        GROUP,
        ++n,
        Width.MEDIUM,
        "Key template");
    def.define(
        NUMERIC_MAPPING,
        Type.STRING,
        "none",
        ConfigDef.CaseInsensitiveValidString.in(
            "none",
            "best_fit_or_decimal",
            "best_fit_or_double",
            "best_fit_or_string",
            "precision_only"),
        Importance.MEDIUM,
        "How NUMBER columns are represented, as Confluent's numeric.mapping. none: a Connect"
            + " Decimal with the column's scale, numeric.default.scale when unconstrained.",
        GROUP,
        ++n,
        Width.MEDIUM,
        "Numeric mapping");
    def.define(
        NUMERIC_DEFAULT_SCALE,
        Type.INT,
        127,
        ConfigDef.Range.between(-84, 127),
        Importance.LOW,
        "The Decimal scale of a NUMBER column declared without precision and scale. Confluent's"
            + " numeric.default.scale.",
        GROUP,
        ++n,
        Width.SHORT,
        "Numeric default scale");
    def.define(
        DATE_MAPPING,
        Type.STRING,
        "date",
        ConfigDef.CaseInsensitiveValidString.in("date", "timestamp"),
        Importance.LOW,
        "DATE columns as a Connect Date, dropping the time of day (date), or as a Connect"
            + " Timestamp keeping it (timestamp). Confluent's oracle.date.mapping.",
        GROUP,
        ++n,
        Width.SHORT,
        "Date mapping");
  }

  private static int field(
      ConfigDef def, int n, String key, String dflt, String doc, String display) {
    def.define(
        key,
        Type.STRING,
        dflt,
        Importance.LOW,
        "Name of the field that holds "
            + Character.toLowerCase(doc.charAt(0))
            + doc.substring(1)
            + PLACEMENT,
        GROUP,
        ++n,
        Width.MEDIUM,
        display);
    return n;
  }

  private static int op(ConfigDef def, int n, String key, String dflt, String what, String name) {
    def.define(
        key,
        Type.STRING,
        dflt,
        new ConfigDef.NonEmptyString(),
        Importance.LOW,
        "The operation value of " + what + ".",
        GROUP,
        ++n,
        Width.SHORT,
        "Operation value: " + name);
    return n;
  }

  /**
   * The settings these keys give, checked: field names well formed and distinct, operation values
   * distinct. {@code tombstones} is whether a delete is followed by a tombstone.
   *
   * @throws ConfigException naming the first key at fault
   */
  public static ConfluentSettings settings(AbstractConfig c, boolean tombstones) {
    ConfluentSettings s =
        new ConfluentSettings(
            c.getString(TABLE_NAME_FIELD).trim(),
            c.getString(SCN_FIELD).trim(),
            c.getString(COMMIT_SCN_FIELD).trim(),
            c.getString(OP_TYPE_FIELD).trim(),
            c.getString(OP_TS_FIELD).trim(),
            c.getString(CURRENT_TS_FIELD).trim(),
            c.getString(ROW_ID_FIELD).trim(),
            c.getString(USERNAME_FIELD).trim(),
            c.getString(REDO_FIELD).trim(),
            c.getString(UNDO_FIELD).trim(),
            c.getString(BEFORE_STATE_FIELD).trim(),
            c.getString(OP_READ),
            c.getString(OP_INSERT),
            c.getString(OP_UPDATE),
            c.getString(OP_DELETE),
            c.getString(OP_TRUNCATE),
            c.getString(KEY_TEMPLATE),
            ConfluentTypes.NumericMapping.valueOf(
                c.getString(NUMERIC_MAPPING).toUpperCase(Locale.ROOT)),
            c.getInt(NUMERIC_DEFAULT_SCALE),
            ConfluentTypes.DateMapping.valueOf(c.getString(DATE_MAPPING).toUpperCase(Locale.ROOT)),
            tombstones);
    Map<String, String> fields =
        new java.util.LinkedHashMap<>(
            Map.of(
                TABLE_NAME_FIELD, s.tableField(),
                SCN_FIELD, s.scnField(),
                COMMIT_SCN_FIELD, s.commitScnField(),
                OP_TYPE_FIELD, s.opTypeField(),
                OP_TS_FIELD, s.opTsField(),
                CURRENT_TS_FIELD, s.currentTsField(),
                ROW_ID_FIELD, s.rowIdField(),
                USERNAME_FIELD, s.usernameField(),
                REDO_FIELD, s.redoField(),
                UNDO_FIELD, s.undoField()));
    for (Map.Entry<String, String> e : fields.entrySet()) {
      String v = e.getValue();
      String path = v.startsWith(FieldLayout.HEADER) ? v.substring(FieldLayout.HEADER.length()) : v;
      if (!v.isEmpty() && (path.isEmpty() || List.of(path.split("\\.", -1)).contains(""))) {
        throw new ConfigException(e.getKey(), v, "is not a field name, header:name or dotted path");
      }
    }
    String before = s.beforeStateField();
    if (before.startsWith(FieldLayout.HEADER) || before.contains(".")) {
      throw new ConfigException(BEFORE_STATE_FIELD, before, "must be a plain field name");
    }
    try {
      FieldLayout layout = FieldLayout.of(s);
      if (!before.isEmpty() && layout.topLevelNames().contains(before)) {
        throw new ConfigException(BEFORE_STATE_FIELD, before, "names another metadata field");
      }
    } catch (IllegalArgumentException e) {
      throw new ConfigException(PREFIX + "*.field", null, e.getMessage());
    }
    Set<String> ops = new HashSet<>();
    List<String> opKeys =
        new ArrayList<>(List.of(OP_READ, OP_INSERT, OP_UPDATE, OP_DELETE, OP_TRUNCATE));
    for (String k : opKeys) {
      if (!ops.add(c.getString(k))) {
        throw new ConfigException(k, c.getString(k), "is the value of another operation too");
      }
    }
    return s;
  }
}
