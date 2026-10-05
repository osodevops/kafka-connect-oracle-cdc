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
package sh.oso.connect.oracle.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.KeySource;
import sh.oso.connect.oracle.core.schema.OracleType;
import sh.oso.connect.oracle.core.schema.TableSchema;

/**
 * Records of the compacted schema topic (PRD-03 section 2): key {@code (server, pdb, owner,
 * table)}, value the table's versions at or after the resume point. Decoding accepts what any JSON
 * converter returns: Structs, or maps and lists without the schema envelope.
 */
public final class SchemaRecords {

  public static final int SCHEMA_VERSION = 1;

  public static final Schema KEY_SCHEMA =
      SchemaBuilder.struct()
          .name("io.oso.cdc.schema.Key")
          .field("server", Schema.STRING_SCHEMA)
          .field("pdb", Schema.OPTIONAL_STRING_SCHEMA)
          .field("owner", Schema.STRING_SCHEMA)
          .field("table", Schema.STRING_SCHEMA)
          .build();

  static final Schema COLUMN_SCHEMA =
      SchemaBuilder.struct()
          .name("io.oso.cdc.schema.Column")
          .field("name", Schema.STRING_SCHEMA)
          .field("position", Schema.INT32_SCHEMA)
          .field("type", Schema.STRING_SCHEMA)
          .field("type_text", Schema.OPTIONAL_STRING_SCHEMA)
          .field("length", Schema.INT32_SCHEMA)
          .field("precision", Schema.INT32_SCHEMA)
          .field("scale", Schema.INT32_SCHEMA)
          .field("nullable", Schema.BOOLEAN_SCHEMA)
          .build();

  static final Schema VERSION_SCHEMA =
      SchemaBuilder.struct()
          .name("io.oso.cdc.schema.Version")
          .field("version", Schema.INT32_SCHEMA)
          .field("valid_from_scn", Schema.INT64_SCHEMA)
          .field("key_source", Schema.STRING_SCHEMA)
          .field("key", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
          .field("supplemental_all", Schema.BOOLEAN_SCHEMA)
          .field("supplemental_pk", Schema.BOOLEAN_SCHEMA)
          .field("columns", SchemaBuilder.array(COLUMN_SCHEMA).build())
          // P1-17: false when the dictionary was already past this version's DDL; absent is true
          .field("exact", Schema.OPTIONAL_BOOLEAN_SCHEMA)
          .build();

  public static final Schema VALUE_SCHEMA =
      SchemaBuilder.struct()
          .name("io.oso.cdc.schema.Table")
          .version(SCHEMA_VERSION)
          .optional()
          .field("v", Schema.INT32_SCHEMA)
          .field("versions", SchemaBuilder.array(VERSION_SCHEMA).build())
          .build();

  private SchemaRecords() {}

  /** Builds the schema topic's records for one server; offsets follow the quiet-heartbeat rule. */
  public record Writer(String topic, String server, Map<String, ?> partition) {
    public org.apache.kafka.connect.source.SourceRecord versions(
        TableId t, List<TableSchema> versions, Map<String, ?> offset) {
      return new org.apache.kafka.connect.source.SourceRecord(
          partition,
          offset,
          topic,
          null,
          KEY_SCHEMA,
          key(server, t),
          VALUE_SCHEMA,
          value(versions));
    }

    public org.apache.kafka.connect.source.SourceRecord removed(TableId t, Map<String, ?> offset) {
      return new org.apache.kafka.connect.source.SourceRecord(
          partition, offset, topic, null, KEY_SCHEMA, key(server, t), VALUE_SCHEMA, null);
    }
  }

  public static Struct key(String server, TableId t) {
    return new Struct(KEY_SCHEMA)
        .put("server", server)
        .put("pdb", t.pdb())
        .put("owner", t.schema())
        .put("table", t.table());
  }

  public static Struct value(List<TableSchema> versions) {
    List<Struct> vs = new ArrayList<>();
    for (TableSchema s : versions) {
      List<Struct> cols = new ArrayList<>();
      for (ColumnSpec c : s.columns()) {
        cols.add(
            new Struct(COLUMN_SCHEMA)
                .put("name", c.name())
                .put("position", c.position())
                .put("type", c.type().name())
                .put("type_text", c.typeText())
                .put("length", c.length())
                .put("precision", c.precision())
                .put("scale", c.scale())
                .put("nullable", c.nullable()));
      }
      vs.add(
          new Struct(VERSION_SCHEMA)
              .put("version", s.version())
              .put("valid_from_scn", s.validFromScn())
              .put("key_source", s.keySource().name())
              .put("key", s.keyColumns())
              .put("supplemental_all", s.supplementalAllColumns())
              .put("supplemental_pk", s.supplementalPrimaryKey())
              .put("columns", cols)
              .put("exact", s.exact()));
    }
    return new Struct(VALUE_SCHEMA).put("v", SCHEMA_VERSION).put("versions", vs);
  }

  /** The table a key names, or null when the key is another server's. */
  public static TableId table(Object key, String server) {
    if (key == null || !server.equals(String.valueOf(field(key, "server")))) {
      return null;
    }
    Object pdb = field(key, "pdb");
    return new TableId(
        pdb == null ? null : pdb.toString(),
        String.valueOf(field(key, "owner")),
        String.valueOf(field(key, "table")));
  }

  /** The versions in a value, oldest first; empty for a tombstone. */
  public static List<TableSchema> versions(TableId t, Object value) {
    List<TableSchema> out = new ArrayList<>();
    if (value == null) {
      return out;
    }
    for (Object v : list(field(value, "versions"))) {
      List<ColumnSpec> cols = new ArrayList<>();
      for (Object c : list(field(v, "columns"))) {
        Object text = field(c, "type_text");
        cols.add(
            new ColumnSpec(
                String.valueOf(field(c, "name")),
                toInt(field(c, "position")),
                OracleType.valueOf(String.valueOf(field(c, "type"))),
                text == null ? null : text.toString(),
                toInt(field(c, "length")),
                toInt(field(c, "precision")),
                toInt(field(c, "scale")),
                Boolean.TRUE.equals(field(c, "nullable"))));
      }
      List<String> key = new ArrayList<>();
      for (Object k : list(field(v, "key"))) {
        key.add(String.valueOf(k));
      }
      out.add(
          new TableSchema(
              t,
              cols,
              key,
              KeySource.valueOf(String.valueOf(field(v, "key_source"))),
              Boolean.TRUE.equals(field(v, "supplemental_all")),
              Boolean.TRUE.equals(field(v, "supplemental_pk")),
              toInt(field(v, "version")),
              ((Number) field(v, "valid_from_scn")).longValue(),
              !Boolean.FALSE.equals(field(v, "exact"))));
    }
    return out;
  }

  private static Object field(Object container, String name) {
    if (container instanceof Struct s) {
      return s.schema().field(name) == null ? null : s.get(name);
    }
    if (container instanceof Map<?, ?> m) {
      return m.get(name);
    }
    return null;
  }

  private static List<?> list(Object o) {
    return o instanceof List<?> l ? l : List.of();
  }

  private static int toInt(Object o) {
    return o == null ? 0 : ((Number) o).intValue();
  }
}
