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
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;

/**
 * Where each metadata field of the Confluent-compatible format goes, from the {@code output.*}
 * style names: a top-level field after the table's columns, a field of a nested struct when the
 * name is a dotted path ({@code meta.op_ts}), a record header when it starts with {@code header:},
 * or nowhere when it is empty. Fields keep Confluent's order; a nested struct takes the place of
 * its first member, so {@code x_meta.op_ts} and {@code x_meta.current_ts} make one struct where
 * {@code op_ts} would be. Every metadata value is an optional string, in the body and in headers
 * alike, and a header with no value is left out (observed with Confluent's 2.15.2 build, 13 October
 * 2026).
 */
final class FieldLayout {

  static final String HEADER = "header:";

  /** Confluent's metadata fields, in the order they follow the columns. */
  enum Meta {
    TABLE,
    SCN,
    COMMIT_SCN,
    OP_TYPE,
    OP_TS,
    CURRENT_TS,
    ROW_ID,
    USERNAME,
    REDO,
    UNDO
  }

  /** A struct level of the body: its fields by name, each a metadata value or a nested level. */
  private static final class Level {
    final Map<String, Object> fields = new LinkedHashMap<>(); // Meta or Level
  }

  private final Level body = new Level();
  private final Map<Meta, String> headers = new EnumMap<>(Meta.class);
  private final Map<Meta, List<String>> paths = new EnumMap<>(Meta.class);

  FieldLayout(Map<Meta, String> names) {
    for (Meta m : Meta.values()) {
      String name = names.getOrDefault(m, "");
      if (name.isEmpty()) {
        continue;
      }
      if (name.startsWith(HEADER)) {
        String header = name.substring(HEADER.length());
        if (headers.containsValue(header)) {
          throw new IllegalArgumentException("'" + name + "' names a header twice");
        }
        headers.put(m, header);
        continue;
      }
      List<String> path = List.of(name.split("\\.", -1));
      Level level = body;
      for (int i = 0; i < path.size() - 1; i++) {
        Object next = level.fields.computeIfAbsent(path.get(i), k -> new Level());
        if (!(next instanceof Level l)) {
          throw new IllegalArgumentException(
              "'" + name + "' nests under '" + path.get(i) + "', which is already a field");
        }
        level = l;
      }
      if (level.fields.putIfAbsent(path.get(path.size() - 1), m) != null) {
        throw new IllegalArgumentException("'" + name + "' names a field twice");
      }
      paths.put(m, path);
    }
  }

  static FieldLayout of(ConfluentSettings s) {
    Map<Meta, String> names = new EnumMap<>(Meta.class);
    names.put(Meta.TABLE, s.tableField());
    names.put(Meta.SCN, s.scnField());
    names.put(Meta.COMMIT_SCN, s.commitScnField());
    names.put(Meta.OP_TYPE, s.opTypeField());
    names.put(Meta.OP_TS, s.opTsField());
    names.put(Meta.CURRENT_TS, s.currentTsField());
    names.put(Meta.ROW_ID, s.rowIdField());
    names.put(Meta.USERNAME, s.usernameField());
    names.put(Meta.REDO, s.redoField());
    names.put(Meta.UNDO, s.undoField());
    return new FieldLayout(names);
  }

  /** Whether the value is wanted anywhere, so that a costly one is computed only then. */
  boolean wants(Meta m) {
    return paths.containsKey(m) || headers.containsKey(m);
  }

  /** The top-level names the metadata takes in the body, which no column may also use. */
  List<String> topLevelNames() {
    return new ArrayList<>(body.fields.keySet());
  }

  /** Adds the body's metadata fields, after the columns. */
  void addTo(SchemaBuilder value) {
    for (Map.Entry<String, Object> e : body.fields.entrySet()) {
      value.field(e.getKey(), schema(e.getValue()));
    }
  }

  private static Schema schema(Object node) {
    if (node instanceof Meta) {
      return Schema.OPTIONAL_STRING_SCHEMA;
    }
    SchemaBuilder b = SchemaBuilder.struct().optional();
    for (Map.Entry<String, Object> e : ((Level) node).fields.entrySet()) {
      b.field(e.getKey(), schema(e.getValue()));
    }
    return b.build();
  }

  /** Puts the metadata values into the value; absent keys are null. */
  void fillBody(Struct value, Map<Meta, String> values) {
    for (Map.Entry<Meta, List<String>> e : paths.entrySet()) {
      List<String> path = e.getValue();
      Struct s = value;
      for (int i = 0; i < path.size() - 1; i++) {
        Struct next = s.getStruct(path.get(i));
        if (next == null) {
          next = new Struct(s.schema().field(path.get(i)).schema());
          s.put(path.get(i), next);
        }
        s = next;
      }
      s.put(path.get(path.size() - 1), values.get(e.getKey()));
    }
  }

  /** Adds the header-placed metadata values that are not null. */
  void fillHeaders(Headers out, Map<Meta, String> values) {
    for (Map.Entry<Meta, String> e : headers.entrySet()) {
      String v = values.get(e.getKey());
      if (v != null) {
        out.addString(e.getValue(), v);
      }
    }
  }
}
