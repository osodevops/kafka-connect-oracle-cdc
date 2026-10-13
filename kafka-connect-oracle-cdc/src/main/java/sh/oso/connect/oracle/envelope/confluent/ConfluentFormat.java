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

import java.math.BigDecimal;
import java.time.Clock;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfig;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.errors.NameCollisionException;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.RowIds;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.position.PositionCodec;
import sh.oso.connect.oracle.core.schema.ColumnFilter;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.KeySource;
import sh.oso.connect.oracle.core.schema.TableSchema;
import sh.oso.connect.oracle.core.snapshot.SnapshotRow;
import sh.oso.connect.oracle.envelope.RecordFormat;
import sh.oso.connect.oracle.envelope.TopicRouter;
import sh.oso.connect.oracle.envelope.confluent.FieldLayout.Meta;

/**
 * The Confluent-compatible record format (ADR-0028, PRD-01 SRC-FMT-2): a flat row, the table's
 * columns in table order followed by Confluent's metadata fields, in an unnamed struct (which
 * Confluent's Avro converter registers as {@code io.confluent.connect.avro.ConnectDefault}, as it
 * does for Confluent's own records). Inserts are {@code I} with the new row, updates {@code U} with
 * the row after the change (and the row before it in the before-state field when one is named),
 * deletes {@code D} with the deleted row, snapshot rows {@code R}. A key change is a delete of the
 * old key, a tombstone and an insert of the new one whatever the tombstone setting. Facts and
 * decisions: {@code reference/confluent-record-format.md}.
 */
public final class ConfluentFormat implements RecordFormat {

  private final TopicRouter router;
  private final Map<String, Object> partition;
  private final String databaseName;
  private final ConfluentSettings settings;
  private final ConfluentTypes types;
  private final FieldLayout layout;
  private final ColumnFilter excluded;
  private final Clock clock;
  private final boolean emitTruncate;
  private final boolean lobTopics;
  private final TopicRouter lobRouter;
  private final long lobMaxBytes;
  private final Map<TableSchema, Schemas> cache = new HashMap<>();

  /**
   * The schemas of one table version: the key (null without a key), the value, the value of an
   * update when a before-state field is named (null otherwise), and the columns that fill them.
   */
  record Schemas(
      Schema key,
      Schema value,
      Schema valueWithBefore,
      List<ColumnSpec> keyColumns,
      List<ColumnSpec> columns,
      List<ColumnSpec> lobs,
      Schema lobPrimaryKey) {}

  public ConfluentFormat(
      OracleCdcSourceConnectorConfig config,
      TopicRouter router,
      String databaseName,
      ConfluentSettings settings,
      Clock clock) {
    this.router = router;
    this.partition = Map.of("server", config.topicPrefix());
    this.databaseName = databaseName;
    this.settings = settings;
    this.types =
        new ConfluentTypes(
            settings.numericMapping(), settings.numericDefaultScale(), settings.dateMapping());
    this.layout = FieldLayout.of(settings);
    this.excluded = config.columnFilter();
    this.clock = clock;
    this.emitTruncate = config.truncateEmit();
    this.lobTopics =
        config.core().lobMode() == sh.oso.connect.oracle.core.config.CoreConfig.LobMode.TOPIC;
    this.lobRouter = router.forLobs(config.lobTopicTemplate(config.core().pdbs().size() > 0));
    this.lobMaxBytes =
        config.core().getLong(sh.oso.connect.oracle.core.config.CoreConfig.LOB_MAX_BYTES);
  }

  @Override
  public Map<String, Object> partition() {
    return partition;
  }

  @Override
  public List<SourceRecord> records(
      CommittedTransaction tx,
      int index,
      long dataCollectionOrder,
      TableSchema schema,
      Position offset) {
    RowChange change = tx.events().get(index);
    if (change.op() == sh.oso.connect.oracle.core.model.Operation.TRUNCATE && !emitTruncate) {
      return List.of();
    }
    Schemas s = schemas(excluded.project(schema));
    boolean lobOnly = lobOnly(change, s);
    String topic = router.topic(change.table());
    Map<String, Object> offsetMap = PositionCodec.write(offset);
    Map<Meta, String> meta = new EnumMap<>(Meta.class);
    meta.put(Meta.TABLE, qualified(change.table()));
    meta.put(Meta.SCN, Long.toString(change.id().scn()));
    meta.put(Meta.COMMIT_SCN, Long.toString(tx.commitScn()));
    if (change.timestamp() != null) {
      meta.put(Meta.OP_TS, Long.toString(change.timestamp().toEpochMilli()));
    }
    meta.put(Meta.ROW_ID, RowIds.real(change.rowId()));
    meta.put(Meta.USERNAME, tx.username());
    if (change.sql() != null) {
      meta.put(Meta.REDO, change.sql().redo());
      meta.put(Meta.UNDO, change.sql().undo());
    }
    Object afterKey = key(s, change.after());
    Object beforeKey = key(s, change.before());
    List<SourceRecord> out = new ArrayList<>(3);
    if (!lobOnly)
      switch (change.op()) {
        case TRUNCATE -> {
          meta.put(Meta.ROW_ID, null);
          SchemaBuilder builder = SchemaBuilder.struct();
          layout.addTo(builder);
          Schema truncateSchema = builder.build();
          Struct value = new Struct(truncateSchema);
          Headers headers = new ConnectHeaders();
          Map<Meta, String> values = withOp(meta, settings.opTruncate());
          layout.fillBody(value, values);
          layout.fillHeaders(headers, values);
          addHeaders(headers, tx, change, index);
          out.add(
              new SourceRecord(
                  partition,
                  offsetMap,
                  topic,
                  null,
                  null,
                  null,
                  truncateSchema,
                  value,
                  null,
                  headers));
        }
        case INSERT ->
            out.add(
                change(
                    topic,
                    s,
                    offsetMap,
                    tx,
                    change,
                    index,
                    afterKey,
                    settings.opInsert(),
                    change.after(),
                    null,
                    meta));
        case DELETE -> {
          if (settings.tombstonesOnDelete()) {
            out.add(tombstone(topic, s, offsetMap, tx, change, index, beforeKey, meta));
          } else {
            out.add(
                change(
                    topic,
                    s,
                    offsetMap,
                    tx,
                    change,
                    index,
                    beforeKey,
                    settings.opDelete(),
                    change.before(),
                    null,
                    meta));
          }
        }
        default -> {
          if (beforeKey != null && afterKey != null && !Objects.equals(beforeKey, afterKey)) {
            // a key change: delete, tombstone and insert, whatever the tombstone setting
            if (settings.tombstonesOnDelete()) {
              out.add(tombstone(topic, s, offsetMap, tx, change, index, beforeKey, meta));
            } else {
              out.add(
                  change(
                      topic,
                      s,
                      offsetMap,
                      tx,
                      change,
                      index,
                      beforeKey,
                      settings.opDelete(),
                      change.before(),
                      null,
                      meta));
            }
            out.add(tombstone(topic, s, offsetMap, tx, change, index, beforeKey, meta));
            out.add(
                change(
                    topic,
                    s,
                    offsetMap,
                    tx,
                    change,
                    index,
                    afterKey,
                    settings.opInsert(),
                    change.after(),
                    null,
                    meta));
          } else {
            Object key = afterKey != null ? afterKey : beforeKey;
            out.add(
                change(
                    topic,
                    s,
                    offsetMap,
                    tx,
                    change,
                    index,
                    key,
                    settings.opUpdate(),
                    change.after(),
                    change.before(),
                    meta));
          }
        }
      }
    if (lobTopics && change.op() != sh.oso.connect.oracle.core.model.Operation.TRUNCATE) {
      Headers lobHeaders = new ConnectHeaders();
      addHeaders(lobHeaders, tx, change, index);
      boolean keyChange =
          beforeKey != null && afterKey != null && !Objects.equals(beforeKey, afterKey);
      if (change.op() == sh.oso.connect.oracle.core.model.Operation.DELETE || keyChange) {
        out.addAll(
            lobRecords(change.table(), s, change.before(), null, offsetMap, lobHeaders, true));
      }
      if (change.op() != sh.oso.connect.oracle.core.model.Operation.DELETE) {
        java.util.Set<String> touched =
            change.op() == sh.oso.connect.oracle.core.model.Operation.INSERT || keyChange
                ? null
                : change.touchedLobs();
        out.addAll(
            lobRecords(change.table(), s, change.after(), touched, offsetMap, lobHeaders, false));
      }
    }
    return out;
  }

  private List<SourceRecord> lobRecords(
      sh.oso.connect.oracle.core.model.TableId table,
      Schemas s,
      Map<String, Object> image,
      java.util.Set<String> selected,
      Map<String, Object> offset,
      Headers headers,
      boolean tombstone) {
    List<SourceRecord> out = new ArrayList<>();
    if (s.lobs().isEmpty()) return out;
    if (image == null || s.keyColumns().stream().anyMatch(c -> image.get(c.name()) == null)) {
      throw new sh.oso.connect.oracle.core.errors.DecodeException(
          "LOB topic row of " + table.fqn() + " lacks its stable key.",
          "Enable supplemental logging for every key column; the connector cannot discard unkeyed"
              + " LOB state.");
    }
    Object primary;
    if (s.lobPrimaryKey().type() == Schema.Type.STRUCT) {
      Struct key = new Struct(s.lobPrimaryKey());
      for (ColumnSpec c : s.keyColumns())
        key.put(
            c.name(),
            types.value(c, s.lobPrimaryKey().field(c.name()).schema(), image.get(c.name())));
      primary = key;
    } else {
      ColumnSpec c = s.keyColumns().get(0);
      primary = types.value(c, s.lobPrimaryKey(), image.get(c.name()));
    }
    Schema keySchema =
        SchemaBuilder.struct()
            .field("table", Schema.STRING_SCHEMA)
            .field("column", Schema.STRING_SCHEMA)
            .field("primary_key", s.lobPrimaryKey())
            .build();
    for (ColumnSpec column : s.lobs()) {
      if (selected != null && !selected.contains(column.name())) continue;
      byte[] bytes = null;
      if (!tombstone) {
        if (!image.containsKey(column.name()))
          throw new sh.oso.connect.oracle.core.errors.DecodeException(
              "Complete LOB value unavailable for " + table.fqn() + "." + column.name() + ".",
              "Restore sufficient undo and flashback access for AS OF commit reselect; topic mode"
                  + " never publishes a placeholder.");
        Object value = image.get(column.name());
        if (value != null) bytes = encodeLob(column, value);
        if (bytes != null && bytes.length > lobMaxBytes)
          throw new sh.oso.connect.oracle.core.errors.LobTooLargeException(
              "LOB topic value exceeds cdc.lob.max.bytes for "
                  + table.fqn()
                  + "."
                  + column.name()
                  + ".",
              "Raise cdc.lob.max.bytes before restarting; topic mode requires complete values.");
      }
      Struct key =
          new Struct(keySchema)
              .put("table", qualified(table))
              .put("column", column.name())
              .put("primary_key", primary);
      out.add(
          new SourceRecord(
              partition,
              offset,
              lobRouter.topic(table, column.name()),
              null,
              keySchema,
              key,
              bytes == null ? null : Schema.BYTES_SCHEMA,
              bytes,
              null,
              headers.duplicate()));
    }
    return out;
  }

  private static byte[] encodeLob(ColumnSpec column, Object value) {
    if (value instanceof byte[] bytes) return bytes;
    java.nio.charset.Charset charset =
        column.type() == sh.oso.connect.oracle.core.schema.OracleType.NCLOB
            ? java.nio.charset.StandardCharsets.UTF_16BE
            : java.nio.charset.StandardCharsets.UTF_8;
    try {
      java.nio.ByteBuffer encoded =
          charset
              .newEncoder()
              .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
              .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT)
              .encode(java.nio.CharBuffer.wrap(value.toString()));
      byte[] bytes = new byte[encoded.remaining()];
      encoded.get(bytes);
      return bytes;
    } catch (java.nio.charset.CharacterCodingException e) {
      throw new sh.oso.connect.oracle.core.errors.DecodeException(
          "LOB text of column " + column.name() + " is not valid " + charset.name() + ".",
          "Repair the source text; the connector refuses replacement characters.");
    }
  }

  private static boolean lobOnly(RowChange change, Schemas s) {
    if (RowIds.isLobGroup(change.rowId())) {
      return true;
    }
    if (change.op() != sh.oso.connect.oracle.core.model.Operation.UPDATE
        || (!RowIds.isSynthetic(change.rowId()) && change.touchedLobs().isEmpty())
        || change.before() == null
        || change.after() == null) {
      return false;
    }
    for (ColumnSpec c : s.columns()) {
      Object before = change.before().get(c.name());
      Object after = change.after().get(c.name());
      boolean same =
          before instanceof BigDecimal a && after instanceof BigDecimal b
              ? a.compareTo(b) == 0
              : Objects.deepEquals(before, after);
      if (!same) {
        return false;
      }
    }
    return true;
  }

  @Override
  public List<SourceRecord> snapshotRecords(
      TableId table,
      SnapshotRow row,
      TableSchema schema,
      long scn,
      long readAtMs,
      String marker,
      Position offset) {
    Schemas s = schemas(excluded.project(schema));
    Map<Meta, String> meta = new EnumMap<>(Meta.class);
    meta.put(Meta.TABLE, qualified(table));
    meta.put(Meta.SCN, Long.toString(scn));
    meta.put(Meta.COMMIT_SCN, Long.toString(scn));
    ConnectHeaders headers = new ConnectHeaders();
    Struct value = row(s.value(), s, row.values(), settings.opRead(), meta, headers);
    headers.addLong("cdc.scn", scn);
    headers.addBoolean("cdc.snapshot", true);
    headers.addInt("cdc.schema_version", schema.version());
    Object key = key(s, row.values());
    List<SourceRecord> out = new ArrayList<>();
    out.add(
        new SourceRecord(
            partition,
            PositionCodec.write(offset),
            router.topic(table),
            null,
            s.key(),
            key,
            s.value(),
            value,
            null,
            headers));
    if (lobTopics)
      out.addAll(
          lobRecords(table, s, row.values(), null, PositionCodec.write(offset), headers, false));
    return out;
  }

  Schemas schemas(TableSchema schema) {
    return cache.computeIfAbsent(schema, this::build);
  }

  private Schemas build(TableSchema schema) {
    if (lobTopics) sh.oso.connect.oracle.core.schema.LobTopicRequirements.requireKey(schema);
    List<ColumnSpec> lobs = schema.columns().stream().filter(ConfluentTypes::lob).toList();
    List<ColumnSpec> keyColumns = new ArrayList<>();
    if (schema.keySource() != KeySource.ROWID && schema.keySource() != KeySource.NONE) {
      for (String k : schema.keyColumns()) {
        keyColumns.add(schema.column(k));
      }
    }
    List<ColumnSpec> columns = new ArrayList<>();
    SchemaBuilder value = SchemaBuilder.struct();
    SchemaBuilder before = SchemaBuilder.struct().optional();
    for (ColumnSpec c : schema.columns()) {
      if (!types.inRecord(c)) {
        continue;
      }
      Schema cs = types.schema(c, keyColumns.contains(c));
      value.field(c.name(), cs);
      before.field(c.name(), cs);
      columns.add(c);
    }
    List<String> metadata = new ArrayList<>(layout.topLevelNames());
    if (!settings.beforeStateField().isEmpty()) {
      metadata.add(settings.beforeStateField());
    }
    for (String name : metadata) {
      if (value.field(name) != null) {
        throw new NameCollisionException(
            "Column "
                + name
                + " of "
                + schema.table().fqn()
                + " has the name of a metadata field of the Confluent-compatible format.",
            "Rename the metadata field with its cdc.output.confluent.* key, or exclude the column"
                + " with cdc.columns.exclude.");
      }
    }
    layout.addTo(value);
    Schema valueWithBefore = null;
    if (!settings.beforeStateField().isEmpty()) {
      SchemaBuilder withBefore = SchemaBuilder.struct();
      for (Field f : value.fields()) {
        withBefore.field(f.name(), f.schema());
      }
      valueWithBefore = withBefore.field(settings.beforeStateField(), before.build()).build();
    }
    Schema lobPrimaryKey =
        keyColumns.isEmpty()
            ? null
            : keyColumns.size() == 1
                ? types.schema(keyColumns.get(0), true)
                : keyStruct(keyColumns);
    return new Schemas(
        keySchema(keyColumns),
        value.build(),
        valueWithBefore,
        keyColumns,
        columns,
        lobs,
        lobPrimaryKey);
  }

  private Schema keySchema(List<ColumnSpec> keyColumns) {
    if (keyColumns.isEmpty()) {
      return settings.keyTemplate().equals(ConfluentSettings.KEY_CONCATENATED)
          ? Schema.OPTIONAL_STRING_SCHEMA
          : null;
    }
    return switch (settings.keyTemplate()) {
      case ConfluentSettings.KEY_CONCATENATED -> Schema.OPTIONAL_STRING_SCHEMA; // as observed
      case ConfluentSettings.KEY_STRUCT -> keyStruct(keyColumns);
      default ->
          keyColumns.size() == 1 ? types.schema(keyColumns.get(0), true) : keyStruct(keyColumns);
    };
  }

  private Schema keyStruct(List<ColumnSpec> keyColumns) {
    SchemaBuilder b = SchemaBuilder.struct();
    for (ColumnSpec c : keyColumns) {
      b.field(c.name(), types.schema(c, true));
    }
    return b.build();
  }

  /** The record key from a row image; null without a key, or when the image lacks a key column. */
  private Object key(Schemas s, Map<String, Object> image) {
    if (s.keyColumns().isEmpty() || image == null) {
      return null;
    }
    for (ColumnSpec c : s.keyColumns()) {
      if (image.get(c.name()) == null) {
        throw new sh.oso.connect.oracle.core.errors.DecodeException(
            "Confluent record image lacks declared key column " + c.name() + ".",
            "Enable supplemental logging for every key column; an unkeyed delete or key move"
                + " cannot maintain the existing topic state.");
      }
    }
    if (s.key().type() == Schema.Type.STRUCT) {
      Struct k = new Struct(s.key());
      for (int i = 0; i < s.keyColumns().size(); i++) {
        ColumnSpec c = s.keyColumns().get(i);
        Field f = s.key().fields().get(i);
        k.put(f, types.value(c, f.schema(), image.get(c.name())));
      }
      return k;
    }
    if (settings.keyTemplate().equals(ConfluentSettings.KEY_CONCATENATED)) {
      StringJoiner j = new StringJoiner("_");
      for (ColumnSpec c : s.keyColumns()) {
        Object v = image.get(c.name());
        j.add(v instanceof BigDecimal d ? d.toPlainString() : String.valueOf(v));
      }
      return j.toString();
    }
    ColumnSpec c = s.keyColumns().get(0);
    return types.value(c, s.key(), image.get(c.name()));
  }

  private SourceRecord change(
      String topic,
      Schemas s,
      Map<String, Object> offset,
      CommittedTransaction tx,
      RowChange change,
      int index,
      Object key,
      String op,
      Map<String, Object> image,
      Map<String, Object> before,
      Map<Meta, String> meta) {
    boolean withBefore = before != null && s.valueWithBefore() != null;
    if (change.partial()) {
      requireCompleteImage(s, image);
      if (withBefore) requireCompleteImage(s, before);
    }
    Schema schema = withBefore ? s.valueWithBefore() : s.value();
    Headers headers = new ConnectHeaders();
    Struct value = row(schema, s, image, op, meta, headers);
    if (withBefore) {
      Struct b = new Struct(schema.field(settings.beforeStateField()).schema());
      fillColumns(b, s, before);
      value.put(settings.beforeStateField(), b);
    }
    addHeaders(headers, tx, change, index);
    return new SourceRecord(
        partition, offset, topic, null, s.key(), key, schema, value, null, headers);
  }

  private Struct row(
      Schema schema,
      Schemas s,
      Map<String, Object> image,
      String op,
      Map<Meta, String> meta,
      Headers headers) {
    Struct v = new Struct(schema);
    fillColumns(v, s, image);
    Map<Meta, String> values = withOp(meta, op);
    layout.fillBody(v, values);
    layout.fillHeaders(headers, values);
    return v;
  }

  private Map<Meta, String> withOp(Map<Meta, String> meta, String op) {
    Map<Meta, String> values = new EnumMap<>(meta);
    values.put(Meta.OP_TYPE, op);
    values.put(Meta.CURRENT_TS, Long.toString(clock.millis()));
    return values;
  }

  private static void requireCompleteImage(Schemas s, Map<String, Object> image) {
    if (image == null || s.columns().stream().anyMatch(c -> !image.containsKey(c.name())))
      throw new sh.oso.connect.oracle.core.errors.DecodeException(
          "Confluent flat row image is incomplete.",
          "Enable ALL-column supplemental logging; unknown values cannot be published as null.");
  }

  private void fillColumns(Struct v, Schemas s, Map<String, Object> image) {
    if (image == null) {
      throw new sh.oso.connect.oracle.core.errors.DecodeException(
          "Confluent flat row has no row image.",
          "Restore supplemental logging; the connector refuses a record without its row.");
    }
    for (ColumnSpec c : s.columns()) {
      Field f = v.schema().field(c.name());
      Object raw = image.get(c.name());
      if (raw == null && !f.schema().isOptional())
        throw new sh.oso.connect.oracle.core.errors.DecodeException(
            "Confluent row lacks required column " + c.name() + ".",
            "Enable ALL-column supplemental logging and verify the column's NOT NULL constraint.");
      v.put(f, types.value(c, f.schema(), raw));
    }
  }

  /**
   * Confluent's fully qualified table name: the PDB, or the database on a non-CDB, then owner and
   * table.
   */
  private String qualified(TableId t) {
    return (t.pdb() != null ? t.pdb() : databaseName) + "." + t.schema() + "." + t.table();
  }

  private SourceRecord tombstone(
      String topic,
      Schemas s,
      Map<String, Object> offset,
      CommittedTransaction tx,
      RowChange change,
      int index,
      Object key,
      Map<Meta, String> meta) {
    // a tombstone carries the header-placed metadata of the delete before it, as Confluent's do
    Headers headers = new ConnectHeaders();
    layout.fillHeaders(headers, withOp(meta, settings.opDelete()));
    addHeaders(headers, tx, change, index);
    return new SourceRecord(
        partition, offset, topic, null, s.key(), key, null, null, null, headers);
  }

  /**
   * The connector's own headers, as in the Debezium-compatible format, after any metadata placed in
   * headers. Confluent's connector writes none of them; a consumer reading keys and values sees no
   * difference.
   */
  private static void addHeaders(Headers h, CommittedTransaction tx, RowChange c, int index) {
    h.addLong("cdc.scn", c.id().scn());
    h.addLong("cdc.commit_scn", tx.commitScn());
    h.addString("cdc.xid", tx.key().xid().toString());
    h.addInt("cdc.event_index", index);
    h.addInt("cdc.event_count", tx.size());
    h.addInt("cdc.schema_version", c.schemaVersion());
  }
}
