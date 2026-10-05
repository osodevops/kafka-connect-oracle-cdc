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
package sh.oso.connect.oracle.envelope;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfig;
import sh.oso.connect.oracle.Version;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.position.PositionCodec;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.KeySource;
import sh.oso.connect.oracle.core.schema.TableSchema;

/**
 * Builds Debezium-compatible records (SRC-FMT-1, SRC-FMT-4, SRC-TOP-4, SRC-TOP-5): envelope with
 * before, after, source, op, ts_ms, ts_us, ts_ns and transaction; key struct from the chosen key;
 * cdc.* headers; a tombstone after each delete; a primary key change as delete, tombstone and
 * create. Every record carries the offset a restart may use once that record is acknowledged.
 * Records and their Connect schemas are built from the table's layout without the columns {@code
 * cdc.columns.exclude} names (SRC-SEL-2), so an excluded column has no field even when a change
 * restored from an older journal still carries it.
 */
public final class DebeziumEnvelope {

  public static final String CONNECTOR_NAME = "oracle-cdc";

  private final OracleCdcSourceConnectorConfig config;
  private final TypeToConnect types;
  private final TopicRouter router;
  private final Map<String, Object> partition;
  private final Map<TableSchema, Schemas> cache = new HashMap<>();
  private final Schema sourceSchema;
  private final Schema transactionSchema;
  private final String databaseName;
  private final sh.oso.connect.oracle.core.config.CoreConfig.LobMode lobMode;
  private final String placeholder;
  private final sh.oso.connect.oracle.core.schema.ColumnFilter excluded;

  record Schemas(Schema key, Schema value, Schema envelope, List<ColumnSpec> keyColumns) {}

  public DebeziumEnvelope(
      OracleCdcSourceConnectorConfig config, TopicRouter router, String databaseName) {
    this.config = config;
    this.types = new TypeToConnect(config.decimalMode(), config.temporalMode());
    this.router = router;
    this.databaseName = databaseName;
    this.partition = Map.of("server", config.topicPrefix());
    this.lobMode = config.core().lobMode();
    this.excluded = config.columnFilter();
    this.placeholder =
        config
            .core()
            .getString(sh.oso.connect.oracle.core.config.CoreConfig.UNAVAILABLE_PLACEHOLDER);
    this.sourceSchema =
        SchemaBuilder.struct()
            .name("io.debezium.connector.oracle.Source")
            .field("version", Schema.STRING_SCHEMA)
            .field("connector", Schema.STRING_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.INT64_SCHEMA)
            .field("snapshot", Schema.OPTIONAL_STRING_SCHEMA)
            .field("db", Schema.STRING_SCHEMA)
            .field("sequence", Schema.OPTIONAL_STRING_SCHEMA)
            .field("schema", Schema.STRING_SCHEMA)
            .field("table", Schema.STRING_SCHEMA)
            .field("txId", Schema.OPTIONAL_STRING_SCHEMA)
            .field("scn", Schema.OPTIONAL_STRING_SCHEMA)
            .field("commit_scn", Schema.OPTIONAL_STRING_SCHEMA)
            .field("rs_id", Schema.OPTIONAL_STRING_SCHEMA)
            .field("ssn", Schema.OPTIONAL_INT64_SCHEMA)
            .field("redo_thread", Schema.OPTIONAL_INT32_SCHEMA)
            .field("user_name", Schema.OPTIONAL_STRING_SCHEMA)
            .field("row_id", Schema.OPTIONAL_STRING_SCHEMA)
            .field("pdb", Schema.OPTIONAL_STRING_SCHEMA)
            .field("reselect", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    this.transactionSchema =
        SchemaBuilder.struct()
            .name("event.block")
            .version(1)
            .optional()
            .field("id", Schema.STRING_SCHEMA)
            .field("total_order", Schema.INT64_SCHEMA)
            .field("data_collection_order", Schema.INT64_SCHEMA)
            .build();
  }

  public Map<String, Object> partition() {
    return partition;
  }

  /**
   * Records for event {@code index} of {@code tx}. {@code dataCollectionOrder} is the 1-based count
   * of events of the same table so far in the transaction; {@code offset} is the position after
   * this event is acknowledged.
   */
  public List<SourceRecord> records(
      CommittedTransaction tx,
      int index,
      long dataCollectionOrder,
      TableSchema schema,
      Position offset) {
    RowChange change = tx.events().get(index);
    schema = excluded.project(schema);
    Schemas s = schemas(schema);
    String topic = router.topic(change.table());
    Map<String, Object> offsetMap = PositionCodec.write(offset);
    Headers headers = headers(tx, change, index);
    long tsMs = tx.commitTimestamp() == null ? 0 : tx.commitTimestamp().toEpochMilli();
    Struct source = source(tx, change);
    if (reselectFailed(schema, change)) {
      source.put("reselect", "failed"); // CORE-DEC-7: the value could not be fetched
    }
    Struct txBlock =
        new Struct(transactionSchema)
            .put("id", tx.key().xid().toString())
            .put("total_order", (long) index + 1)
            .put("data_collection_order", dataCollectionOrder);
    List<SourceRecord> out = new ArrayList<>(3);
    Object afterKey = keyOf(s, schema, change, change.after());
    Object beforeKey = keyOf(s, schema, change, change.before());
    switch (change.op()) {
      case INSERT:
        out.add(
            record(
                topic,
                s,
                offsetMap,
                headers,
                afterKey,
                envelope(s, "c", null, row(s, change.after()), source, tsMs, txBlock)));
        break;
      case DELETE:
        out.add(
            record(
                topic,
                s,
                offsetMap,
                headers,
                beforeKey,
                envelope(s, "d", row(s, change.before()), null, source, tsMs, txBlock)));
        if (config.tombstonesOnDelete() && beforeKey != null) {
          out.add(tombstone(topic, s, offsetMap, headers, beforeKey));
        }
        break;
      default:
        {
          Struct before = change.before() == null ? null : row(s, change.before());
          Struct after = row(s, change.after());
          Object oldKey = beforeKey;
          Object newKey = afterKey;
          if (oldKey != null && newKey != null && !Objects.equals(oldKey, newKey)) {
            // SRC-TOP-4: a key change is a delete of the old key and a create of the new one
            out.add(
                record(
                    topic,
                    s,
                    offsetMap,
                    headers,
                    oldKey,
                    envelope(s, "d", before, null, source, tsMs, txBlock)));
            if (config.tombstonesOnDelete()) {
              out.add(tombstone(topic, s, offsetMap, headers, oldKey));
            }
            out.add(
                record(
                    topic,
                    s,
                    offsetMap,
                    headers,
                    newKey,
                    envelope(s, "c", null, after, source, tsMs, txBlock)));
          } else {
            out.add(
                record(
                    topic,
                    s,
                    offsetMap,
                    headers,
                    newKey != null ? newKey : oldKey,
                    envelope(s, "u", before, after, source, tsMs, txBlock)));
          }
        }
    }
    return out;
  }

  private SourceRecord record(
      String topic,
      Schemas s,
      Map<String, Object> offset,
      Headers headers,
      Object key,
      Struct value) {
    return new SourceRecord(
        partition,
        offset,
        topic,
        null,
        key == null ? null : s.key(),
        key,
        s.envelope(),
        value,
        null,
        headers);
  }

  /**
   * PRD-02 SNAP-8: a row read by a snapshot chunk as of {@code scn}, as {@code op=r} with {@code
   * source.snapshot} set to {@code marker} ({@code first}, {@code true} or {@code last}).
   */
  public SourceRecord snapshotRecord(
      TableId table,
      sh.oso.connect.oracle.core.snapshot.SnapshotRow row,
      TableSchema schema,
      long scn,
      long readAtMs,
      String marker,
      Position offset) {
    schema = excluded.project(schema);
    Schemas s = schemas(schema);
    RowChange change =
        new RowChange(
            table,
            sh.oso.connect.oracle.core.model.Operation.INSERT,
            null,
            row.values(),
            false,
            row.rowId(),
            null,
            null,
            null,
            schema.version());
    Struct source =
        new Struct(sourceSchema)
            .put("version", Version.VERSION)
            .put("connector", CONNECTOR_NAME)
            .put("name", config.topicPrefix())
            .put("ts_ms", readAtMs)
            .put("snapshot", marker)
            .put("db", table.pdb() != null ? table.pdb() : databaseName)
            .put("schema", table.schema())
            .put("table", table.table())
            .put("scn", Long.toString(scn))
            .put("row_id", row.rowId())
            .put("pdb", table.pdb());
    ConnectHeaders h = new ConnectHeaders();
    h.addLong("cdc.scn", scn);
    h.addBoolean("cdc.snapshot", true);
    h.addInt("cdc.schema_version", schema.version());
    return record(
        router.topic(table),
        s,
        PositionCodec.write(offset),
        h,
        keyOf(s, schema, change, row.values()),
        envelope(s, "r", null, row(s, row.values()), source, readAtMs, null));
  }

  private SourceRecord tombstone(
      String topic, Schemas s, Map<String, Object> offset, Headers headers, Object key) {
    return new SourceRecord(
        partition, offset, topic, null, s.key(), key, null, null, null, headers);
  }

  private Struct envelope(
      Schemas s, String op, Struct before, Struct after, Struct source, long tsMs, Struct txBlock) {
    Struct v = new Struct(s.envelope());
    v.put("before", before);
    v.put("after", after);
    v.put("source", source);
    v.put("op", op);
    v.put("ts_ms", tsMs);
    v.put("ts_us", tsMs * 1_000L);
    v.put("ts_ns", tsMs * 1_000_000L);
    v.put("transaction", txBlock);
    return v;
  }

  private Struct source(CommittedTransaction tx, RowChange c) {
    TableId t = c.table();
    return new Struct(sourceSchema)
        .put("version", Version.VERSION)
        .put("connector", CONNECTOR_NAME)
        .put("name", config.topicPrefix())
        .put("ts_ms", c.timestamp() == null ? 0L : c.timestamp().toEpochMilli())
        .put("snapshot", "false")
        .put("db", t.pdb() != null ? t.pdb() : databaseName)
        .put("schema", t.schema())
        .put("table", t.table())
        .put("txId", tx.key().xid().toString())
        .put("scn", Long.toString(c.id().scn()))
        .put("commit_scn", Long.toString(tx.commitScn()))
        .put("rs_id", c.id().rsId().trim())
        .put("ssn", c.id().ssn())
        .put("redo_thread", tx.thread())
        .put("user_name", tx.username())
        .put("row_id", sh.oso.connect.oracle.core.model.RowIds.real(c.rowId()))
        .put("pdb", t.pdb());
  }

  /** Reselect mode and an INSERT or UPDATE still missing a CLOB, NCLOB or BLOB value. */
  private boolean reselectFailed(TableSchema schema, RowChange c) {
    if (lobMode != sh.oso.connect.oracle.core.config.CoreConfig.LobMode.RESELECT
        || c.after() == null
        || c.op() == sh.oso.connect.oracle.core.model.Operation.DELETE) {
      return false;
    }
    for (ColumnSpec col : schema.columns()) {
      sh.oso.connect.oracle.core.schema.OracleType t = col.type();
      if ((t == sh.oso.connect.oracle.core.schema.OracleType.CLOB
              || t == sh.oso.connect.oracle.core.schema.OracleType.NCLOB
              || t == sh.oso.connect.oracle.core.schema.OracleType.BLOB)
          && !c.after().containsKey(col.name())) {
        return true;
      }
    }
    return false;
  }

  private Headers headers(CommittedTransaction tx, RowChange c, int index) {
    ConnectHeaders h = new ConnectHeaders();
    h.addLong("cdc.scn", c.id().scn());
    h.addLong("cdc.commit_scn", tx.commitScn());
    h.addString("cdc.xid", tx.key().xid().toString());
    h.addInt("cdc.event_index", index);
    h.addInt("cdc.event_count", tx.size());
    h.addInt("cdc.schema_version", c.schemaVersion());
    return h;
  }

  private Struct row(Schemas s, Map<String, Object> image) {
    if (image == null) {
      return null;
    }
    Struct v = new Struct(s.value());
    for (org.apache.kafka.connect.data.Field f : s.value().fields()) {
      ColumnSpec col = columnOf(s, f.name());
      if (image.containsKey(f.name())) {
        v.put(f, types.value(col, f.schema(), image.get(f.name())));
      } else if (col.type().isLob()) {
        // SRC-LOB-1: a LOB the redo did not carry (or too large) is unavailable, not null
        v.put(
            f,
            f.schema().type() == Schema.Type.BYTES
                ? java.nio.ByteBuffer.wrap(
                    placeholder.getBytes(java.nio.charset.StandardCharsets.UTF_8))
                : placeholder);
      }
    }
    return v;
  }

  private Object key(Schemas s, Map<String, Object> image) {
    if (s.key() == null || image == null) {
      return null;
    }
    Struct k = new Struct(s.key());
    for (org.apache.kafka.connect.data.Field f : s.key().fields()) {
      if (!image.containsKey(f.name())) {
        return null; // partial image without the key: no key
      }
      ColumnSpec col = columnOf(s, f.name());
      k.put(f, types.value(col, f.schema(), image.get(f.name())));
    }
    return k;
  }

  private final Map<Schemas, Map<String, ColumnSpec>> columnIndex = new HashMap<>();

  private ColumnSpec columnOf(Schemas s, String name) {
    return columnIndex.get(s).get(name);
  }

  private Object keyOf(Schemas s, TableSchema schema, RowChange change, Map<String, Object> image) {
    if (schema.keySource() == KeySource.ROWID) {
      String rowId = sh.oso.connect.oracle.core.model.RowIds.real(change.rowId());
      return s.key() == null || rowId == null ? null : new Struct(s.key()).put("ROWID", rowId);
    }
    return key(s, image);
  }

  Schemas schemas(TableSchema schema) {
    Schemas cached = cache.get(schema);
    if (cached != null) {
      return cached;
    }
    TableId t = schema.table();
    String base =
        config.topicPrefix()
            + "."
            + (t.pdb() == null ? "" : t.pdb() + ".")
            + t.schema()
            + "."
            + t.table();
    SchemaBuilder value = SchemaBuilder.struct().name(base + ".Value").optional();
    Map<String, ColumnSpec> byName = new HashMap<>();
    for (ColumnSpec c : schema.columns()) {
      if (c.type().isLob()
          && lobMode == sh.oso.connect.oracle.core.config.CoreConfig.LobMode.SKIP) {
        continue; // SRC-LOB-1: cdc.lob.mode=skip leaves LOB columns out of the records
      }
      // value fields are optional: a partial before image (primary-key-only logging) omits columns
      value.field(
          c.name(),
          types.schema(
              new ColumnSpec(
                  c.name(),
                  c.position(),
                  c.type(),
                  c.typeText(),
                  c.length(),
                  c.precision(),
                  c.scale(),
                  true)));
      byName.put(c.name(), c);
    }
    Schema valueSchema = value.build();
    Schema keySchema = null;
    List<ColumnSpec> keyCols = new ArrayList<>();
    if (schema.keySource() == KeySource.ROWID) {
      keySchema =
          SchemaBuilder.struct().name(base + ".Key").field("ROWID", Schema.STRING_SCHEMA).build();
    } else if (!schema.keyColumns().isEmpty()) {
      SchemaBuilder key = SchemaBuilder.struct().name(base + ".Key");
      for (String k : schema.keyColumns()) {
        ColumnSpec c = schema.column(k);
        keyCols.add(c);
        key.field(
            c.name(),
            types.schema(
                new ColumnSpec(
                    c.name(),
                    c.position(),
                    c.type(),
                    c.typeText(),
                    c.length(),
                    c.precision(),
                    c.scale(),
                    false)));
      }
      keySchema = key.build();
    }
    Schema envelope =
        SchemaBuilder.struct()
            .name(base + ".Envelope")
            .version(1)
            .field("before", valueSchema)
            .field("after", valueSchema)
            .field("source", sourceSchema)
            .field("op", Schema.STRING_SCHEMA)
            .field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
            .field("ts_us", Schema.OPTIONAL_INT64_SCHEMA)
            .field("ts_ns", Schema.OPTIONAL_INT64_SCHEMA)
            .field("transaction", transactionSchema)
            .build();
    Schemas s = new Schemas(keySchema, valueSchema, envelope, keyCols);
    cache.put(schema, s);
    columnIndex.put(s, byName);
    return s;
  }
}
