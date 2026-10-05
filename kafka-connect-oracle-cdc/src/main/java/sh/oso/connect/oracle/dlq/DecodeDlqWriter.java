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
package sh.oso.connect.oracle.dlq;

import java.time.Duration;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import sh.oso.connect.oracle.core.buffer.TransactionBuffer;
import sh.oso.connect.oracle.core.errors.DecodeException;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.position.PositionCodec;

/**
 * Records for the decode DLQ topic (SRC-ERR-2, CORE-TX-6): a row the connector could not decode or
 * that LogMiner marked unsupported, with its raw SQL_REDO, or a transaction discarded by the long
 * transaction policy. Every record names the redo position, the transaction and the reason, so the
 * operator can reconcile the table from the database.
 */
public final class DecodeDlqWriter {

  public static final int SCHEMA_VERSION = 1;

  public static final Schema KEY_SCHEMA =
      SchemaBuilder.struct()
          .name("io.oso.cdc.dlq.Key")
          .field("server", Schema.STRING_SCHEMA)
          .field("xid", Schema.STRING_SCHEMA)
          .build();

  public static final Schema VALUE_SCHEMA =
      SchemaBuilder.struct()
          .name("io.oso.cdc.dlq.Record")
          .version(SCHEMA_VERSION)
          .field("v", Schema.INT32_SCHEMA)
          .field("kind", Schema.STRING_SCHEMA)
          .field("ts_ms", Schema.INT64_SCHEMA)
          .field("server", Schema.STRING_SCHEMA)
          .field("con_id", Schema.INT32_SCHEMA)
          .field("xid", Schema.STRING_SCHEMA)
          .field("pdb", Schema.OPTIONAL_STRING_SCHEMA)
          .field("schema", Schema.OPTIONAL_STRING_SCHEMA)
          .field("table", Schema.OPTIONAL_STRING_SCHEMA)
          .field("scn", Schema.OPTIONAL_INT64_SCHEMA)
          .field("rs_id", Schema.OPTIONAL_STRING_SCHEMA)
          .field("ssn", Schema.OPTIONAL_INT64_SCHEMA)
          .field("operation", Schema.OPTIONAL_STRING_SCHEMA)
          .field("status", Schema.OPTIONAL_INT32_SCHEMA)
          .field("info", Schema.OPTIONAL_STRING_SCHEMA)
          .field("sql_redo", Schema.OPTIONAL_STRING_SCHEMA)
          .field("sql_undo", Schema.OPTIONAL_STRING_SCHEMA)
          .field("schema_version", Schema.OPTIONAL_INT64_SCHEMA)
          .field("exception", Schema.OPTIONAL_STRING_SCHEMA)
          .field("message", Schema.OPTIONAL_STRING_SCHEMA)
          .field("user", Schema.OPTIONAL_STRING_SCHEMA)
          .field("first_scn", Schema.OPTIONAL_INT64_SCHEMA)
          .field("last_scn", Schema.OPTIONAL_INT64_SCHEMA)
          .field("events", Schema.OPTIONAL_INT32_SCHEMA)
          .field("age_ms", Schema.OPTIONAL_INT64_SCHEMA)
          .build();

  private final String topic;
  private final String server;
  private final Map<String, Object> partition;

  public DecodeDlqWriter(String topic, String server, Map<String, Object> partition) {
    this.topic = topic;
    this.server = server;
    this.partition = partition;
  }

  public String topic() {
    return topic;
  }

  /** A row that could not be decoded (cdc.on.decode.error=dlq). */
  public SourceRecord decodeError(
      MiningEvent.Dml d, DecodeException cause, long schemaEpoch, Position offset, long nowMs) {
    Struct v =
        base("decode-error", d.tx().srcConId(), d.tx().xid().toString(), nowMs)
            .put("pdb", d.table().pdb())
            .put("schema", d.table().schema())
            .put("table", d.table().table())
            .put("scn", d.id().scn())
            .put("rs_id", d.id().rsId())
            .put("ssn", d.id().ssn())
            .put("operation", d.op().name())
            .put("status", d.status())
            .put("info", d.info())
            .put("sql_redo", d.sqlRedo())
            .put("sql_undo", d.sqlUndo())
            .put("schema_version", schemaEpoch)
            .put("exception", cause.getClass().getName())
            .put("message", cause.getMessage())
            .put("user", d.username());
    return record(d.tx().xid().toString(), v, offset, nowMs);
  }

  /** A row LogMiner marked UNSUPPORTED for a captured table. */
  public SourceRecord unsupported(MiningEvent.Unsupported u, Position offset, long nowMs) {
    Struct v =
        base("unsupported-row", u.tx().srcConId(), u.tx().xid().toString(), nowMs)
            .put("pdb", u.table().pdb())
            .put("schema", u.table().schema())
            .put("table", u.table().table())
            .put("scn", u.id().scn())
            .put("rs_id", u.id().rsId())
            .put("ssn", u.id().ssn())
            .put("status", u.status())
            .put("info", u.info())
            .put("sql_redo", u.sqlRedo());
    return record(u.tx().xid().toString(), v, offset, nowMs);
  }

  /** A transaction discarded by cdc.transaction.max.age.action=discard. */
  public SourceRecord discarded(
      TransactionBuffer.OpenTransaction t, Duration age, Position offset, long nowMs) {
    Struct v =
        base("transaction-discarded", t.key().srcConId(), t.key().xid().toString(), nowMs)
            .put("user", t.username())
            .put("first_scn", t.firstScn())
            .put("last_scn", t.lastScn())
            .put("events", t.events())
            .put("age_ms", age.toMillis());
    return record(t.key().xid().toString(), v, offset, nowMs);
  }

  private Struct base(String kind, int conId, String xid, long nowMs) {
    return new Struct(VALUE_SCHEMA)
        .put("v", SCHEMA_VERSION)
        .put("kind", kind)
        .put("ts_ms", nowMs)
        .put("server", server)
        .put("con_id", conId)
        .put("xid", xid);
  }

  private SourceRecord record(String xid, Struct value, Position offset, long nowMs) {
    Struct key = new Struct(KEY_SCHEMA).put("server", server).put("xid", xid);
    return new SourceRecord(
        partition,
        offset == null ? null : PositionCodec.write(offset),
        topic,
        null,
        KEY_SCHEMA,
        key,
        VALUE_SCHEMA,
        value,
        nowMs);
  }
}
