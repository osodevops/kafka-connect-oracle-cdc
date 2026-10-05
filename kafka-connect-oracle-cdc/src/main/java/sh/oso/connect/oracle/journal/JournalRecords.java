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
package sh.oso.connect.oracle.journal;

import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import sh.oso.connect.oracle.core.buffer.JournalChunk;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.position.PositionCodec;

/**
 * Journal topic records (ADR-0003): a chunk per record keyed by server, container, XID, chunk
 * number and generation; a tombstone per chunk when the transaction ends. Both directions live here
 * so the loader reads exactly what the sink wrote.
 */
public final class JournalRecords {

  public static final int SCHEMA_VERSION = 1;

  public static final Schema KEY_SCHEMA =
      SchemaBuilder.struct()
          .name("io.oso.cdc.txjournal.Key")
          .field("server", Schema.STRING_SCHEMA)
          .field("con_id", Schema.INT32_SCHEMA)
          .field("xid", Schema.STRING_SCHEMA)
          .field("chunk", Schema.INT32_SCHEMA)
          .field("generation", Schema.INT64_SCHEMA)
          .build();

  public static final Schema VALUE_SCHEMA =
      SchemaBuilder.struct()
          .name("io.oso.cdc.txjournal.Chunk")
          .version(SCHEMA_VERSION)
          .field("v", Schema.INT32_SCHEMA)
          .field("first_scn", Schema.INT64_SCHEMA)
          .field("first_rs_id", Schema.OPTIONAL_STRING_SCHEMA)
          .field("first_ssn", Schema.INT64_SCHEMA)
          .field("last_scn", Schema.INT64_SCHEMA)
          .field("last_rs_id", Schema.OPTIONAL_STRING_SCHEMA)
          .field("last_ssn", Schema.INT64_SCHEMA)
          .field("events", Schema.INT32_SCHEMA)
          .field("undos", Schema.INT32_SCHEMA)
          .field("first_captured_scn", Schema.INT64_SCHEMA)
          .field("first_captured_rs_id", Schema.OPTIONAL_STRING_SCHEMA)
          .field("first_captured_ssn", Schema.INT64_SCHEMA)
          .field("start_scn", Schema.OPTIONAL_INT64_SCHEMA)
          .field("start_rs_id", Schema.OPTIONAL_STRING_SCHEMA)
          .field("start_ssn", Schema.OPTIONAL_INT64_SCHEMA)
          .field("thread", Schema.INT32_SCHEMA)
          .field("username", Schema.OPTIONAL_STRING_SCHEMA)
          .field("client_id", Schema.OPTIONAL_STRING_SCHEMA)
          .field("payload", Schema.BYTES_SCHEMA)
          .build();

  /** Identity of one chunk record in the topic. */
  public record ChunkKey(TxKey key, int chunk, long generation) {}

  private final String topic;
  private final String server;
  private final Map<String, Object> partition;

  public JournalRecords(String topic, String server, Map<String, Object> partition) {
    this.topic = topic;
    this.server = server;
    this.partition = partition;
  }

  public String topic() {
    return topic;
  }

  public SourceRecord chunk(JournalChunk c, Position offset) {
    Struct value =
        new Struct(VALUE_SCHEMA)
            .put("v", SCHEMA_VERSION)
            .put("first_scn", c.first().scn())
            .put("first_rs_id", c.first().rsId())
            .put("first_ssn", c.first().ssn())
            .put("last_scn", c.last().scn())
            .put("last_rs_id", c.last().rsId())
            .put("last_ssn", c.last().ssn())
            .put("events", c.events())
            .put("undos", c.undos())
            .put("first_captured_scn", c.firstCaptured().scn())
            .put("first_captured_rs_id", c.firstCaptured().rsId())
            .put("first_captured_ssn", c.firstCaptured().ssn())
            .put("start_scn", c.startId() == null ? null : c.startId().scn())
            .put("start_rs_id", c.startId() == null ? null : c.startId().rsId())
            .put("start_ssn", c.startId() == null ? null : c.startId().ssn())
            .put("thread", c.thread())
            .put("username", c.username())
            .put("client_id", c.clientId())
            .put("payload", c.payload());
    return new SourceRecord(
        partition,
        offset == null ? null : PositionCodec.write(offset),
        topic,
        null,
        KEY_SCHEMA,
        key(new ChunkKey(c.key(), c.chunk(), c.generation())),
        VALUE_SCHEMA,
        value,
        null);
  }

  public SourceRecord tombstone(ChunkKey k, Position offset) {
    return new SourceRecord(
        partition,
        offset == null ? null : PositionCodec.write(offset),
        topic,
        null,
        KEY_SCHEMA,
        key(k),
        null,
        null,
        null);
  }

  private Struct key(ChunkKey k) {
    return new Struct(KEY_SCHEMA)
        .put("server", server)
        .put("con_id", k.key().srcConId())
        .put("xid", k.key().xid().toString())
        .put("chunk", k.chunk())
        .put("generation", k.generation());
  }

  /** Reads a key back from a Struct or, with a schemaless converter, a Map; null when foreign. */
  public static ChunkKey parseKey(Object key, String server) {
    Object srv = field(key, "server");
    if (srv == null || !server.equals(srv.toString())) {
      return null;
    }
    String[] p = String.valueOf(field(key, "xid")).split("\\.");
    TxKey tx =
        new TxKey(
            toInt(field(key, "con_id")),
            new Xid(Long.parseLong(p[0]), Long.parseLong(p[1]), Long.parseLong(p[2])));
    return new ChunkKey(tx, toInt(field(key, "chunk")), toLong(field(key, "generation")));
  }

  /** Reads a chunk value back; {@code value} is a Struct or a Map. */
  public static JournalChunk parseValue(ChunkKey k, Object value) {
    RedoRecordId start =
        field(value, "start_scn") == null
            ? null
            : new RedoRecordId(
                toLong(field(value, "start_scn")),
                str(field(value, "start_rs_id")),
                toLong(field(value, "start_ssn")));
    return new JournalChunk(
        k.key(),
        k.chunk(),
        k.generation(),
        new RedoRecordId(
            toLong(field(value, "first_scn")),
            str(field(value, "first_rs_id")),
            toLong(field(value, "first_ssn"))),
        new RedoRecordId(
            toLong(field(value, "last_scn")),
            str(field(value, "last_rs_id")),
            toLong(field(value, "last_ssn"))),
        toInt(field(value, "events")),
        toInt(field(value, "undos")),
        new RedoRecordId(
            toLong(field(value, "first_captured_scn")),
            str(field(value, "first_captured_rs_id")),
            toLong(field(value, "first_captured_ssn"))),
        start,
        toInt(field(value, "thread")),
        str(field(value, "username")),
        str(field(value, "client_id")),
        bytes(field(value, "payload")));
  }

  private static Object field(Object container, String name) {
    if (container instanceof Struct s) {
      return s.schema().field(name) == null ? null : s.get(name);
    }
    if (container instanceof Map<?, ?> m) {
      return m.get(name);
    }
    throw new IllegalArgumentException(
        "journal record is neither a Struct nor a Map: "
            + (container == null ? "null" : container.getClass().getName()));
  }

  private static int toInt(Object o) {
    return ((Number) o).intValue();
  }

  private static long toLong(Object o) {
    return ((Number) o).longValue();
  }

  private static String str(Object o) {
    return o == null ? null : o.toString();
  }

  private static byte[] bytes(Object o) {
    if (o instanceof byte[] b) {
      return b;
    }
    if (o instanceof java.nio.ByteBuffer bb) {
      byte[] b = new byte[bb.remaining()];
      bb.duplicate().get(b);
      return b;
    }
    if (o instanceof String s) {
      return java.util.Base64.getDecoder().decode(s); // schemaless JSON carries bytes as base64
    }
    throw new IllegalArgumentException("journal payload of unexpected type " + o.getClass());
  }
}
