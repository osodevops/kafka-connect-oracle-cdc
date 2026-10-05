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
package sh.oso.connect.oracle.ops;

import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.position.PositionCodec;

/** Builds ops topic records; they ride the same queue as change records and carry the position. */
public final class OpsEventWriter {

  public static final Schema KEY_SCHEMA =
      SchemaBuilder.struct()
          .name("io.oso.cdc.ops.Key")
          .field("server", Schema.STRING_SCHEMA)
          .build();
  public static final Schema VALUE_SCHEMA =
      SchemaBuilder.struct()
          .name("io.oso.cdc.ops.Event")
          .version(OpsEvent.SCHEMA_VERSION)
          .field("v", Schema.INT32_SCHEMA)
          .field("type", Schema.STRING_SCHEMA)
          .field("ts_ms", Schema.INT64_SCHEMA)
          .field("server", Schema.STRING_SCHEMA)
          .field("resume_scn", Schema.OPTIONAL_INT64_SCHEMA)
          .field(
              "details",
              SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().build())
          .build();

  private final String topic;
  private final String server;
  private final Map<String, Object> partition;

  public OpsEventWriter(String topic, String server, Map<String, Object> partition) {
    this.topic = topic;
    this.server = server;
    this.partition = partition;
  }

  public String topic() {
    return topic;
  }

  public SourceRecord record(OpsEvent e, Position position) {
    Struct key = new Struct(KEY_SCHEMA).put("server", server);
    Struct value =
        new Struct(VALUE_SCHEMA)
            .put("v", OpsEvent.SCHEMA_VERSION)
            .put("type", e.type().wire())
            .put("ts_ms", e.tsMs())
            .put("server", server)
            .put("resume_scn", e.resumeScn())
            .put("details", e.details());
    return new SourceRecord(
        partition,
        position == null ? null : PositionCodec.write(position),
        topic,
        null,
        KEY_SCHEMA,
        key,
        VALUE_SCHEMA,
        value,
        e.tsMs());
  }
}
