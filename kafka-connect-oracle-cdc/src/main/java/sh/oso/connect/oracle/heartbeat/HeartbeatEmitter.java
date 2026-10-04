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
package sh.oso.connect.oracle.heartbeat;

import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.position.PositionCodec;

/**
 * Heartbeat records (SRC-HB-1, CORE-POS-5): a small record to the heartbeat topic whose offset is
 * the current position. Written once at start, so the start SCN of a new connector is durable
 * before the first change record (a task killed earlier would otherwise restart from a later
 * current SCN and skip the window in between), and then whenever no change record has flowed for
 * the heartbeat interval, so offsets advance on a quiet database without writing to the source.
 */
public final class HeartbeatEmitter {

  public static final Schema KEY_SCHEMA =
      SchemaBuilder.struct()
          .name("io.oso.cdc.heartbeat.Key")
          .field("server", Schema.STRING_SCHEMA)
          .build();
  public static final Schema VALUE_SCHEMA =
      SchemaBuilder.struct()
          .name("io.oso.cdc.heartbeat.Value")
          .field("ts_ms", Schema.INT64_SCHEMA)
          .field("resume_scn", Schema.INT64_SCHEMA)
          .field("mined_to_scn", Schema.OPTIONAL_INT64_SCHEMA)
          .field("reason", Schema.STRING_SCHEMA)
          .build();

  private final String topic;
  private final String server;
  private final Map<String, Object> partition;

  public HeartbeatEmitter(String topic, String server, Map<String, Object> partition) {
    this.topic = topic;
    this.server = server;
    this.partition = partition;
  }

  public SourceRecord record(Position position, Long minedToScn, String reason, long nowMs) {
    Struct key = new Struct(KEY_SCHEMA).put("server", server);
    Struct value =
        new Struct(VALUE_SCHEMA)
            .put("ts_ms", nowMs)
            .put("resume_scn", position.resumeScn())
            .put("mined_to_scn", minedToScn)
            .put("reason", reason);
    return new SourceRecord(
        partition,
        PositionCodec.write(position),
        topic,
        null,
        KEY_SCHEMA,
        key,
        VALUE_SCHEMA,
        value,
        nowMs);
  }

  public String topic() {
    return topic;
  }
}
