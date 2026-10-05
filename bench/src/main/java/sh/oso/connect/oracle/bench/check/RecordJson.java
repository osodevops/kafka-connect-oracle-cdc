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
package sh.oso.connect.oracle.bench.check;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

/**
 * One Debezium-shaped change record as JSON, from a JsonConverter with or without schemas. The
 * oracle works on this shape alone, so it also runs against Debezium's Oracle connector.
 */
public final class RecordJson {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public final String topic;
  public final int partition;
  public final long offset;
  public final JsonNode key;
  public final JsonNode value;
  public final boolean tombstone;
  public final String op;
  public final JsonNode before;
  public final JsonNode after;
  public final String xid;
  public final long commitScn;
  public final long scn;
  public final String db;
  public final String schema;
  public final String table;
  public final Integer eventIndex;
  public final Integer eventCount;

  public RecordJson(ConsumerRecord<String, String> r) throws IOException {
    this.topic = r.topic();
    this.partition = r.partition();
    this.offset = r.offset();
    this.key = unwrap(r.key() == null ? null : MAPPER.readTree(r.key()));
    this.tombstone = r.value() == null;
    this.value = tombstone ? null : unwrap(MAPPER.readTree(r.value()));
    JsonNode source = value == null ? null : value.path("source");
    this.op = value == null ? null : value.path("op").asText(null);
    this.before = value == null || value.path("before").isNull() ? null : value.path("before");
    this.after = value == null || value.path("after").isNull() ? null : value.path("after");
    this.xid = source == null ? header(r, "cdc.xid") : source.path("txId").asText(null);
    this.commitScn =
        source == null
            ? parseLong(header(r, "cdc.commit_scn"))
            : parseLong(source.path("commit_scn").asText(null));
    this.scn =
        source == null
            ? parseLong(header(r, "cdc.scn"))
            : parseLong(source.path("scn").asText(null));
    this.db = source == null ? null : source.path("db").asText(null);
    this.schema = source == null ? null : source.path("schema").asText(null);
    this.table = source == null ? null : source.path("table").asText(null);
    String idx = header(r, "cdc.event_index");
    String cnt = header(r, "cdc.event_count");
    this.eventIndex = idx == null ? null : Integer.valueOf(idx);
    this.eventCount = cnt == null ? null : Integer.valueOf(cnt);
  }

  /** A JsonConverter with schemas enabled wraps the document in {schema, payload}. */
  static JsonNode unwrap(JsonNode n) {
    if (n != null && n.isObject() && n.has("schema") && n.has("payload")) {
      return n.get("payload");
    }
    return n;
  }

  private static String header(ConsumerRecord<String, String> r, String name) {
    Header h = r.headers().lastHeader(name);
    return h == null || h.value() == null ? null : new String(h.value(), StandardCharsets.UTF_8);
  }

  private static long parseLong(String s) {
    return s == null || s.isEmpty() ? -1 : Long.parseLong(s);
  }

  /** The key as a canonical string so rows can be matched between Kafka and the database. */
  public String keyText() {
    return key == null ? null : key.toString();
  }
}
