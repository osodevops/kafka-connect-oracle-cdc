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
package sh.oso.connect.oracle.doctor.admin;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfig;
import sh.oso.connect.oracle.ops.OpsEvent;
import sh.oso.connect.oracle.ops.OpsEventWriter;

/**
 * The records the admin commands write: ops events in the connector's own ops record shape ({@link
 * OpsEventWriter}, key and value through the JSON converter) and signals on the signals topic,
 * keyed by connector name: {@code {"id": ..., "type": "snapshot", "data": {"tables": [...]}}}.
 */
public final class AdminRecords {

  /** JSON shape of the ops records: plain, or with the converter's schema envelope. */
  public enum OpsFormat {
    AUTO,
    JSON,
    JSON_SCHEMAS
  }

  static final String JSON_CONVERTER = JsonConverter.class.getName();

  private static final ObjectMapper JSON = new ObjectMapper();

  private final OracleCdcSourceConnectorConfig config;
  private final String connector;
  private final boolean schemas;

  public AdminRecords(
      OracleCdcSourceConnectorConfig config,
      Map<String, String> props,
      String connector,
      OpsFormat format) {
    this.config = config;
    this.connector = connector;
    this.schemas = schemas(props, format);
  }

  /**
   * Whether ops records carry the schema envelope: as asked, else as the connector's own JSON value
   * converter is set, else plain JSON. Another converter cannot be matched and is refused.
   */
  static boolean schemas(Map<String, String> props, OpsFormat format) {
    if (format == OpsFormat.JSON) {
      return false;
    }
    if (format == OpsFormat.JSON_SCHEMAS) {
      return true;
    }
    String converter = props.get("value.converter");
    if (converter == null || converter.isBlank()) {
      return false;
    }
    if (!JSON_CONVERTER.equals(converter.trim())) {
      throw AdminException.usage(
          "The connector's value.converter is "
              + converter
              + "; oracle-cdc-admin writes ops events as JSON only. Pass --ops-format json or"
              + " json_schemas to choose the JSON shape explicitly.");
    }
    return Boolean.parseBoolean(
        props
            .getOrDefault("value.converter.schemas.enable", "true")
            .trim()
            .toLowerCase(Locale.ROOT));
  }

  public boolean schemas() {
    return schemas;
  }

  /** Key and value bytes of an ops event, exactly as the connector's worker would write them. */
  public byte[][] opsRecord(OpsEvent e) {
    SourceRecord r =
        new OpsEventWriter(config.opsTopic(), config.topicPrefix(), Map.of()).record(e, null);
    JsonConverter keys = converter(true);
    JsonConverter values = converter(false);
    return new byte[][] {
      keys.fromConnectData(r.topic(), r.keySchema(), r.key()),
      values.fromConnectData(r.topic(), r.valueSchema(), r.value())
    };
  }

  private JsonConverter converter(boolean isKey) {
    JsonConverter c = new JsonConverter();
    c.configure(Map.of("schemas.enable", Boolean.toString(schemas)), isKey);
    return c;
  }

  /** Writes an ops event and waits for the brokers. */
  public void ops(sh.oso.connect.oracle.doctor.kafka.KafkaPort kafka, OpsEvent e) throws Exception {
    byte[][] kv = opsRecord(e);
    kafka.send(config.opsTopic(), kv[0], kv[1]);
  }

  /** The snapshot signal (PRD-02 SNAP-6, SNAP-7) value for the given tables. */
  public static String snapshotSignal(String id, List<String> tables) {
    Map<String, Object> data = new LinkedHashMap<>();
    data.put("tables", tables);
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("id", id);
    m.put("type", "snapshot");
    m.put("data", data);
    try {
      return JSON.writeValueAsString(m);
    } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  /** Writes a snapshot signal and waits for the brokers; returns the signal id. */
  public String snapshot(sh.oso.connect.oracle.doctor.kafka.KafkaPort kafka, List<String> tables)
      throws Exception {
    String id = "oracle-cdc-admin-" + UUID.randomUUID();
    kafka.send(
        config.signalsTopic(),
        connector.getBytes(StandardCharsets.UTF_8),
        snapshotSignal(id, tables).getBytes(StandardCharsets.UTF_8));
    return id;
  }

  public String opsTopic() {
    return config.opsTopic();
  }

  public String signalsTopic() {
    return config.signalsTopic();
  }
}
