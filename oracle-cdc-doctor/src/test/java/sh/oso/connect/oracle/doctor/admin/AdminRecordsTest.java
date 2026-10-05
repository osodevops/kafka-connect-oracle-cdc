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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfig;
import sh.oso.connect.oracle.doctor.testing.FakeEnv;
import sh.oso.connect.oracle.ops.OpsEvent;

class AdminRecordsTest {

  static final ObjectMapper JSON = new ObjectMapper();

  private static AdminRecords records(Map<String, String> extra, AdminRecords.OpsFormat f) {
    Map<String, String> props = new HashMap<>(FakeEnv.connectorConfig());
    props.putAll(extra);
    return new AdminRecords(new OracleCdcSourceConnectorConfig(props), props, "orders", f);
  }

  @Test
  void opsRecordsHaveTheConnectorsOpsShape() throws Exception {
    AdminRecords r = records(Map.of(), AdminRecords.OpsFormat.AUTO);
    assertThat(r.schemas()).isFalse();
    assertThat(r.opsTopic()).isEqualTo("cdc.cdc.ops");
    assertThat(r.signalsTopic()).isEqualTo("cdc.cdc.signals");
    byte[][] kv =
        r.opsRecord(OpsEvent.of(OpsEvent.Type.OFFSETS_SET, 42L, 1200L, "reason", "why", "x", null));
    assertThat(new String(kv[0], StandardCharsets.UTF_8)).isEqualTo("{\"server\":\"cdc\"}");
    JsonNode v = JSON.readTree(kv[1]);
    assertThat(v.path("v").asInt()).isEqualTo(1);
    assertThat(v.path("type").asText()).isEqualTo("offsets-set");
    assertThat(v.path("ts_ms").asLong()).isEqualTo(42);
    assertThat(v.path("server").asText()).isEqualTo("cdc");
    assertThat(v.path("resume_scn").asLong()).isEqualTo(1200);
    assertThat(v.path("details").path("reason").asText()).isEqualTo("why");
    assertThat(v.path("details").has("x")).isFalse();
  }

  @Test
  void theJsonShapeFollowsTheConverterOrTheOption() {
    Map<String, String> json = Map.of("value.converter", AdminRecords.JSON_CONVERTER);
    assertThat(records(json, AdminRecords.OpsFormat.AUTO).schemas()).isTrue();
    assertThat(
            records(
                    Map.of(
                        "value.converter",
                        AdminRecords.JSON_CONVERTER,
                        "value.converter.schemas.enable",
                        "false"),
                    AdminRecords.OpsFormat.AUTO)
                .schemas())
        .isFalse();
    assertThat(records(Map.of(), AdminRecords.OpsFormat.JSON_SCHEMAS).schemas()).isTrue();
    Map<String, String> avro = Map.of("value.converter", "io.confluent.connect.avro.AvroConverter");
    assertThat(records(avro, AdminRecords.OpsFormat.JSON).schemas()).isFalse();
    assertThatThrownBy(() -> records(avro, AdminRecords.OpsFormat.AUTO))
        .isInstanceOf(AdminException.class)
        .hasMessageContaining("AvroConverter");
  }

  @Test
  void theSnapshotSignalHasTheSignalShape() throws Exception {
    JsonNode s =
        JSON.readTree(
            AdminRecords.snapshotSignal("id-1", List.of("FREEPDB1.APP.A", "FREEPDB1.APP.B")));
    assertThat(s.toString())
        .isEqualTo(
            "{\"id\":\"id-1\",\"type\":\"snapshot\",\"data\":{\"tables\":[\"FREEPDB1.APP.A\","
                + "\"FREEPDB1.APP.B\"]}}");
  }
}
