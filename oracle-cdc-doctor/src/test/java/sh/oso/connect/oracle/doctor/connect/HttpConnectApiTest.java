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
package sh.oso.connect.oracle.doctor.connect;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** HttpConnectApi against a local stub of the Connect REST API. */
class HttpConnectApiTest {

  static final ObjectMapper JSON = new ObjectMapper();

  record Call(String method, String path, String body) {}

  HttpServer server;
  final List<Call> calls = new ArrayList<>();
  HttpConnectApi api;

  private void respond(String path, int status, String body) {
    server.createContext(
        path,
        x -> {
          String in = new String(x.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
          calls.add(new Call(x.getRequestMethod(), x.getRequestURI().getRawPath(), in));
          byte[] b = body.getBytes(StandardCharsets.UTF_8);
          x.getResponseHeaders().add("Content-Type", "application/json");
          x.sendResponseHeaders(status, b.length == 0 ? -1 : b.length);
          if (b.length > 0) {
            try (OutputStream o = x.getResponseBody()) {
              o.write(b);
            }
          }
          x.close();
        });
  }

  @BeforeEach
  void start() throws IOException {
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    respond("/connectors/my conn/config", 200, "{\"cdc.topic.prefix\":\"cdc\",\"n\":3}");
    respond(
        "/connectors/my conn/status",
        200,
        "{\"name\":\"my"
            + " conn\",\"connector\":{\"state\":\"STOPPED\",\"worker_id\":\"w\"},\"tasks\":[]}");
    respond(
        "/connectors/my conn/offsets",
        200,
        "{\"offsets\":[{\"partition\":{\"server\":\"cdc\"},\"offset\":{\"v\":1,\"resume_scn\":1500}}]}");
    respond("/connectors/my conn/stop", 202, "");
    respond("/connectors/my conn/resume", 202, "");
    respond(
        "/connectors/broken/status",
        404,
        "{\"error_code\":404,\"message\":\"Connector broken not found\"}");
    respond("/connectors/plain/status", 500, "not json");
    respond(
        "/connector-plugins/OracleCdcSourceConnector/config/validate",
        200,
        "{\"name\":\"x\",\"error_count\":1,\"configs\":[{\"value\":{\"name\":\"exactly.once.support\",\"errors\":[\"no"
            + " exactly-once here\"]}},{\"value\":{\"name\":\"cdc.topic.prefix\","
            + "\"errors\":[]}}]}");
    server.start();
    api = new HttpConnectApi(URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/"));
  }

  @AfterEach
  void stop() {
    server.stop(0);
  }

  @Test
  void readsConfigStatusAndOffsets() throws Exception {
    assertThat(api.config("my conn"))
        .containsEntry("cdc.topic.prefix", "cdc")
        .containsEntry("n", "3");
    assertThat(api.state("my conn")).isEqualTo("STOPPED");
    List<ConnectApi.OffsetEntry> offsets = api.offsets("my conn");
    assertThat(offsets)
        .singleElement()
        .satisfies(
            o -> {
              assertThat(o.partition()).containsEntry("server", "cdc");
              assertThat(o.offset()).containsEntry("resume_scn", 1500);
            });
  }

  @Test
  void patchesStopsResumesAndValidates() throws Exception {
    api.patchOffset("my conn", Map.of("server", "cdc"), Map.of("v", 1, "resume_scn", 1200));
    api.stop("my conn");
    api.resume("my conn");
    Map<String, List<String>> errors =
        api.validate("sh.oso.connect.oracle.OracleCdcSourceConnector", Map.of("a", "b"));
    assertThat(errors).containsOnlyKeys("exactly.once.support");
    assertThat(calls).extracting(Call::method).containsExactly("PATCH", "PUT", "PUT", "PUT");
    JsonNode patch = JSON.readTree(calls.get(0).body());
    assertThat(patch.path("offsets").get(0).path("partition").path("server").asText())
        .isEqualTo("cdc");
    assertThat(patch.path("offsets").get(0).path("offset").path("resume_scn").asLong())
        .isEqualTo(1200);
    assertThat(calls.get(1).path()).isEqualTo("/connectors/my%20conn/stop");
    assertThat(JSON.readTree(calls.get(3).body()).path("a").asText()).isEqualTo("b");
  }

  @Test
  void errorsCarryTheWorkersMessage() {
    assertThatThrownBy(() -> api.state("broken"))
        .isInstanceOf(IOException.class)
        .hasMessageContaining("HTTP 404")
        .hasMessageContaining("Connector broken not found");
    assertThatThrownBy(() -> api.state("plain")).hasMessageContaining("HTTP 500: not json");
  }
}
