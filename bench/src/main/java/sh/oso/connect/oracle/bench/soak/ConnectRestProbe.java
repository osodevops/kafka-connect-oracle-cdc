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
package sh.oso.connect.oracle.bench.soak;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The connector through the Kafka Connect REST API: {@code GET /connectors/{name}/offsets}
 * (KIP-875, Kafka 3.6 and later) for the committed position and {@code GET
 * /connectors/{name}/status} for a FAILED connector or task.
 */
public final class ConnectRestProbe implements Soak.Connector {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Pattern CDC_CODE = Pattern.compile("CDC-\\d{4}");

  private final HttpClient http;
  private final String base;

  public ConnectRestProbe(HttpClient http, String connectUrl, String connector) {
    this.http = http;
    String root =
        connectUrl.endsWith("/") ? connectUrl.substring(0, connectUrl.length() - 1) : connectUrl;
    this.base =
        root
            + "/connectors/"
            + URLEncoder.encode(connector, StandardCharsets.UTF_8).replace("+", "%20");
  }

  @Override
  public OptionalLong resumeScn() throws IOException, InterruptedException {
    return minResumeScn(get("/offsets"));
  }

  @Override
  public String failure() throws IOException, InterruptedException {
    return failureOf(get("/status"));
  }

  private JsonNode get(String path) throws IOException, InterruptedException {
    HttpRequest req =
        HttpRequest.newBuilder(URI.create(base + path))
            .timeout(Duration.ofSeconds(30))
            .header("Accept", "application/json")
            .GET()
            .build();
    HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
    if (resp.statusCode() == 404) {
      throw new IOException("Kafka Connect does not know the connector (HTTP 404 on " + path + ")");
    }
    if (resp.statusCode() / 100 != 2) {
      throw new IOException("Kafka Connect answered HTTP " + resp.statusCode() + " on " + path);
    }
    return MAPPER.readTree(resp.body());
  }

  /**
   * The lowest {@code resume_scn} over the connector's offsets (a number or a numeric string):
   * every commit below it has been delivered and acknowledged. Empty when no offset carries one.
   */
  static OptionalLong minResumeScn(JsonNode offsets) {
    long min = Long.MAX_VALUE;
    boolean any = false;
    for (JsonNode e : offsets.path("offsets")) {
      JsonNode v = e.path("offset").path("resume_scn");
      long scn;
      if (v.isIntegralNumber()) {
        scn = v.asLong();
      } else if (v.isTextual() && v.asText().strip().matches("\\d+")) {
        scn = Long.parseLong(v.asText().strip());
      } else {
        continue;
      }
      any = true;
      min = Math.min(min, scn);
    }
    return any ? OptionalLong.of(min) : OptionalLong.empty();
  }

  /**
   * "connector FAILED" or "task 0 FAILED with CDC-2001" when something has failed, else null. Only
   * the CDC code is taken from a trace, never its text, so no row value can reach the soak's log.
   */
  static String failureOf(JsonNode status) {
    if ("FAILED".equals(status.path("connector").path("state").asText())) {
      return "connector FAILED" + code(status.path("connector").path("trace").asText(null));
    }
    for (JsonNode t : status.path("tasks")) {
      if ("FAILED".equals(t.path("state").asText())) {
        return "task "
            + t.path("id").asText("?")
            + " FAILED"
            + code(t.path("trace").asText(null))
            + "; read its trace with GET /connectors/{name}/status";
      }
    }
    return null;
  }

  private static String code(String trace) {
    if (trace == null) {
      return "";
    }
    Matcher m = CDC_CODE.matcher(trace);
    return m.find() ? " with " + m.group() : " (no CDC code: a framework or Kafka failure)";
  }
}
