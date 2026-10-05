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

import com.fasterxml.jackson.core.type.TypeReference;
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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** {@link ConnectApi} over the worker's REST endpoint with the JDK HTTP client. */
public final class HttpConnectApi implements ConnectApi {

  private static final ObjectMapper JSON = new ObjectMapper();
  private static final Duration TIMEOUT = Duration.ofSeconds(30);

  private final URI base;
  private final HttpClient http;

  public HttpConnectApi(URI base) {
    String s = base.toString();
    this.base = URI.create(s.endsWith("/") ? s.substring(0, s.length() - 1) : s);
    this.http = HttpClient.newBuilder().connectTimeout(TIMEOUT).build();
  }

  private static String seg(String s) {
    return URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20");
  }

  private JsonNode call(String method, String path, Object body) throws IOException {
    HttpRequest.Builder b =
        HttpRequest.newBuilder(URI.create(base + path))
            .timeout(TIMEOUT)
            .header("Accept", "application/json");
    if (body == null) {
      b.method(method, HttpRequest.BodyPublishers.noBody());
    } else {
      b.header("Content-Type", "application/json")
          .method(method, HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)));
    }
    HttpResponse<String> r;
    try {
      r = http.send(b.build(), HttpResponse.BodyHandlers.ofString());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("interrupted while calling " + method + " " + path, e);
    }
    if (r.statusCode() / 100 != 2) {
      String message = r.body();
      try {
        JsonNode err = JSON.readTree(r.body());
        if (err != null && err.hasNonNull("message")) {
          message = err.get("message").asText();
        }
      } catch (IOException ignore) {
        // not JSON: keep the raw body
      }
      throw new IOException(
          method + " " + path + " returned HTTP " + r.statusCode() + ": " + message);
    }
    String text = r.body();
    return text == null || text.isBlank() ? null : JSON.readTree(text);
  }

  @Override
  public Map<String, String> config(String connector) throws IOException {
    JsonNode n = call("GET", "/connectors/" + seg(connector) + "/config", null);
    Map<String, String> out = new LinkedHashMap<>();
    if (n != null) {
      n.fields()
          .forEachRemaining(
              e ->
                  out.put(
                      e.getKey(),
                      e.getValue().isValueNode()
                          ? e.getValue().asText()
                          : e.getValue().toString()));
    }
    return out;
  }

  @Override
  public String state(String connector) throws IOException {
    JsonNode n = call("GET", "/connectors/" + seg(connector) + "/status", null);
    return n == null ? null : n.path("connector").path("state").asText(null);
  }

  @Override
  public List<OffsetEntry> offsets(String connector) throws IOException {
    JsonNode n = call("GET", "/connectors/" + seg(connector) + "/offsets", null);
    List<OffsetEntry> out = new ArrayList<>();
    if (n == null) {
      return out;
    }
    TypeReference<Map<String, Object>> map = new TypeReference<>() {};
    for (JsonNode o : n.path("offsets")) {
      out.add(
          new OffsetEntry(
              JSON.convertValue(o.path("partition"), map),
              o.path("offset").isNull() ? null : JSON.convertValue(o.path("offset"), map)));
    }
    return out;
  }

  @Override
  public void patchOffset(
      String connector, Map<String, Object> partition, Map<String, Object> offset)
      throws IOException {
    Map<String, Object> entry = new LinkedHashMap<>();
    entry.put("partition", partition);
    entry.put("offset", offset);
    call("PATCH", "/connectors/" + seg(connector) + "/offsets", Map.of("offsets", List.of(entry)));
  }

  @Override
  public void stop(String connector) throws IOException {
    call("PUT", "/connectors/" + seg(connector) + "/stop", null);
  }

  @Override
  public void resume(String connector) throws IOException {
    call("PUT", "/connectors/" + seg(connector) + "/resume", null);
  }

  @Override
  public Map<String, List<String>> validate(String connectorClass, Map<String, String> config)
      throws IOException {
    String plugin = connectorClass.substring(connectorClass.lastIndexOf('.') + 1);
    JsonNode n = call("PUT", "/connector-plugins/" + seg(plugin) + "/config/validate", config);
    Map<String, List<String>> out = new LinkedHashMap<>();
    if (n == null) {
      return out;
    }
    for (JsonNode c : n.path("configs")) {
      JsonNode v = c.path("value");
      List<String> errors = new ArrayList<>();
      for (JsonNode e : v.path("errors")) {
        errors.add(e.asText());
      }
      if (!errors.isEmpty()) {
        out.put(v.path("name").asText(), errors);
      }
    }
    return out;
  }
}
