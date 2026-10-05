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
package sh.oso.connect.oracle.doctor.metrics;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import sh.oso.connect.oracle.core.doctor.MetricsSample;

/**
 * Reads the task metrics from a Prometheus text endpoint served by the JMX exporter with the rules
 * in {@code ops/jmx-exporter/oracle-cdc.yml}: {@code oracle_cdc_<attribute in snake case>}, with
 * {@code _total} on counters, labelled {@code server}. The exporter has no {@code
 * LargestTransactions}.
 */
public final class PrometheusMetricsSource implements MetricsSource {

  private final URI url;
  private final HttpClient http;
  private final Clock clock;

  public PrometheusMetricsSource(URI url, Clock clock) {
    this.url = url;
    this.clock = clock;
    this.http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
  }

  /** {@code MillisBehindSource} becomes {@code millis_behind_source}. */
  static String snake(String attribute) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < attribute.length(); i++) {
      char c = attribute.charAt(i);
      if (Character.isUpperCase(c) && i > 0) {
        sb.append('_');
      }
      sb.append(Character.toLowerCase(c));
    }
    return sb.toString();
  }

  @Override
  public MetricsSample read(String server) throws Exception {
    HttpResponse<String> r;
    try {
      r =
          http.send(
              HttpRequest.newBuilder(url).timeout(Duration.ofSeconds(30)).GET().build(),
              HttpResponse.BodyHandlers.ofString());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("interrupted while reading " + url, e);
    }
    if (r.statusCode() / 100 != 2) {
      throw new IOException("GET " + url + " returned HTTP " + r.statusCode());
    }
    return new MetricsSample(clock.instant(), parse(r.body(), server), List.of());
  }

  /** The samples labelled with the server, by attribute name. */
  static Map<String, Long> parse(String text, String server) {
    Map<String, Double> byName = new HashMap<>();
    for (String line : text.split("\n")) {
      line = line.trim();
      if (line.isEmpty() || line.startsWith("#") || !line.startsWith("oracle_cdc_")) {
        continue;
      }
      int brace = line.indexOf('{');
      int close = line.indexOf('}');
      String name;
      String rest;
      if (brace > 0 && close > brace) {
        name = line.substring(0, brace);
        if (!labels(line.substring(brace + 1, close)).getOrDefault("server", "").equals(server)) {
          continue;
        }
        rest = line.substring(close + 1).trim();
      } else {
        int sp = line.indexOf(' ');
        if (sp < 0) {
          continue;
        }
        name = line.substring(0, sp);
        rest = line.substring(sp + 1).trim();
      }
      String value = rest.split("\\s+")[0];
      try {
        byName.put(name, Double.parseDouble(value));
      } catch (NumberFormatException ignore) {
        // NaN and friends are skipped
      }
    }
    Map<String, Long> out = new LinkedHashMap<>();
    for (String attr : MetricsSample.ATTRIBUTES) {
      String base = "oracle_cdc_" + snake(attr);
      Double v = byName.containsKey(base) ? byName.get(base) : byName.get(base + "_total");
      if (v != null && !v.isNaN()) {
        out.put(attr, Math.round(v));
      }
    }
    return out;
  }

  private static Map<String, String> labels(String inside) {
    Map<String, String> out = new HashMap<>();
    int i = 0;
    while (i < inside.length()) {
      int eq = inside.indexOf('=', i);
      if (eq < 0) {
        break;
      }
      String key = inside.substring(i, eq).trim().replace(",", "").toLowerCase(Locale.ROOT);
      int q1 = inside.indexOf('"', eq);
      StringBuilder v = new StringBuilder();
      int j = q1 + 1;
      while (j < inside.length() && inside.charAt(j) != '"') {
        if (inside.charAt(j) == '\\' && j + 1 < inside.length()) {
          j++;
        }
        v.append(inside.charAt(j));
        j++;
      }
      out.put(key, v.toString());
      i = j + 1;
    }
    return out;
  }
}
