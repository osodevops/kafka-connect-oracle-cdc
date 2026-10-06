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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.function.Predicate;

/**
 * A reader for the Prometheus text exposition format, as the JMX exporter serves it: comment and
 * blank lines are skipped, a sample line is {@code name{label="value",...} value [timestamp]}.
 * Lines it cannot read are skipped, never fatal: a scrape is evidence, not a gate.
 */
public final class PrometheusText {

  /** One sample line. */
  public record Sample(String name, Map<String, String> labels, double value) {}

  private final List<Sample> samples;

  private PrometheusText(List<Sample> samples) {
    this.samples = samples;
  }

  public static PrometheusText parse(String text) {
    List<Sample> out = new ArrayList<>();
    for (String raw : text.split("\n", -1)) {
      String line = raw.strip();
      if (line.isEmpty() || line.startsWith("#")) {
        continue;
      }
      Sample s = parseLine(line);
      if (s != null) {
        out.add(s);
      }
    }
    return new PrometheusText(out);
  }

  public List<Sample> samples() {
    return samples;
  }

  /** The largest value of {@code name} over the series whose labels match; empty when none. */
  public OptionalDouble max(String name, Predicate<Map<String, String>> labels) {
    return samples.stream()
        .filter(s -> s.name().equals(name) && labels.test(s.labels()) && !Double.isNaN(s.value()))
        .mapToDouble(Sample::value)
        .max();
  }

  /** The sum of {@code name} over the series whose labels match; empty when none. */
  public OptionalDouble sum(String name, Predicate<Map<String, String>> labels) {
    List<Sample> m =
        samples.stream()
            .filter(
                s -> s.name().equals(name) && labels.test(s.labels()) && !Double.isNaN(s.value()))
            .toList();
    if (m.isEmpty()) {
      return OptionalDouble.empty();
    }
    return OptionalDouble.of(m.stream().mapToDouble(Sample::value).sum());
  }

  static Sample parseLine(String line) {
    int i = 0;
    int n = line.length();
    while (i < n && line.charAt(i) != '{' && !Character.isWhitespace(line.charAt(i))) {
      i++;
    }
    String name = line.substring(0, i);
    if (name.isEmpty()) {
      return null;
    }
    Map<String, String> labels = new LinkedHashMap<>();
    if (i < n && line.charAt(i) == '{') {
      i++;
      while (true) {
        while (i < n && (line.charAt(i) == ',' || line.charAt(i) == ' ')) {
          i++;
        }
        if (i >= n) {
          return null;
        }
        if (line.charAt(i) == '}') {
          i++;
          break;
        }
        int eq = line.indexOf('=', i);
        if (eq < 0 || eq + 1 >= n || line.charAt(eq + 1) != '"') {
          return null;
        }
        String key = line.substring(i, eq).strip();
        StringBuilder value = new StringBuilder();
        int j = eq + 2;
        boolean closed = false;
        while (j < n) {
          char c = line.charAt(j);
          if (c == '\\' && j + 1 < n) {
            char e = line.charAt(j + 1);
            value.append(e == 'n' ? '\n' : e);
            j += 2;
          } else if (c == '"') {
            closed = true;
            j++;
            break;
          } else {
            value.append(c);
            j++;
          }
        }
        if (!closed) {
          return null;
        }
        labels.put(key, value.toString());
        i = j;
      }
    }
    String rest = line.substring(i).strip();
    if (rest.isEmpty()) {
      return null;
    }
    String[] parts = rest.split("\\s+");
    Double v = number(parts[0]);
    return v == null ? null : new Sample(name, Map.copyOf(labels), v);
  }

  static Double number(String s) {
    switch (s) {
      case "NaN":
        return Double.NaN;
      case "+Inf":
      case "Inf":
        return Double.POSITIVE_INFINITY;
      case "-Inf":
        return Double.NEGATIVE_INFINITY;
      default:
        try {
          return Double.parseDouble(s);
        } catch (NumberFormatException e) {
          return null;
        }
    }
  }
}
