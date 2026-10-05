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
package sh.oso.connect.oracle.signals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;

/**
 * SRC-SIG-1: one command, {@code {"id": "...", "type": "snapshot", "data": {"tables": [...],
 * "predicate": "..."}}}.
 */
public record Signal(String id, String type, List<String> tables, String predicate) {

  private static final ObjectMapper JSON = new ObjectMapper();

  /** Parses a signal; anything that is not a JSON object with a type is an error. */
  public static Signal parse(String value) {
    JsonNode n;
    try {
      n = value == null ? null : JSON.readTree(value);
    } catch (java.io.IOException e) {
      throw new IllegalArgumentException("not JSON: " + e.getMessage(), e);
    }
    if (n == null || !n.isObject() || !n.path("type").isTextual()) {
      throw new IllegalArgumentException("a signal is a JSON object with a \"type\"");
    }
    List<String> tables = new ArrayList<>();
    for (JsonNode t : n.path("data").path("tables")) {
      tables.add(t.asText());
    }
    JsonNode predicate = n.path("data").path("predicate");
    return new Signal(
        n.path("id").isMissingNode() ? null : n.path("id").asText(),
        n.path("type").asText(),
        tables,
        predicate.isTextual() && !predicate.asText().isBlank() ? predicate.asText() : null);
  }
}
