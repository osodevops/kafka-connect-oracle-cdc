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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/** Reads connector configurations and Kafka client property files given on the command line. */
public final class ConfigFiles {

  private ConfigFiles() {}

  /** A connector config file: either a bare {@code {"k": "v"}} map or a REST envelope. */
  public static Map<String, String> connector(Path file) throws IOException {
    JsonNode root = new ObjectMapper().readTree(Files.readString(file));
    if (root == null || !root.isObject()) {
      throw new IOException(file + " does not hold a JSON object");
    }
    JsonNode cfg = root.has("config") && root.get("config").isObject() ? root.get("config") : root;
    Map<String, String> out = new LinkedHashMap<>();
    for (Iterator<Map.Entry<String, JsonNode>> it = cfg.fields(); it.hasNext(); ) {
      Map.Entry<String, JsonNode> e = it.next();
      out.put(
          e.getKey(), e.getValue().isValueNode() ? e.getValue().asText() : e.getValue().toString());
    }
    return out;
  }

  /** A Kafka client properties file, as the Kafka command line tools take it. */
  public static Properties properties(Path file) throws IOException {
    Properties p = new Properties();
    try (InputStream in = Files.newInputStream(file)) {
      p.load(in);
    }
    return p;
  }
}
