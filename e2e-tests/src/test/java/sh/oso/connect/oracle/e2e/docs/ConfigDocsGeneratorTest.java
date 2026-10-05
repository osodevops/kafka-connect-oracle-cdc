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
package sh.oso.connect.oracle.e2e.docs;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfig;

/**
 * Generates the configuration reference from the connector's ConfigDef, one page per group, and
 * fails when the committed pages differ. Run with {@code -Dconfigdocs.update=true} to rewrite them.
 */
class ConfigDocsGeneratorTest {

  static final Path DOCS =
      Path.of(System.getProperty("repo.root", ".."))
          .toAbsolutePath()
          .normalize()
          .resolve("website/docs/reference/configuration");

  @Test
  void generatedConfigurationReferenceIsUpToDate() throws IOException {
    Map<String, String> pages = render(OracleCdcSourceConnectorConfig.configDef());
    boolean update = Boolean.getBoolean("configdocs.update");
    List<String> stale = new ArrayList<>();
    for (Map.Entry<String, String> e : pages.entrySet()) {
      Path file = DOCS.resolve(e.getKey());
      if (update) {
        Files.createDirectories(DOCS);
        Files.writeString(file, e.getValue(), StandardCharsets.UTF_8);
      } else if (!Files.exists(file) || !Files.readString(file).equals(e.getValue())) {
        stale.add(e.getKey());
      }
    }
    // the index lists every group page between markers, so a new group cannot be missed
    Path index = DOCS.resolve("index.md");
    String current = Files.readString(index, StandardCharsets.UTF_8);
    String begin = "<!-- BEGIN GENERATED: configuration groups";
    String end = "<!-- END GENERATED: configuration groups -->";
    int b = current.indexOf("\n", current.indexOf(begin)) + 1;
    int x = current.indexOf(end);
    StringBuilder groups = new StringBuilder();
    for (Map.Entry<String, String> e : pages.entrySet()) {
      String title = e.getValue().lines().skip(1).findFirst().orElse("title: \"\"");
      groups
          .append("- [")
          .append(title.substring(title.indexOf('"') + 1, title.lastIndexOf('"')))
          .append("](")
          .append(e.getKey())
          .append(")\n");
    }
    String wanted = current.substring(0, b) + groups + current.substring(x);
    if (update) {
      Files.writeString(index, wanted, StandardCharsets.UTF_8);
    } else if (!current.equals(wanted)) {
      stale.add("index.md");
    }
    assertThat(stale)
        .as(
            "configuration reference pages out of date; run mvn -pl e2e-tests test"
                + " -Dtest=ConfigDocsGeneratorTest -Dconfigdocs.update=true")
        .isEmpty();
  }

  static Map<String, String> render(ConfigDef def) {
    Map<String, List<ConfigDef.ConfigKey>> byGroup = new LinkedHashMap<>();
    for (ConfigDef.ConfigKey k : def.configKeys().values()) {
      byGroup.computeIfAbsent(k.group == null ? "Other" : k.group, g -> new ArrayList<>()).add(k);
    }
    Map<String, String> pages = new LinkedHashMap<>();
    int position = 1;
    for (Map.Entry<String, List<ConfigDef.ConfigKey>> e : byGroup.entrySet()) {
      List<ConfigDef.ConfigKey> keys = new ArrayList<>(e.getValue());
      keys.sort(
          (a, b) ->
              a.orderInGroup != b.orderInGroup
                  ? Integer.compare(a.orderInGroup, b.orderInGroup)
                  : a.name.compareTo(b.name));
      StringBuilder sb = new StringBuilder();
      sb.append("---\ntitle: \"").append(e.getKey()).append("\"\n");
      sb.append("description: \"")
          .append(e.getKey())
          .append(" properties of the OSO CDC Connector for Oracle Database.\"\n");
      sb.append("sidebar_position: ").append(++position).append("\n---\n\n");
      sb.append("# ").append(e.getKey()).append(" properties\n\n");
      sb.append(
          "Generated from the connector's `ConfigDef` by `ConfigDocsGeneratorTest`; do not edit by"
              + " hand.\n\n");
      sb.append(
          "| Property | Type | Default | Importance | Description |\n|---|---|---|---|---|\n");
      for (ConfigDef.ConfigKey k : keys) {
        sb.append("| `")
            .append(k.name)
            .append("` | ")
            .append(k.type.name().toLowerCase(Locale.ROOT))
            .append(" | ")
            .append(defaultOf(k))
            .append(" | ")
            .append(k.importance.name().toLowerCase(Locale.ROOT))
            .append(" | ")
            .append(k.documentation == null ? "" : mdx(k.documentation))
            .append(" |\n");
      }
      pages.put(slug(e.getKey()) + ".md", sb.toString());
    }
    return pages;
  }

  static String defaultOf(ConfigDef.ConfigKey k) {
    if (!k.hasDefault()) {
      return "required";
    }
    if (k.defaultValue == null) {
      return "none";
    }
    if (k.type == ConfigDef.Type.PASSWORD) {
      return "[hidden]";
    }
    String v = k.defaultValue.toString();
    if (v.isEmpty() || "[]".equals(v)) {
      return "empty";
    }
    return "`" + v + "`";
  }

  /** MDX reads braces as expressions and angle brackets as tags; escape them in prose. */
  static String mdx(String doc) {
    return doc.replace("|", "\\|")
        .replace("\n", " ")
        .replace("{", "\\{")
        .replace("}", "\\}")
        .replace("<", "&lt;")
        .replace(">", "&gt;");
  }

  static String slug(String group) {
    return group.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "-").replaceAll("^-|-$", "");
  }
}
