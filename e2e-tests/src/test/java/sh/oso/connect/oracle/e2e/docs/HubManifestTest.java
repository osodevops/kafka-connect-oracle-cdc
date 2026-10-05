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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Proves the {@code manifest.json} inside the Connect plugin ZIP is a valid Confluent Hub component
 * manifest for this build: every required field present and well formed, the version of this build,
 * the archive directory named owner, name and version, and the Oracle trademark and non-affiliation
 * notice in the listing text (CLAUDE.md). Runs under surefire in this module after the reactor has
 * packaged the connector module, so it needs no Docker; the release workflow runs it on the tagged
 * commit before anything is published.
 */
class HubManifestTest {

  static final String OWNER = "osodevops";
  static final String NAME = "kafka-connect-oracle-cdc";
  static final String TITLE = "OSO CDC Connector for Oracle Database";
  static final String TRADEMARK_NOTICE =
      "Oracle and Java are registered trademarks of Oracle and/or its affiliates.";
  static final String AFFILIATION_NOTICE =
      "This project is not affiliated with or endorsed by Oracle.";

  static final Set<String> COMPONENT_TYPES = Set.of("source", "sink", "transform", "converter");
  static final Set<String> DELIVERY_GUARANTEES = Set.of("at_least_once", "exactly_once");

  static String version;
  static String manifestText;
  static JsonNode manifest;
  static Set<String> entries;

  @BeforeAll
  static void readManifestFromPluginZip() throws IOException {
    version = System.getProperty("project.version", "0.1.0-SNAPSHOT");
    Path repoRoot = Path.of(System.getProperty("repo.root", "..")).toAbsolutePath().normalize();
    Path zip =
        repoRoot.resolve(
            "kafka-connect-oracle-cdc/target/"
                + NAME
                + "-"
                + version
                + "-kafka-connect-plugin.zip");
    assertThat(zip).as("plugin ZIP (run mvn package first)").exists();

    entries = new HashSet<>();
    List<ZipEntry> manifests = new ArrayList<>();
    try (ZipFile zf = new ZipFile(zip.toFile())) {
      Enumeration<? extends ZipEntry> en = zf.entries();
      while (en.hasMoreElements()) {
        ZipEntry e = en.nextElement();
        entries.add(e.getName());
        if (e.getName().matches("[^/]+/manifest\\.json")) {
          manifests.add(e);
        }
      }
      assertThat(manifests).as("exactly one manifest.json at the component root").hasSize(1);
      manifestText =
          new String(zf.getInputStream(manifests.get(0)).readAllBytes(), StandardCharsets.UTF_8);
    }
    manifest = new ObjectMapper().readTree(manifestText);
  }

  @Test
  void requiredHubFieldsArePresentAndWellFormed() {
    assertThat(text("name")).isEqualTo(NAME).matches("[a-z0-9_-]+");
    assertThat(text("version")).isEqualTo(version).matches("[A-Za-z0-9._-]+");
    assertThat(text("title")).isEqualTo(TITLE);
    assertThat(text("description")).isNotBlank();

    JsonNode owner = manifest.path("owner");
    assertThat(owner.path("username").asText())
        .as("owner.username: two to 255 lowercase letters, digits or underscores")
        .isEqualTo(OWNER)
        .matches("[a-z0-9_]{2,255}");
    assertThat(owner.path("type").asText()).isIn("organization", "user");
    assertThat(owner.path("name").asText()).isNotBlank();
    assertThat(owner.path("url").asText()).startsWith("https://");

    List<String> types = strings(manifest.path("component_types"));
    assertThat(types).containsExactly("source");
    assertThat(COMPONENT_TYPES).containsAll(types);

    assertThat(manifest.has("confluent_verified"))
        .as("confluent_verified is set by Confluent, never by the submitter")
        .isFalse();
    assertThat(manifestText).as("every Maven placeholder resolved").doesNotContain("${");
  }

  @Test
  void archiveDirectoryIsOwnerNameAndVersion() {
    String base = OWNER + "-" + NAME + "-" + version + "/";
    assertThat(entries).allMatch(n -> n.startsWith(base));
    assertThat(entries).contains(base + "manifest.json");
  }

  @Test
  void licenceSupportAndLinksPointAtThisProject() {
    JsonNode licences = manifest.path("license");
    assertThat(licences.isArray() && licences.size() > 0).as("at least one licence").isTrue();
    JsonNode apache = licences.get(0);
    assertThat(apache.path("name").asText()).isEqualTo("Apache License 2.0");
    assertThat(apache.path("url").asText())
        .isEqualTo("https://www.apache.org/licenses/LICENSE-2.0");

    JsonNode support = manifest.path("support");
    assertThat(support.path("provider_name").asText()).isEqualTo("OSO");
    assertThat(support.path("url").asText())
        .isEqualTo("https://github.com/osodevops/kafka-connect-oracle-cdc/blob/main/SUPPORT.md");
    assertThat(support.path("summary").asText().toLowerCase(Locale.ROOT))
        .as("support claims stay within SUPPORT.md (no round-the-clock desk)")
        .isNotBlank()
        .doesNotContain("24x7")
        .doesNotContain("24/7");

    assertThat(text("documentation_url")).isEqualTo("https://kafkacdcconnector.com");
    assertThat(text("source_url"))
        .isEqualTo("https://github.com/osodevops/kafka-connect-oracle-cdc");
    assertThat(strings(manifest.path("tags"))).isNotEmpty();
    assertThat(strings(manifest.path("requirements"))).isNotEmpty();

    List<String> delivery = strings(manifest.path("features").path("delivery_guarantee"));
    assertThat(delivery).isNotEmpty();
    assertThat(DELIVERY_GUARANTEES).containsAll(delivery);
  }

  @Test
  void listingCarriesTheTrademarkAndNonAffiliationNotice() {
    assertThat(text("description")).contains(TRADEMARK_NOTICE).contains(AFFILIATION_NOTICE);
  }

  @Test
  void productNameNeverUsesOracleExceptAsForOracleDatabase() {
    String title = text("title");
    assertThat(title).endsWith(" for Oracle Database");
    String product = title.substring(0, title.length() - " for Oracle Database".length());
    assertThat(product.toLowerCase(Locale.ROOT)).doesNotContain("oracle");
    assertThat(manifest.path("owner").path("name").asText().toLowerCase(Locale.ROOT))
        .doesNotContain("oracle");
    assertThat(manifest.path("support").path("provider_name").asText().toLowerCase(Locale.ROOT))
        .doesNotContain("oracle");
  }

  @Test
  void everyLogoIsAFileInTheAssetsDirectory() {
    String base = OWNER + "-" + NAME + "-" + version + "/";
    List<String> logos = new ArrayList<>();
    collectLogos(manifest, logos);
    for (String logo : logos) {
      assertThat(logo).as("Hub logos live directly in assets/").matches("assets/[^/]+");
      assertThat(entries).contains(base + logo);
    }
  }

  private static void collectLogos(JsonNode node, List<String> out) {
    if (node.isObject()) {
      for (Map.Entry<String, JsonNode> e : node.properties()) {
        if (e.getKey().equals("logo") && e.getValue().isTextual()) {
          out.add(e.getValue().asText());
        } else {
          collectLogos(e.getValue(), out);
        }
      }
    } else if (node.isArray()) {
      node.forEach(n -> collectLogos(n, out));
    }
  }

  private static String text(String field) {
    JsonNode n = manifest.path(field);
    assertThat(n.isTextual()).as(field + " is a string").isTrue();
    return n.asText();
  }

  private static List<String> strings(JsonNode array) {
    assertThat(array.isArray()).as("expected a JSON array").isTrue();
    List<String> out = new ArrayList<>();
    array.forEach(
        n -> {
          assertThat(n.isTextual()).isTrue();
          out.add(n.asText());
        });
    return out;
  }
}
