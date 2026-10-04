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
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.junit.jupiter.api.Test;

/**
 * Proves the Connect plugin ZIP built by the connector module follows the Confluent Hub component
 * layout, bundles the Oracle JDBC driver with its licence, and excludes jars the worker provides.
 * Runs under surefire in this module because the reactor packages the connector module first.
 */
class PluginZipLayoutTest {

  @Test
  void pluginZipHasHubLayout() throws IOException {
    String version = System.getProperty("project.version", "0.1.0-SNAPSHOT");
    Path repoRoot = Path.of(System.getProperty("repo.root", "..")).toAbsolutePath().normalize();
    Path zip =
        repoRoot.resolve(
            "kafka-connect-oracle-cdc/target/kafka-connect-oracle-cdc-"
                + version
                + "-kafka-connect-plugin.zip");
    assertThat(zip).as("plugin ZIP (run mvn package first)").exists();

    String base = "osodevops-kafka-connect-oracle-cdc-" + version + "/";
    List<String> names = new ArrayList<>();
    String manifest;
    try (ZipFile zf = new ZipFile(zip.toFile())) {
      Enumeration<? extends ZipEntry> en = zf.entries();
      while (en.hasMoreElements()) {
        names.add(en.nextElement().getName());
      }
      ZipEntry m = zf.getEntry(base + "manifest.json");
      assertThat(m).as("manifest.json at the component root").isNotNull();
      manifest = new String(zf.getInputStream(m).readAllBytes(), StandardCharsets.UTF_8);
    }

    assertThat(names).allMatch(n -> n.startsWith(base));
    assertThat(names).anyMatch(n -> n.startsWith(base + "lib/kafka-connect-oracle-cdc-" + version));
    assertThat(names).anyMatch(n -> n.startsWith(base + "lib/oracle-cdc-core-" + version));
    assertThat(names).anyMatch(n -> n.matches(base + "lib/ojdbc11-.*\\.jar"));
    assertThat(names)
        .contains(base + "doc/LICENSE", base + "doc/NOTICE", base + "doc/licenses/ojdbc-FUTC.txt");
    assertThat(names).contains(base + "etc/connector-example.json");
    assertThat(names)
        .as("jars the Connect worker already provides must not be bundled")
        .noneMatch(n -> n.contains("/lib/connect-api-"))
        .noneMatch(n -> n.contains("/lib/kafka-clients-"))
        .noneMatch(n -> n.contains("/lib/slf4j-api-"));

    assertThat(manifest)
        .contains("\"name\": \"kafka-connect-oracle-cdc\"")
        .contains("\"version\": \"" + version + "\"")
        .contains("\"exactly_once\"")
        .doesNotContain("${");
    assertThat(Files.size(zip)).isGreaterThan(1_000_000L);
  }
}
