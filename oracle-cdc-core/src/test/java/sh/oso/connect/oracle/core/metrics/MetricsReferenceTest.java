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
package sh.oso.connect.oracle.core.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * Generates website/docs/reference/metrics.md and ops/jmx-exporter/oracle-cdc.yml from {@link
 * TaskMetricsMXBean} and checks that the dashboard and alert rules use only metrics that exist
 * (ADR-0013). Rewrite with {@code -Dmetricsdocs.update=true}.
 */
class MetricsReferenceTest {

  static final Path ROOT =
      Path.of(System.getProperty("repo.root", "..")).toAbsolutePath().normalize();

  record Metric(String attribute, String prometheus, Description d) {}

  static List<Metric> metrics() {
    List<Metric> out = new ArrayList<>();
    for (Method m : TaskMetricsMXBean.class.getMethods()) {
      Description d = m.getAnnotation(Description.class);
      assertThat(d).as("@Description on %s", m.getName()).isNotNull();
      String attr = m.getName().replaceFirst("^(get|is)", "");
      String snake =
          attr.replaceAll("([a-z0-9])([A-Z])", "$1_$2").toLowerCase(java.util.Locale.ROOT);
      String name = "oracle_cdc_" + snake + (d.kind() == Description.Kind.COUNTER ? "_total" : "");
      out.add(new Metric(attr, name, d));
    }
    out.sort(Comparator.comparing(Metric::attribute));
    return out;
  }

  static boolean numeric(Metric m) {
    return !m.attribute().equals("LargestTransactions");
  }

  @Test
  void referencePageAndExporterRulesAreGeneratedFromTheMxBean() throws IOException {
    StringBuilder page = new StringBuilder();
    page.append("---\ntitle: Metrics\n")
        .append("description: JMX attributes of each capture task and their Prometheus names.\n")
        .append("---\n\n# Metrics\n\n")
        .append(
            "Generated from `TaskMetricsMXBean` by `MetricsReferenceTest`; do not edit by"
                + " hand.\n\n")
        .append(
            "Each capture task registers one MXBean, `sh.oso.cdc:type=task,server=<prefix>`, where")
        .append(" the prefix is `cdc.topic.prefix`. The JMX Prometheus exporter configuration in")
        .append(
            " `ops/jmx-exporter/oracle-cdc.yml` publishes every numeric attribute under the name")
        .append(
            " below with a `server` label. A Grafana dashboard"
                + " (`ops/grafana/oracle-cdc-connector.json`)")
        .append(
            " and Prometheus alert rules (`ops/alerts/prometheus-rules.yaml`) use these names.\n\n")
        .append("| Attribute | Prometheus name | Kind | Description |\n|---|---|---|---|\n");
    StringBuilder rules = new StringBuilder();
    rules
        .append(
            "# Generated from TaskMetricsMXBean by MetricsReferenceTest; do not edit by hand.\n")
        .append("# JMX Prometheus exporter rules for the OSO CDC Connector for Oracle Database.\n")
        .append("lowercaseOutputName: false\n")
        .append("lowercaseOutputLabelNames: true\n")
        .append("rules:\n");
    for (Metric m : metrics()) {
      boolean num = numeric(m);
      page.append("| `")
          .append(m.attribute())
          .append("` | ")
          .append(num ? "`" + m.prometheus() + "`" : "not exported (composite)")
          .append(" | ")
          .append(num ? m.d().kind().name().toLowerCase(java.util.Locale.ROOT) : "table")
          .append(" | ")
          .append(m.d().value())
          .append(" |\n");
      if (num) {
        rules
            .append("  - pattern: 'sh.oso.cdc<type=task, server=(.+)><>")
            .append(m.attribute())
            .append("'\n    name: ")
            .append(m.prometheus())
            .append("\n    type: ")
            .append(m.d().kind() == Description.Kind.COUNTER ? "COUNTER" : "GAUGE")
            .append("\n    help: \"")
            .append(m.d().value().replace("\"", "'"))
            .append("\"\n    labels:\n      server: \"$1\"\n");
      }
    }
    check(ROOT.resolve("website/docs/reference/metrics.md"), page.toString());
    check(ROOT.resolve("ops/jmx-exporter/oracle-cdc.yml"), rules.toString());
  }

  @Test
  void theDashboardAndAlertRulesUseOnlyExportedMetrics() throws IOException {
    Set<String> exported = new TreeSet<>();
    for (Metric m : metrics()) {
      if (numeric(m)) {
        exported.add(m.prometheus());
      }
    }
    for (String file :
        List.of("ops/grafana/oracle-cdc-connector.json", "ops/alerts/prometheus-rules.yaml")) {
      String text = Files.readString(ROOT.resolve(file), StandardCharsets.UTF_8);
      Matcher mm = Pattern.compile("oracle_cdc_[a-z0-9_]+").matcher(text);
      Set<String> used = new TreeSet<>();
      while (mm.find()) {
        used.add(mm.group());
      }
      assertThat(used).as("metrics used in %s", file).isNotEmpty();
      assertThat(exported).as("every metric %s uses is exported", file).containsAll(used);
    }
  }

  private static void check(Path file, String content) throws IOException {
    if (Boolean.getBoolean("metricsdocs.update")) {
      Files.createDirectories(file.getParent());
      Files.writeString(file, content, StandardCharsets.UTF_8);
      return;
    }
    assertThat(file).as("%s (run with -Dmetricsdocs.update=true to create it)", file).exists();
    assertThat(Files.readString(file, StandardCharsets.UTF_8))
        .as("%s is out of date; run with -Dmetricsdocs.update=true", file)
        .isEqualTo(content);
  }
}
