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
package sh.oso.connect.oracle.e2e.regression;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * ADR-0011: keeps {@code docs/testing_strategy.md} section 5 and the regression corpus in step.
 * Every row that is not pending names suites that exist and carry the row's tags; a pending row
 * says why; every {@code dbz-} tag in the test sources is in the table; and every suite in this
 * package carries a tag from the table, a tier suffix and, for an IT, its tier tag. Runs under
 * surefire without Docker.
 */
class RegressionCorpusIndexTest {

  static final Path ROOT =
      Path.of(System.getProperty("repo.root", "..")).toAbsolutePath().normalize();
  static final Path STRATEGY = ROOT.resolve("docs/testing_strategy.md");
  static final Path REGRESSION =
      ROOT.resolve("e2e-tests/src/test/java/sh/oso/connect/oracle/e2e/regression");
  static final List<Path> SOURCES =
      List.of(
          ROOT.resolve("e2e-tests/src/test/java"),
          ROOT.resolve("oracle-cdc-core/src/test/java"),
          ROOT.resolve("kafka-connect-oracle-cdc/src/test/java"),
          ROOT.resolve("oracle-cdc-doctor/src/test/java"),
          ROOT.resolve("bench/src/test/java"));

  static final Pattern TAG =
      Pattern.compile("@(?:org\\.junit\\.jupiter\\.api\\.)?Tag\\(\"([^\"]+)\"\\)");
  static final Pattern CODE = Pattern.compile("`([^`]+)`");

  /** One row of the corpus table. */
  record Row(String issue, List<String> tags, List<String> suites, String tier, String status) {
    boolean pending() {
      return status.startsWith("pending");
    }
  }

  static List<Row> rows() throws IOException {
    String doc = Files.readString(STRATEGY, StandardCharsets.UTF_8);
    int from = doc.indexOf("## 5. Debezium regression corpus");
    int to = doc.indexOf("\n## 6.", from);
    assertThat(from).as("section 5 of %s", STRATEGY).isNotNegative();
    List<Row> out = new ArrayList<>();
    for (String line : doc.substring(from, to).split("\n")) {
      if (!line.startsWith("| ") || line.startsWith("| Issue") || line.startsWith("|---")) {
        continue;
      }
      String[] cells = line.substring(1, line.length() - 1).split("\\|", -1);
      assertThat(cells).as("six cells in %s", line).hasSize(6);
      out.add(
          new Row(
              cells[0].trim(), codes(cells[1]), codes(cells[3]), cells[4].trim(), cells[5].trim()));
    }
    return out;
  }

  private static List<String> codes(String cell) {
    List<String> out = new ArrayList<>();
    Matcher m = CODE.matcher(cell);
    while (m.find()) {
      out.add(m.group(1));
    }
    return out;
  }

  /** Simple class name (the file name) to the tags written anywhere in the file. */
  static Map<String, Set<String>> tagsByClass(List<Path> roots) throws IOException {
    Map<String, Set<String>> out = new TreeMap<>();
    for (Path root : roots) {
      if (!Files.isDirectory(root)) {
        continue;
      }
      try (Stream<Path> files = Files.walk(root)) {
        for (Path f : files.filter(p -> p.toString().endsWith(".java")).toList()) {
          String name = f.getFileName().toString().replace(".java", "");
          Set<String> tags = out.computeIfAbsent(name, k -> new TreeSet<>());
          Matcher m = TAG.matcher(Files.readString(f, StandardCharsets.UTF_8));
          while (m.find()) {
            tags.add(m.group(1));
          }
        }
      }
    }
    return out;
  }

  @Test
  void everyRowHasItsSuitesAndEveryPendingRowAReason() throws IOException {
    List<Row> rows = rows();
    assertThat(rows).as("the corpus table").hasSizeGreaterThanOrEqualTo(10);
    Map<String, Set<String>> code = tagsByClass(SOURCES);
    for (Row row : rows) {
      assertThat(row.tags()).as("tags of %s", row.issue()).isNotEmpty();
      if (row.pending()) {
        assertThat(row.status().replaceFirst("^pending:?", "").trim())
            .as("the reason %s is pending", row.issue())
            .hasSizeGreaterThan(10);
        continue;
      }
      assertThat(row.suites()).as("suites of %s", row.issue()).isNotEmpty();
      for (String suite : row.suites()) {
        assertThat(code).as("suite %s of %s", suite, row.issue()).containsKey(suite);
        assertThat(code.get(suite))
            .as("tags of %s, named for %s", suite, row.issue())
            .containsAll(row.tags());
      }
    }
  }

  @Test
  void everyDebeziumTagInTheCodeIsInTheTable() throws IOException {
    Set<String> listed = new TreeSet<>();
    rows().forEach(r -> listed.addAll(r.tags()));
    Map<String, Set<String>> code = tagsByClass(SOURCES);
    for (Map.Entry<String, Set<String>> e : code.entrySet()) {
      for (String tag : e.getValue()) {
        if (tag.startsWith("dbz-")) {
          assertThat(listed).as("%s on %s is in section 5", tag, e.getKey()).contains(tag);
        }
      }
    }
  }

  @Test
  void everySuiteInTheRegressionPackageIsPartOfTheCorpus() throws IOException {
    Set<String> listed = new TreeSet<>();
    rows().forEach(r -> listed.addAll(r.tags()));
    Map<String, Set<String>> here = tagsByClass(List.of(REGRESSION));
    int suites = 0;
    for (Map.Entry<String, Set<String>> e : here.entrySet()) {
      String name = e.getKey();
      if (name.equals(getClass().getSimpleName())
          || !(name.endsWith("Test") || name.endsWith("IT"))) {
        continue; // this index and support classes
      }
      suites++;
      assertThat(name)
          .as("tier suffix of %s", name)
          .matches(".*(Test|EngineIT|ConnectorIT|NightlyIT)");
      assertThat(e.getValue()).as("a corpus tag on %s", name).containsAnyElementsOf(listed);
      if (name.endsWith("EngineIT")) {
        assertThat(e.getValue()).as("tier tag of %s", name).contains("engine");
      } else if (name.endsWith("ConnectorIT")) {
        assertThat(e.getValue()).as("tier tag of %s", name).contains("connector");
      } else if (name.endsWith("NightlyIT")) {
        assertThat(e.getValue()).as("tier tag of %s", name).contains("nightly");
      }
    }
    assertThat(suites).isGreaterThanOrEqualTo(10);
  }
}
