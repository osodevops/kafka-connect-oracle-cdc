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
package sh.oso.connect.oracle.ops;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Generates the event table of website/docs/reference/ops-topic.md from {@link OpsEvent.Type} and
 * fails when the committed table differs; the rest of the page is hand-written. Rewrite the table
 * with {@code -Dopsdocs.update=true}. Also checks that the types documented as emitted are emitted
 * by some code path and that reserved types are not.
 */
class OpsTopicReferenceTest {

  static final Path ROOT =
      Path.of(System.getProperty("repo.root", "..")).toAbsolutePath().normalize();
  static final Path PAGE = ROOT.resolve("website/docs/reference/ops-topic.md");
  static final String BEGIN =
      "<!-- BEGIN GENERATED: ops event types (OpsTopicReferenceTest; do not edit by hand) -->";
  static final String END = "<!-- END GENERATED: ops event types -->";

  /** Main source trees that may write ops events. */
  static final List<String> SOURCES =
      List.of(
          "oracle-cdc-core/src/main/java",
          "kafka-connect-oracle-cdc/src/main/java",
          "oracle-cdc-doctor/src/main/java");

  private static final Pattern KEY = Pattern.compile("cdc\\.[a-z0-9_.]*[a-z0-9](=[a-z_]+)?");

  @Test
  void documentedTypesAreEmittedAndReservedTypesAreNot() throws IOException {
    String code = mainSources();
    List<String> wrong = new ArrayList<>();
    for (OpsEvent.Type t : OpsEvent.Type.values()) {
      boolean emitted =
          Pattern.compile("\\bType\\." + t.name() + "\\b").matcher(code).find()
              || doctorSources().contains("\"" + t.wire() + "\"");
      if (emitted && t.reserved()) {
        wrong.add(t.wire() + " is emitted but reserved; give it a description and details");
      }
      if (!emitted && !t.reserved()) {
        wrong.add(t.wire() + " is described but no code path emits it; make it reserved");
      }
      if (!t.reserved()) {
        assertThat(t.description()).as("description of %s", t).endsWith(".");
        assertThat(t.details()).as("details of %s", t).isNotEmpty();
      }
    }
    assertThat(wrong).isEmpty();
  }

  @Test
  void eventTableIsGeneratedFromTheTypes() throws IOException {
    String text = Files.readString(PAGE, StandardCharsets.UTF_8);
    int begin = text.indexOf(BEGIN);
    int end = text.indexOf(END);
    assertThat(begin).as("%s must hold the line %s", PAGE, BEGIN).isNotNegative();
    assertThat(end)
        .as("%s must hold the line %s after the begin marker", PAGE, END)
        .isGreaterThan(begin);
    String expected = "\n\n" + render() + "\n";
    String current = text.substring(begin + BEGIN.length(), end);
    if (Boolean.getBoolean("opsdocs.update")) {
      Files.writeString(
          PAGE,
          text.substring(0, begin + BEGIN.length()) + expected + text.substring(end),
          StandardCharsets.UTF_8);
      return;
    }
    assertThat(current)
        .as("the event table in %s is out of date; run with -Dopsdocs.update=true", PAGE)
        .isEqualTo(expected);
  }

  static String render() {
    StringBuilder sb = new StringBuilder("| Type | When | Details |\n|---|---|---|\n");
    List<String> reserved = new ArrayList<>();
    for (OpsEvent.Type t : OpsEvent.Type.values()) {
      if (t.reserved()) {
        reserved.add("`" + t.wire() + "`");
        continue;
      }
      List<String> details = new ArrayList<>();
      for (String d : t.details()) {
        int space = d.indexOf(' ');
        details.add(
            space < 0
                ? "`" + d + "`"
                : "`" + d.substring(0, space) + "`" + prose(d.substring(space)));
      }
      sb.append("| `")
          .append(t.wire())
          .append("` | ")
          .append(prose(t.description()))
          .append(" | ")
          .append(String.join(", ", details))
          .append(" |\n");
    }
    if (!reserved.isEmpty()) {
      sb.append("\nThe following types are reserved for features that are not built yet. No code")
          .append(" path writes them, so they do not appear on the topic: ")
          .append(String.join(", ", reserved))
          .append(".\n");
    }
    return sb.toString();
  }

  /** Puts configuration keys in code spans and escapes what MDX would read as markup. */
  static String prose(String text) {
    String escaped =
        text.replace("|", "\\|")
            .replace("{", "\\{")
            .replace("}", "\\}")
            .replace("<", "&lt;")
            .replace(">", "&gt;");
    Matcher m = KEY.matcher(escaped);
    StringBuilder out = new StringBuilder();
    while (m.find()) {
      m.appendReplacement(out, Matcher.quoteReplacement("`" + m.group() + "`"));
    }
    m.appendTail(out);
    return out.toString();
  }

  private static String mainSources() throws IOException {
    StringBuilder sb = new StringBuilder();
    for (String dir : SOURCES) {
      sb.append(read(ROOT.resolve(dir), true));
    }
    return sb.toString();
  }

  private static String doctorSources() throws IOException {
    return read(ROOT.resolve("oracle-cdc-doctor/src/main/java"), false);
  }

  private static String read(Path dir, boolean skipOpsEvent) throws IOException {
    if (!Files.isDirectory(dir)) {
      return "";
    }
    StringBuilder sb = new StringBuilder();
    try (Stream<Path> files = Files.walk(dir)) {
      for (Path f : files.filter(p -> p.toString().endsWith(".java")).sorted().toList()) {
        if (skipOpsEvent && f.endsWith(Path.of("ops", "OpsEvent.java"))) {
          continue; // the declarations themselves
        }
        sb.append(Files.readString(f, StandardCharsets.UTF_8)).append('\n');
      }
    }
    return sb.toString();
  }
}
