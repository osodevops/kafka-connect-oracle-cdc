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
package sh.oso.connect.oracle.core.errors;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;

/**
 * Every {@link ErrorCode} slug has a runbook under website/docs/operations/runbooks/, served at the
 * URL the error message prints, with real content in the four sections an operator needs; and no
 * runbook exists for a code that does not.
 */
class RunbookPresenceTest {

  static final Path RUNBOOKS =
      Path.of(System.getProperty("repo.root", ".."))
          .toAbsolutePath()
          .normalize()
          .resolve("website/docs/operations/runbooks");

  static final List<String> SECTIONS =
      List.of(
          "## What the connector observed",
          "## Why it stopped",
          "## Confirm the cause",
          "## Recover");

  /** Wording the placeholder pages carried before the runbooks were written. */
  static final List<String> PLACEHOLDERS =
      List.of(
          "to be written",
          "placeholder until",
          "exact recovery steps, including",
          "commands that confirm the condition from the database");

  @Test
  void everyErrorCodeHasARunbookWithRealContent() throws IOException {
    List<String> problems = new ArrayList<>();
    for (ErrorCode c : ErrorCode.values()) {
      Path page = RUNBOOKS.resolve(c.runbookSlug() + ".md");
      if (!Files.exists(page)) {
        problems.add(c.code() + ": no page " + page);
        continue;
      }
      String text = Files.readString(page, StandardCharsets.UTF_8);
      if (!text.contains("\nslug: /runbooks/" + c.runbookSlug() + "\n")) {
        problems.add(c.code() + ": front matter lacks slug: /runbooks/" + c.runbookSlug());
      }
      if (!text.contains("title: \"" + c.code() + " ")) {
        problems.add(c.code() + ": title does not start with the code");
      }
      if (!text.contains(c.name())) {
        problems.add(c.code() + ": page does not name " + c.name());
      }
      String lower = text.toLowerCase(Locale.ROOT);
      for (String p : PLACEHOLDERS) {
        if (lower.contains(p)) {
          problems.add(c.code() + ": placeholder text \"" + p + "\"");
        }
      }
      int previous = -1;
      for (int i = 0; i < SECTIONS.size(); i++) {
        int at = text.indexOf("\n" + SECTIONS.get(i));
        if (at < 0 || at < previous) {
          problems.add(c.code() + ": missing or misplaced section " + SECTIONS.get(i));
          continue;
        }
        int next = i + 1 < SECTIONS.size() ? text.indexOf("\n" + SECTIONS.get(i + 1), at + 1) : -1;
        String body =
            text.substring(text.indexOf('\n', at + 1) + 1, next < 0 ? text.length() : next).trim();
        if (body.length() < 80) {
          problems.add(c.code() + ": section " + SECTIONS.get(i) + " is too short to help");
        }
        previous = at;
      }
    }
    assertThat(problems).as("runbook problems").isEmpty();
  }

  @Test
  void everyRunbookBelongsToAnErrorCode() throws IOException {
    Set<String> slugs = new TreeSet<>();
    for (ErrorCode c : ErrorCode.values()) {
      slugs.add(c.runbookSlug() + ".md");
    }
    slugs.add("index.md");
    Set<String> pages = new TreeSet<>();
    try (Stream<Path> s = Files.list(RUNBOOKS)) {
      s.map(p -> p.getFileName().toString()).filter(n -> n.endsWith(".md")).forEach(pages::add);
    }
    assertThat(slugs).as("runbook pages in %s", RUNBOOKS).containsAll(pages);
  }
}
