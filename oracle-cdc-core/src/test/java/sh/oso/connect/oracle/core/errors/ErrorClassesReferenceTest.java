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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/**
 * Generates website/docs/reference/error-classes.md from {@link ErrorCode} and fails when the
 * committed page differs. Rewrite with {@code -Derrordocs.update=true}.
 */
class ErrorClassesReferenceTest {

  static final Path ROOT =
      Path.of(System.getProperty("repo.root", "..")).toAbsolutePath().normalize();
  static final Path PAGE = ROOT.resolve("website/docs/reference/error-classes.md");

  /** A configuration key, optionally with the value it is set to, is a genuine identifier. */
  private static final Pattern KEY = Pattern.compile("cdc\\.[a-z0-9_.]*[a-z0-9](=[a-z_]+)?");

  @Test
  void everyCodeHasADescriptionForOperators() {
    for (ErrorCode c : ErrorCode.values()) {
      assertThat(c.description()).as("description of %s", c).isNotBlank().endsWith(".");
      assertThat(c.description()).as("description of %s", c).doesNotContain("—", "|");
    }
  }

  @Test
  void errorClassesPageIsGeneratedFromTheErrorCodes() throws IOException {
    String page = render();
    if (Boolean.getBoolean("errordocs.update")) {
      Files.writeString(PAGE, page, StandardCharsets.UTF_8);
      return;
    }
    assertThat(PAGE).as("%s (run with -Derrordocs.update=true to create it)", PAGE).exists();
    assertThat(Files.readString(PAGE, StandardCharsets.UTF_8))
        .as("%s is out of date; run with -Derrordocs.update=true", PAGE)
        .isEqualTo(page);
  }

  static String render() {
    StringBuilder sb = new StringBuilder();
    sb.append("---\ntitle: Error classes\n")
        .append(
            "description: The CDC error codes, what each means, whether the engine retries and"
                + " where the runbook is.\n")
        .append("---\n\n# Error classes\n\n")
        .append(
            "Generated from `ErrorCode` by `ErrorClassesReferenceTest`; do not edit by hand.\n\n")
        .append(
            "Every condition that stops a capture task has a stable code, an operator action and a")
        .append(" runbook. Kafka Connect shows the task's error message in the task status in the")
        .append(" form `[CDC-2002] what happened Operator action: what to do Runbook: link`, and")
        .append(" the `stop` event on the [ops topic](ops-topic.md) carries the code, the action")
        .append(" and the link as separate fields. Codes are never reused or renumbered.\n\n")
        .append("Codes marked as retried are handled inside the task first: the engine reconnects")
        .append(" or mines the same range again, and the task stops with the code only when")
        .append(" retrying has not helped. Every other code stops the task at once. Nothing is")
        .append(" skipped in either case: once the cause is fixed and the task restarted, it")
        .append(" resumes from its last acknowledged position.\n\n")
        .append("| Code | Name | Retried | What the connector saw | Runbook |\n")
        .append("|---|---|---|---|---|\n");
    for (ErrorCode c : ErrorCode.values()) {
      sb.append("| ")
          .append(c.code())
          .append(" | `")
          .append(c.name())
          .append("` | ")
          .append(c.retriable() ? "yes" : "no")
          .append(" | ")
          .append(prose(c.description()))
          .append(" | [")
          .append(c.runbookSlug())
          .append("](../operations/runbooks/")
          .append(c.runbookSlug())
          .append(".md) |\n");
    }
    return sb.toString();
  }

  /** Puts configuration keys in code spans and escapes what MDX would read as markup. */
  static String prose(String text) {
    String escaped =
        text.replace("{", "\\{").replace("}", "\\}").replace("<", "&lt;").replace(">", "&gt;");
    Matcher m = KEY.matcher(escaped);
    StringBuilder out = new StringBuilder();
    while (m.find()) {
      m.appendReplacement(out, Matcher.quoteReplacement("`" + m.group() + "`"));
    }
    m.appendTail(out);
    return out.toString();
  }
}
