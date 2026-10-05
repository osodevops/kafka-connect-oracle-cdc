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
package sh.oso.connect.oracle.core.doctor;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The database setup page shows the script {@code oracle-cdc-doctor setup-sql} writes for a
 * container database with the production profile. This test generates that block from {@link
 * SetupSql} and fails when the page differs; rewrite it with {@code -Dsetupsqldocs.update=true}.
 */
class SetupSqlDocsTest {

  static final Path PAGE =
      Path.of(System.getProperty("repo.root", ".."))
          .toAbsolutePath()
          .normalize()
          .resolve("website/docs/database-setup/index.md");
  static final String BEGIN =
      "<!-- BEGIN GENERATED: setup-sql production profile (SetupSqlDocsTest; do not edit by"
          + " hand) -->";
  static final String END = "<!-- END GENERATED: setup-sql production profile -->";

  @Test
  void setupPageShowsTheGeneratedProductionScript() throws IOException {
    String sql =
        SetupSql.generate(
            "c##cdc",
            "<change-me>",
            true,
            SetupSql.Profile.PRODUCTION,
            SetupSql.Platform.ONPREM,
            List.of());
    String expected = "\n\n```sql\n" + sql + "```\n\n";
    String text = Files.readString(PAGE, StandardCharsets.UTF_8);
    int begin = text.indexOf(BEGIN);
    int end = text.indexOf(END);
    assertThat(begin).as("%s must hold the line %s", PAGE, BEGIN).isNotNegative();
    assertThat(end)
        .as("%s must hold the line %s after the begin marker", PAGE, END)
        .isGreaterThan(begin);
    if (Boolean.getBoolean("setupsqldocs.update")) {
      Files.writeString(
          PAGE,
          text.substring(0, begin + BEGIN.length()) + expected + text.substring(end),
          StandardCharsets.UTF_8);
      return;
    }
    assertThat(text.substring(begin + BEGIN.length(), end))
        .as("the setup script in %s is out of date; run with -Dsetupsqldocs.update=true", PAGE)
        .isEqualTo(expected);
  }
}
