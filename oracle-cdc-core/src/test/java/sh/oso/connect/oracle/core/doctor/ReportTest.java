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

import java.util.List;
import org.junit.jupiter.api.Test;

class ReportTest {

  @Test
  void junitHasOneCasePerRuleAndFailsOnlyOnBlockingFindings() {
    Report r =
        new Report(
            List.of(
                Finding.blocking("DOC-3", "T <one> & \"two\"", "ALTER TABLE T ...;"),
                Finding.blocking("DOC-3", "second", null),
                Finding.warning("DOC-9", "too many switches\u0001", null),
                Finding.info("DOC-20", "lag recovery possible")),
            List.of("DOC-1", "DOC-3", "DOC-9", "DOC-20"));
    String xml = r.toJUnitXml();
    assertThat(xml)
        .startsWith("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
        .contains(
            "<testsuite name=\"oracle-cdc-doctor\" tests=\"4\" failures=\"1\" errors=\"0\""
                + " skipped=\"0\">")
        .contains("<testcase name=\"DOC-1\" classname=\"oracle-cdc-doctor\"/>")
        .contains("<failure message=\"T &lt;one&gt; &amp; &quot;two&quot;\" type=\"BLOCKING\">")
        .contains("second\n")
        .contains("ALTER TABLE T ...;")
        .contains("<system-out>WARNING: too many switches\n</system-out>")
        .contains("<system-out>INFO: lag recovery possible\n</system-out>")
        .doesNotContain("\u0001")
        .endsWith("</testsuite>\n");
    assertThat(r.exitCode()).isEqualTo(Report.EXIT_BLOCKING);
  }

  @Test
  void rulesDefaultToTheOnesWithFindingsAndAnEmptyRunHasOneCase() {
    Report connect = new Report(List.of(Finding.blocking("CONNECT", "refused", null)));
    assertThat(connect.rules()).containsExactly("CONNECT");
    assertThat(connect.toJUnitXml()).contains("tests=\"1\" failures=\"1\"");
    Report empty = new Report(List.of());
    assertThat(empty.toJUnitXml())
        .contains("tests=\"1\" failures=\"0\"")
        .contains("<testcase name=\"all rules\" classname=\"oracle-cdc-doctor\"/>");
    Report warn = new Report(List.of(Finding.warning("DOC-9", "w", null)));
    assertThat(warn.exitCode()).isEqualTo(Report.EXIT_WARNINGS);
  }

  @Test
  void jsonEscapesControlCharacters() {
    Report r = new Report(List.of(Finding.info("DOC-8", "a\tb\r\u0002\"q\"\\")));
    assertThat(r.toJson())
        .isEqualTo(
            "{\"exitCode\":0,\"findings\":[{\"rule\":\"DOC-8\",\"severity\":\"INFO\",\"message\":"
                + "\"a\\tb\\r\\u0002\\\"q\\\"\\\\\",\"fixSql\":null}]}");
  }
}
