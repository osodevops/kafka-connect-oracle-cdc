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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * The outcome of a doctor run with the PRD-05 exit codes. {@code rules} lists the rules that ran,
 * in order, so the JUnit report has one test case per rule whether or not it found anything.
 */
public record Report(List<Finding> findings, List<String> rules) {

  public static final int EXIT_OK = 0;
  public static final int EXIT_BLOCKING = 1;
  public static final int EXIT_WARNINGS = 2;

  public Report {
    findings = List.copyOf(findings);
    Set<String> ids = new LinkedHashSet<>(rules);
    for (Finding f : findings) {
      ids.add(f.rule());
    }
    rules = List.copyOf(ids);
  }

  /** A report whose rules are the ones that produced the findings. */
  public Report(List<Finding> findings) {
    this(findings, List.of());
  }

  public boolean hasBlocking() {
    return findings.stream().anyMatch(f -> f.severity() == Severity.BLOCKING);
  }

  public boolean hasWarnings() {
    return findings.stream().anyMatch(f -> f.severity() == Severity.WARNING);
  }

  public int exitCode() {
    return hasBlocking() ? EXIT_BLOCKING : hasWarnings() ? EXIT_WARNINGS : EXIT_OK;
  }

  public String toMarkdown() {
    StringBuilder sb = new StringBuilder("# oracle-cdc-doctor report\n\n");
    if (findings.isEmpty()) {
      return sb.append("No findings. The database is ready for capture.\n").toString();
    }
    sb.append("| Rule | Severity | Finding |\n|---|---|---|\n");
    for (Finding f : findings) {
      sb.append("| ")
          .append(f.rule())
          .append(" | ")
          .append(f.severity())
          .append(" | ")
          .append(f.message().replace("|", "\\|"))
          .append(" |\n");
    }
    boolean anyFix = findings.stream().anyMatch(f -> f.fixSql() != null);
    if (anyFix) {
      sb.append("\n## SQL to run\n\n```sql\n");
      for (Finding f : findings) {
        if (f.fixSql() != null) {
          sb.append("-- ").append(f.rule()).append('\n').append(f.fixSql()).append("\n\n");
        }
      }
      sb.append("```\n");
    }
    return sb.toString();
  }

  public String toJson() {
    StringBuilder sb =
        new StringBuilder("{\"exitCode\":").append(exitCode()).append(",\"findings\":[");
    for (int i = 0; i < findings.size(); i++) {
      Finding f = findings.get(i);
      sb.append(i > 0 ? "," : "")
          .append("{\"rule\":\"")
          .append(f.rule())
          .append("\",\"severity\":\"")
          .append(f.severity())
          .append("\",\"message\":")
          .append(quote(f.message()))
          .append(",\"fixSql\":")
          .append(f.fixSql() == null ? "null" : quote(f.fixSql()))
          .append('}');
    }
    return sb.append("]}").toString();
  }

  /**
   * JUnit XML for CI: one test case per rule. A rule with a blocking finding fails; warnings and
   * information go to the case's output, so a pipeline shows them without failing on them.
   */
  public String toJUnitXml() {
    List<String> cases = rules.isEmpty() ? List.of("all rules") : rules;
    long failures =
        cases.stream()
            .filter(
                r ->
                    findings.stream()
                        .anyMatch(f -> f.rule().equals(r) && f.severity() == Severity.BLOCKING))
            .count();
    StringBuilder sb =
        new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
            .append("<testsuite name=\"oracle-cdc-doctor\" tests=\"")
            .append(cases.size())
            .append("\" failures=\"")
            .append(failures)
            .append("\" errors=\"0\" skipped=\"0\">\n");
    for (String rule : cases) {
      List<Finding> mine = new ArrayList<>();
      for (Finding f : findings) {
        if (f.rule().equals(rule)) {
          mine.add(f);
        }
      }
      sb.append("  <testcase name=\"")
          .append(xml(rule))
          .append("\" classname=\"oracle-cdc-doctor\"");
      if (mine.isEmpty()) {
        sb.append("/>\n");
        continue;
      }
      sb.append('>');
      List<Finding> blocking =
          mine.stream().filter(f -> f.severity() == Severity.BLOCKING).toList();
      if (!blocking.isEmpty()) {
        sb.append("<failure message=\"")
            .append(xml(blocking.get(0).message()))
            .append("\" type=\"BLOCKING\">");
        for (Finding f : blocking) {
          sb.append(xml(f.message())).append('\n');
          if (f.fixSql() != null) {
            sb.append(xml(f.fixSql())).append('\n');
          }
        }
        sb.append("</failure>");
      }
      StringBuilder outText = new StringBuilder();
      for (Finding f : mine) {
        if (f.severity() != Severity.BLOCKING) {
          outText.append(f.severity()).append(": ").append(f.message()).append('\n');
        }
      }
      if (outText.length() > 0) {
        sb.append("<system-out>").append(xml(outText.toString())).append("</system-out>");
      }
      sb.append("</testcase>\n");
    }
    return sb.append("</testsuite>\n").toString();
  }

  private static String quote(String s) {
    StringBuilder sb = new StringBuilder("\"");
    for (char c : s.toCharArray()) {
      switch (c) {
        case '\\' -> sb.append("\\\\");
        case '"' -> sb.append("\\\"");
        case '\n' -> sb.append("\\n");
        case '\r' -> sb.append("\\r");
        case '\t' -> sb.append("\\t");
        default -> {
          if (c < 0x20) {
            sb.append(String.format(java.util.Locale.ROOT, "\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
        }
      }
    }
    return sb.append('"').toString();
  }

  /**
   * XML 1.0 text: markup characters escaped, control characters other than tab and line dropped.
   */
  static String xml(String s) {
    StringBuilder sb = new StringBuilder();
    for (char c : s.toCharArray()) {
      switch (c) {
        case '&' -> sb.append("&amp;");
        case '<' -> sb.append("&lt;");
        case '>' -> sb.append("&gt;");
        case '"' -> sb.append("&quot;");
        default -> {
          if (c >= 0x20 || c == '\n' || c == '\t' || c == '\r') {
            sb.append(c);
          }
        }
      }
    }
    return sb.toString();
  }
}
