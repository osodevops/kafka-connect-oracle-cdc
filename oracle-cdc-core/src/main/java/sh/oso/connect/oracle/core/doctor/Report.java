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

import java.util.List;

/** The outcome of a doctor run with the PRD-05 exit codes. */
public record Report(List<Finding> findings) {

  public static final int EXIT_OK = 0;
  public static final int EXIT_BLOCKING = 1;
  public static final int EXIT_WARNINGS = 2;

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

  public String toJUnitXml() {
    StringBuilder sb =
        new StringBuilder(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
                    + "<testsuite name=\"oracle-cdc-doctor\" tests=\"")
            .append(Math.max(1, findings.size()))
            .append("\" failures=\"")
            .append(findings.stream().filter(f -> f.severity() == Severity.BLOCKING).count())
            .append("\">\n");
    if (findings.isEmpty()) {
      sb.append("  <testcase name=\"all rules\" classname=\"doctor\"/>\n");
    }
    for (Finding f : findings) {
      sb.append("  <testcase name=\"").append(f.rule()).append("\" classname=\"doctor\">");
      if (f.severity() == Severity.BLOCKING) {
        sb.append("<failure message=\"").append(xml(f.message())).append("\"/>");
      } else {
        sb.append("<system-out>")
            .append(xml(f.severity() + ": " + f.message()))
            .append("</system-out>");
      }
      sb.append("</testcase>\n");
    }
    return sb.append("</testsuite>\n").toString();
  }

  private static String quote(String s) {
    return '"' + s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n") + '"';
  }

  private static String xml(String s) {
    return s.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace("\"", "&quot;");
  }
}
