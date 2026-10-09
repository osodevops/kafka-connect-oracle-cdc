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
package sh.oso.connect.oracle.e2e.support;

import java.util.ArrayList;
import java.util.List;

/**
 * Splits a {@code setup-sql} script into statements a JDBC {@code execute} accepts: SQL statements
 * end with a semicolon, PL/SQL blocks run from {@code BEGIN} to a line holding only a slash, and
 * comments and SQL*Plus directives ({@code WHENEVER}, {@code SET}, {@code EXEC}) are handled as
 * SQL*Plus would.
 */
public final class SqlScript {

  private SqlScript() {}

  public static List<String> statements(String script) {
    List<String> out = new ArrayList<>();
    StringBuilder block = null;
    for (String raw : script.split("\\R")) {
      String line = raw.strip();
      if (block != null) {
        if (line.equals("/")) {
          out.add(block.toString().strip());
          block = null;
        } else {
          block.append(raw).append('\n');
        }
        continue;
      }
      if (line.isEmpty() || line.startsWith("--")) {
        continue;
      }
      String upper = line.toUpperCase(java.util.Locale.ROOT);
      if (upper.startsWith("WHENEVER ") || upper.startsWith("SET ")) {
        continue;
      }
      if (upper.equals("BEGIN") || upper.startsWith("BEGIN ") || upper.startsWith("DECLARE")) {
        block = new StringBuilder(raw).append('\n');
        continue;
      }
      if (upper.startsWith("EXEC ")) {
        String call = line.substring(5).strip();
        out.add("BEGIN " + (call.endsWith(";") ? call : call + ";") + " END;");
        continue;
      }
      out.add(line.endsWith(";") ? line.substring(0, line.length() - 1) : line);
    }
    if (block != null) {
      throw new IllegalArgumentException("a PL/SQL block has no closing slash");
    }
    return out;
  }
}
