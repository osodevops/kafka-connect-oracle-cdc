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
package sh.oso.connect.oracle.core.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Classifies a DDL statement from a LogMiner DDL row (PRD-03 section 3 step 2, SCH-1, SCH-5). The
 * schema itself always comes from the dictionary; the text is only read to decide what happened. A
 * hand-written tokenizer reads words and quoted identifiers and skips string literals and comments.
 */
public final class DdlClassifier {

  /** What a DDL statement does to a captured table. */
  public enum Kind {
    /** CREATE TABLE: a new version when the table is captured. */
    CREATE_TABLE,
    /** Columns added, dropped, modified or renamed: a new version. */
    COLUMNS,
    /** Constraints added, dropped, enabled or disabled: a new version (the key may change). */
    CONSTRAINTS,
    /** Supplemental logging changed: a new version (before images may change). */
    SUPPLEMENTAL_LOG,
    /** ALTER TABLE ... RENAME TO or RENAME a TO b. */
    RENAME_TABLE,
    /** Partition maintenance: object ids change (the step cut refreshes them), columns do not. */
    PARTITION,
    TRUNCATE,
    DROP_TABLE,
    /** Indexes, comments, grants, storage and similar: nothing to do. */
    NO_SCHEMA_CHANGE,
    /** Not recognised: a stop when the table is captured (SCH-5). */
    UNKNOWN
  }

  private static final Set<String> STORAGE_CLAUSES =
      Set.of(
          "MOVE",
          "SHRINK",
          "COMPRESS",
          "NOCOMPRESS",
          "LOGGING",
          "NOLOGGING",
          "PARALLEL",
          "NOPARALLEL",
          "CACHE",
          "NOCACHE",
          "READ",
          "ALLOCATE",
          "DEALLOCATE",
          "PCTFREE",
          "PCTUSED",
          "INITRANS",
          "STORAGE",
          "INMEMORY",
          "NO",
          "ILM",
          "FLASHBACK",
          "ROW",
          "MONITORING",
          "NOMONITORING",
          "RESULT_CACHE",
          "UPGRADE",
          "MEMOPTIMIZE");

  private DdlClassifier() {}

  public static Kind classify(String ddl) {
    if (ddl == null) {
      return Kind.UNKNOWN;
    }
    List<String> t = words(ddl);
    if (t.isEmpty()) {
      return Kind.UNKNOWN;
    }
    switch (t.get(0)) {
      case "CREATE":
        return create(t);
      case "ALTER":
        return at(t, 1).equals("TABLE") ? alterTable(t) : Kind.NO_SCHEMA_CHANGE;
      case "DROP":
        return at(t, 1).equals("TABLE") ? Kind.DROP_TABLE : Kind.NO_SCHEMA_CHANGE;
      case "TRUNCATE":
        return at(t, 1).equals("TABLE") ? Kind.TRUNCATE : Kind.NO_SCHEMA_CHANGE;
      case "RENAME":
        return Kind.RENAME_TABLE;
      case "COMMENT":
      case "GRANT":
      case "REVOKE":
      case "ANALYZE":
      case "AUDIT":
      case "NOAUDIT":
      case "PURGE":
      case "ASSOCIATE":
      case "DISASSOCIATE":
        return Kind.NO_SCHEMA_CHANGE;
      default:
        return Kind.UNKNOWN;
    }
  }

  private static Kind create(List<String> t) {
    int i = 1;
    if (at(t, i).equals("OR") && at(t, i + 1).equals("REPLACE")) {
      i += 2;
    }
    while (Set.of(
            "GLOBAL", "PRIVATE", "TEMPORARY", "SHARDED", "DUPLICATED", "BLOCKCHAIN", "IMMUTABLE")
        .contains(at(t, i))) {
      i++;
    }
    return at(t, i).equals("TABLE") ? Kind.CREATE_TABLE : Kind.NO_SCHEMA_CHANGE;
  }

  /** ALTER TABLE [schema.]name action ... */
  private static Kind alterTable(List<String> t) {
    int i = 2;
    i++; // the table name; the tokenizer joins schema.name
    String action = at(t, i);
    String next = at(t, i + 1);
    switch (action) {
      case "ADD":
        if (Set.of("CONSTRAINT", "PRIMARY", "UNIQUE", "FOREIGN", "CHECK").contains(next)) {
          return Kind.CONSTRAINTS;
        }
        if (next.equals("SUPPLEMENTAL")) {
          return Kind.SUPPLEMENTAL_LOG;
        }
        if (next.equals("PARTITION") || next.equals("SUBPARTITION")) {
          return Kind.PARTITION;
        }
        if (next.equals("(")
            && Set.of("CONSTRAINT", "PRIMARY", "UNIQUE", "FOREIGN", "CHECK")
                .contains(at(t, i + 2))) {
          return Kind.CONSTRAINTS;
        }
        return Kind.COLUMNS;
      case "DROP":
        if (Set.of("CONSTRAINT", "PRIMARY", "UNIQUE").contains(next)) {
          return Kind.CONSTRAINTS;
        }
        if (next.equals("SUPPLEMENTAL")) {
          return Kind.SUPPLEMENTAL_LOG;
        }
        if (next.equals("PARTITION") || next.equals("SUBPARTITION")) {
          return Kind.PARTITION;
        }
        if (next.equals("UNUSED")) {
          return Kind.NO_SCHEMA_CHANGE; // the columns left the schema at SET UNUSED
        }
        return Kind.COLUMNS;
      case "SET":
        return next.equals("UNUSED") ? Kind.COLUMNS : storage(next);
      case "MODIFY":
        if (next.equals("PARTITION") || next.equals("SUBPARTITION") || next.equals("DEFAULT")) {
          return Kind.PARTITION;
        }
        if (Set.of("CONSTRAINT", "PRIMARY", "UNIQUE").contains(next)) {
          return Kind.CONSTRAINTS;
        }
        if (next.equals("LOB") || next.equals("VARRAY") || next.equals("NESTED")) {
          return Kind.NO_SCHEMA_CHANGE;
        }
        return Kind.COLUMNS;
      case "RENAME":
        if (next.equals("COLUMN")) {
          return Kind.COLUMNS;
        }
        if (next.equals("TO")) {
          return Kind.RENAME_TABLE;
        }
        if (next.equals("CONSTRAINT") || next.equals("PARTITION") || next.equals("SUBPARTITION")) {
          return Kind.NO_SCHEMA_CHANGE;
        }
        return Kind.UNKNOWN;
      case "ENABLE":
      case "DISABLE":
        if (Set.of("CONSTRAINT", "PRIMARY", "UNIQUE", "NOVALIDATE", "VALIDATE").contains(next)) {
          return Kind.CONSTRAINTS;
        }
        return Kind.NO_SCHEMA_CHANGE; // row movement, table lock, triggers
      case "SPLIT":
      case "MERGE":
      case "TRUNCATE":
      case "EXCHANGE":
      case "COALESCE":
        return Kind.PARTITION;
      default:
        return storage(action);
    }
  }

  private static Kind storage(String word) {
    return STORAGE_CLAUSES.contains(word) ? Kind.NO_SCHEMA_CHANGE : Kind.UNKNOWN;
  }

  private static String at(List<String> t, int i) {
    return i < t.size() ? t.get(i) : "";
  }

  /**
   * Upper-cased words and quoted identifiers ({@code schema.name} joined as one word), with "(" as
   * a word of its own; string literals, comments and other punctuation dropped.
   */
  static List<String> words(String s) {
    List<String> out = new ArrayList<>();
    int i = 0;
    StringBuilder w = new StringBuilder();
    while (i < s.length()) {
      char c = s.charAt(i);
      if (c == '-' && i + 1 < s.length() && s.charAt(i + 1) == '-') {
        while (i < s.length() && s.charAt(i) != '\n') {
          i++;
        }
        continue;
      }
      if (c == '/' && i + 1 < s.length() && s.charAt(i + 1) == '*') {
        int end = s.indexOf("*/", i + 2);
        i = end < 0 ? s.length() : end + 2;
        continue;
      }
      if (c == '\'') {
        flush(w, out);
        i++;
        while (i < s.length()) {
          if (s.charAt(i) == '\'' && i + 1 < s.length() && s.charAt(i + 1) == '\'') {
            i += 2;
          } else if (s.charAt(i) == '\'') {
            i++;
            break;
          } else {
            i++;
          }
        }
        continue;
      }
      if (c == '"') {
        int end = s.indexOf('"', i + 1);
        w.append(s, i + 1, end < 0 ? s.length() : end);
        i = end < 0 ? s.length() : end + 1;
        continue;
      }
      if (Character.isLetterOrDigit(c) || c == '_' || c == '$' || c == '#' || c == '.') {
        w.append(Character.toUpperCase(c));
        i++;
        continue;
      }
      flush(w, out);
      if (c == '(') {
        out.add("(");
      }
      i++;
    }
    flush(w, out);
    return out;
  }

  private static void flush(StringBuilder w, List<String> out) {
    if (w.length() > 0) {
      out.add(w.toString().toUpperCase(Locale.ROOT));
      w.setLength(0);
    }
  }
}
