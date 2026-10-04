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
package sh.oso.connect.oracle.core.testkit;

import java.util.List;
import sh.oso.connect.oracle.core.decode.ColumnValue;
import sh.oso.connect.oracle.core.decode.ParsedDml;
import sh.oso.connect.oracle.core.decode.SqlLiteral;

/** Renders a {@link ParsedDml} the way LogMiner writes SQL_REDO; the inverse of the parser. */
public final class SqlRedoRenderer {

  private SqlRedoRenderer() {}

  public static String render(ParsedDml d) {
    StringBuilder sb = new StringBuilder();
    switch (d.op()) {
      case INSERT -> {
        sb.append("insert into ").append(table(d)).append('(');
        join(sb, d.set(), c -> q(c.column()));
        sb.append(") values (");
        join(sb, d.set(), c -> literal(c.value()));
        sb.append(')');
      }
      case UPDATE -> {
        sb.append("update ").append(table(d)).append(" set ");
        joinWith(sb, d.set(), ", ", c -> q(c.column()) + " = " + literal(c.value()));
        where(sb, d);
      }
      default -> {
        sb.append("delete from ").append(table(d));
        where(sb, d);
      }
    }
    return sb.toString();
  }

  private static void where(StringBuilder sb, ParsedDml d) {
    if (d.where().isEmpty() && d.rowId() == null) {
      return;
    }
    sb.append(" where ");
    boolean first = true;
    for (ColumnValue c : d.where()) {
      if (!first) {
        sb.append(" and ");
      }
      first = false;
      sb.append(q(c.column()));
      sb.append(c.value().isNull() ? " IS NULL" : " = " + literal(c.value()));
    }
    if (d.rowId() != null) {
      if (!first) {
        sb.append(" and ");
      }
      sb.append("ROWID = ").append(s(d.rowId()));
    }
  }

  private static String table(ParsedDml d) {
    return q(d.owner()) + "." + q(d.table());
  }

  public static String literal(SqlLiteral l) {
    return switch (l.kind()) {
      case STRING -> s(l.value());
      case NULL -> "NULL";
      case TO_DATE -> "TO_DATE(" + s(l.value()) + ", " + s(l.format()) + ")";
      case TO_TIMESTAMP -> "TO_TIMESTAMP(" + s(l.value()) + ")";
      case TO_TIMESTAMP_TZ -> "TO_TIMESTAMP_TZ(" + s(l.value()) + ")";
      case TO_YMINTERVAL -> "TO_YMINTERVAL(" + s(l.value()) + ")";
      case TO_DSINTERVAL -> "TO_DSINTERVAL(" + s(l.value()) + ")";
      case HEXTORAW -> "HEXTORAW(" + s(l.value()) + ")";
      case UNISTR -> "UNISTR(" + s(unistrEscape(l.value())) + ")";
      case EMPTY_CLOB -> "EMPTY_CLOB()";
      case EMPTY_BLOB -> "EMPTY_BLOB()";
    };
  }

  static String unistrEscape(String v) {
    StringBuilder sb = new StringBuilder();
    for (char c : v.toCharArray()) {
      if (c == '\\') {
        sb.append("\\\\");
      } else if (c > 127) {
        sb.append(String.format("\\%04X", (int) c));
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  private static String q(String ident) {
    return '"' + ident.replace("\"", "\"\"") + '"';
  }

  private static String s(String str) {
    return '\'' + str.replace("'", "''") + '\'';
  }

  private interface Fn {
    String apply(ColumnValue c);
  }

  private static void join(StringBuilder sb, List<ColumnValue> items, Fn fn) {
    joinWith(sb, items, ",", fn);
  }

  private static void joinWith(StringBuilder sb, List<ColumnValue> items, String sep, Fn fn) {
    boolean first = true;
    for (ColumnValue c : items) {
      if (!first) {
        sb.append(sep);
      }
      first = false;
      sb.append(fn.apply(c));
    }
  }
}
