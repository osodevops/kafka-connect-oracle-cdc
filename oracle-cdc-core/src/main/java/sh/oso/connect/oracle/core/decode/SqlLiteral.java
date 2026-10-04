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
package sh.oso.connect.oracle.core.decode;

import java.util.Objects;

/**
 * One value as LogMiner renders it in SQL_REDO (reference/sql-redo-shapes.md). Numbers arrive as
 * quoted strings; temporal values as TO_DATE (with a format mask), TO_TIMESTAMP and TO_TIMESTAMP_TZ
 * (without); national strings as UNISTR; RAW as HEXTORAW; LOB placeholders as EMPTY_CLOB and
 * EMPTY_BLOB. {@code value} is the decoded argument (quotes unescaped, UNISTR escapes resolved).
 */
public record SqlLiteral(Kind kind, String value, String format) {

  public enum Kind {
    STRING,
    NULL,
    TO_DATE,
    TO_TIMESTAMP,
    TO_TIMESTAMP_TZ,
    TO_YMINTERVAL,
    TO_DSINTERVAL,
    HEXTORAW,
    UNISTR,
    EMPTY_CLOB,
    EMPTY_BLOB
  }

  public static final SqlLiteral NULL = new SqlLiteral(Kind.NULL, null, null);

  public SqlLiteral {
    Objects.requireNonNull(kind, "kind");
    if (kind == Kind.NULL || kind == Kind.EMPTY_CLOB || kind == Kind.EMPTY_BLOB) {
      value = null;
      format = null;
    } else {
      Objects.requireNonNull(value, "value");
    }
  }

  public static SqlLiteral string(String s) {
    return new SqlLiteral(Kind.STRING, s, null);
  }

  public static SqlLiteral of(Kind kind, String value) {
    return new SqlLiteral(kind, value, null);
  }

  public boolean isNull() {
    return kind == Kind.NULL;
  }
}
