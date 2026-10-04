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

import sh.oso.connect.oracle.core.errors.DecodeException;

/**
 * Tokeniser for LogMiner's reconstructed SQL. Hand-written, no regular expressions: words,
 * double-quoted identifiers (with doubled quotes), single-quoted strings (with doubled quotes),
 * parentheses, commas, equals and dots. Anything else is a {@link DecodeException}.
 */
final class SqlRedoScanner {

  enum Kind {
    WORD,
    QUOTED,
    STRING,
    LPAREN,
    RPAREN,
    COMMA,
    EQ,
    DOT,
    EOF
  }

  record Token(Kind kind, String text, int pos) {}

  private final String s;
  private int i;

  SqlRedoScanner(String sql) {
    this.s = sql;
  }

  int position() {
    return i;
  }

  Token next() {
    while (i < s.length() && Character.isWhitespace(s.charAt(i))) {
      i++;
    }
    if (i >= s.length()) {
      return new Token(Kind.EOF, "", i);
    }
    int start = i;
    char c = s.charAt(i);
    switch (c) {
      case '(':
        i++;
        return new Token(Kind.LPAREN, "(", start);
      case ')':
        i++;
        return new Token(Kind.RPAREN, ")", start);
      case ',':
        i++;
        return new Token(Kind.COMMA, ",", start);
      case '=':
        i++;
        return new Token(Kind.EQ, "=", start);
      case '.':
        i++;
        return new Token(Kind.DOT, ".", start);
      case '"':
        return new Token(Kind.QUOTED, quoted('"'), start);
      case '\'':
        return new Token(Kind.STRING, quoted('\''), start);
      default:
        if (isWordChar(c)) {
          while (i < s.length() && isWordChar(s.charAt(i))) {
            i++;
          }
          return new Token(Kind.WORD, s.substring(start, i), start);
        }
        throw error("unexpected character '" + c + "'", start);
    }
  }

  private String quoted(char q) {
    int start = i;
    i++; // opening quote
    StringBuilder sb = new StringBuilder();
    while (true) {
      if (i >= s.length()) {
        throw error("unterminated " + (q == '"' ? "identifier" : "string"), start);
      }
      char c = s.charAt(i++);
      if (c == q) {
        if (i < s.length() && s.charAt(i) == q) {
          sb.append(q);
          i++;
        } else {
          return sb.toString();
        }
      } else {
        sb.append(c);
      }
    }
  }

  private static boolean isWordChar(char c) {
    return Character.isLetterOrDigit(c)
        || c == '_'
        || c == '$'
        || c == '#'
        || c == ':'
        || c == '-'
        || c == '+';
  }

  DecodeException error(String what, int pos) {
    int from = Math.max(0, pos - 20);
    int to = Math.min(s.length(), pos + 20);
    return new DecodeException(
        "SQL_REDO " + what + " at offset " + pos + " near \"" + s.substring(from, to) + "\"",
        "Report the statement shape with the Oracle version; the connector stops rather than"
            + " guess.");
  }
}
