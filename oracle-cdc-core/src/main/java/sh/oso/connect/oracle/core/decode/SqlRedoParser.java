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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import sh.oso.connect.oracle.core.decode.SqlRedoScanner.Kind;
import sh.oso.connect.oracle.core.decode.SqlRedoScanner.Token;
import sh.oso.connect.oracle.core.errors.DecodeException;
import sh.oso.connect.oracle.core.model.Operation;

/**
 * Recursive-descent parser for LogMiner INSERT, UPDATE and DELETE statements (CORE-DEC-1). The
 * grammar is exactly what LogMiner emits with NO_ROWID_IN_STMT and NO_SQL_DELIMITER:
 *
 * <pre>
 * insert into "O"."T"("C",...) values (lit,...)
 * update "O"."T" set "C" = lit, ... where pred and ...
 * delete from "O"."T" where pred and ...
 * pred := "C" = lit | "C" IS NULL | ROWID = 'x'
 * lit  := 'string' | NULL | FUNC('arg'[, 'arg'])
 * </pre>
 *
 * Only {@link DecodeException} ever leaves {@link #parse}.
 */
public final class SqlRedoParser {

  private final SqlRedoScanner sc;
  private Token t;

  private SqlRedoParser(String sql) {
    this.sc = new SqlRedoScanner(sql);
    this.t = sc.next();
  }

  public static ParsedDml parse(String sql) {
    if (sql == null) {
      throw new DecodeException(
          "SQL_REDO is null", "Report the row with its OPERATION and STATUS.");
    }
    try {
      return new SqlRedoParser(sql).statement();
    } catch (DecodeException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new DecodeException(
          "SQL_REDO could not be parsed: " + e.getMessage(),
          "Report the statement shape with the Oracle version.",
          e);
    }
  }

  /**
   * Parses the PL/SQL block of a LOB_WRITE, LOB_TRIM or LOB_ERASE row: the declarations are
   * skipped, then {@code select "COL" into loc from "OWNER"."TABLE" where ... for update;} followed
   * by buffer assignments and {@code dbms_lob.write}, {@code trim} or {@code erase} calls up to
   * {@code END;}.
   */
  public static LobRedo parseLob(String sql) {
    if (sql == null) {
      throw new DecodeException(
          "SQL_REDO of a LOB row is null", "Report the row with its OPERATION and STATUS.");
    }
    try {
      return new SqlRedoParser(sql).lobBlock();
    } catch (DecodeException e) {
      throw e;
    } catch (RuntimeException e) {
      throw new DecodeException(
          "LOB SQL_REDO could not be parsed: " + e.getMessage(),
          "Report the statement shape with the Oracle version.",
          e);
    }
  }

  private LobRedo lobBlock() {
    keyword("declare");
    while (!(t.kind() == Kind.WORD && t.text().equalsIgnoreCase("begin"))) {
      if (t.kind() == Kind.EOF) {
        throw sc.error("no BEGIN in LOB block", t.pos());
      }
      t = sc.next();
    }
    t = sc.next();
    keyword("select");
    String column = expect(Kind.QUOTED, "LOB column").text();
    keyword("into");
    word();
    keyword("from");
    String[] name = tableName();
    List<ColumnValue> where = new ArrayList<>();
    whereClause(where);
    keyword("for");
    keyword("update");
    expect(Kind.SEMI, ";");
    java.util.Map<String, SqlLiteral> buffers = new java.util.HashMap<>();
    java.util.Map<String, Long> numbers = new java.util.HashMap<>();
    List<LobRedo.Op> ops = new ArrayList<>();
    while (!acceptKeyword("end")) {
      String head = word();
      if (head.equalsIgnoreCase("dbms_lob")) {
        expect(Kind.DOT, ".");
        String call = word().toLowerCase(Locale.ROOT);
        expect(Kind.LPAREN, "(");
        word(); // the locator
        switch (call) {
          case "write":
            {
              expect(Kind.COMMA, ",");
              long amount = number(numbers);
              expect(Kind.COMMA, ",");
              long offset = number(numbers);
              expect(Kind.COMMA, ",");
              Token buf = expect(Kind.WORD, "buffer variable");
              SqlLiteral data = buffers.get(buf.text().toLowerCase(Locale.ROOT));
              if (data == null) {
                throw sc.error("buffer " + buf.text() + " was never assigned", buf.pos());
              }
              ops.add(new LobRedo.Write(amount, offset, data));
              break;
            }
          case "trim":
            expect(Kind.COMMA, ",");
            ops.add(new LobRedo.Trim(number(numbers)));
            break;
          case "erase":
            {
              expect(Kind.COMMA, ",");
              long amount = number(numbers);
              expect(Kind.COMMA, ",");
              ops.add(new LobRedo.Erase(amount, number(numbers)));
              break;
            }
          default:
            throw sc.error("unsupported call dbms_lob." + call, t.pos());
        }
        expect(Kind.RPAREN, ")");
      } else {
        // an assignment: the scanner reads ':=' as the word ':' followed by '='
        Token colon = expect(Kind.WORD, ":=");
        if (!colon.text().equals(":")) {
          throw sc.error("expected := after " + head, colon.pos());
        }
        expect(Kind.EQ, ":=");
        String var = head.toLowerCase(Locale.ROOT);
        if (t.kind() == Kind.WORD && isNumber(t.text())) {
          numbers.put(var, Long.parseLong(t.text()));
          t = sc.next();
        } else {
          buffers.put(var, literal());
        }
      }
      expect(Kind.SEMI, ";");
    }
    accept(Kind.SEMI);
    expect(Kind.EOF, "end of LOB block");
    if (ops.isEmpty()) {
      throw sc.error("LOB block has no dbms_lob call", 0);
    }
    return new LobRedo(name[0], name[1], column, where, ops);
  }

  private long number(java.util.Map<String, Long> variables) {
    Token tok = expect(Kind.WORD, "number");
    if (isNumber(tok.text())) {
      return Long.parseLong(tok.text());
    }
    Long v = variables.get(tok.text().toLowerCase(Locale.ROOT));
    if (v == null) {
      throw sc.error("expected a number but found '" + tok.text() + "'", tok.pos());
    }
    return v;
  }

  private static boolean isNumber(String text) {
    if (text.isEmpty() || text.length() > 18) {
      return false;
    }
    for (int i = 0; i < text.length(); i++) {
      if (!Character.isDigit(text.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  private ParsedDml statement() {
    String verb = word();
    ParsedDml dml;
    switch (verb.toLowerCase(Locale.ROOT)) {
      case "insert":
        dml = insert();
        break;
      case "update":
        dml = update();
        break;
      case "delete":
        dml = delete();
        break;
      default:
        throw sc.error("does not start with insert, update or delete", 0);
    }
    expect(Kind.EOF, "end of statement");
    return dml;
  }

  private ParsedDml insert() {
    keyword("into");
    String[] name = tableName();
    expect(Kind.LPAREN, "(");
    List<String> cols = new ArrayList<>();
    do {
      cols.add(expect(Kind.QUOTED, "column name").text());
    } while (accept(Kind.COMMA));
    expect(Kind.RPAREN, ")");
    keyword("values");
    expect(Kind.LPAREN, "(");
    List<ColumnValue> set = new ArrayList<>();
    int i = 0;
    do {
      if (i >= cols.size()) {
        throw sc.error("more values than columns", t.pos());
      }
      set.add(new ColumnValue(cols.get(i++), literal()));
    } while (accept(Kind.COMMA));
    expect(Kind.RPAREN, ")");
    if (i != cols.size()) {
      throw sc.error("fewer values than columns", t.pos());
    }
    return new ParsedDml(Operation.INSERT, name[0], name[1], set, List.of(), null);
  }

  private ParsedDml update() {
    String[] name = tableName();
    keyword("set");
    List<ColumnValue> set = new ArrayList<>();
    do {
      String col = expect(Kind.QUOTED, "column name").text();
      expect(Kind.EQ, "=");
      set.add(new ColumnValue(col, literal()));
    } while (accept(Kind.COMMA));
    List<ColumnValue> where = new ArrayList<>();
    String rowId = whereClause(where);
    return new ParsedDml(Operation.UPDATE, name[0], name[1], set, where, rowId);
  }

  private ParsedDml delete() {
    keyword("from");
    String[] name = tableName();
    List<ColumnValue> where = new ArrayList<>();
    String rowId = whereClause(where);
    return new ParsedDml(Operation.DELETE, name[0], name[1], List.of(), where, rowId);
  }

  /** Parses an optional where clause into {@code where}; returns the ROWID predicate if any. */
  private String whereClause(List<ColumnValue> where) {
    if (!acceptKeyword("where")) {
      return null;
    }
    String rowId = null;
    do {
      if (t.kind() == Kind.WORD && t.text().equalsIgnoreCase("ROWID")) {
        t = sc.next();
        expect(Kind.EQ, "=");
        rowId = expect(Kind.STRING, "ROWID string").text();
        continue;
      }
      String col = expect(Kind.QUOTED, "column name").text();
      if (acceptKeyword("is")) {
        keyword("null");
        where.add(new ColumnValue(col, SqlLiteral.NULL));
      } else {
        expect(Kind.EQ, "= or IS NULL");
        where.add(new ColumnValue(col, literal()));
      }
    } while (acceptKeyword("and"));
    return rowId;
  }

  private String[] tableName() {
    String owner = expect(Kind.QUOTED, "owner").text();
    expect(Kind.DOT, ".");
    String table = expect(Kind.QUOTED, "table name").text();
    return new String[] {owner, table};
  }

  private SqlLiteral literal() {
    Token tok = t;
    switch (tok.kind()) {
      case STRING:
        t = sc.next();
        return SqlLiteral.string(tok.text());
      case WORD:
        t = sc.next();
        if (tok.text().equalsIgnoreCase("NULL")) {
          return SqlLiteral.NULL;
        }
        return function(tok);
      default:
        throw sc.error("expected a literal", tok.pos());
    }
  }

  private SqlLiteral function(Token name) {
    expect(Kind.LPAREN, "(");
    List<String> args = new ArrayList<>();
    if (t.kind() == Kind.STRING) {
      do {
        args.add(expect(Kind.STRING, "string argument").text());
      } while (accept(Kind.COMMA));
    }
    expect(Kind.RPAREN, ")");
    String fn = name.text().toUpperCase(Locale.ROOT);
    switch (fn) {
      case "TO_DATE":
        need(args, 2, name);
        return new SqlLiteral(SqlLiteral.Kind.TO_DATE, args.get(0), args.get(1));
      case "TO_TIMESTAMP":
        need(args, 1, name);
        return SqlLiteral.of(SqlLiteral.Kind.TO_TIMESTAMP, args.get(0));
      case "TO_TIMESTAMP_TZ":
        need(args, 1, name);
        return SqlLiteral.of(SqlLiteral.Kind.TO_TIMESTAMP_TZ, args.get(0));
      case "TO_YMINTERVAL":
        need(args, 1, name);
        return SqlLiteral.of(SqlLiteral.Kind.TO_YMINTERVAL, args.get(0));
      case "TO_DSINTERVAL":
        need(args, 1, name);
        return SqlLiteral.of(SqlLiteral.Kind.TO_DSINTERVAL, args.get(0));
      case "HEXTORAW":
        need(args, 1, name);
        return SqlLiteral.of(SqlLiteral.Kind.HEXTORAW, args.get(0));
      case "UNISTR":
        need(args, 1, name);
        return SqlLiteral.of(SqlLiteral.Kind.UNISTR, unistr(args.get(0), name.pos()));
      case "EMPTY_CLOB":
        need(args, 0, name);
        return new SqlLiteral(SqlLiteral.Kind.EMPTY_CLOB, null, null);
      case "EMPTY_BLOB":
        need(args, 0, name);
        return new SqlLiteral(SqlLiteral.Kind.EMPTY_BLOB, null, null);
      default:
        throw sc.error("unknown function " + name.text(), name.pos());
    }
  }

  private void need(List<String> args, int n, Token name) {
    if (args.size() != n) {
      throw sc.error(name.text() + " takes " + n + " argument(s), got " + args.size(), name.pos());
    }
  }

  /** Resolves UNISTR escapes: {@code \XXXX} is a UTF-16 code unit, {@code \\} a backslash. */
  static String unistr(String raw, int pos) {
    StringBuilder sb = new StringBuilder(raw.length());
    for (int i = 0; i < raw.length(); i++) {
      char c = raw.charAt(i);
      if (c != '\\') {
        sb.append(c);
        continue;
      }
      if (i + 1 < raw.length() && raw.charAt(i + 1) == '\\') {
        sb.append('\\');
        i++;
        continue;
      }
      if (i + 4 >= raw.length()) {
        throw new DecodeException(
            "UNISTR escape truncated at offset " + pos, "Report the statement shape.");
      }
      String hex = raw.substring(i + 1, i + 5);
      try {
        sb.append((char) Integer.parseInt(hex, 16));
      } catch (NumberFormatException e) {
        throw new DecodeException(
            "UNISTR escape \\" + hex + " is not hexadecimal", "Report the statement shape.", e);
      }
      i += 4;
    }
    return sb.toString();
  }

  private String word() {
    return expect(Kind.WORD, "keyword").text();
  }

  private void keyword(String kw) {
    Token tok = t;
    if (tok.kind() != Kind.WORD || !tok.text().equalsIgnoreCase(kw)) {
      throw sc.error("expected '" + kw + "' but found '" + tok.text() + "'", tok.pos());
    }
    t = sc.next();
  }

  private boolean acceptKeyword(String kw) {
    if (t.kind() == Kind.WORD && t.text().equalsIgnoreCase(kw)) {
      t = sc.next();
      return true;
    }
    return false;
  }

  private boolean accept(Kind k) {
    if (t.kind() == k) {
      t = sc.next();
      return true;
    }
    return false;
  }

  private Token expect(Kind k, String what) {
    Token tok = t;
    if (tok.kind() != k) {
      throw sc.error("expected " + what + " but found '" + tok.text() + "'", tok.pos());
    }
    t = sc.next();
    return tok;
  }
}
