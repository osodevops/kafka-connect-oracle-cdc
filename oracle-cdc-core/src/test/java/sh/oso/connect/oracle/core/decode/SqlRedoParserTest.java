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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.errors.DecodeException;
import sh.oso.connect.oracle.core.model.Operation;

class SqlRedoParserTest {

  private static final Path CORPUS =
      Path.of("src/test/resources/sqlredo-corpus/types-and-lobs.sql");

  @Test
  void parsesInsertWithEveryLiteralKind() {
    ParsedDml d =
        SqlRedoParser.parse(
            "insert into \"T_SCHEMA\".\"CLASSIC_ALL\"(\"ID\",\"C0\",\"C1\",\"C2\",\"C9\",\"C10\","
                + "\"C12\",\"C14\",\"C15\",\"C16\",\"C17\",\"C18\") values ('1','abc       ',"
                + "UNISTR('nch\\00E1r     '),'plain ''quoted'' text',"
                + "TO_DATE('2026-02-28 13:45:59', 'YYYY-MM-DD HH24:MI:SS'),"
                + "TO_TIMESTAMP('2026-03-29 01:30:00.123456000'),"
                + "TO_TIMESTAMP_TZ('2026-03-29 01:30:00.500000000 +05:30'),"
                + "TO_YMINTERVAL('+0012-03'),TO_DSINTERVAL('+00005 04:03:02.123456'),"
                + "HEXTORAW('deadbeef00ff'),EMPTY_CLOB(),NULL)");
    assertThat(d.op()).isEqualTo(Operation.INSERT);
    assertThat(d.owner()).isEqualTo("T_SCHEMA");
    assertThat(d.table()).isEqualTo("CLASSIC_ALL");
    assertThat(d.where()).isEmpty();
    assertThat(d.rowId()).isNull();
    assertThat(d.set())
        .extracting(ColumnValue::column)
        .containsExactly(
            "ID", "C0", "C1", "C2", "C9", "C10", "C12", "C14", "C15", "C16", "C17", "C18");
    List<SqlLiteral> v = d.set().stream().map(ColumnValue::value).toList();
    assertThat(v.get(0)).isEqualTo(SqlLiteral.string("1"));
    assertThat(v.get(1).value()).isEqualTo("abc       ");
    assertThat(v.get(2)).isEqualTo(SqlLiteral.of(SqlLiteral.Kind.UNISTR, "nchár     "));
    assertThat(v.get(3).value()).isEqualTo("plain 'quoted' text");
    assertThat(v.get(4))
        .isEqualTo(
            new SqlLiteral(
                SqlLiteral.Kind.TO_DATE, "2026-02-28 13:45:59", "YYYY-MM-DD HH24:MI:SS"));
    assertThat(v.get(5).kind()).isEqualTo(SqlLiteral.Kind.TO_TIMESTAMP);
    assertThat(v.get(6).value()).isEqualTo("2026-03-29 01:30:00.500000000 +05:30");
    assertThat(v.get(7).kind()).isEqualTo(SqlLiteral.Kind.TO_YMINTERVAL);
    assertThat(v.get(8).kind()).isEqualTo(SqlLiteral.Kind.TO_DSINTERVAL);
    assertThat(v.get(9)).isEqualTo(SqlLiteral.of(SqlLiteral.Kind.HEXTORAW, "deadbeef00ff"));
    assertThat(v.get(10).kind()).isEqualTo(SqlLiteral.Kind.EMPTY_CLOB);
    assertThat(v.get(11).isNull()).isTrue();
  }

  @Test
  void parsesUpdateAndDeleteWithNullPredicatesAndRowid() {
    ParsedDml u =
        SqlRedoParser.parse(
            "update \"S\".\"T\" set \"C2\" = 'changed', \"C4\" = '1' where \"ID\" = '1' and \"C0\""
                + " IS NULL and \"C9\" = TO_DATE('1900-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS')");
    assertThat(u.op()).isEqualTo(Operation.UPDATE);
    assertThat(u.set()).extracting(ColumnValue::column).containsExactly("C2", "C4");
    assertThat(u.where()).extracting(ColumnValue::column).containsExactly("ID", "C0", "C9");
    assertThat(u.where().get(1).value().isNull()).isTrue();

    ParsedDml d =
        SqlRedoParser.parse(
            "delete from \"S\".\"T\" where \"ID\" = '3' and ROWID = 'AAAR9zAAHAAAACrAAA'");
    assertThat(d.op()).isEqualTo(Operation.DELETE);
    assertThat(d.where()).hasSize(1);
    assertThat(d.rowId()).isEqualTo("AAAR9zAAHAAAACrAAA");

    ParsedDml bare = SqlRedoParser.parse("delete from \"S\".\"T\"");
    assertThat(bare.where()).isEmpty();
    assertThat(
            SqlRedoParser.parse(
                    "update \"S\".\"T\" set \"COL 2\" = HEXTORAW('00') where \"COL 1\" ="
                        + " HEXTORAW('01')")
                .set()
                .get(0)
                .column())
        .isEqualTo("COL 2");
  }

  @Test
  void onlyDecodeExceptionEverEscapes() {
    for (String bad :
        List.of(
            "",
            "merge into \"S\".\"T\"",
            "insert into \"S\".\"T\"(\"A\") values ('1','2')",
            "insert into \"S\".\"T\"(\"A\",\"B\") values ('1')",
            "insert into \"S\".\"T\"(\"A\") values (SYSDATE)",
            "insert into \"S\".\"T\"(\"A\") values (TO_DATE('x'))",
            "update \"S\".\"T\" set \"A\" = 'x' where \"B\" > '1'",
            "delete from \"S\".\"T\" where \"A\" = 'unterminated",
            "delete from \"S\".\"T\" where \"A\" = 'x' or \"B\" = 'y'",
            "insert into \"S\".\"T\"(\"A\") values (UNISTR('\\00G1'))",
            "insert into \"S\".\"T\"(\"A\") values (UNISTR('\\00'))",
            "insert into \"S\".\"T\"(\"A\") values ('1') ;",
            "insert into \"S\".\"T\"(\"A\") values ('1') extra")) {
      assertThatThrownBy(() -> SqlRedoParser.parse(bad), bad)
          .isInstanceOf(DecodeException.class)
          .hasMessageContaining("CDC-3001");
    }
    assertThatThrownBy(() -> SqlRedoParser.parse(null)).isInstanceOf(DecodeException.class);
  }

  @Test
  void unistrEscapes() {
    assertThat(SqlRedoParser.unistr("\\00FCn\\00EFcode \\2603 text", 0))
        .isEqualTo("ünïcode ☃ text");
    assertThat(SqlRedoParser.unistr("a\\\\b", 0)).isEqualTo("a\\b");
  }

  @Test
  void everyDmlLineOfTheOracleGeneratedCorpusParses() throws IOException {
    assertThat(CORPUS).exists();
    int parsed = 0;
    for (String line : Files.readAllLines(CORPUS)) {
      String lower = line.toLowerCase(Locale.ROOT);
      if (lower.startsWith("insert ")
          || lower.startsWith("update ")
          || lower.startsWith("delete ")) {
        ParsedDml d = SqlRedoParser.parse(line);
        assertThat(d.owner()).isEqualTo("T_SCHEMA");
        parsed++;
      }
    }
    assertThat(parsed).isGreaterThan(40);
  }

  @Test
  void throughputIsPrintedNotAsserted() throws IOException {
    List<String> lines =
        Files.readAllLines(CORPUS).stream()
            .filter(
                l -> l.startsWith("insert ") || l.startsWith("update ") || l.startsWith("delete "))
            .toList();
    int n = 0;
    long start = System.nanoTime();
    for (int round = 0; round < 2000; round++) {
      for (String l : lines) {
        SqlRedoParser.parse(l);
        n++;
      }
    }
    long ns = System.nanoTime() - start;
    System.out.printf(
        Locale.ROOT,
        "sqlredo-parser: %d statements in %d ms (%.0f per second)%n",
        n,
        ns / 1_000_000,
        n / (ns / 1e9));
  }
}
