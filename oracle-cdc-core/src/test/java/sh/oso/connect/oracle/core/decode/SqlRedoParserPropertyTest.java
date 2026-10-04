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

import java.util.List;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.testkit.SqlRedoRenderer;

/** parse(render(x)) == x for every statement the renderer can produce (ADR-0011 style property). */
class SqlRedoParserPropertyTest {

  @Property(tries = 500)
  void renderThenParseIsIdentity(@ForAll("dml") ParsedDml dml) {
    String sql = SqlRedoRenderer.render(dml);
    ParsedDml back = SqlRedoParser.parse(sql);
    assertThat(back).as(sql).isEqualTo(dml);
  }

  @Provide
  Arbitrary<ParsedDml> dml() {
    Arbitrary<String> ident =
        Arbitraries.strings()
            .withCharRange('A', 'Z')
            .withChars(' ', '_', '#', '$', '"', 'é')
            .ofMinLength(1)
            .ofMaxLength(12);
    Arbitrary<String> text =
        Arbitraries.strings().ascii().ofMaxLength(20).map(s -> s.replace("\u0000", ""));
    Arbitrary<String> national = Arbitraries.strings().ofMaxLength(12);
    Arbitrary<SqlLiteral> literal =
        Arbitraries.oneOf(
            text.map(SqlLiteral::string),
            Arbitraries.just(SqlLiteral.NULL),
            Combinators.combine(text, text)
                .as((v, f) -> new SqlLiteral(SqlLiteral.Kind.TO_DATE, v, f)),
            text.map(v -> SqlLiteral.of(SqlLiteral.Kind.TO_TIMESTAMP, v)),
            text.map(v -> SqlLiteral.of(SqlLiteral.Kind.TO_TIMESTAMP_TZ, v)),
            text.map(v -> SqlLiteral.of(SqlLiteral.Kind.TO_YMINTERVAL, v)),
            text.map(v -> SqlLiteral.of(SqlLiteral.Kind.TO_DSINTERVAL, v)),
            Arbitraries.strings()
                .withCharRange('0', '9')
                .withCharRange('a', 'f')
                .ofMaxLength(16)
                .map(v -> SqlLiteral.of(SqlLiteral.Kind.HEXTORAW, v)),
            national.map(v -> SqlLiteral.of(SqlLiteral.Kind.UNISTR, v)),
            Arbitraries.just(new SqlLiteral(SqlLiteral.Kind.EMPTY_CLOB, null, null)),
            Arbitraries.just(new SqlLiteral(SqlLiteral.Kind.EMPTY_BLOB, null, null)));
    Arbitrary<ColumnValue> cv = Combinators.combine(ident, literal).as(ColumnValue::new);
    Arbitrary<List<ColumnValue>> some = cv.list().ofMinSize(1).ofMaxSize(6);
    Arbitrary<List<ColumnValue>> any = cv.list().ofMaxSize(6);
    Arbitrary<String> rowid = Arbitraries.strings().alpha().numeric().ofLength(18).injectNull(0.7);
    Arbitrary<Operation> op = Arbitraries.of(Operation.INSERT, Operation.UPDATE, Operation.DELETE);
    return Combinators.combine(op, ident, ident, some, any, rowid)
        .as(
            (o, owner, table, set, where, rid) ->
                switch (o) {
                  case INSERT -> new ParsedDml(o, owner, table, set, List.of(), null);
                  case UPDATE -> new ParsedDml(o, owner, table, set, where, rid);
                  default -> new ParsedDml(o, owner, table, List.of(), where, rid);
                });
  }
}
