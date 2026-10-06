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
package sh.oso.connect.oracle.core.errors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.PrintWriter;
import java.io.StringWriter;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.decode.OracleTypeCodec;
import sh.oso.connect.oracle.core.decode.SqlRedoParser;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.OracleType;

/** Row values stay out of decode error messages unless the connector reveals them. */
class DecodeExceptionTest {

  private static String trace(Throwable t) {
    StringWriter out = new StringWriter();
    t.printStackTrace(new PrintWriter(out));
    return out.toString();
  }

  @Test
  void aWithheldValueIsNeitherInTheMessageNorInTheTrace() {
    DecodeException e =
        DecodeException.withValue(
            "Column X is bad", "'secret'", "act", new NumberFormatException("secret"));
    assertThat(e.getMessage()).contains("Column X is bad").contains("withheld");
    assertThat(trace(e)).doesNotContain("secret");
    assertThat(e.hasWithheldValue()).isTrue();
    assertThat(e.code()).isEqualTo(ErrorCode.DECODE);
  }

  @Test
  void revealingPutsTheValueAndItsCauseBack() {
    NumberFormatException cause = new NumberFormatException("secret");
    DecodeException r =
        DecodeException.withValue("Column X is bad", "'secret'", "act", cause).revealed();
    assertThat(r.getMessage()).contains("Column X is bad: 'secret'").doesNotContain("withheld");
    assertThat(r.getCause()).isSameAs(cause);
    assertThat(r.operatorAction()).isEqualTo("act");
  }

  @Test
  void aPlainDecodeExceptionRevealsAsItself() {
    DecodeException e = new DecodeException("plain", "act");
    assertThat(e.revealed()).isSameAs(e);
    assertThat(e.getMessage()).doesNotContain("withheld");
  }

  @Test
  void aWrapperIsRevealedThroughItsCause() {
    RuntimeException wrapper =
        new IllegalStateException(
            "snapshot read failed", DecodeException.withValue("bad", "'secret'", "act", null));
    assertThat(DecodeException.reveal(wrapper).getMessage()).contains("'secret'");
    RuntimeException other = new IllegalStateException("x");
    assertThat(DecodeException.reveal(other)).isSameAs(other);
  }

  @Test
  void anInvalidLiteralIsWithheld() {
    ColumnSpec col = new ColumnSpec("CARD", 1, OracleType.NUMBER, "NUMBER", 22, 0, 0, true);
    var literal = SqlRedoParser.parse("insert into \"APP\".\"T\"(\"CARD\") values ('4111x')");
    assertThatThrownBy(() -> OracleTypeCodec.decode(col, literal.set().get(0).value()))
        .isInstanceOfSatisfying(
            DecodeException.class,
            e -> {
              assertThat(e.getMessage()).doesNotContain("4111").contains("CARD");
              assertThat(trace(e)).doesNotContain("4111");
              assertThat(e.revealed().getMessage()).contains("4111x");
            });
  }

  @Test
  void aParseErrorDoesNotQuoteTheStatement() {
    assertThatThrownBy(
            () ->
                SqlRedoParser.parse(
                    "insert into \"APP\".\"T\"(\"A\",\"B\") values ('Jane Doe' 'x')"))
        .isInstanceOfSatisfying(
            DecodeException.class,
            e -> {
              assertThat(e.getMessage()).doesNotContain("Jane").doesNotContain("'x'");
              assertThat(e.revealed().getMessage()).contains("Jane Doe");
            });
  }
}
