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
package sh.oso.connect.oracle.envelope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

/** ADR-0020: the adjustment rules for schema and field names, mode by mode. */
class NameAdjustmentTest {

  static final Pattern AVRO_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
  static final Pattern AVRO_FULL_NAME =
      Pattern.compile("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)*");

  static final List<String> AWKWARD =
      List.of(
          "ORDER#",
          "ORDER$",
          "ORDER_",
          "order lines",
          "/BIC/AZSALES00",
          "lower_case",
          "1ST_QUARTER",
          "2ND_QUARTER",
          "_HIDDEN",
          "A.B",
          "Gr\u00f6\u00dfe",
          "\u2603");

  @Test
  void validNamesAreLeftAsTheyAreInEveryMode() {
    for (NameAdjustment m : NameAdjustment.values()) {
      assertThat(m.simpleName("ORDERS")).isEqualTo("ORDERS");
      assertThat(m.simpleName("Orders2")).isEqualTo("Orders2");
      assertThat(m.fullName("cdc.FREEPDB1.APP.ORDERS.Value"))
          .isEqualTo("cdc.FREEPDB1.APP.ORDERS.Value");
    }
    assertThat(NameAdjustment.AVRO.simpleName("ORDER_LINES")).isEqualTo("ORDER_LINES");
    assertThat(NameAdjustment.AVRO.simpleName("_X")).isEqualTo("_X");
  }

  @Test
  void noneChangesNothing() {
    for (String n : AWKWARD) {
      assertThat(NameAdjustment.NONE.simpleName(n)).isSameAs(n);
      assertThat(NameAdjustment.NONE.fullName("my-cdc.APP." + n + ".Value"))
          .isEqualTo("my-cdc.APP." + n + ".Value");
    }
  }

  @Test
  void avroReplacesEachInvalidCharacterWithAnUnderscore() {
    NameAdjustment a = NameAdjustment.AVRO;
    assertThat(a.simpleName("ORDER#")).isEqualTo("ORDER_");
    assertThat(a.simpleName("ORDER$")).isEqualTo("ORDER_");
    assertThat(a.simpleName("T$WITH#CHARS")).isEqualTo("T_WITH_CHARS");
    assertThat(a.simpleName("order lines")).isEqualTo("order_lines");
    assertThat(a.simpleName("/BIC/AZSALES00")).isEqualTo("_BIC_AZSALES00");
    assertThat(a.simpleName("lower_case")).as("lower case is valid").isEqualTo("lower_case");
    assertThat(a.simpleName("Gr\u00f6\u00dfe")).as("letters outside ASCII").isEqualTo("Gr__e");
    assertThat(a.simpleName("A.B")).as("a field name has no namespace").isEqualTo("A_B");
    assertThat(a.simpleName("")).isEmpty();
  }

  @Test
  void avroPutsAnUnderscoreBeforeALeadingDigitAndKeepsTheDigit() {
    NameAdjustment a = NameAdjustment.AVRO;
    assertThat(a.simpleName("1ST_QUARTER")).isEqualTo("_1ST_QUARTER");
    assertThat(a.simpleName("24ColumnName")).isEqualTo("_24ColumnName");
    assertThat(a.simpleName("44ColumnName")).isEqualTo("_44ColumnName");
    assertThat(a.simpleName("Q1")).as("a digit after the first character").isEqualTo("Q1");
    assertThat(a.fullName("2024orders.APP.9T.Value")).isEqualTo("_2024orders.APP._9T.Value");
  }

  @Test
  void avroAdjustsAFullNamePartByPartAndKeepsTheDots() {
    NameAdjustment a = NameAdjustment.AVRO;
    assertThat(a.fullName("my-cdc.FREEPDB1.APP.ORDER#.Value"))
        .isEqualTo("my_cdc.FREEPDB1.APP.ORDER_.Value");
    assertThat(a.fullName("prod.orders-cdc.APP./BIC/AZSALES00.Envelope"))
        .isEqualTo("prod.orders_cdc.APP._BIC_AZSALES00.Envelope");
    assertThat(a.fullName("cdc.APP.order lines.Key")).isEqualTo("cdc.APP.order_lines.Key");
  }

  @Test
  void avroUnicodeEscapesInvalidCharactersAndTheUnderscoreAsUtf16CodeUnits() {
    NameAdjustment u = NameAdjustment.AVRO_UNICODE;
    assertThat(u.simpleName("ORDER#")).isEqualTo("ORDER_u0023");
    assertThat(u.simpleName("ORDER$")).isEqualTo("ORDER_u0024");
    assertThat(u.simpleName("ORDER_")).isEqualTo("ORDER_u005f");
    assertThat(u.simpleName("ORDER_LINES")).isEqualTo("ORDER_u005fLINES");
    assertThat(u.simpleName("_HIDDEN")).isEqualTo("_u005fHIDDEN");
    assertThat(u.simpleName("order lines")).isEqualTo("order_u0020lines");
    assertThat(u.simpleName("/BIC/AZ")).isEqualTo("_u002fBIC_u002fAZ");
    assertThat(u.simpleName("1ST")).as("a leading digit is escaped").isEqualTo("_u0031ST");
    assertThat(u.simpleName("Q1")).isEqualTo("Q1");
    assertThat(u.simpleName("Gr\u00f6\u00dfe")).isEqualTo("Gr_u00f6_u00dfe");
    assertThat(u.simpleName("\u2603")).as("lower-case hex").isEqualTo("_u2603");
    assertThat(u.simpleName("\uD83D\uDE00"))
        .as("a character outside the BMP is two code units")
        .isEqualTo("_ud83d_ude00");
    assertThat(u.fullName("my-cdc.APP.ORDER_LINES.Value"))
        .isEqualTo("my_u002dcdc.APP.ORDER_u005fLINES.Value");
  }

  @Test
  void everyAdjustedNameIsAValidAvroName() {
    for (NameAdjustment m : List.of(NameAdjustment.AVRO, NameAdjustment.AVRO_UNICODE)) {
      for (String n : AWKWARD) {
        assertThat(m.simpleName(n)).as("%s %s", m, n).matches(AVRO_NAME);
        assertThat(m.fullName("my-cdc.FREEPDB1.APP." + n + ".Value"))
            .as("%s %s", m, n)
            .matches(AVRO_FULL_NAME);
      }
    }
  }

  @Test
  void avroUnicodeNeverMapsTwoNamesToOne() {
    Set<String> seen = new HashSet<>();
    for (String n : AWKWARD) {
      assertThat(seen.add(NameAdjustment.AVRO_UNICODE.simpleName(n))).as(n).isTrue();
    }
    assertThat(NameAdjustment.AVRO.simpleName("ORDER#"))
        .as("avro can: that is what CDC-6004 guards")
        .isEqualTo(NameAdjustment.AVRO.simpleName("ORDER$"));
  }

  @Test
  void parsesTheConfigurationValues() {
    assertThat(NameAdjustment.parse("none")).isEqualTo(NameAdjustment.NONE);
    assertThat(NameAdjustment.parse("AVRO")).isEqualTo(NameAdjustment.AVRO);
    assertThat(NameAdjustment.parse(" avro_unicode ")).isEqualTo(NameAdjustment.AVRO_UNICODE);
    assertThatThrownBy(() -> NameAdjustment.parse("avro-unicode"))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
