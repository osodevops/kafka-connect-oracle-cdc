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

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.errors.DecodeException;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.OracleType;

class OracleTypeCodecTest {

  private static ColumnSpec col(OracleType t) {
    return ColumnSpec.of("C", 1, t);
  }

  private static Object dec(OracleType t, SqlLiteral l) {
    return OracleTypeCodec.decode(col(t), l);
  }

  @Test
  void decodesEveryCorpusShape() {
    assertThat(dec(OracleType.CHAR, SqlLiteral.string("abc       "))).isEqualTo("abc       ");
    assertThat(dec(OracleType.NVARCHAR2, SqlLiteral.of(SqlLiteral.Kind.UNISTR, "ünï")))
        .isEqualTo("ünï");
    assertThat(dec(OracleType.NUMBER, SqlLiteral.string("1234567890.123456789")))
        .isEqualTo(new BigDecimal("1234567890.123456789"));
    assertThat(dec(OracleType.NUMBER, SqlLiteral.string("-42.5")))
        .isEqualTo(new BigDecimal("-42.5"));
    assertThat(dec(OracleType.FLOAT, SqlLiteral.string("3.14159")))
        .isEqualTo(new BigDecimal("3.14159"));
    assertThat(dec(OracleType.BINARY_FLOAT, SqlLiteral.string("1.50000005E+010")))
        .isEqualTo(1.50000005E10f);
    assertThat(dec(OracleType.BINARY_DOUBLE, SqlLiteral.string("0"))).isEqualTo(0d);
    assertThat(
            dec(
                OracleType.DATE,
                new SqlLiteral(
                    SqlLiteral.Kind.TO_DATE, "2026-02-28 13:45:59", "YYYY-MM-DD HH24:MI:SS")))
        .isEqualTo(LocalDateTime.of(2026, 2, 28, 13, 45, 59));
    assertThat(
            dec(
                OracleType.TIMESTAMP,
                SqlLiteral.of(SqlLiteral.Kind.TO_TIMESTAMP, "2026-03-29 01:30:00.123456789")))
        .isEqualTo(LocalDateTime.of(2026, 3, 29, 1, 30, 0, 123456789));
    assertThat(
            dec(
                OracleType.TIMESTAMP,
                SqlLiteral.of(SqlLiteral.Kind.TO_TIMESTAMP, "2026-03-29 01:30:00")))
        .isEqualTo(LocalDateTime.of(2026, 3, 29, 1, 30, 0));
    assertThat(
            dec(
                OracleType.TIMESTAMP_TZ,
                SqlLiteral.of(
                    SqlLiteral.Kind.TO_TIMESTAMP_TZ, "2026-03-29 01:30:00.500000000 +05:30")))
        .isEqualTo(
            OffsetDateTime.of(
                2026, 3, 29, 1, 30, 0, 500_000_000, ZoneOffset.ofHoursMinutes(5, 30)));
    assertThat(
            dec(
                OracleType.TIMESTAMP_LTZ,
                SqlLiteral.of(SqlLiteral.Kind.TO_TIMESTAMP_TZ, "2026-10-25 02:30:00.250000000")))
        .isEqualTo(Instant.parse("2026-10-25T02:30:00.250Z"));
    assertThat(
            dec(OracleType.INTERVAL_YM, SqlLiteral.of(SqlLiteral.Kind.TO_YMINTERVAL, "+0012-03")))
        .isEqualTo(Period.of(12, 3, 0));
    assertThat(
            dec(OracleType.INTERVAL_YM, SqlLiteral.of(SqlLiteral.Kind.TO_YMINTERVAL, "-0001-11")))
        .isEqualTo(Period.of(-1, -11, 0));
    assertThat(
            dec(
                OracleType.INTERVAL_DS,
                SqlLiteral.of(SqlLiteral.Kind.TO_DSINTERVAL, "+00005 04:03:02.123456")))
        .isEqualTo(
            Duration.ofDays(5).plusHours(4).plusMinutes(3).plusSeconds(2).plusNanos(123_456_000));
    assertThat(
            dec(
                OracleType.INTERVAL_DS,
                SqlLiteral.of(SqlLiteral.Kind.TO_DSINTERVAL, "-00000 00:00:01")))
        .isEqualTo(Duration.ofSeconds(-1));
    assertThat(
            (byte[]) dec(OracleType.RAW, SqlLiteral.of(SqlLiteral.Kind.HEXTORAW, "deadbeef00ff")))
        .containsExactly(0xde, 0xad, 0xbe, 0xef, 0x00, 0xff);
    assertThat(
            (byte[]) dec(OracleType.BLOB, new SqlLiteral(SqlLiteral.Kind.EMPTY_BLOB, null, null)))
        .isEmpty();
    assertThat((byte[]) dec(OracleType.BLOB, SqlLiteral.of(SqlLiteral.Kind.HEXTORAW, "cafebabe")))
        .hasSize(4);
    assertThat(dec(OracleType.CLOB, new SqlLiteral(SqlLiteral.Kind.EMPTY_CLOB, null, null)))
        .isEqualTo("");
    assertThat(dec(OracleType.CLOB, SqlLiteral.string("short clob"))).isEqualTo("short clob");
    assertThat(dec(OracleType.VARCHAR2, SqlLiteral.NULL)).isNull();
  }

  @Test
  void refusesWrongMasksKindsAndUnsupportedTypes() {
    assertThatThrownBy(
            () ->
                dec(
                    OracleType.DATE,
                    new SqlLiteral(SqlLiteral.Kind.TO_DATE, "28-FEB-26", "DD-MON-RR")))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining("NLS_DATE_FORMAT");
    assertThatThrownBy(() -> dec(OracleType.NUMBER, SqlLiteral.string("abc")))
        .isInstanceOf(DecodeException.class);
    assertThatThrownBy(() -> dec(OracleType.TIMESTAMP, SqlLiteral.string("2026-01-01 00:00:00")))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining("expected TO_TIMESTAMP");
    assertThatThrownBy(() -> dec(OracleType.RAW, SqlLiteral.of(SqlLiteral.Kind.HEXTORAW, "abc")))
        .isInstanceOf(DecodeException.class);
    assertThatThrownBy(() -> dec(OracleType.RAW, SqlLiteral.of(SqlLiteral.Kind.HEXTORAW, "zz")))
        .isInstanceOf(DecodeException.class);
    assertThatThrownBy(
            () ->
                dec(OracleType.INTERVAL_DS, SqlLiteral.of(SqlLiteral.Kind.TO_DSINTERVAL, "5 days")))
        .isInstanceOf(DecodeException.class);
    assertThatThrownBy(
            () -> dec(OracleType.INTERVAL_YM, SqlLiteral.of(SqlLiteral.Kind.TO_YMINTERVAL, "12")))
        .isInstanceOf(DecodeException.class);
    assertThatThrownBy(
            () -> dec(OracleType.VARCHAR2, SqlLiteral.of(SqlLiteral.Kind.HEXTORAW, "00")))
        .isInstanceOf(DecodeException.class);
    assertThatThrownBy(() -> dec(OracleType.BOOLEAN, SqlLiteral.string("1")))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining("does not support");
    assertThatThrownBy(() -> dec(OracleType.UNKNOWN, SqlLiteral.string("1")))
        .isInstanceOf(DecodeException.class);
  }
}
