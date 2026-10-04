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

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneOffset;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfig.DecimalMode;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfig.TemporalMode;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.OracleType;

class TypeToConnectTest {

  static ColumnSpec col(OracleType t, int precision, int scale) {
    return new ColumnSpec("C", 1, t, t.name(), 0, precision, scale, true);
  }

  @Test
  void preciseNumbersFollowDebeziumSizing() {
    TypeToConnect m = new TypeToConnect(DecimalMode.PRECISE, TemporalMode.ADAPTIVE);
    assertThat(m.schema(col(OracleType.NUMBER, 2, 0)).type()).isEqualTo(Schema.Type.INT8);
    assertThat(m.schema(col(OracleType.NUMBER, 4, 0)).type()).isEqualTo(Schema.Type.INT16);
    assertThat(m.schema(col(OracleType.NUMBER, 9, 0)).type()).isEqualTo(Schema.Type.INT32);
    assertThat(m.schema(col(OracleType.NUMBER, 18, 0)).type()).isEqualTo(Schema.Type.INT64);
    Schema dec = m.schema(col(OracleType.NUMBER, 10, 2));
    assertThat(dec.name()).isEqualTo(Decimal.LOGICAL_NAME);
    assertThat(dec.parameters()).containsEntry(Decimal.SCALE_FIELD, "2");
    assertThat(m.value(col(OracleType.NUMBER, 10, 2), dec, new BigDecimal("-42.5")))
        .isEqualTo(new BigDecimal("-42.50"));
    assertThat(
            m.value(
                col(OracleType.NUMBER, 9, 0),
                m.schema(col(OracleType.NUMBER, 9, 0)),
                new BigDecimal("7")))
        .isEqualTo(7);
    assertThat(
            m.value(
                col(OracleType.NUMBER, 18, 0),
                m.schema(col(OracleType.NUMBER, 18, 0)),
                new BigDecimal("7")))
        .isEqualTo(7L);
    Schema vsd = m.schema(col(OracleType.NUMBER, -1, -1));
    assertThat(vsd.name()).isEqualTo(TypeToConnect.VARIABLE_SCALE_DECIMAL);
    Struct v =
        (Struct)
            m.value(col(OracleType.NUMBER, -1, -1), vsd, new BigDecimal("1234567890.123456789"));
    assertThat(v.getInt32("scale")).isEqualTo(9);
    assertThat(new BigDecimal(new java.math.BigInteger(((ByteBuffer) v.get("value")).array()), 9))
        .isEqualByComparingTo("1234567890.123456789");
    Struct trailing =
        (Struct) m.value(col(OracleType.FLOAT, -1, -1), vsd, new BigDecimal("100.00"));
    assertThat(trailing.getInt32("scale")).isEqualTo(0);
    assertThat(m.schema(col(OracleType.NUMBER, -1, -1)).isOptional()).isTrue();
  }

  @Test
  void stringAndDoubleDecimalModes() {
    TypeToConnect s = new TypeToConnect(DecimalMode.STRING, TemporalMode.ADAPTIVE);
    assertThat(s.schema(col(OracleType.NUMBER, 10, 2)).type()).isEqualTo(Schema.Type.STRING);
    assertThat(
            s.value(
                col(OracleType.NUMBER, 10, 2),
                s.schema(col(OracleType.NUMBER, 10, 2)),
                new BigDecimal("1E+3")))
        .isEqualTo("1000");
    TypeToConnect d = new TypeToConnect(DecimalMode.DOUBLE, TemporalMode.ADAPTIVE);
    assertThat(
            d.value(
                col(OracleType.NUMBER, 10, 2),
                d.schema(col(OracleType.NUMBER, 10, 2)),
                new BigDecimal("2.5")))
        .isEqualTo(2.5d);
  }

  @Test
  void temporalsAdaptiveAndIso() {
    TypeToConnect a = new TypeToConnect(DecimalMode.PRECISE, TemporalMode.ADAPTIVE);
    LocalDateTime t = LocalDateTime.of(2026, 3, 29, 1, 30, 0, 123_456_789);
    Schema date = a.schema(col(OracleType.DATE, -1, -1));
    assertThat(date.name()).isEqualTo(TypeToConnect.TIMESTAMP);
    assertThat(a.value(col(OracleType.DATE, -1, -1), date, t))
        .isEqualTo(t.toInstant(ZoneOffset.UTC).toEpochMilli());
    Schema ts3 = a.schema(col(OracleType.TIMESTAMP, -1, 3));
    assertThat(ts3.name()).isEqualTo(TypeToConnect.TIMESTAMP);
    Schema ts6 = a.schema(col(OracleType.TIMESTAMP, -1, 6));
    assertThat(ts6.name()).isEqualTo(TypeToConnect.MICRO_TIMESTAMP);
    assertThat(a.value(col(OracleType.TIMESTAMP, -1, 6), ts6, t))
        .isEqualTo(t.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000L + 123_456L);
    Schema ts9 = a.schema(col(OracleType.TIMESTAMP, -1, 9));
    assertThat(ts9.name()).isEqualTo(TypeToConnect.NANO_TIMESTAMP);
    assertThat(a.value(col(OracleType.TIMESTAMP, -1, 9), ts9, t))
        .isEqualTo(t.toInstant(ZoneOffset.UTC).getEpochSecond() * 1_000_000_000L + 123_456_789L);
    Schema tz = a.schema(col(OracleType.TIMESTAMP_TZ, -1, 6));
    assertThat(tz.name()).isEqualTo(TypeToConnect.ZONED_TIMESTAMP);
    assertThat(
            a.value(
                col(OracleType.TIMESTAMP_TZ, -1, 6),
                tz,
                OffsetDateTime.of(
                    2026, 3, 29, 1, 30, 0, 500_000_000, ZoneOffset.ofHoursMinutes(5, 30))))
        .isEqualTo("2026-03-29T01:30:00.5+05:30");
    assertThat(
            a.value(
                col(OracleType.TIMESTAMP_LTZ, -1, 6), tz, Instant.parse("2026-10-25T02:30:00.25Z")))
        .isEqualTo("2026-10-25T02:30:00.25Z");
    Schema dur = a.schema(col(OracleType.INTERVAL_DS, -1, -1));
    assertThat(dur.name()).isEqualTo(TypeToConnect.MICRO_DURATION);
    assertThat(a.value(col(OracleType.INTERVAL_DS, -1, -1), dur, Duration.ofSeconds(90)))
        .isEqualTo(90_000_000d);
    assertThat((Double) a.value(col(OracleType.INTERVAL_YM, -1, -1), dur, Period.of(1, 0, 0)))
        .isCloseTo(12 * TypeToConnect.MICROS_PER_MONTH, org.assertj.core.data.Offset.offset(1d));
    TypeToConnect iso = new TypeToConnect(DecimalMode.PRECISE, TemporalMode.ISO_STRING);
    assertThat(iso.schema(col(OracleType.DATE, -1, -1)).type()).isEqualTo(Schema.Type.STRING);
    assertThat(iso.value(col(OracleType.DATE, -1, -1), Schema.OPTIONAL_STRING_SCHEMA, t))
        .isEqualTo("2026-03-29T01:30:00.123456789");
    assertThat(
            iso.value(
                col(OracleType.INTERVAL_YM, -1, -1),
                Schema.OPTIONAL_STRING_SCHEMA,
                Period.of(12, 3, 0)))
        .isEqualTo("P12Y3M");
    assertThat(
            iso.value(
                col(OracleType.INTERVAL_DS, -1, -1),
                Schema.OPTIONAL_STRING_SCHEMA,
                Duration.ofSeconds(5)))
        .isEqualTo("PT5S");
  }

  @Test
  void stringsBinariesAndFloats() {
    TypeToConnect m = new TypeToConnect(DecimalMode.PRECISE, TemporalMode.ADAPTIVE);
    assertThat(m.schema(col(OracleType.VARCHAR2, -1, -1)).type()).isEqualTo(Schema.Type.STRING);
    assertThat(m.schema(col(OracleType.CLOB, -1, -1)).type()).isEqualTo(Schema.Type.STRING);
    assertThat(m.schema(col(OracleType.RAW, -1, -1)).type()).isEqualTo(Schema.Type.BYTES);
    assertThat(
            ((ByteBuffer)
                    m.value(
                        col(OracleType.RAW, -1, -1),
                        Schema.OPTIONAL_BYTES_SCHEMA,
                        new byte[] {1, 2}))
                .array())
        .containsExactly(1, 2);
    assertThat(m.schema(col(OracleType.BINARY_FLOAT, -1, -1)).type())
        .isEqualTo(Schema.Type.FLOAT32);
    assertThat(m.schema(col(OracleType.BINARY_DOUBLE, -1, -1)).type())
        .isEqualTo(Schema.Type.FLOAT64);
    assertThat(m.value(col(OracleType.BINARY_FLOAT, -1, -1), Schema.OPTIONAL_FLOAT32_SCHEMA, 1.5f))
        .isEqualTo(1.5f);
    assertThat(m.value(col(OracleType.VARCHAR2, -1, -1), Schema.OPTIONAL_STRING_SCHEMA, null))
        .isNull();
    assertThat(m.value(col(OracleType.VARCHAR2, -1, -1), Schema.OPTIONAL_STRING_SCHEMA, "x"))
        .isEqualTo("x");
    ColumnSpec notNull = new ColumnSpec("ID", 1, OracleType.NUMBER, "NUMBER", 0, 9, 0, false);
    assertThat(m.schema(notNull).isOptional()).isFalse();
  }
}
