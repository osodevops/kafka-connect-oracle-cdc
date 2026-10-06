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
package sh.oso.connect.oracle.e2e.support;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import io.apicurio.registry.utils.converter.avro.AvroData;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfig.DecimalMode;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfig.TemporalMode;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.OracleType;
import sh.oso.connect.oracle.envelope.TypeToConnect;

/**
 * {@link AvroValues} reads back every column type of TypeRoundTripEngineIT as the engine decoded
 * it: the connector's own Connect mapping ({@link TypeToConnect}, the defaults), Apicurio's
 * AvroData, the schema text parsed again and Avro binary encoding, as a converter writes a record
 * and a deserializer reads it. No registry and no Docker; AvroConverterConnectorIT does the rest.
 */
class AvroValuesTest {

  static final double MICROS_PER_MONTH = 365.25 / 12 * 24 * 60 * 60 * 1_000_000d;

  static ColumnSpec col(String name, int pos, OracleType type, int precision, int scale) {
    return new ColumnSpec(name, pos, type, type.name(), 0, precision, scale, true);
  }

  static final List<ColumnSpec> COLUMNS =
      List.of(
          col("ID", 1, OracleType.NUMBER, -1, -1),
          col("C_VARCHAR", 2, OracleType.VARCHAR2, -1, -1),
          col("C_CHAR", 3, OracleType.CHAR, -1, -1),
          col("C_NVARCHAR", 4, OracleType.NVARCHAR2, -1, -1),
          col("C_NCHAR", 5, OracleType.NCHAR, -1, -1),
          col("C_NUMBER", 6, OracleType.NUMBER, -1, -1),
          col("C_NUMBER_S", 7, OracleType.NUMBER, 10, 2),
          col("C_FLOAT", 8, OracleType.FLOAT, 126, -1),
          col("C_BF", 9, OracleType.BINARY_FLOAT, -1, -1),
          col("C_BD", 10, OracleType.BINARY_DOUBLE, -1, -1),
          col("C_DATE", 11, OracleType.DATE, -1, -1),
          col("C_TS", 12, OracleType.TIMESTAMP, -1, 6),
          col("C_TS9", 13, OracleType.TIMESTAMP, -1, 9),
          col("C_TSTZ", 14, OracleType.TIMESTAMP_TZ, -1, 6),
          col("C_TSLTZ", 15, OracleType.TIMESTAMP_LTZ, -1, 6),
          col("C_IYM", 16, OracleType.INTERVAL_YM, 4, -1),
          col("C_IDS", 17, OracleType.INTERVAL_DS, 5, 6),
          col("C_RAW", 18, OracleType.RAW, -1, -1),
          col("C_NUMBER9", 19, OracleType.NUMBER, 9, 0));

  /** The values TypeRoundTripEngineIT decodes from its first insert, plus a NUMBER(9) column. */
  static Map<String, Object> decoded() {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("ID", new BigDecimal("1"));
    m.put("C_VARCHAR", "plain 'quoted' text");
    m.put("C_CHAR", "abc       ");
    m.put("C_NVARCHAR", "ünïcode ☃ text");
    m.put("C_NCHAR", "nchár     ");
    m.put("C_NUMBER", new BigDecimal("1234567890.123456789"));
    m.put("C_NUMBER_S", new BigDecimal("-42.5"));
    m.put("C_FLOAT", new BigDecimal("3.14159"));
    m.put("C_BF", 1.5E10f);
    m.put("C_BD", 2.5d);
    m.put("C_DATE", LocalDateTime.of(2026, 2, 28, 13, 45, 59));
    m.put("C_TS", LocalDateTime.of(2026, 3, 29, 1, 30, 0, 123_456_000));
    m.put("C_TS9", LocalDateTime.of(2026, 3, 29, 1, 30, 0, 123_456_789));
    m.put(
        "C_TSTZ",
        OffsetDateTime.of(2026, 3, 29, 1, 30, 0, 500_000_000, ZoneOffset.ofHoursMinutes(5, 30)));
    m.put("C_TSLTZ", Instant.parse("2026-10-25T02:30:00.250Z"));
    m.put("C_IYM", Period.of(12, 3, 0));
    m.put(
        "C_IDS",
        Duration.ofDays(5).plusHours(4).plusMinutes(3).plusSeconds(2).plusNanos(123_456_000));
    m.put("C_RAW", new byte[] {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef, 0, -1});
    m.put("C_NUMBER9", new BigDecimal("42"));
    return m;
  }

  /** A row struct as the envelope builds it, written and read as a converter pair would. */
  static GenericRecord throughAvro(Map<String, Object> values) throws Exception {
    TypeToConnect types = new TypeToConnect(DecimalMode.PRECISE, TemporalMode.ADAPTIVE);
    SchemaBuilder b = SchemaBuilder.struct().name("cdc.FREEPDB1.APP.ALLTYPES.Value").optional();
    for (ColumnSpec c : COLUMNS) {
      b.field(c.name(), types.schema(c));
    }
    Schema schema = b.build();
    Struct row = new Struct(schema);
    for (ColumnSpec c : COLUMNS) {
      Object v = values.get(c.name());
      row.put(c.name(), types.value(c, schema.field(c.name()).schema(), v));
    }
    AvroData avro = new AvroData(100);
    org.apache.avro.Schema written = avro.fromConnectSchema(schema);
    org.apache.avro.Schema parsed = new org.apache.avro.Schema.Parser().parse(written.toString());
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    BinaryEncoder enc = EncoderFactory.get().binaryEncoder(bytes, null);
    new GenericDatumWriter<Object>(written).write(avro.fromConnectData(schema, row), enc);
    enc.flush();
    return (GenericRecord)
        new GenericDatumReader<Object>(parsed)
            .read(null, DecoderFactory.get().binaryDecoder(bytes.toByteArray(), null));
  }

  @Test
  void readsEveryRoundTripTypeBackAsTheEngineDecodedIt() throws Exception {
    Map<String, Object> want = decoded();
    GenericRecord r = throughAvro(want);
    Map<String, Object> got = AvroValues.row(r);
    assertThat(got.keySet()).containsExactlyElementsOf(want.keySet());
    for (String f : List.of("ID", "C_NUMBER", "C_NUMBER_S", "C_FLOAT", "C_RAW", "C_TSTZ")) {
      assertThat(AvroValues.same(got.get(f), want.get(f)))
          .as("%s: %s against %s", f, got.get(f), want.get(f))
          .isTrue();
    }
    assertThat(got.get("C_TSTZ")).isEqualTo(want.get("C_TSTZ")); // the offset is kept as well
    for (String f :
        List.of(
            "C_VARCHAR",
            "C_CHAR",
            "C_NVARCHAR",
            "C_NCHAR",
            "C_BF",
            "C_BD",
            "C_DATE",
            "C_TS",
            "C_TS9")) {
      assertThat(got.get(f)).as(f).isEqualTo(want.get(f));
    }
    assertThat(((OffsetDateTime) got.get("C_TSLTZ")).toInstant()).isEqualTo(want.get("C_TSLTZ"));
    assertThat((Double) got.get("C_IYM")).isCloseTo(147 * MICROS_PER_MONTH, within(1.0));
    assertThat((Double) got.get("C_IDS")).isCloseTo(446_582_123_456d, within(1.0));
    assertThat(got.get("C_NUMBER9")).isEqualTo(42); // NUMBER(9) is a plain int32
  }

  @Test
  void keepsTheDebeziumSemanticNamesOnTheAvroTypes() throws Exception {
    org.apache.avro.Schema s = throughAvro(decoded()).getSchema();
    Map<String, String> want =
        Map.ofEntries(
            Map.entry("ID", AvroValues.VARIABLE_SCALE_DECIMAL),
            Map.entry("C_NUMBER", AvroValues.VARIABLE_SCALE_DECIMAL),
            Map.entry("C_FLOAT", AvroValues.VARIABLE_SCALE_DECIMAL),
            Map.entry("C_NUMBER_S", AvroValues.DECIMAL),
            Map.entry("C_DATE", AvroValues.TIMESTAMP),
            Map.entry("C_TS", AvroValues.MICRO_TIMESTAMP),
            Map.entry("C_TS9", AvroValues.NANO_TIMESTAMP),
            Map.entry("C_TSTZ", AvroValues.ZONED_TIMESTAMP),
            Map.entry("C_TSLTZ", AvroValues.ZONED_TIMESTAMP),
            Map.entry("C_IYM", AvroValues.MICRO_DURATION),
            Map.entry("C_IDS", AvroValues.MICRO_DURATION));
    want.forEach(
        (field, name) ->
            assertThat(AvroValues.connectName(s.getField(field).schema()))
                .as(field)
                .isEqualTo(name));
    assertThat(AvroValues.connectName(s.getField("C_VARCHAR").schema())).isNull();
  }

  @Test
  void readsNullsAsNull() throws Exception {
    Map<String, Object> nulls = new LinkedHashMap<>();
    for (ColumnSpec c : COLUMNS) {
      nulls.put(c.name(), null);
    }
    nulls.put("ID", new BigDecimal("2"));
    Map<String, Object> got = AvroValues.row(throughAvro(nulls));
    assertThat(got)
        .hasSize(COLUMNS.size())
        .containsEntry("C_RAW", null)
        .containsEntry("C_TSTZ", null);
    assertThat((BigDecimal) got.get("ID")).isEqualByComparingTo("2");
  }
}
