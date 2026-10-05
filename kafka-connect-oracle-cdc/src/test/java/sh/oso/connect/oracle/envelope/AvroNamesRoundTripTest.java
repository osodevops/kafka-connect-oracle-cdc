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

import io.apicurio.registry.utils.converter.avro.AvroData;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
import org.apache.avro.Schema.Parser;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.KeySource;
import sh.oso.connect.oracle.core.schema.OracleType;
import sh.oso.connect.oracle.core.schema.TableSchema;

/**
 * ADR-0020: records of a table whose names Avro rejects convert to Avro and back with the Avro
 * converter's AvroData (Apicurio Registry, Apache-2.0) once the names are adjusted, for every
 * column type TypeRoundTripEngineIT covers; without adjustment the conversion fails on the names.
 * The schema text is parsed again by a fresh Avro parser and each record goes through Avro binary
 * encoding, as a converter would write and read it.
 */
class AvroNamesRoundTripTest {

  /** A quoted lower-case table name with # and $, and a topic prefix with a hyphen. */
  static final TableId TABLE = new TableId("FREEPDB1", "APP", "order#items$");

  static ColumnSpec col(String name, int pos, OracleType type, int precision, int scale) {
    return new ColumnSpec(name, pos, type, type.name(), 0, precision, scale, pos > 1);
  }

  static final TableSchema SCHEMA =
      new TableSchema(
          TABLE,
          List.of(
              col("ID#", 1, OracleType.NUMBER, 9, 0),
              col("c_varchar$", 2, OracleType.VARCHAR2, -1, -1),
              col("C CHAR", 3, OracleType.CHAR, -1, -1),
              col("C/NVARCHAR", 4, OracleType.NVARCHAR2, -1, -1),
              col("c-nchar", 5, OracleType.NCHAR, -1, -1),
              col("C_NUMBER", 6, OracleType.NUMBER, -1, -1),
              col("C_NUMBER#S", 7, OracleType.NUMBER, 10, 2),
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
              col("2ND_VALUE", 19, OracleType.VARCHAR2, -1, -1)),
          List.of("ID#"),
          KeySource.PRIMARY_KEY,
          true,
          false);

  static Map<String, Object> row(int id) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("ID#", new BigDecimal(id));
    m.put("c_varchar$", "plain 'quoted' text");
    m.put("C CHAR", "abc       ");
    m.put("C/NVARCHAR", "\u00fcn\u00efcode \u2603 text");
    m.put("c-nchar", "nch\u00e1r");
    m.put("C_NUMBER", new BigDecimal("1234567890.123456789"));
    m.put("C_NUMBER#S", new BigDecimal("-42.50"));
    m.put("C_FLOAT", new BigDecimal("3.14159"));
    m.put("C_BF", 1.5E10f);
    m.put("C_BD", 2.5d);
    m.put("C_DATE", LocalDateTime.of(2026, 2, 28, 13, 45, 59));
    m.put("C_TS", LocalDateTime.of(2026, 3, 29, 1, 30, 0, 123_456_000));
    m.put("C_TS9", LocalDateTime.of(2026, 3, 29, 1, 30, 0, 123_456_789));
    m.put(
        "C_TSTZ",
        OffsetDateTime.of(2026, 3, 29, 1, 30, 0, 500_000_000, ZoneOffset.ofHoursMinutes(5, 30)));
    m.put("C_TSLTZ", Instant.parse("2026-10-25T02:30:00.25Z"));
    m.put("C_IYM", Period.of(12, 3, 0));
    m.put("C_IDS", Duration.parse("P5DT4H3M2.123456S"));
    m.put("C_RAW", new byte[] {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef, 0, -1});
    m.put("2ND_VALUE", null);
    return m;
  }

  static List<SourceRecord> records(String schemaMode, String fieldMode) {
    DebeziumEnvelope e =
        DebeziumEnvelopeNamesTest.envelope(
            DebeziumEnvelopeNamesTest.config("my-cdc", schemaMode, fieldMode));
    Map<String, Object> changed = row(1);
    changed.put("c_varchar$", "changed");
    changed.put("C_NUMBER#S", new BigDecimal("1.00"));
    CommittedTransaction tx =
        DebeziumEnvelopeTest.tx(
            DebeziumEnvelopeNamesTest.change(TABLE, Operation.INSERT, null, row(1)),
            DebeziumEnvelopeNamesTest.change(TABLE, Operation.UPDATE, row(1), changed),
            DebeziumEnvelopeNamesTest.change(TABLE, Operation.DELETE, changed, null));
    List<SourceRecord> out = new java.util.ArrayList<>();
    for (int i = 0; i < 3; i++) {
      out.addAll(e.records(tx, i, i + 1, SCHEMA, DebeziumEnvelopeTest.offset()));
    }
    return out;
  }

  /** Connect to Avro, through the schema text and Avro binary, and back to Connect. */
  static SchemaAndValue roundTrip(AvroData avro, Schema schema, Object value) throws IOException {
    org.apache.avro.Schema written = avro.fromConnectSchema(schema);
    org.apache.avro.Schema parsed = new Parser().parse(written.toString());
    Object datum = avro.fromConnectData(schema, value);
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    BinaryEncoder enc = EncoderFactory.get().binaryEncoder(bytes, null);
    new GenericDatumWriter<Object>(written).write(datum, enc);
    enc.flush();
    Object read =
        new GenericDatumReader<Object>(written, parsed)
            .read(null, DecoderFactory.get().binaryDecoder(bytes.toByteArray(), null));
    return avro.toConnectData(parsed, read);
  }

  @Test
  void avroModeConvertsEverySchemaAndRecordToAvroAndBack() throws IOException {
    AvroData avro = new AvroData(100);
    List<SourceRecord> out = records("avro", "avro");
    assertThat(out).hasSize(4);
    for (SourceRecord r : out) {
      SchemaAndValue key = roundTrip(avro, r.keySchema(), r.key());
      assertThat(key.schema().name()).isEqualTo("my_cdc.FREEPDB1.APP.order_items_.Key");
      assertThat(key.value()).isEqualTo(r.key());
      if (r.value() == null) {
        continue; // the tombstone
      }
      SchemaAndValue value = roundTrip(avro, r.valueSchema(), r.value());
      assertThat(value.schema().name()).isEqualTo("my_cdc.FREEPDB1.APP.order_items_.Envelope");
      assertThat(value.schema()).isEqualTo(r.valueSchema());
      assertThat(value.value()).isEqualTo(r.value());
    }
    Struct after = ((Struct) out.get(0).value()).getStruct("after");
    assertThat(after.schema().fields().stream().map(f -> f.name()).toList())
        .containsExactly(
            "ID_",
            "c_varchar_",
            "C_CHAR",
            "C_NVARCHAR",
            "c_nchar",
            "C_NUMBER",
            "C_NUMBER_S",
            "C_FLOAT",
            "C_BF",
            "C_BD",
            "C_DATE",
            "C_TS",
            "C_TS9",
            "C_TSTZ",
            "C_TSLTZ",
            "C_IYM",
            "C_IDS",
            "C_RAW",
            "_2ND_VALUE");
    assertThat(after.getString("C_NVARCHAR")).isEqualTo("\u00fcn\u00efcode \u2603 text");
    assertThat(after.get("C_NUMBER_S")).isEqualTo(new BigDecimal("-42.50"));
  }

  @Test
  void avroUnicodeModeConvertsToAvroAndBackToo() throws IOException {
    AvroData avro = new AvroData(100);
    SourceRecord r = records("avro_unicode", "avro_unicode").get(1);
    SchemaAndValue value = roundTrip(avro, r.valueSchema(), r.value());
    assertThat(value.schema().name())
        .isEqualTo("my_u002dcdc.FREEPDB1.APP.order_u0023items_u0024.Envelope");
    assertThat(value.value()).isEqualTo(r.value());
  }

  @Test
  void withoutAdjustmentTheAvroConverterRejectsTheNames() {
    AvroData avro = new AvroData(100);
    SourceRecord raw = records("none", "none").get(0);
    assertThatThrownBy(() -> avro.fromConnectSchema(raw.valueSchema()))
        .isInstanceOf(SchemaParseException.class)
        .hasMessageContaining("ID#");
    assertThatThrownBy(() -> avro.fromConnectSchema(raw.keySchema()))
        .as("the key field")
        .isInstanceOf(SchemaParseException.class)
        .hasMessageContaining("ID#");
    SourceRecord schemasOnly = records("avro", "none").get(0);
    assertThatThrownBy(() -> avro.fromConnectSchema(schemasOnly.valueSchema()))
        .as("valid schema names do not make the field names valid")
        .isInstanceOf(SchemaParseException.class);
    // AvroData builds a record with an invalid namespace without complaint, but the schema text
    // it produces cannot be parsed back, so a schema registry refuses to register it
    SourceRecord fieldsOnly = records("none", "avro").get(0);
    String text = avro.fromConnectSchema(fieldsOnly.valueSchema()).toString();
    assertThatThrownBy(() -> new Parser().parse(text))
        .as("valid field names do not make the schema name valid")
        .isInstanceOf(SchemaParseException.class)
        .hasMessageContaining("Namespace part \"my-cdc\" is invalid");
    assertThatThrownBy(() -> roundTrip(avro, fieldsOnly.valueSchema(), fieldsOnly.value()))
        .isInstanceOf(SchemaParseException.class);
  }
}
