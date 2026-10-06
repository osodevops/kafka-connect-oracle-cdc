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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/**
 * Reads the values of a Debezium-format record that an Avro deserializer returned back into the
 * Java values the engine decodes ({@code BigDecimal}, {@code LocalDateTime}, {@code
 * OffsetDateTime}, {@code byte[]}, {@code String}), so a suite can compare them with the database
 * as {@code TypeRoundTripEngineIT} compares decoded rows. A consumer sees only the Avro schema, so
 * the reading follows the schema: its type, its logical type, and the {@code connect.name} the
 * converter keeps for Debezium's semantic types. Intervals ({@code io.debezium.time.MicroDuration})
 * stay a double of microseconds.
 */
public final class AvroValues {

  /** The property the Connect Avro converters use for the Connect schema name. */
  public static final String CONNECT_NAME = "connect.name";

  public static final String VARIABLE_SCALE_DECIMAL = "io.debezium.data.VariableScaleDecimal";
  public static final String DECIMAL = "org.apache.kafka.connect.data.Decimal";
  public static final String TIMESTAMP = "io.debezium.time.Timestamp";
  public static final String MICRO_TIMESTAMP = "io.debezium.time.MicroTimestamp";
  public static final String NANO_TIMESTAMP = "io.debezium.time.NanoTimestamp";
  public static final String ZONED_TIMESTAMP = "io.debezium.time.ZonedTimestamp";
  public static final String MICRO_DURATION = "io.debezium.time.MicroDuration";

  private AvroValues() {}

  /** The schema without its null branch, for the optional fields of a row. */
  public static Schema nonNull(Schema s) {
    if (s.getType() != Schema.Type.UNION) {
      return s;
    }
    for (Schema branch : s.getTypes()) {
      if (branch.getType() != Schema.Type.NULL) {
        return branch;
      }
    }
    throw new IllegalArgumentException("union without a non-null branch: " + s);
  }

  /** The Connect schema name the converter recorded for a type, or null for a plain type. */
  public static String connectName(Schema s) {
    Schema t = nonNull(s);
    String name = t.getProp(CONNECT_NAME);
    if (name == null && t.getType() == Schema.Type.RECORD) {
      name = t.getFullName();
    }
    return name;
  }

  /** Every field of a row record, by field name, read with {@link #decode}; null for no row. */
  public static Map<String, Object> row(Object record) {
    if (record == null) {
      return null;
    }
    GenericRecord r = (GenericRecord) record;
    Map<String, Object> out = new LinkedHashMap<>();
    for (Schema.Field f : r.getSchema().getFields()) {
      out.put(f.name(), decode(r.get(f.pos()), f.schema()));
    }
    return out;
  }

  /** One field of a record read with {@link #decode}; fails when the record has no such field. */
  public static Object field(GenericRecord r, String name) {
    Schema.Field f = r.getSchema().getField(name);
    if (f == null) {
      throw new AssertionError(
          "no field " + name + " in " + r.getSchema().getFullName() + ": " + r.getSchema());
    }
    return decode(r.get(f.pos()), f.schema());
  }

  /** A value as the engine decoded it, read through the field's Avro schema. */
  public static Object decode(Object value, Schema schema) {
    if (value == null) {
      return null;
    }
    Schema t = nonNull(schema);
    String name = connectName(t);
    switch (t.getType()) {
      case RECORD:
        if (VARIABLE_SCALE_DECIMAL.equals(name)) {
          GenericRecord r = (GenericRecord) value;
          return new BigDecimal(
              new BigInteger(bytes(r.get("value"))), ((Number) r.get("scale")).intValue());
        }
        return value;
      case BYTES:
        {
          Integer scale = decimalScale(t, name);
          if (scale != null) {
            return value instanceof BigDecimal d
                ? d
                : new BigDecimal(new BigInteger(bytes(value)), scale);
          }
          return bytes(value);
        }
      case LONG:
        {
          long n = ((Number) value).longValue();
          if (TIMESTAMP.equals(name)) {
            return utc(Instant.ofEpochMilli(n));
          }
          if (MICRO_TIMESTAMP.equals(name)) {
            return utc(
                Instant.ofEpochSecond(
                    Math.floorDiv(n, 1_000_000L), Math.floorMod(n, 1_000_000L) * 1_000L));
          }
          if (NANO_TIMESTAMP.equals(name)) {
            return utc(
                Instant.ofEpochSecond(
                    Math.floorDiv(n, 1_000_000_000L), Math.floorMod(n, 1_000_000_000L)));
          }
          return n;
        }
      case STRING:
        return ZONED_TIMESTAMP.equals(name)
            ? OffsetDateTime.parse(value.toString())
            : value.toString();
      default:
        return value; // int, float, double (MicroDuration included), boolean
    }
  }

  /**
   * Whether two decoded values are the same value: decimals by value (scale aside), byte arrays by
   * content, time zone values by instant, anything else by equals.
   */
  public static boolean same(Object a, Object b) {
    if (a instanceof BigDecimal x && b instanceof BigDecimal y) {
      return x.compareTo(y) == 0;
    }
    if (a instanceof byte[] x && b instanceof byte[] y) {
      return Arrays.equals(x, y);
    }
    if (a instanceof OffsetDateTime x && b instanceof OffsetDateTime y) {
      return x.toInstant().equals(y.toInstant());
    }
    return Objects.equals(a, b);
  }

  private static Integer decimalScale(Schema t, String name) {
    LogicalType logical = t.getLogicalType();
    if (logical instanceof LogicalTypes.Decimal d) {
      return d.getScale();
    }
    if (DECIMAL.equals(name)) {
      Object params = t.getObjectProp("connect.parameters");
      if (params instanceof Map<?, ?> m && m.get("scale") != null) {
        return Integer.parseInt(m.get("scale").toString());
      }
    }
    return null;
  }

  private static LocalDateTime utc(Instant i) {
    return LocalDateTime.ofInstant(i, ZoneOffset.UTC);
  }

  /** The bytes of an Avro {@code bytes} value (a ByteBuffer, read without moving it). */
  public static byte[] bytes(Object v) {
    if (v instanceof byte[] b) {
      return b;
    }
    ByteBuffer buf = ((ByteBuffer) v).duplicate();
    byte[] out = new byte[buf.remaining()];
    buf.get(out);
    return out;
  }
}
