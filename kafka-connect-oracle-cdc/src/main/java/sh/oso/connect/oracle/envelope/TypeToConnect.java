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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfig.DecimalMode;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfig.TemporalMode;
import sh.oso.connect.oracle.core.schema.ColumnSpec;

/**
 * Oracle column types to Connect schemas and decoded Java values to Connect values (SRC-FMT-5),
 * following the Debezium Oracle connector's names so existing consumers keep working.
 */
public final class TypeToConnect {

  public static final String VARIABLE_SCALE_DECIMAL = "io.debezium.data.VariableScaleDecimal";
  public static final String TIMESTAMP = "io.debezium.time.Timestamp";
  public static final String MICRO_TIMESTAMP = "io.debezium.time.MicroTimestamp";
  public static final String NANO_TIMESTAMP = "io.debezium.time.NanoTimestamp";
  public static final String ZONED_TIMESTAMP = "io.debezium.time.ZonedTimestamp";
  public static final String MICRO_DURATION = "io.debezium.time.MicroDuration";

  /** Debezium's average month for INTERVAL YEAR TO MONTH as microseconds: 365.25 / 12 days. */
  static final double MICROS_PER_MONTH = 365.25 / 12 * 24 * 60 * 60 * 1_000_000d;

  private final DecimalMode decimals;
  private final TemporalMode temporals;

  public TypeToConnect(DecimalMode decimals, TemporalMode temporals) {
    this.decimals = decimals;
    this.temporals = temporals;
  }

  public Schema schema(ColumnSpec c) {
    SchemaBuilder b = builder(c);
    if (c.nullable()) {
      b.optional();
    }
    return b.build();
  }

  private SchemaBuilder builder(ColumnSpec c) {
    switch (c.type()) {
      case VARCHAR2:
      case CHAR:
      case NVARCHAR2:
      case NCHAR:
      case CLOB:
      case NCLOB:
      case XMLTYPE:
      case LONG:
      case ROWID:
      case UROWID:
        return SchemaBuilder.string();
      case RAW:
      case LONG_RAW:
      case BLOB:
        return SchemaBuilder.bytes();
      case BINARY_FLOAT:
        return SchemaBuilder.float32();
      case BINARY_DOUBLE:
        return SchemaBuilder.float64();
      case NUMBER:
      case FLOAT:
        return numberBuilder(c);
      case DATE:
        return temporals == TemporalMode.ISO_STRING
            ? SchemaBuilder.string()
            : SchemaBuilder.int64().name(TIMESTAMP).version(1);
      case TIMESTAMP:
        if (temporals == TemporalMode.ISO_STRING) {
          return SchemaBuilder.string();
        }
        int p = c.scale() < 0 ? 6 : c.scale();
        return SchemaBuilder.int64()
            .name(p <= 3 ? TIMESTAMP : p <= 6 ? MICRO_TIMESTAMP : NANO_TIMESTAMP)
            .version(1);
      case TIMESTAMP_TZ:
      case TIMESTAMP_LTZ:
        return temporals == TemporalMode.ISO_STRING
            ? SchemaBuilder.string()
            : SchemaBuilder.string().name(ZONED_TIMESTAMP).version(1);
      case INTERVAL_YM:
      case INTERVAL_DS:
        return temporals == TemporalMode.ISO_STRING
            ? SchemaBuilder.string()
            : SchemaBuilder.float64().name(MICRO_DURATION).version(1);
      default:
        throw new DataException(
            "Column " + c.name() + " has type " + c.typeText() + " which has no Connect mapping");
    }
  }

  private SchemaBuilder numberBuilder(ColumnSpec c) {
    switch (decimals) {
      case STRING:
        return SchemaBuilder.string();
      case DOUBLE:
        return SchemaBuilder.float64();
      default:
        break;
    }
    if (c.type() == sh.oso.connect.oracle.core.schema.OracleType.FLOAT || c.precision() < 0) {
      return variableScaleDecimal();
    }
    int scale = Math.max(0, c.scale());
    if (scale == 0) {
      if (c.precision() < 3) {
        return SchemaBuilder.int8();
      }
      if (c.precision() < 5) {
        return SchemaBuilder.int16();
      }
      if (c.precision() < 10) {
        return SchemaBuilder.int32();
      }
      if (c.precision() < 19) {
        return SchemaBuilder.int64();
      }
    }
    return Decimal.builder(scale);
  }

  static SchemaBuilder variableScaleDecimal() {
    return SchemaBuilder.struct()
        .name(VARIABLE_SCALE_DECIMAL)
        .version(1)
        .field("scale", Schema.INT32_SCHEMA)
        .field("value", Schema.BYTES_SCHEMA);
  }

  /** Converts a decoded value for {@code c} to the value of {@link #schema(ColumnSpec)}. */
  public Object value(ColumnSpec c, Schema schema, Object v) {
    if (v == null) {
      return null;
    }
    switch (c.type()) {
      case NUMBER:
      case FLOAT:
        return number(schema, (BigDecimal) v);
      case BINARY_FLOAT:
        return v instanceof Float ? v : ((Number) v).floatValue();
      case BINARY_DOUBLE:
        return v instanceof Double ? v : ((Number) v).doubleValue();
      case DATE:
      case TIMESTAMP:
        {
          LocalDateTime t = (LocalDateTime) v;
          if (temporals == TemporalMode.ISO_STRING) {
            return t.toString();
          }
          Instant i = t.toInstant(ZoneOffset.UTC);
          String name = schema.name();
          if (MICRO_TIMESTAMP.equals(name)) {
            return i.getEpochSecond() * 1_000_000L + i.getNano() / 1_000L;
          }
          if (NANO_TIMESTAMP.equals(name)) {
            return i.getEpochSecond() * 1_000_000_000L + i.getNano();
          }
          return i.toEpochMilli();
        }
      case TIMESTAMP_TZ:
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format((OffsetDateTime) v);
      case TIMESTAMP_LTZ:
        return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(
            ((Instant) v).atOffset(ZoneOffset.UTC));
      case INTERVAL_YM:
        {
          Period p = (Period) v;
          if (temporals == TemporalMode.ISO_STRING) {
            return p.toString();
          }
          return p.toTotalMonths() * MICROS_PER_MONTH + p.getDays() * 86_400_000_000d;
        }
      case INTERVAL_DS:
        {
          Duration d = (Duration) v;
          if (temporals == TemporalMode.ISO_STRING) {
            return d.toString();
          }
          return (double) TimeUnit.NANOSECONDS.toMicros(d.toNanos());
        }
      case RAW:
      case LONG_RAW:
      case BLOB:
        return ByteBuffer.wrap((byte[]) v);
      default:
        return v.toString();
    }
  }

  private Object number(Schema schema, BigDecimal d) {
    if (schema.type() == Schema.Type.STRING) {
      return d.toPlainString();
    }
    if (schema.type() == Schema.Type.FLOAT64) {
      return d.doubleValue();
    }
    if (VARIABLE_SCALE_DECIMAL.equals(schema.name())) {
      BigDecimal stripped = d.stripTrailingZeros();
      if (stripped.scale() < 0) {
        stripped = stripped.setScale(0);
      }
      return new Struct(schema)
          .put("scale", stripped.scale())
          .put("value", ByteBuffer.wrap(stripped.unscaledValue().toByteArray()));
    }
    if (Decimal.LOGICAL_NAME.equals(schema.name())) {
      int scale = Integer.parseInt(schema.parameters().get(Decimal.SCALE_FIELD));
      return d.setScale(scale, RoundingMode.UNNECESSARY);
    }
    switch (schema.type()) {
      case INT8:
        return d.byteValueExact();
      case INT16:
        return d.shortValueExact();
      case INT32:
        return d.intValueExact();
      case INT64:
        return d.longValueExact();
      default:
        throw new DataException("cannot map a NUMBER value to " + schema.type());
    }
  }
}
