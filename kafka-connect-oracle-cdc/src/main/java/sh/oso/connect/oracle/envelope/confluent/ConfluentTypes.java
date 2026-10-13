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
package sh.oso.connect.oracle.envelope.confluent;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import sh.oso.connect.oracle.core.errors.DecodeException;
import sh.oso.connect.oracle.core.schema.ColumnSpec;

/**
 * Oracle column types as Confluent's Oracle CDC Source connector writes them (ADR-0028), as its
 * 2.15.2 build did on 13 October 2026 (oracle-cdc-core reference {@code
 * confluent-record-format.md}): NUMBER a Connect Decimal carrying {@code connect.decimal.precision}
 * (unconstrained NUMBER at the default scale, 127, and precision 38), FLOAT a double, character
 * types strings, DATE a Connect Date with the time dropped, every TIMESTAMP a Connect Timestamp in
 * UTC milliseconds, RAW bytes. BINARY_FLOAT, BINARY_DOUBLE and both INTERVAL types are left out, as
 * Confluent leaves them out; LOB columns are left out of the table record (they go to LOB topics).
 */
public final class ConfluentTypes {

  /** Confluent's {@code numeric.mapping}. */
  public enum NumericMapping {
    NONE,
    BEST_FIT_OR_DECIMAL,
    BEST_FIT_OR_DOUBLE,
    BEST_FIT_OR_STRING,
    PRECISION_ONLY
  }

  /** Confluent's {@code oracle.date.mapping}. */
  public enum DateMapping {
    DATE,
    TIMESTAMP
  }

  static final String PRECISION = "connect.decimal.precision";

  /** Precision Confluent gives an unconstrained NUMBER. */
  static final int MAX_PRECISION = 38;

  private final NumericMapping numeric;
  private final int defaultScale;
  private final DateMapping dates;

  public ConfluentTypes(NumericMapping numeric, int defaultScale, DateMapping dates) {
    this.numeric = numeric;
    this.defaultScale = defaultScale;
    this.dates = dates;
  }

  /** False for the types Confluent leaves out of the table record. */
  public boolean inRecord(ColumnSpec c) {
    switch (c.type()) {
      case BINARY_FLOAT:
      case BINARY_DOUBLE:
      case INTERVAL_YM:
      case INTERVAL_DS:
      case CLOB:
      case NCLOB:
      case BLOB:
      case XMLTYPE:
        return false;
      default:
        return true;
    }
  }

  /** True for the LOB types Confluent writes to LOB topics. */
  public static boolean lob(ColumnSpec c) {
    switch (c.type()) {
      case CLOB:
      case NCLOB:
      case BLOB:
      case XMLTYPE:
        return true;
      default:
        return false;
    }
  }

  /** The column's schema; key columns are required, every other column optional. */
  public Schema schema(ColumnSpec c, boolean key) {
    SchemaBuilder b = builder(c);
    if (!key && c.nullable()) {
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
      case LONG:
      case ROWID:
      case UROWID:
        return SchemaBuilder.string();
      case RAW:
      case LONG_RAW:
        return SchemaBuilder.bytes();
      case FLOAT:
        return SchemaBuilder.float64();
      case NUMBER:
        return number(c);
      case DATE:
        return dates == DateMapping.DATE ? Date.builder() : Timestamp.builder();
      case TIMESTAMP:
      case TIMESTAMP_TZ:
      case TIMESTAMP_LTZ:
        return Timestamp.builder();
      default:
        throw new DataException(
            "Column "
                + c.name()
                + " has type "
                + c.typeText()
                + ", which the Confluent format"
                + " leaves out");
    }
  }

  private SchemaBuilder number(ColumnSpec c) {
    int precision = c.precision() < 0 ? MAX_PRECISION : c.precision();
    if (numeric != NumericMapping.NONE) {
      boolean integer =
          c.precision() > 0
              && (numeric == NumericMapping.PRECISION_ONLY ? c.scale() == 0 : c.scale() <= 0);
      if (integer && precision < 19) {
        if (precision < 3) {
          return SchemaBuilder.int8();
        }
        if (precision < 5) {
          return SchemaBuilder.int16();
        }
        return precision < 10 ? SchemaBuilder.int32() : SchemaBuilder.int64();
      }
      if (numeric != NumericMapping.PRECISION_ONLY) {
        if (c.precision() < 0 || c.scale() > 0) {
          return SchemaBuilder.float64();
        }
        if (numeric == NumericMapping.BEST_FIT_OR_DOUBLE) {
          return SchemaBuilder.float64();
        }
        if (numeric == NumericMapping.BEST_FIT_OR_STRING) {
          return SchemaBuilder.string();
        }
      }
    }
    return Decimal.builder(scaleOf(c)).parameter(PRECISION, Integer.toString(precision));
  }

  /** The Decimal scale: the column's, or the default scale for an unconstrained NUMBER. */
  int scaleOf(ColumnSpec c) {
    if (c.precision() < 0 && c.scale() < 0) {
      return defaultScale; // NUMBER without precision or scale
    }
    return c.precision() < 0 ? Math.max(0, c.scale()) : c.scale();
  }

  /** A decoded value as the value of {@link #schema(ColumnSpec, boolean)}. */
  public Object value(ColumnSpec c, Schema schema, Object v) {
    if (v == null) {
      return null;
    }
    switch (c.type()) {
      case NUMBER:
        {
          BigDecimal number = (BigDecimal) v;
          if (!Decimal.LOGICAL_NAME.equals(schema.name())) {
            try {
              return switch (schema.type()) {
                case INT8 -> number.byteValueExact();
                case INT16 -> number.shortValueExact();
                case INT32 -> number.intValueExact();
                case INT64 -> number.longValueExact();
                case FLOAT64 -> number.doubleValue();
                case STRING -> number.stripTrailingZeros().toPlainString();
                default ->
                    throw new IllegalStateException("Unexpected NUMBER schema " + schema.type());
              };
            } catch (ArithmeticException e) {
              throw new DecodeException(
                  "NUMBER column "
                      + c.name()
                      + " does not fit its mapped integer type "
                      + schema.type()
                      + ".",
                  "Use cdc.output.confluent.numeric.mapping=none and a compatible Decimal schema"
                      + " before restarting; the connector never wraps an integer value.");
            }
          }
          int scale = Integer.parseInt(schema.parameters().get(Decimal.SCALE_FIELD));
          try {
            return number.setScale(scale, RoundingMode.UNNECESSARY);
          } catch (ArithmeticException e) {
            throw new DecodeException(
                "NUMBER column "
                    + c.name()
                    + " cannot be represented exactly at scale "
                    + scale
                    + ".",
                "Increase cdc.output.confluent.numeric.default.scale to represent every captured"
                    + " value exactly, and restart from the stored offset.");
          }
        }
      case FLOAT:
        return ((BigDecimal) v).doubleValue();
      case DATE:
        {
          LocalDateTime t = (LocalDateTime) v;
          if (dates == DateMapping.DATE) {
            // Confluent drops the time part of a DATE
            return new java.util.Date(t.toLocalDate().toEpochDay() * 86_400_000L);
          }
          return new java.util.Date(t.toInstant(ZoneOffset.UTC).toEpochMilli());
        }
      case TIMESTAMP:
        return new java.util.Date(((LocalDateTime) v).toInstant(ZoneOffset.UTC).toEpochMilli());
      case TIMESTAMP_TZ:
        return new java.util.Date(((OffsetDateTime) v).toInstant().toEpochMilli());
      case TIMESTAMP_LTZ:
        return new java.util.Date(((Instant) v).toEpochMilli());
      case RAW:
      case LONG_RAW:
        return ByteBuffer.wrap((byte[]) v);
      default:
        return v.toString();
    }
  }
}
