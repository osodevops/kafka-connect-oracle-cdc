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
package sh.oso.connect.oracle.bench.check;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Locale;

/**
 * The canonical text of a column value, from the database (JDBC) and from a record (JSON with
 * Debezium semantic types), so the two can be compared. Shared in spirit with verify_cutover.py
 * (ADR-0012): numbers lose trailing zeros, temporals become ISO text in UTC, binaries become hex.
 */
public final class Normaliser {

  /** What the oracle needs to know about a column to read its record value. */
  public record Column(String name, int jdbcType, String typeName, int precision, int scale) {
    boolean isTimestampTz() {
      return typeName.contains("WITH TIME ZONE") || typeName.contains("WITH LOCAL TIME ZONE");
    }

    boolean isTimestamp() {
      return typeName.startsWith("TIMESTAMP") && !isTimestampTz();
    }

    boolean isInterval() {
      return typeName.startsWith("INTERVAL");
    }
  }

  static final double MICROS_PER_MONTH = 365.25 / 12 * 24 * 60 * 60 * 1_000_000d;

  private Normaliser() {}

  public static String fromDatabase(ResultSet rs, int index, Column c) throws SQLException {
    Object v = rs.getObject(index);
    if (v == null) {
      return null;
    }
    switch (c.jdbcType()) {
      case Types.NUMERIC:
      case Types.DECIMAL:
      case Types.INTEGER:
      case Types.BIGINT:
      case Types.SMALLINT:
      case Types.TINYINT:
      case Types.FLOAT:
      case Types.DOUBLE:
      case Types.REAL:
        if ("BINARY_FLOAT".equals(c.typeName())) {
          return Float.toString(rs.getFloat(index));
        }
        if ("BINARY_DOUBLE".equals(c.typeName())) {
          return Double.toString(rs.getDouble(index));
        }
        return number(rs.getBigDecimal(index));
      case Types.DATE:
        return rs.getObject(index, LocalDateTime.class).toString();
      case Types.TIMESTAMP:
        return rs.getObject(index, LocalDateTime.class).toString();
      case Types.TIMESTAMP_WITH_TIMEZONE:
      case -101: // Oracle TIMESTAMP WITH TIME ZONE
      case -102: // Oracle TIMESTAMP WITH LOCAL TIME ZONE
        return rs.getObject(index, OffsetDateTime.class).toInstant().toString();
      case Types.BINARY:
      case Types.VARBINARY:
      case Types.LONGVARBINARY:
      case Types.BLOB:
        return HexFormat.of().formatHex(rs.getBytes(index));
      case Types.CLOB:
      case Types.NCLOB:
        return rs.getString(index);
      default:
        if (c.isInterval()) {
          return intervalMicros(rs.getString(index), c.typeName());
        }
        if (c.isTimestampTz()) {
          return rs.getObject(index, OffsetDateTime.class).toInstant().toString();
        }
        return rs.getString(index);
    }
  }

  public static String fromRecord(JsonNode v, Column c) {
    if (v == null || v.isNull()) {
      return null;
    }
    String t = c.typeName().toUpperCase(Locale.ROOT);
    if ("BINARY_FLOAT".equals(t)) {
      return Float.toString((float) v.asDouble());
    }
    if ("BINARY_DOUBLE".equals(t)) {
      return Double.toString(v.asDouble());
    }
    if (t.equals("NUMBER") || t.equals("FLOAT") || t.startsWith("NUMBER(")) {
      return number(decimal(v));
    }
    if (t.equals("DATE")) {
      return epochToLocal(v, 3);
    }
    if (c.isTimestamp()) {
      int p = c.scale() < 0 ? 6 : c.scale();
      return epochToLocal(v, p <= 3 ? 3 : p <= 6 ? 6 : 9);
    }
    if (c.isTimestampTz()) {
      return OffsetDateTime.parse(v.asText(), DateTimeFormatter.ISO_OFFSET_DATE_TIME)
          .toInstant()
          .toString();
    }
    if (c.isInterval()) {
      if (v.isNumber()) {
        return Long.toString(Math.round(v.asDouble()));
      }
      return intervalMicros(v.asText(), c.typeName());
    }
    if (t.equals("RAW") || t.equals("BLOB") || t.equals("LONG RAW")) {
      return v.isTextual()
          ? HexFormat.of().formatHex(Base64.getDecoder().decode(v.asText()))
          : v.toString();
    }
    return v.asText();
  }

  static BigDecimal decimal(JsonNode v) {
    if (v.isNumber()) {
      return v.decimalValue();
    }
    if (v.isObject() && v.has("scale") && v.has("value")) { // io.debezium.data.VariableScaleDecimal
      byte[] unscaled = Base64.getDecoder().decode(v.get("value").asText());
      return new BigDecimal(new BigInteger(unscaled), v.get("scale").asInt());
    }
    if (v.isTextual()) {
      String s = v.asText();
      try {
        return new BigDecimal(s);
      } catch (NumberFormatException e) {
        // a Connect Decimal encoded as base64 bytes by a converter in BASE64 mode has no scale
        // here; callers using the oracle set value.converter.decimal.format=NUMERIC
        throw new IllegalArgumentException(
            "decimal value " + s + " is not numeric; set decimal.format=NUMERIC", e);
      }
    }
    throw new IllegalArgumentException("not a number: " + v);
  }

  static String number(BigDecimal d) {
    if (d == null) {
      return null;
    }
    BigDecimal s = d.stripTrailingZeros();
    return s.compareTo(BigDecimal.ZERO) == 0 ? "0" : s.toPlainString();
  }

  static String epochToLocal(JsonNode v, int fraction) {
    long n = v.asLong();
    Instant i;
    switch (fraction) {
      case 3:
        i = Instant.ofEpochMilli(n);
        break;
      case 6:
        i =
            Instant.ofEpochSecond(
                Math.floorDiv(n, 1_000_000L), Math.floorMod(n, 1_000_000L) * 1_000L);
        break;
      default:
        i =
            Instant.ofEpochSecond(
                Math.floorDiv(n, 1_000_000_000L), Math.floorMod(n, 1_000_000_000L));
    }
    return LocalDateTime.ofInstant(i, ZoneOffset.UTC).toString();
  }

  /** {@code +12-03} or {@code +05 04:03:02.123456} to microseconds, as Debezium's MicroDuration. */
  static String intervalMicros(String text, String typeName) {
    String s = text.trim();
    int sign = s.startsWith("-") ? -1 : 1;
    if (s.startsWith("+") || s.startsWith("-")) {
      s = s.substring(1);
    }
    if (typeName.contains("YEAR")) {
      String[] ym = s.split("-");
      long months = Long.parseLong(ym[0]) * 12 + Long.parseLong(ym[1]);
      return Long.toString(Math.round(sign * months * MICROS_PER_MONTH));
    }
    String[] ds = s.split(" ");
    String[] hms = ds[1].split(":");
    double seconds = Double.parseDouble(hms[2]);
    Duration d =
        Duration.ofDays(Long.parseLong(ds[0]))
            .plusHours(Long.parseLong(hms[0]))
            .plusMinutes(Long.parseLong(hms[1]))
            .plusNanos(Math.round(seconds * 1_000_000_000d));
    return Long.toString(sign * (d.toNanos() / 1000));
  }
}
