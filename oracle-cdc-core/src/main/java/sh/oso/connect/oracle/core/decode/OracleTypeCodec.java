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

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.Locale;
import sh.oso.connect.oracle.core.errors.DecodeException;
import sh.oso.connect.oracle.core.jdbc.SessionInitializer;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.OracleType;

/**
 * Converts a {@link SqlLiteral} to a Java value for a column type (CORE-DEC-5). Temporal literals
 * are parsed with the session formats CORE-CONN-4 fixes, so a TO_DATE with another mask is an
 * error, not a guess. Values: String, BigDecimal, Float, Double, LocalDateTime, OffsetDateTime,
 * Instant (local time zone timestamps, rendered in the UTC session), Period, Duration, byte[].
 */
public final class OracleTypeCodec {

  private static final DateTimeFormatter DATE =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", Locale.ROOT);
  private static final DateTimeFormatter TIMESTAMP =
      new DateTimeFormatterBuilder()
          .appendPattern("yyyy-MM-dd HH:mm:ss")
          .optionalStart()
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
          .optionalEnd()
          .toFormatter(Locale.ROOT);
  private static final DateTimeFormatter TIMESTAMP_TZ =
      new DateTimeFormatterBuilder()
          .append(TIMESTAMP)
          .appendLiteral(' ')
          .appendOffset("+HH:MM", "+00:00")
          .toFormatter(Locale.ROOT);

  private OracleTypeCodec() {}

  public static Object decode(ColumnSpec column, SqlLiteral literal) {
    if (literal.isNull()) {
      return null;
    }
    OracleType type = column.type();
    try {
      switch (type) {
        case VARCHAR2:
        case CHAR:
        case NVARCHAR2:
        case NCHAR:
        case LONG:
        case ROWID:
        case UROWID:
          return text(column, literal);
        case CLOB:
        case NCLOB:
        case XMLTYPE:
          if (literal.kind() == SqlLiteral.Kind.EMPTY_CLOB) {
            return "";
          }
          return text(column, literal);
        case NUMBER:
        case FLOAT:
          return new BigDecimal(text(column, literal).trim());
        case BINARY_FLOAT:
          return Float.parseFloat(text(column, literal).trim());
        case BINARY_DOUBLE:
          return Double.parseDouble(text(column, literal).trim());
        case DATE:
          return date(column, literal);
        case TIMESTAMP:
          return LocalDateTime.parse(
              expect(column, literal, SqlLiteral.Kind.TO_TIMESTAMP), TIMESTAMP);
        case TIMESTAMP_TZ:
          return OffsetDateTime.parse(
              expect(column, literal, SqlLiteral.Kind.TO_TIMESTAMP_TZ), TIMESTAMP_TZ);
        case TIMESTAMP_LTZ:
          return LocalDateTime.parse(
                  expect(column, literal, SqlLiteral.Kind.TO_TIMESTAMP_TZ).trim(), TIMESTAMP)
              .toInstant(ZoneOffset.UTC);
        case INTERVAL_YM:
          return yearMonth(expect(column, literal, SqlLiteral.Kind.TO_YMINTERVAL));
        case INTERVAL_DS:
          return daySecond(expect(column, literal, SqlLiteral.Kind.TO_DSINTERVAL));
        case RAW:
        case LONG_RAW:
          return hex(expect(column, literal, SqlLiteral.Kind.HEXTORAW));
        case BLOB:
          if (literal.kind() == SqlLiteral.Kind.EMPTY_BLOB) {
            return new byte[0];
          }
          return hex(expect(column, literal, SqlLiteral.Kind.HEXTORAW));
        default:
          throw new DecodeException(
              "Column "
                  + column.name()
                  + " has type "
                  + column.typeText()
                  + " which the decoder does not support",
              "Exclude the column's table or wait for a release that supports the type (DOC-5).");
      }
    } catch (DateTimeParseException | NumberFormatException e) {
      throw new DecodeException(
          "Column "
              + column.name()
              + " ("
              + column.typeText()
              + ") literal "
              + literal.kind()
              + " '"
              + literal.value()
              + "' is not valid: "
              + e.getMessage(),
          "Check the mining session NLS settings (CORE-CONN-4) and report the row.",
          e);
    }
  }

  private static String text(ColumnSpec column, SqlLiteral l) {
    switch (l.kind()) {
      case STRING:
      case UNISTR:
        return l.value();
      default:
        throw mismatch(column, l, "a string");
    }
  }

  private static String expect(ColumnSpec column, SqlLiteral l, SqlLiteral.Kind kind) {
    if (l.kind() != kind) {
      throw mismatch(column, l, kind.name());
    }
    return l.value();
  }

  private static DecodeException mismatch(ColumnSpec column, SqlLiteral l, String wanted) {
    return new DecodeException(
        "Column "
            + column.name()
            + " ("
            + column.typeText()
            + ") expected "
            + wanted
            + " but SQL_REDO has "
            + l.kind(),
        "Check the mining session NLS settings (CORE-CONN-4) and report the row.");
  }

  private static LocalDateTime date(ColumnSpec column, SqlLiteral l) {
    String v = expect(column, l, SqlLiteral.Kind.TO_DATE);
    if (!SessionInitializer.DATE_FORMAT.equals(l.format())) {
      throw new DecodeException(
          "Column "
              + column.name()
              + " DATE literal uses mask '"
              + l.format()
              + "' instead of the session mask '"
              + SessionInitializer.DATE_FORMAT
              + "'",
          "The mining session must set NLS_DATE_FORMAT (CORE-CONN-4); check SessionInitializer"
              + " ran.");
    }
    return LocalDateTime.parse(v.trim(), DATE);
  }

  /** {@code +0012-03} or {@code -0001-11}: years and months with the sign applying to both. */
  static Period yearMonth(String v) {
    String s = v.trim();
    int sign = s.startsWith("-") ? -1 : 1;
    if (s.startsWith("+") || s.startsWith("-")) {
      s = s.substring(1);
    }
    int dash = s.indexOf('-');
    if (dash < 0) {
      throw new NumberFormatException("interval year to month " + v);
    }
    int years = Integer.parseInt(s.substring(0, dash));
    int months = Integer.parseInt(s.substring(dash + 1));
    return Period.of(sign * years, sign * months, 0);
  }

  /** {@code +00005 04:03:02.123456}: days, hours, minutes, seconds and a fraction. */
  static Duration daySecond(String v) {
    String s = v.trim();
    int sign = s.startsWith("-") ? -1 : 1;
    if (s.startsWith("+") || s.startsWith("-")) {
      s = s.substring(1);
    }
    int space = s.indexOf(' ');
    if (space < 0) {
      throw new NumberFormatException("interval day to second " + v);
    }
    long days = Long.parseLong(s.substring(0, space));
    String[] hms = s.substring(space + 1).split(":");
    if (hms.length != 3) {
      throw new NumberFormatException("interval day to second " + v);
    }
    long hours = Long.parseLong(hms[0]);
    long minutes = Long.parseLong(hms[1]);
    String sec = hms[2];
    long seconds;
    long nanos = 0;
    int dot = sec.indexOf('.');
    if (dot >= 0) {
      seconds = Long.parseLong(sec.substring(0, dot));
      String frac = (sec.substring(dot + 1) + "000000000").substring(0, 9);
      nanos = Long.parseLong(frac);
    } else {
      seconds = Long.parseLong(sec);
    }
    Duration d =
        Duration.ofDays(days)
            .plusHours(hours)
            .plusMinutes(minutes)
            .plusSeconds(seconds)
            .plusNanos(nanos);
    return sign < 0 ? d.negated() : d;
  }

  static byte[] hex(String v) {
    String s = v.trim();
    if (s.length() % 2 != 0) {
      throw new NumberFormatException("odd-length hex " + v);
    }
    byte[] out = new byte[s.length() / 2];
    for (int i = 0; i < out.length; i++) {
      int hi = Character.digit(s.charAt(2 * i), 16);
      int lo = Character.digit(s.charAt(2 * i + 1), 16);
      if (hi < 0 || lo < 0) {
        throw new NumberFormatException("not hex " + v);
      }
      out[i] = (byte) ((hi << 4) | lo);
    }
    return out;
  }

  /** Exposed for tests of the LTZ rule: the session runs in UTC, so the literal is UTC. */
  static Instant utc(LocalDateTime t) {
    return t.toInstant(ZoneOffset.UTC);
  }
}
