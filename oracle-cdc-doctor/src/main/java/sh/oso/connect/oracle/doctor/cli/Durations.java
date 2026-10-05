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
package sh.oso.connect.oracle.doctor.cli;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import picocli.CommandLine;

/**
 * Durations on the command line: ISO-8601 ({@code PT2H}) or {@code 2h}, {@code 90m}, {@code 7d}.
 */
public final class Durations implements CommandLine.ITypeConverter<Duration> {

  private static final Pattern PART = Pattern.compile("(\\d+)\\s*(ms|d|h|m|s)");

  @Override
  public Duration convert(String value) {
    return parse(value);
  }

  public static Duration parse(String value) {
    String v = value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
    if (v.startsWith("p")) {
      try {
        return Duration.parse(v.toUpperCase(Locale.ROOT));
      } catch (DateTimeParseException e) {
        throw new CommandLine.TypeConversionException("not a duration: " + value);
      }
    }
    Matcher m = PART.matcher(v);
    Duration d = Duration.ZERO;
    int end = 0;
    boolean any = false;
    while (m.find()) {
      if (!v.substring(end, m.start()).isBlank()) {
        throw new CommandLine.TypeConversionException("not a duration: " + value);
      }
      long n = Long.parseLong(m.group(1));
      d =
          switch (m.group(2)) {
            case "d" -> d.plusDays(n);
            case "h" -> d.plusHours(n);
            case "m" -> d.plusMinutes(n);
            case "s" -> d.plusSeconds(n);
            default -> d.plusMillis(n);
          };
      end = m.end();
      any = true;
    }
    if (!any || end != v.length()) {
      throw new CommandLine.TypeConversionException(
          "not a duration: " + value + " (use for example 2h, 90m, 7d or PT2H)");
    }
    return d;
  }
}
