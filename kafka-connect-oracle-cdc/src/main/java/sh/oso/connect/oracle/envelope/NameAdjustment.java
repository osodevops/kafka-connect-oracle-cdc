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

import java.util.Locale;

/**
 * How per-table schema names and column-derived field names are adjusted for converters with strict
 * naming rules ({@code cdc.schema.name.adjustment.mode}, {@code cdc.field.name.adjustment.mode}).
 * An Avro name is {@code [A-Za-z_][A-Za-z0-9_]*}; a full name is a dot-separated sequence of such
 * names.
 *
 * <ul>
 *   <li>{@link #NONE}: names are used as Oracle stores them.
 *   <li>{@link #AVRO}: every character not allowed in an Avro name becomes {@code _}, and a name
 *       that starts with a digit gets a leading {@code _} (the digit is kept, so {@code 1A} and
 *       {@code 2A} stay distinct).
 *   <li>{@link #AVRO_UNICODE}: every character not allowed, and {@code _} itself, becomes {@code
 *       _u} followed by the four lower-case hex digits of its UTF-16 code unit; a leading digit is
 *       not allowed there and is escaped the same way. {@code _} acts as the escape character, so
 *       different names never adjust to the same name.
 * </ul>
 *
 * <p>Written from Debezium's documented semantics for {@code schema.name.adjustment.mode} and
 * {@code field.name.adjustment.mode} (ADR-0020).
 */
public enum NameAdjustment {
  NONE,
  AVRO,
  AVRO_UNICODE;

  /** The configuration value: {@code none}, {@code avro} or {@code avro_unicode}. */
  public static NameAdjustment parse(String value) {
    return valueOf(value.trim().toUpperCase(Locale.ROOT));
  }

  /** A dotted full name, adjusted component by component; the dots are kept. */
  public String fullName(String name) {
    if (this == NONE) {
      return name;
    }
    String[] parts = name.split("\\.", -1);
    StringBuilder out = new StringBuilder(name.length() + 8);
    for (int i = 0; i < parts.length; i++) {
      if (i > 0) {
        out.append('.');
      }
      out.append(simpleName(parts[i]));
    }
    return out.toString();
  }

  /** One name with no namespace, such as a field name: a dot is replaced like any other. */
  public String simpleName(String name) {
    if (this == NONE) {
      return name;
    }
    StringBuilder out = null;
    for (int i = 0; i < name.length(); i++) {
      char c = name.charAt(i);
      boolean letter = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z');
      boolean digit = c >= '0' && c <= '9';
      boolean keep =
          this == AVRO ? letter || c == '_' || (digit && i > 0) : letter || (digit && i > 0);
      if (keep) {
        if (out != null) {
          out.append(c);
        }
        continue;
      }
      if (out == null) {
        out = new StringBuilder(name.length() + 8).append(name, 0, i);
      }
      if (this == AVRO) {
        out.append('_');
        if (digit) {
          out.append(c); // a leading digit is kept behind the underscore
        }
      } else {
        out.append("_u").append(String.format(Locale.ROOT, "%04x", (int) c));
      }
    }
    return out == null ? name : out.toString();
  }
}
