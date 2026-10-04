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
package sh.oso.connect.oracle.core.schema;

import java.util.Objects;

/** One column of a captured table as the dictionary describes it. */
public record ColumnSpec(
    String name,
    int position,
    OracleType type,
    String typeText,
    int length,
    int precision,
    int scale,
    boolean nullable) {

  public ColumnSpec {
    Objects.requireNonNull(name, "name");
    Objects.requireNonNull(type, "type");
  }

  public static ColumnSpec of(String name, int position, OracleType type) {
    return new ColumnSpec(name, position, type, type.name(), 0, 0, 0, true);
  }
}
