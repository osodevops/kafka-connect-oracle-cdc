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
package sh.oso.connect.oracle.core.model;

import java.util.Locale;
import java.util.Objects;

/** A captured table: PDB (null in a non-CDB), schema and table name, as Oracle stores them. */
public record TableId(String pdb, String schema, String table) {

  public TableId {
    Objects.requireNonNull(schema, "schema");
    Objects.requireNonNull(table, "table");
  }

  /**
   * {@code PDB.SCHEMA.TABLE} in a CDB, {@code SCHEMA.TABLE} otherwise; the include pattern form.
   */
  public String fqn() {
    return (pdb == null || pdb.isEmpty() ? "" : pdb + ".") + schema + "." + table;
  }

  public String fqnUpper() {
    return fqn().toUpperCase(Locale.ROOT);
  }

  @Override
  public String toString() {
    return fqn();
  }
}
