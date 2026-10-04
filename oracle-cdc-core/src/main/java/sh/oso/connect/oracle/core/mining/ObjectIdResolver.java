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
package sh.oso.connect.oracle.core.mining;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import sh.oso.connect.oracle.core.model.TableId;

/**
 * Resolves include and exclude patterns to the object ids LogMiner reports as DATA_OBJ#
 * (CORE-MINE-3). Patterns match {@code PDB.SCHEMA.TABLE} (or {@code SCHEMA.TABLE} in a non-CDB),
 * case-insensitively unless asked otherwise. Re-run after any DDL that creates a segment, because a
 * new partition carries an id this resolution has not seen (ADR-0001).
 */
public final class ObjectIdResolver {

  private final ObjectCatalog catalog;
  private final List<Pattern> include;
  private final List<Pattern> exclude;
  private final Set<String> pdbs;

  public ObjectIdResolver(
      ObjectCatalog catalog,
      List<String> include,
      List<String> exclude,
      List<String> pdbs,
      boolean caseSensitive) {
    this.catalog = catalog;
    int flags = caseSensitive ? 0 : Pattern.CASE_INSENSITIVE;
    this.include = include.stream().map(p -> Pattern.compile(p, flags)).toList();
    this.exclude = exclude.stream().map(p -> Pattern.compile(p, flags)).toList();
    this.pdbs = new HashSet<>();
    for (String p : pdbs) {
      this.pdbs.add(p.toUpperCase(java.util.Locale.ROOT));
    }
  }

  public ResolvedObjects resolve() throws SQLException {
    Map<Long, TableId> byId = new HashMap<>();
    Set<TableId> tables = new HashSet<>();
    Set<String> owners = new HashSet<>();
    for (CapturedObject o : catalog.objects()) {
      TableId t = o.table();
      if (t.pdb() != null
          && !pdbs.isEmpty()
          && !pdbs.contains(t.pdb().toUpperCase(java.util.Locale.ROOT))) {
        continue;
      }
      if (matches(t)) {
        byId.put(o.objectId(), t);
        tables.add(t);
        owners.add(t.schema());
      }
    }
    return new ResolvedObjects(byId, tables, owners);
  }

  public boolean matches(TableId t) {
    String fqn = t.fqn();
    boolean in = include.isEmpty() || include.stream().anyMatch(p -> p.matcher(fqn).matches());
    return in && exclude.stream().noneMatch(p -> p.matcher(fqn).matches());
  }
}
