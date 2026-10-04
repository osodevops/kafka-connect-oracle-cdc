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
package sh.oso.connect.oracle.core.mining.step;

import java.util.Locale;
import java.util.Set;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;

/**
 * Decides whether a DDL changes the set of object ids the mining filter pushes down (ADR-0001,
 * reference/object-id-stability.md): DDL that creates, exchanges or drops a segment. TRUNCATE,
 * MOVE, SHRINK and column DDL keep DATA_OBJ# and need no cut.
 */
public final class DdlStepCut {

  private final Set<String> capturedOwners;

  public DdlStepCut(Set<String> capturedOwners) {
    this.capturedOwners = Set.copyOf(capturedOwners);
  }

  public boolean requiresCut(MiningEvent.Ddl ddl) {
    if (createsOrDropsTable(ddl.sql())) {
      return true; // any owner: a new table may match the include patterns (SRC-SEL-4)
    }
    if (ddl.owner() == null || !capturedOwners.contains(ddl.owner())) {
      return false;
    }
    return changesObjectIds(ddl.sql());
  }

  static boolean createsOrDropsTable(String sql) {
    if (sql == null) {
      return false;
    }
    String s = sql.trim().replaceAll("\\s+", " ").toUpperCase(Locale.ROOT);
    return s.startsWith("CREATE TABLE")
        || s.startsWith("CREATE GLOBAL TEMPORARY TABLE")
        || s.startsWith("DROP TABLE");
  }

  static boolean changesObjectIds(String sql) {
    if (sql == null) {
      return false;
    }
    String s = sql.trim().replaceAll("\\s+", " ").toUpperCase(Locale.ROOT);
    if (s.startsWith("CREATE TABLE") || s.startsWith("CREATE GLOBAL TEMPORARY TABLE")) {
      return true;
    }
    if (s.startsWith("DROP TABLE")) {
      return true;
    }
    if (s.startsWith("ALTER TABLE")) {
      return s.contains(" ADD PARTITION")
          || s.contains(" ADD SUBPARTITION")
          || s.contains(" SPLIT PARTITION")
          || s.contains(" SPLIT SUBPARTITION")
          || s.contains(" MERGE PARTITIONS")
          || s.contains(" MERGE SUBPARTITIONS")
          || s.contains(" EXCHANGE PARTITION")
          || s.contains(" EXCHANGE SUBPARTITION")
          || s.contains(" DROP PARTITION")
          || s.contains(" DROP SUBPARTITION")
          || s.contains(" RENAME TO ");
    }
    return false;
  }
}
