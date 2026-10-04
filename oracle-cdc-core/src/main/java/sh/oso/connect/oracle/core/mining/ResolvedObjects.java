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

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import sh.oso.connect.oracle.core.model.TableId;

/** Outcome of object-id resolution: id to table, the captured tables and their owners. */
public record ResolvedObjects(
    Map<Long, TableId> byObjectId, Set<TableId> tables, Set<String> owners) {

  public ResolvedObjects {
    byObjectId = Map.copyOf(new TreeMap<>(byObjectId));
    tables = Set.copyOf(tables);
    owners = Set.copyOf(new TreeSet<>(owners));
  }

  public MiningFilter filter(Set<String> excludedUsers, int inlistMax) {
    return new MiningFilter(byObjectId.keySet(), owners, excludedUsers, inlistMax);
  }

  public boolean isEmpty() {
    return byObjectId.isEmpty();
  }
}
