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

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import sh.oso.connect.oracle.core.model.TableId;

/**
 * Outcome of object-id resolution: container-qualified id to table, the tables and their owners.
 */
public record ResolvedObjects(
    Map<ObjectKey, TableId> byObject, Set<TableId> tables, Set<String> owners) {

  public ResolvedObjects {
    byObject = Collections.unmodifiableMap(new TreeMap<>(byObject));
    tables = Set.copyOf(tables);
    owners = Collections.unmodifiableSet(new TreeSet<>(owners));
  }

  /** Object ids grouped by source container, the shape the mining query pushes down. */
  public Map<Integer, Set<Long>> idsByContainer() {
    TreeMap<Integer, Set<Long>> out = new TreeMap<>();
    for (ObjectKey k : byObject.keySet()) {
      out.computeIfAbsent(k.conId(), c -> new TreeSet<>()).add(k.objectId());
    }
    return out;
  }

  public MiningFilter filter(Set<String> excludedUsers, int inlistMax) {
    return new MiningFilter(idsByContainer(), owners, excludedUsers, inlistMax);
  }

  public int objectCount() {
    return byObject.size();
  }

  public boolean isEmpty() {
    return byObject.isEmpty();
  }
}
