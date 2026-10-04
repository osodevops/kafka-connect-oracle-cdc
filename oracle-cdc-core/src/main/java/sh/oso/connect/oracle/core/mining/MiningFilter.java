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

/**
 * What the mining query pushes down to the server (CORE-MINE-2, ADR-0001): row changes on captured
 * object ids per source container, DDL by non-Oracle owners, transaction control rows minus
 * excluded users. Object ids are per container, so they are grouped by SRC_CON_ID. Collections are
 * copied and sorted so the generated SQL is deterministic.
 */
public record MiningFilter(
    Map<Integer, Set<Long>> objectIdsByContainer,
    Set<String> ddlOwners,
    Set<String> excludedUsers,
    int inlistMax) {

  public MiningFilter {
    TreeMap<Integer, Set<Long>> sorted = new TreeMap<>();
    for (Map.Entry<Integer, Set<Long>> e : objectIdsByContainer.entrySet()) {
      if (!e.getValue().isEmpty()) {
        sorted.put(e.getKey(), Collections.unmodifiableSet(new TreeSet<>(e.getValue())));
      }
    }
    objectIdsByContainer = Collections.unmodifiableMap(sorted);
    ddlOwners = Collections.unmodifiableSet(new TreeSet<>(ddlOwners));
    excludedUsers = Collections.unmodifiableSet(new TreeSet<>(excludedUsers));
    if (inlistMax < 1 || inlistMax > 1000) {
      throw new IllegalArgumentException("inlistMax must be 1..1000 (Oracle in-list limit)");
    }
  }

  public static MiningFilter of(
      Map<Integer, Set<Long>> objectIdsByContainer, Set<String> ddlOwners) {
    return new MiningFilter(objectIdsByContainer, ddlOwners, Set.of(), 1000);
  }

  /** Convenience for a single container (or a non-CDB with container 0). */
  public static MiningFilter of(int conId, Set<Long> objectIds, Set<String> ddlOwners) {
    return of(Map.of(conId, objectIds), ddlOwners);
  }

  public boolean isEmpty() {
    return objectIdsByContainer.isEmpty();
  }
}
