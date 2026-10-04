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

/**
 * An object id qualified by its container. OBJECT_IDs are allocated per container, so the same
 * DATA_OBJ# names different tables in CDB$ROOT and in each PDB; the pushdown and the row adapter
 * must pair SRC_CON_ID with DATA_OBJ# (found by the Strimzi oracle_restart case: an AWR table in
 * the root shared an id with a captured PDB table). A non-CDB uses container 0.
 */
public record ObjectKey(int conId, long objectId) implements Comparable<ObjectKey> {
  @Override
  public int compareTo(ObjectKey o) {
    int c = Integer.compare(conId, o.conId);
    return c == 0 ? Long.compare(objectId, o.objectId) : c;
  }
}
