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
package sh.oso.connect.oracle.core.logs;

/**
 * One redo log as the inventory sees it: an archived copy or an online member. {@code nextScn} is
 * exclusive, as in V$ARCHIVED_LOG.NEXT_CHANGE#. For a CURRENT online log {@code nextScn} is {@link
 * Long#MAX_VALUE}.
 */
public record RedoLog(
    int thread,
    long sequence,
    long firstScn,
    long nextScn,
    String path,
    boolean archived,
    String status,
    boolean deleted,
    int destId,
    boolean dictionaryBegin,
    boolean dictionaryEnd) {

  public boolean covers(long scn) {
    return scn >= firstScn && scn < nextScn;
  }

  public boolean overlaps(long startScn, long endScn) {
    return nextScn > startScn && firstScn <= endScn;
  }

  /** Archived copy marked deleted or expired in the catalog. */
  public boolean purgedInCatalog() {
    return deleted || "D".equals(status) || "X".equals(status);
  }

  public String describe() {
    return "thread "
        + thread
        + " sequence "
        + sequence
        + " (SCN "
        + firstScn
        + " to "
        + (nextScn == Long.MAX_VALUE ? "current" : nextScn)
        + ", "
        + (archived ? "archived " + path : "online " + path)
        + ")";
  }
}
