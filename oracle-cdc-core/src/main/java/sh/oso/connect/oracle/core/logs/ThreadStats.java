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

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import sh.oso.connect.oracle.core.topology.CatalogSource;
import sh.oso.connect.oracle.core.topology.ThreadInfo;

/** Per-thread figures for metrics and the doctor (PRD-00 CORE-LOG-7, PRD-05 DOC-9). */
public final class ThreadStats {

  public record Stat(
      int thread, long currentSequence, long oldestNeededSequence, double switchesPerHour) {}

  private ThreadStats() {}

  public static Map<Integer, Stat> compute(
      CatalogSource catalog, int destId, long resumeScn, Instant now) throws SQLException {
    Map<Integer, Stat> out = new TreeMap<>();
    List<RedoLog> lastHour = catalog.archivedSince(now.minus(Duration.ofHours(1)), destId);
    List<RedoLog> needed = catalog.archivedLogs(resumeScn, resumeScn, destId);
    for (ThreadInfo t : catalog.threads()) {
      if (!t.enabled()) {
        continue;
      }
      long switches = lastHour.stream().filter(l -> l.thread() == t.thread()).count();
      long oldest =
          needed.stream()
              .filter(l -> l.thread() == t.thread())
              .mapToLong(RedoLog::sequence)
              .min()
              .orElse(t.currentSequence());
      out.put(t.thread(), new Stat(t.thread(), t.currentSequence(), oldest, switches));
    }
    return out;
  }
}
