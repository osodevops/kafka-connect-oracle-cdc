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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import sh.oso.connect.oracle.core.config.CoreConfig.CaptureMode;
import sh.oso.connect.oracle.core.errors.OracleCdcGapException;
import sh.oso.connect.oracle.core.errors.OracleCdcPurgedException;
import sh.oso.connect.oracle.core.topology.CatalogSource;
import sh.oso.connect.oracle.core.topology.ThreadInfo;

/**
 * Builds the log set for an SCN range and verifies it before every step (PRD-00 CORE-LOG-1 to
 * CORE-LOG-6). Archived copies are preferred; online logs are added only in {@code online} mode for
 * the part of the range no archive covers. A gap stops with {@link OracleCdcGapException}; a purged
 * log stops with {@link OracleCdcPurgedException}. No later start is ever chosen.
 */
public final class LogInventory {

  private final CatalogSource catalog;
  private final CaptureMode mode;
  private final int destId;

  public LogInventory(CatalogSource catalog, CaptureMode mode, int destId) {
    this.catalog = catalog;
    this.mode = mode;
    this.destId = destId;
  }

  /** Logs covering [startScn, endScn], continuity-checked per required thread. */
  public LogSet forRange(long startScn, long endScn) throws SQLException {
    List<RedoLog> archived = catalog.archivedLogs(startScn, endScn, destId);
    List<RedoLog> online = mode == CaptureMode.ONLINE ? catalog.onlineLogs() : List.of();
    List<ThreadInfo> threads = catalog.threads();

    List<RedoLog> chosen = new ArrayList<>();
    Set<Integer> requiredThreads = new HashSet<>();
    for (RedoLog l : archived) {
      requiredThreads.add(l.thread());
    }
    for (ThreadInfo t : threads) {
      if (t.enabled()) {
        requiredThreads.add(t.thread());
      }
    }
    for (int thread : requiredThreads) {
      List<RedoLog> threadArchived =
          archived.stream()
              .filter(l -> l.thread() == thread)
              .sorted(Comparator.comparingLong(RedoLog::sequence))
              .toList();
      // one archived copy per sequence: prefer a readable one
      List<RedoLog> dedup = new ArrayList<>();
      for (RedoLog l : threadArchived) {
        if (!dedup.isEmpty() && dedup.get(dedup.size() - 1).sequence() == l.sequence()) {
          if (dedup.get(dedup.size() - 1).purgedInCatalog() && !l.purgedInCatalog()) {
            dedup.set(dedup.size() - 1, l);
          }
          continue;
        }
        dedup.add(l);
      }
      for (RedoLog l : dedup) {
        if (l.purgedInCatalog()) {
          throw new OracleCdcPurgedException(
              "Redo log "
                  + l.describe()
                  + " is needed for SCN "
                  + startScn
                  + " to "
                  + endScn
                  + " but the catalog marks it deleted.",
              "Restore the archived log, or run oracle-cdc-admin resnapshot for the captured"
                  + " tables. The connector never skips to a later SCN.");
        }
      }
      chosen.addAll(dedup);
      long archivedEnd = dedup.isEmpty() ? startScn : dedup.get(dedup.size() - 1).nextScn();
      if (archivedEnd <= endScn) {
        // the tail of the range is still in online logs for this thread
        List<RedoLog> tail =
            online.stream()
                .filter(
                    l ->
                        l.thread() == thread && l.nextScn() > archivedEnd && l.firstScn() <= endScn)
                .filter(l -> dedup.stream().noneMatch(a -> a.sequence() == l.sequence()))
                .sorted(Comparator.comparingLong(RedoLog::sequence))
                .toList();
        chosen.addAll(tail);
      }
    }
    LogSet set =
        new LogSet(
            startScn,
            endScn,
            chosen.stream()
                .sorted(
                    Comparator.comparingInt(RedoLog::thread).thenComparingLong(RedoLog::sequence))
                .toList());
    ContinuityChecker.check(set, requiredThreads, threads);
    return set;
  }

  /**
   * P1-17: the newest dictionary build complete in a log that ends at or before {@code scn}, whose
   * logs the catalog still holds. A redo-dictionary session that starts at {@code scn} reads it.
   */
  public java.util.Optional<DictionaryBuild> dictionaryBuildBefore(long scn) throws SQLException {
    DictionaryBuild best = null;
    RedoLog begin = null;
    for (RedoLog l : catalog.dictionaryLogs(destId)) {
      if (begin != null && begin.thread() != l.thread()) {
        begin = null;
      }
      if (l.dictionaryBegin()) {
        begin = l;
      }
      if (l.dictionaryEnd() && begin != null) {
        DictionaryBuild b = new DictionaryBuild(l.thread(), begin.firstScn(), l.nextScn());
        boolean readable = !begin.purgedInCatalog() && !l.purgedInCatalog();
        if (readable && b.endNextScn() <= scn && (best == null || b.firstScn() > best.firstScn())) {
          best = b;
        }
        begin = null;
      }
    }
    return java.util.Optional.ofNullable(best);
  }

  /** Archive-only safe end: the highest SCN every enabled thread has archived to (CORE-LOG-6). */
  public long archiveOnlySafeEnd(long fromScn) throws SQLException {
    long safe = Long.MAX_VALUE;
    boolean any = false;
    for (ThreadInfo t : catalog.threads()) {
      if (!t.enabled()) {
        continue;
      }
      long threadMax =
          catalog.archivedLogs(fromScn, Long.MAX_VALUE - 1, destId).stream()
              .filter(l -> l.thread() == t.thread() && !l.purgedInCatalog())
              .mapToLong(RedoLog::nextScn)
              .max()
              .orElse(fromScn);
      safe = Math.min(safe, threadMax);
      any = true;
    }
    return any ? Math.max(fromScn, safe - 1) : fromScn;
  }
}
