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
 *
 * <p>Thread states (CORE-LOG-3, ADR-0026), on a database whose V$THREAD lists more than one thread:
 * a CLOSED thread whose last log (V$THREAD.SEQUENCE#) is archived is closed at that log's
 * NEXT_CHANGE#. Its redo up to there is required as before; past it the thread is not waited for,
 * so it no longer holds back the archive-only safe end or cuts online steps. A thread that opens
 * again, or is enabled for the first time, is required from its first new log: an SCN gap before
 * that log is no gap when the log of the previous sequence ends at or before the range (the thread
 * wrote nothing in between), or when it is the thread's first log. A DISABLED thread's logs in the
 * range are mined and it is never waited for, as before. The closed rule applies only while some
 * enabled thread is not closed at an archived log. On a database whose V$THREAD lists one thread
 * none of these rules applies and the catalog is asked for nothing more.
 */
public final class LogInventory {

  private final CatalogSource catalog;
  private final CaptureMode mode;
  private final int destId;

  /** CORE-LOG-3: each thread's state at the previous read, and the changes not yet drained. */
  private final java.util.Map<Integer, String> seen = new java.util.TreeMap<>();

  private final List<ThreadStateChange> changes = new ArrayList<>();

  /** Where each closed thread's redo ends, by thread: the sequence and the NEXT_CHANGE# found. */
  private final java.util.Map<Integer, long[]> closedLogs = new java.util.HashMap<>();

  public LogInventory(CatalogSource catalog, CaptureMode mode, int destId) {
    this.catalog = catalog;
    this.mode = mode;
    this.destId = destId;
  }

  /**
   * Logs covering [startScn, endScn], continuity-checked per required thread. In {@code online}
   * mode the returned set may end earlier than {@code endScn}: while a log switch is in progress
   * the current SCN can pass the end of a thread's newest log before the next log is listed in
   * {@code V$LOG}. No sequence is missing then; the redo after that log is not written yet. The set
   * ends at the newest log's last SCN and the next step reads on. A sequence that is skipped is
   * still a gap, because a later log is then listed.
   */
  public LogSet forRange(long startScn, long endScn) throws SQLException {
    List<RedoLog> archived = catalog.archivedLogs(startScn, endScn, destId);
    List<RedoLog> online = mode == CaptureMode.ONLINE ? catalog.onlineLogs() : List.of();
    List<ThreadInfo> threads = catalog.threads(); // read last: a thread open in it has its logs
    java.util.Map<Integer, Long> closedAt = closedAt(threads, archived);
    observe(threads, closedAt);

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
    // a thread closed at or before the start has no redo in the range
    closedAt.forEach(
        (thread, at) -> {
          if (at <= startScn) {
            requiredThreads.remove(thread);
          }
        });
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
    long end = endScn;
    if (mode == CaptureMode.ONLINE) {
      for (int thread : requiredThreads) {
        if (threads.stream().noneMatch(t -> t.thread() == thread && t.enabled())
            || closedAt.containsKey(thread)) {
          continue; // no later log is coming: disabled, or closed with its last log archived
        }
        RedoLog last =
            chosen.stream()
                .filter(l -> l.thread() == thread)
                .max(Comparator.comparingLong(RedoLog::sequence))
                .orElse(null);
        if (last == null || last.nextScn() > endScn) {
          continue;
        }
        boolean later =
            java.util.stream.Stream.concat(archived.stream(), online.stream())
                .anyMatch(l -> l.thread() == thread && l.sequence() > last.sequence());
        if (!later) {
          end = Math.min(end, last.nextScn() - 1); // the switch to the next log is under way
        }
      }
    }
    LogSet set =
        new LogSet(
            startScn,
            end,
            chosen.stream()
                .sorted(
                    Comparator.comparingInt(RedoLog::thread).thenComparingLong(RedoLog::sequence))
                .toList());
    ContinuityChecker.check(
        set,
        requiredThreads,
        threads,
        closedAt,
        threads.size() < 2 ? null : (thread, seq) -> neighbour(thread, seq, archived, online));
    return set;
  }

  /**
   * ADR-0026: the log of {@code thread} with {@code sequence} from the listings already read, else
   * from the catalog; null when there is none.
   */
  private RedoLog neighbour(int thread, long sequence, List<RedoLog> archived, List<RedoLog> online)
      throws SQLException {
    if (sequence < 1) {
      return null;
    }
    for (RedoLog l : archived) {
      if (l.thread() == thread && l.sequence() == sequence) {
        return l;
      }
    }
    for (RedoLog l : online) {
      if (l.thread() == thread && l.sequence() == sequence) {
        return l;
      }
    }
    return catalog.archivedLog(thread, sequence, destId).orElse(null);
  }

  /**
   * ADR-0026: where each enabled CLOSED thread's redo ends, by thread, when its last log
   * (V$THREAD.SEQUENCE#) is archived. Empty on a database whose V$THREAD lists one thread, and when
   * every enabled thread is closed, so that the rules before ADR-0026 apply there.
   */
  private java.util.Map<Integer, Long> closedAt(List<ThreadInfo> threads, List<RedoLog> archived)
      throws SQLException {
    java.util.Map<Integer, Long> out = new java.util.TreeMap<>();
    if (threads.size() < 2) {
      return out;
    }
    boolean anyOther = false;
    for (ThreadInfo t : threads) {
      if (!t.enabled()) {
        continue;
      }
      if (!ThreadStateChange.CLOSED.equals(ThreadStateChange.of(t))) {
        anyOther = true;
        continue;
      }
      Long end = lastLogEnd(t, archived);
      if (end == null) {
        anyOther = true; // closed, but its last log is not archived yet: still waited for
      } else {
        out.put(t.thread(), end);
      }
    }
    return anyOther ? out : java.util.Map.of();
  }

  /** The NEXT_CHANGE# of the archived copy of a thread's current log, or null without one. */
  private Long lastLogEnd(ThreadInfo t, List<RedoLog> archived) throws SQLException {
    long[] known = closedLogs.get(t.thread());
    if (known != null && known[0] == t.currentSequence()) {
      return known[1];
    }
    RedoLog last = null;
    for (RedoLog l : archived) {
      if (l.thread() == t.thread() && l.sequence() == t.currentSequence()) {
        last = l;
      }
    }
    if (last == null) {
      last = catalog.archivedLog(t.thread(), t.currentSequence(), destId).orElse(null);
    }
    if (last == null) {
      return null;
    }
    closedLogs.put(t.thread(), new long[] {t.currentSequence(), last.nextScn()});
    return last.nextScn();
  }

  /** CORE-LOG-3: records each thread's state; a state unlike the previous read is a change. */
  private synchronized void observe(
      List<ThreadInfo> threads, java.util.Map<Integer, Long> closedAt) {
    for (ThreadInfo t : threads) {
      String state = ThreadStateChange.of(t);
      String before = seen.put(t.thread(), state);
      if (before != null && !before.equals(state) && changes.size() < 64) {
        changes.add(
            new ThreadStateChange(
                t.thread(), before, state, t.currentSequence(), closedAt.get(t.thread())));
      }
    }
  }

  /**
   * CORE-LOG-3: the thread state changes seen since the previous call, oldest first. The first read
   * of a thread records its state without a change.
   */
  public synchronized List<ThreadStateChange> drainThreadChanges() {
    List<ThreadStateChange> out = List.copyOf(changes);
    changes.clear();
    return out;
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

  /**
   * Archive-only safe end: the highest SCN every enabled thread has archived to (CORE-LOG-6), and
   * never past the database's current SCN. On a primary that bound never binds; on a physical
   * standby the current SCN is the applied one, and redo received but not applied would be decoded
   * against a dictionary that does not have its DDL yet (ADR-0025). A thread closed with its last
   * log archived has nothing more to archive and does not hold it back (ADR-0026). V$THREAD is read
   * after the archived logs, so a thread enabled before an archived log was listed is counted.
   */
  public long archiveOnlySafeEnd(long fromScn) throws SQLException {
    List<RedoLog> archived = catalog.archivedLogs(fromScn, Long.MAX_VALUE - 1, destId);
    List<ThreadInfo> threads = catalog.threads();
    java.util.Map<Integer, Long> closedAt = closedAt(threads, archived);
    observe(threads, closedAt);
    long safe = Long.MAX_VALUE;
    boolean any = false;
    for (ThreadInfo t : threads) {
      if (!t.enabled() || closedAt.containsKey(t.thread())) {
        continue;
      }
      long threadMax =
          archived.stream()
              .filter(l -> l.thread() == t.thread() && !l.purgedInCatalog())
              .mapToLong(RedoLog::nextScn)
              .max()
              .orElse(fromScn);
      safe = Math.min(safe, threadMax);
      any = true;
    }
    if (!any) {
      return fromScn;
    }
    return Math.max(fromScn, Math.min(safe - 1, catalog.currentScn()));
  }
}
