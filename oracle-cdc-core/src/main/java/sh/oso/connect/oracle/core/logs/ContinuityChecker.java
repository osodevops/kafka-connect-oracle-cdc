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
import java.util.List;
import java.util.Map;
import java.util.Set;
import sh.oso.connect.oracle.core.errors.OracleCdcGapException;
import sh.oso.connect.oracle.core.topology.ThreadInfo;

/**
 * Sequences for every required thread must be contiguous from the log containing the start SCN to
 * the log containing the end SCN (PRD-00 CORE-LOG-2, CORE-LOG-3). PUBLIC or PRIVATE thread status
 * is irrelevant: a thread with redo in the range is required.
 *
 * <p>ADR-0026: with more than one redo thread, an enabled thread need not cover the whole range by
 * SCN when its log sequences prove it wrote nothing in the uncovered part: its first log in the
 * range starts after the start, and the log of the previous sequence ends at or before the start
 * (or there is none: sequence 1); its last log ends before the end, and the thread is closed at
 * that log or the log of the next sequence starts after the end. Sequences have no holes, so no
 * redo of the thread lies between those logs.
 */
public final class ContinuityChecker {

  /** The log of a thread with a given sequence, from the listings or the catalog; null if none. */
  @FunctionalInterface
  public interface Neighbours {
    RedoLog log(int thread, long sequence) throws SQLException;
  }

  private ContinuityChecker() {}

  /** The check before ADR-0026, which a database with one redo thread keeps. */
  public static void check(LogSet set, Set<Integer> requiredThreads, List<ThreadInfo> threads) {
    try {
      check(set, requiredThreads, threads, Map.of(), null);
    } catch (SQLException e) {
      throw new IllegalStateException("no lookups without neighbours", e);
    }
  }

  /**
   * {@code closedAt} holds, per closed thread whose last log is archived, that log's NEXT_CHANGE#;
   * {@code neighbours} is null on a database with one redo thread, which turns the ADR-0026 rules
   * off.
   */
  public static void check(
      LogSet set,
      Set<Integer> requiredThreads,
      List<ThreadInfo> threads,
      Map<Integer, Long> closedAt,
      Neighbours neighbours)
      throws SQLException {
    for (int thread : requiredThreads) {
      List<RedoLog> logs = set.byThread().getOrDefault(thread, List.of());
      ThreadInfo info = threads.stream().filter(t -> t.thread() == thread).findFirst().orElse(null);
      boolean enabled = info != null && info.enabled();
      if (logs.isEmpty()) {
        if (enabled && threadHasRedoInRange(thread, set) && !quietOver(info, set, neighbours)) {
          throw gap(thread, "no logs found", set);
        }
        continue;
      }
      RedoLog first = logs.get(0);
      if (first.firstScn() > set.startScn()
          && enabled
          && !nothingBefore(first, set.startScn(), neighbours)) {
        throw gap(
            thread,
            "the earliest log is sequence "
                + first.sequence()
                + " starting at SCN "
                + first.firstScn()
                + ", after the start SCN",
            set);
      }
      for (int i = 1; i < logs.size(); i++) {
        long expected = logs.get(i - 1).sequence() + 1;
        if (logs.get(i).sequence() != expected) {
          throw gap(
              thread,
              "sequence "
                  + expected
                  + " is missing between "
                  + logs.get(i - 1).sequence()
                  + " and "
                  + logs.get(i).sequence(),
              set);
        }
      }
      RedoLog last = logs.get(logs.size() - 1);
      if (last.nextScn() <= set.endScn()
          && enabled
          && !nothingAfter(last, set.endScn(), closedAt, neighbours)) {
        throw gap(
            thread,
            "the latest log is sequence "
                + last.sequence()
                + " ending at SCN "
                + last.nextScn()
                + ", before the end SCN",
            set);
      }
    }
  }

  /** ADR-0026: no redo of the thread lies between the start and {@code first}. */
  private static boolean nothingBefore(RedoLog first, long startScn, Neighbours neighbours)
      throws SQLException {
    if (neighbours == null) {
      return false;
    }
    if (first.sequence() == 1) {
      return true; // the thread's first log: it was enabled after the start
    }
    RedoLog previous = neighbours.log(first.thread(), first.sequence() - 1);
    return previous != null
        && previous.nextScn() <= startScn
        && previous.nextScn() <= first.firstScn();
  }

  /** ADR-0026: no redo of the thread lies between {@code last} and the end. */
  private static boolean nothingAfter(
      RedoLog last, long endScn, Map<Integer, Long> closedAt, Neighbours neighbours)
      throws SQLException {
    Long closed = closedAt.get(last.thread());
    if (closed != null && closed == last.nextScn()) {
      return true; // closed at its last log: it writes nothing until it opens again
    }
    if (neighbours == null) {
      return false;
    }
    RedoLog next = neighbours.log(last.thread(), last.sequence() + 1);
    return next != null && next.firstScn() > endScn;
  }

  /**
   * ADR-0026: an open thread with no log in the range whose current log starts after the end, while
   * nothing lies before that log down to the start.
   */
  private static boolean quietOver(ThreadInfo info, LogSet set, Neighbours neighbours)
      throws SQLException {
    if (neighbours == null) {
      return false;
    }
    RedoLog current = neighbours.log(info.thread(), info.currentSequence());
    return current != null
        && current.firstScn() > set.endScn()
        && nothingBefore(current, set.startScn(), neighbours);
  }

  private static boolean threadHasRedoInRange(int thread, LogSet set) {
    // a required thread with no log at all only matters when it is enabled; callers pass enabled
    // threads
    return true;
  }

  private static OracleCdcGapException gap(int thread, String detail, LogSet set) {
    return new OracleCdcGapException(
        "Redo for thread "
            + thread
            + " is not contiguous over SCN "
            + set.startScn()
            + " to "
            + set.endScn()
            + ": "
            + detail
            + ".",
        "Check archive destinations and RMAN retention for thread "
            + thread
            + ". The connector never skips redo; restore the missing log or run oracle-cdc-admin"
            + " resnapshot.");
  }
}
