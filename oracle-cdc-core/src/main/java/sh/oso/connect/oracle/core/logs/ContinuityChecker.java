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

import java.util.List;
import java.util.Set;
import sh.oso.connect.oracle.core.errors.OracleCdcGapException;
import sh.oso.connect.oracle.core.topology.ThreadInfo;

/**
 * Sequences for every required thread must be contiguous from the log containing the start SCN to
 * the log containing the end SCN (PRD-00 CORE-LOG-2, CORE-LOG-3). PUBLIC or PRIVATE thread status
 * is irrelevant: a thread with redo in the range is required.
 */
public final class ContinuityChecker {

  private ContinuityChecker() {}

  public static void check(LogSet set, Set<Integer> requiredThreads, List<ThreadInfo> threads) {
    for (int thread : requiredThreads) {
      List<RedoLog> logs = set.byThread().getOrDefault(thread, List.of());
      boolean enabled = threads.stream().anyMatch(t -> t.thread() == thread && t.enabled());
      if (logs.isEmpty()) {
        if (enabled && threadHasRedoInRange(thread, set)) {
          throw gap(thread, "no logs found", set);
        }
        continue;
      }
      RedoLog first = logs.get(0);
      if (first.firstScn() > set.startScn() && enabled) {
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
      if (last.nextScn() <= set.endScn() && enabled) {
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
