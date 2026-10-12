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
package sh.oso.connect.oracle.core.engine;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.errors.ResumeFloorViolationException;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.mining.step.StepCursor;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.position.ResumePoint;
import sh.oso.connect.oracle.core.position.ThreadMark;

/**
 * ADR-0026 decision 4 (CDC-7003): per redo thread, what a restart from a position already handed to
 * the sink, or from the start position, would not deliver again. Such a restart reads each thread
 * again from its resume address, inclusive, and skips that thread's commits at or before its last
 * acknowledged commit (the skip rule). A row of thread t applied below t's highest resume address
 * handed so far, or a commit of t emitted at or before t's last commit handed so far, would be lost
 * by that restart, so the task stops before it goes further.
 *
 * <p>Everything is compared by redo byte address (RS_ID, then SSN) within one thread, never by SCN
 * and never across threads. A late-bound record carries an SCN below records already mined, but its
 * address is after them, and a restart from an address mines it again (ADR-0014); an SCN floor
 * would stop healthy capture there. Records without an address (an all-zero RS_ID) are not
 * compared. A floor that names no thread (a position from before ADR-0026) applies to every thread
 * without a floor of its own, as the step cursor's thread-less mark does, until a floor on a real
 * thread replaces it.
 *
 * <p>Each thread's rows reach the engine in that thread's redo order, after the step cursor's mark,
 * and every handed resume address is at or below what was applied, so this never fires while
 * LogMiner and the cursor behave: it is a defensive stop instead of a loss on the next restart.
 */
final class ResumeFloor {

  /** Per thread, the highest resume address handed over, or the start's. */
  private final SortedMap<Integer, RedoRecordId> resume = new TreeMap<>();

  /** Per thread, the last commit handed over, or the start's acknowledged one. */
  private final Map<Integer, RedoRecordId> commit = new HashMap<>();

  /** Threads whose last commit was handed over in this run, not read from the start position. */
  private final Set<Integer> handedThisRun = new HashSet<>();

  /** The floor of a task that starts from {@code start}: what that restart does not deliver. */
  static ResumeFloor from(Position start) {
    ResumeFloor f = new ResumeFloor();
    if (start.perThread()) {
      for (Map.Entry<Integer, ThreadMark> e : start.threads().entrySet()) {
        f.raise(e.getKey(), e.getValue().resume());
        if (e.getValue().hasCommit() && e.getValue().commit().hasRba()) {
          f.commit.put(e.getKey(), e.getValue().commit());
        }
      }
    } else {
      f.raise(StepCursor.ANY_THREAD, start.resumePoint());
      if (start.hasCommit() && start.lastCommitId().hasRba()) {
        f.commit.put(start.lastCommitThread(), start.lastCommitId());
      }
    }
    return f;
  }

  /** A resume point was handed to the sink. */
  void handed(ResumePoint point) {
    if (point.byThread().isEmpty()) {
      raise(StepCursor.ANY_THREAD, point.legacy());
    } else {
      point.byThread().forEach(this::raise);
    }
  }

  /** A commit was handed to the sink. */
  void handed(CommittedTransaction tx) {
    RedoRecordId id = tx.commitId();
    if (id == null || !id.hasRba()) {
      return;
    }
    RedoRecordId had = commit.get(tx.thread());
    if (had == null || compare(id, had) >= 0) {
      commit.put(tx.thread(), id);
    }
    handedThisRun.add(tx.thread());
  }

  /** Stops the task when {@code e}, about to be applied, lies below its thread's resume floor. */
  void checkRow(MiningEvent e) {
    if (e.id() == null || !e.id().hasRba()) {
      return;
    }
    RedoRecordId floor =
        resume.containsKey(e.thread()) ? resume.get(e.thread()) : resume.get(StepCursor.ANY_THREAD);
    if (floor == null || compare(e.id(), floor) >= 0) {
      return;
    }
    throw new ResumeFloorViolationException(
        "LogMiner returned "
            + e.getClass().getSimpleName()
            + " "
            + e.id()
            + (e.txOrNull() == null ? "" : " of transaction " + e.txOrNull())
            + " on thread "
            + e.thread()
            + ", below "
            + floor
            + ", where a restart from an offset already handed to Kafka Connect reads thread "
            + e.thread()
            + " again. The restart would never see it, so nothing of the transaction was"
            + " emitted (ADR-0026).",
        OPERATOR_ACTION);
  }

  /** Stops the task when {@code tx}, about to be emitted, is at or before its thread's commit. */
  void checkCommit(CommittedTransaction tx) {
    RedoRecordId id = tx.commitId();
    RedoRecordId last = commit.get(tx.thread());
    if (id == null || !id.hasRba() || last == null) {
      return;
    }
    int c = compare(id, last);
    if (c > 0 || (c == 0 && !handedThisRun.contains(tx.thread()))) {
      return; // equal to the start's commit: the skip rule resumes it at its event index
    }
    throw new ResumeFloorViolationException(
        "The COMMIT of transaction "
            + tx.key()
            + " on thread "
            + tx.thread()
            + " at "
            + id
            + " is at or before "
            + last
            + ", the last commit of thread "
            + tx.thread()
            + " in an offset already handed to Kafka Connect. A restart from that offset skips"
            + " the thread's commits up to there, so the transaction would be lost; it was not"
            + " emitted (ADR-0026).",
        OPERATOR_ACTION);
  }

  static final String OPERATOR_ACTION =
      "LogMiner returned a redo thread out of redo order, or the connector computed an offset"
          + " ahead of what it had applied; neither is expected. Restart the task: it mines again"
          + " from its last acknowledged offset. If it stops again at the same place, keep the task"
          + " log and the message and report it (runbook resume-floor-violation).";

  private void raise(int thread, RedoRecordId id) {
    if (id == null || !id.hasRba()) {
      return;
    }
    if (thread == StepCursor.ANY_THREAD && resume.keySet().stream().anyMatch(t -> t != thread)) {
      return; // a floor on a real thread superseded the thread-less one
    }
    RedoRecordId had = resume.get(thread);
    if (had == null || compare(id, had) > 0) {
      resume.put(thread, id);
    }
    if (thread != StepCursor.ANY_THREAD) {
      resume.remove(StepCursor.ANY_THREAD);
    }
  }

  /** Redo order of two addresses of one thread: RS_ID, then SSN; SCNs are not compared. */
  static int compare(RedoRecordId a, RedoRecordId b) {
    int c = a.rsId().trim().compareTo(b.rsId().trim());
    return c != 0 ? c : Long.compare(a.ssn(), b.ssn());
  }
}
