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
package sh.oso.connect.oracle.core.mining.step;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import sh.oso.connect.oracle.core.model.RedoRecordId;

/**
 * Where the next step starts: an SCN for START_LOGMNR plus, per redo thread, the last record
 * already applied, so re-mined rows are not applied twice. A redo byte address is only ordered
 * within its thread (RS_ID begins with the thread's own log sequence), so every mark belongs to one
 * thread and is only compared with rows of that thread (ADR-0026).
 *
 * <p>A cursor resumed from a position that names no thread carries one mark under {@link
 * #ANY_THREAD}, which applies to every thread. That is exact while a single thread is mined
 * (ADR-0023 refuses a second one); the first record a step applies on a real thread replaces it.
 */
public record StepCursor(long scn, SortedMap<Integer, RedoRecordId> marks, boolean inclusive) {

  /** The key of a mark that names no thread; never a THREAD# value. */
  public static final int ANY_THREAD = Integer.MIN_VALUE;

  public StepCursor {
    TreeMap<Integer, RedoRecordId> m = new TreeMap<>();
    if (marks != null) {
      marks.forEach(
          (t, id) -> {
            if (id != null) {
              m.put(t, id);
            }
          });
    }
    if (m.size() > 1) {
      m.remove(ANY_THREAD); // a mark on a real thread supersedes the thread-less one
    }
    marks = Collections.unmodifiableSortedMap(m);
  }

  /** A cursor at an SCN with no redo byte address: rows are selected by SCN only (legacy). */
  public static StepCursor at(long scn) {
    return new StepCursor(scn, (SortedMap<Integer, RedoRecordId>) null, false);
  }

  /**
   * A cursor from a position: re-read from the resume record inclusive, so the first row of an open
   * transaction is mined again; without a redo byte address it is an SCN cursor. The position names
   * no thread, so the mark applies to every thread ({@link #ANY_THREAD}).
   */
  public static StepCursor resume(RedoRecordId point) {
    return point == null || !point.hasRba()
        ? at(point == null ? 0 : point.scn())
        : new StepCursor(point.scn(), new TreeMap<>(Map.of(ANY_THREAD, point)), true);
  }

  /** A cursor after {@code id}, mined on {@code thread}, at {@code scn}; an SCN cursor if null. */
  public static StepCursor of(long scn, int thread, RedoRecordId id) {
    return id == null ? at(scn) : new StepCursor(scn, new TreeMap<>(Map.of(thread, id)), false);
  }

  /** The cursor after a step: later rows of each thread have a greater redo byte address. */
  public StepCursor after(long minedToScn, Map<Integer, RedoRecordId> applied) {
    return new StepCursor(minedToScn, new TreeMap<>(applied), false);
  }

  /** True when any thread's mark carries a redo byte address. */
  public boolean hasRba() {
    for (RedoRecordId id : marks.values()) {
      if (id.hasRba()) {
        return true;
      }
    }
    return false;
  }

  /**
   * The threads whose marks carry a redo byte address, in order; {@code [ANY_THREAD]} for a
   * thread-less cursor. The query selects each listed thread's rows after its mark and every other
   * thread's rows by SCN.
   */
  public List<Integer> rbaThreads() {
    return marks.entrySet().stream()
        .filter(e -> e.getValue().hasRba())
        .map(Map.Entry::getKey)
        .toList();
  }

  /** The mark that applies to {@code thread}: its own, else the thread-less one, else null. */
  public RedoRecordId markFor(int thread) {
    RedoRecordId own = marks.get(thread);
    return own != null ? own : marks.get(ANY_THREAD);
  }

  /**
   * The single mark of a cursor over one thread, or null without one. Callers that need a single
   * record (the resume calculation) only ever see one thread while ADR-0023 refuses a second.
   */
  public RedoRecordId lastApplied() {
    if (marks.isEmpty()) {
      return null;
    }
    if (marks.size() > 1) {
      throw new IllegalStateException(
          "The step cursor holds marks for threads "
              + marks.keySet()
              + ", so it names no single last record (ADR-0026).");
    }
    return marks.values().iterator().next();
  }

  /** True when {@code id}, mined on {@code thread}, was already applied under this cursor. */
  public boolean alreadyApplied(int thread, RedoRecordId id) {
    RedoRecordId mark = markFor(thread);
    if (mark == null) {
      return false;
    }
    int c = id.compareTo(mark);
    return inclusive ? c < 0 : c <= 0;
  }
}
