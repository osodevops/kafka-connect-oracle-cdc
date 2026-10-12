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
package sh.oso.connect.oracle.core.position;

import java.util.Collections;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;
import sh.oso.connect.oracle.core.model.RedoRecordId;

/**
 * ADR-0026: where a restart may mine from, per redo thread. {@code byThread} holds, for each
 * thread, the record its redo is read again from (inclusive); a redo byte address in it is compared
 * only with records of the same thread. {@code legacy} is the single point the legacy offset keys
 * carry: with at most one thread it is that thread's point exactly as before ADR-0026, so a
 * single-instance offset is unchanged; with several it is the lowest SCN and no redo byte address,
 * because no one address places two threads.
 */
public record ResumePoint(RedoRecordId legacy, SortedMap<Integer, RedoRecordId> byThread) {

  public ResumePoint {
    Objects.requireNonNull(legacy, "legacy");
    TreeMap<Integer, RedoRecordId> m = new TreeMap<>();
    if (byThread != null) {
      byThread.forEach((t, id) -> m.put(t, Objects.requireNonNull(id, "resume of thread " + t)));
    }
    byThread = Collections.unmodifiableSortedMap(m);
  }

  /** A point that names no thread (a cursor resumed from a single-thread position, or a test). */
  public static ResumePoint single(RedoRecordId point) {
    return new ResumePoint(point, null);
  }

  /** The point of a single thread: the legacy keys carry it unchanged. */
  public static ResumePoint single(int thread, RedoRecordId point) {
    TreeMap<Integer, RedoRecordId> m = new TreeMap<>();
    m.put(thread, point);
    return new ResumePoint(point, m);
  }

  /**
   * One point per thread. With several threads the legacy point is the lowest SCN without a redo
   * byte address, which log selection and LogMiner's start bound use.
   */
  public static ResumePoint of(SortedMap<Integer, RedoRecordId> byThread) {
    if (byThread.isEmpty()) {
      throw new IllegalArgumentException("a per-thread resume point needs at least one thread");
    }
    if (byThread.size() == 1) {
      return single(byThread.firstKey(), byThread.get(byThread.firstKey()));
    }
    long lowest = Long.MAX_VALUE;
    for (RedoRecordId id : byThread.values()) {
      lowest = Math.min(lowest, id.scn());
    }
    return new ResumePoint(new RedoRecordId(lowest, null, 0), byThread);
  }

  /** True when the point covers more than one thread, so the offset needs its per-thread block. */
  public boolean perThread() {
    return byThread.size() > 1;
  }

  /**
   * The point a restart needs while a transaction of {@code thread} whose first record is {@code
   * first} is not wholly acknowledged: only that thread's point is lowered to it, because a redo
   * byte address says nothing about another thread. Without a per-thread map the legacy point is
   * lowered exactly as before ADR-0026.
   */
  public ResumePoint withEarlier(int thread, RedoRecordId first) {
    if (byThread.isEmpty()) {
      return single(earlier(legacy, first));
    }
    TreeMap<Integer, RedoRecordId> m = new TreeMap<>(byThread);
    m.merge(thread, first, ResumePoint::earlier);
    return m.size() == 1 ? new ResumePoint(earlier(legacy, first), m) : of(m);
  }

  /**
   * The earlier of two resume points of one thread, in redo order, with the lower SCN as its floor;
   * by SCN when either has no redo byte address.
   */
  public static RedoRecordId earlier(RedoRecordId a, RedoRecordId b) {
    long scn = Math.min(a.scn(), b.scn());
    if (!a.hasRba() || !b.hasRba()) {
      return a.scn() <= b.scn()
          ? new RedoRecordId(scn, a.rsId(), a.ssn())
          : new RedoRecordId(scn, b.rsId(), b.ssn());
    }
    return a.compareTo(b) <= 0
        ? new RedoRecordId(scn, a.rsId(), a.ssn())
        : new RedoRecordId(scn, b.rsId(), b.ssn());
  }
}
