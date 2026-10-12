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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.TxKey;

/**
 * The connector's durable position (CORE-POS-1, ADR-0010), version 1.
 *
 * <p>Invariant: every field describes only what Kafka Connect has acknowledged. {@code resumeScn}
 * is the SCN mining restarts from; it is never above the first captured record of an open,
 * non-journaled transaction. {@code lastCommit*} name the last transaction whose records were
 * acknowledged, and {@code eventIndex} how many of its events were, so a restart replays exactly
 * the unacknowledged suffix (CORE-POS-3). {@code released} lists transactions the orphan detector
 * let go (ADR-0006). {@code extras} carries fields this version does not know, so a one-version
 * downgrade keeps them (CORE-POS-6).
 */
public record Position(
    int version,
    long resumeScn,
    long lastCommitScn,
    TxKey lastCommitKey,
    int lastCommitThread,
    int eventIndex,
    long journalGeneration,
    long schemaEpoch,
    DatabaseIdentity identity,
    List<String> released,
    Map<String, Object> snapshot,
    Map<String, Object> extras,
    String resumeRsId,
    long resumeSsn,
    String lastCommitRsId,
    long lastCommitSsn,
    java.util.SortedMap<Integer, ThreadMark> threads) {

  /**
   * The single-thread shape: no per-thread block. Every position of a single-instance database has
   * this shape, so its offset is written exactly as before ADR-0026.
   */
  public Position(
      int version,
      long resumeScn,
      long lastCommitScn,
      TxKey lastCommitKey,
      int lastCommitThread,
      int eventIndex,
      long journalGeneration,
      long schemaEpoch,
      DatabaseIdentity identity,
      List<String> released,
      Map<String, Object> snapshot,
      Map<String, Object> extras,
      String resumeRsId,
      long resumeSsn,
      String lastCommitRsId,
      long lastCommitSsn) {
    this(
        version,
        resumeScn,
        lastCommitScn,
        lastCommitKey,
        lastCommitThread,
        eventIndex,
        journalGeneration,
        schemaEpoch,
        identity,
        released,
        snapshot,
        extras,
        resumeRsId,
        resumeSsn,
        lastCommitRsId,
        lastCommitSsn,
        null);
  }

  /** The pre-ADR-0014 shape: no redo byte addresses. */
  public Position(
      int version,
      long resumeScn,
      long lastCommitScn,
      TxKey lastCommitKey,
      int lastCommitThread,
      int eventIndex,
      long journalGeneration,
      long schemaEpoch,
      DatabaseIdentity identity,
      List<String> released,
      Map<String, Object> snapshot,
      Map<String, Object> extras) {
    this(
        version,
        resumeScn,
        lastCommitScn,
        lastCommitKey,
        lastCommitThread,
        eventIndex,
        journalGeneration,
        schemaEpoch,
        identity,
        released,
        snapshot,
        extras,
        null,
        0,
        null,
        0);
  }

  public static final int CURRENT_VERSION = 1;

  public Position {
    if (version < 1) {
      throw new IllegalArgumentException("position version must be positive");
    }
    if (resumeScn < 0 || lastCommitScn < 0 || eventIndex < 0) {
      throw new IllegalArgumentException("position fields cannot be negative");
    }
    Objects.requireNonNull(identity, "identity");
    released = List.copyOf(released);
    snapshot = snapshot == null ? null : Map.copyOf(snapshot);
    extras = Map.copyOf(extras);
    threads =
        threads == null || threads.isEmpty()
            ? java.util.Collections.emptySortedMap()
            : java.util.Collections.unmodifiableSortedMap(new java.util.TreeMap<>(threads));
  }

  /** The starting position for a fresh connector on this database. */
  public static Position initial(long startScn, DatabaseIdentity identity) {
    return new Position(
        CURRENT_VERSION, startScn, 0, null, 0, 0, 0, 0, identity, List.of(), null, Map.of());
  }

  /**
   * ADR-0019: the first start at {@code startScn} when transactions may already be open. {@code
   * open} are the transactions {@code GV$TRANSACTION} listed, read after SCN {@code openScn} and
   * before {@code startScn} (so {@code openScn <= startScn}), and {@code oldestOpenStart} the
   * earliest of their start SCNs, or -1 when none was open. Mining begins at the earlier of {@code
   * openScn} and {@code oldestOpenStart}, so every transaction open at {@code startScn} is read
   * whole: it is either in {@code open} or began at or after {@code openScn}. Below {@code
   * startScn} only those transactions are kept and their commits below it are skipped. The fields
   * live in the extras, so a restart before the first acknowledged commit applies them again.
   */
  public static Position firstStart(
      long startScn,
      long openScn,
      long oldestOpenStart,
      java.util.Collection<TxKey> open,
      DatabaseIdentity identity) {
    if (openScn > startScn) {
      throw new IllegalArgumentException("openScn " + openScn + " is after startScn " + startScn);
    }
    long mineFrom = oldestOpenStart >= 0 ? Math.min(openScn, oldestOpenStart) : openScn;
    if (mineFrom >= startScn) {
      return initial(startScn, identity);
    }
    return initial(mineFrom, identity)
        .withExtra(START_FLOOR_SCN, startScn)
        .withExtra(START_OPEN_SCN, openScn)
        .withExtra(
            START_OPEN_XIDS,
            open.isEmpty()
                ? null
                : String.join(",", open.stream().map(TxKey::toString).sorted().toList()));
  }

  /** The extras keys of {@link #firstStart}; dropped once a commit is acknowledged. */
  public static final String START_FLOOR_SCN = "start_floor_scn";

  public static final String START_OPEN_SCN = "start_open_scn";
  public static final String START_OPEN_XIDS = "start_open_xids";

  /** {@link #START_OPEN_SCN}, or 0 without a first-start floor. */
  public long startOpenScn() {
    return longExtra(START_OPEN_SCN);
  }

  /** The transactions open at the first start ({@link TxKey#toString()} forms). */
  public java.util.Set<String> startOpenTransactions() {
    Object v = extras.get(START_OPEN_XIDS);
    return v == null || v.toString().isEmpty()
        ? java.util.Set.of()
        : java.util.Set.of(v.toString().split(","));
  }

  private long longExtra(String key) {
    Object v = extras.get(key);
    if (v instanceof Number n) {
      return n.longValue();
    }
    return v == null ? 0 : Long.parseLong(v.toString());
  }

  /**
   * Commits below this SCN were before the connector's first start and are skipped while no commit
   * has been acknowledged; 0 when there is no floor.
   */
  public long startFloorScn() {
    return longExtra(START_FLOOR_SCN);
  }

  /** True when {@code scn} is below the first-start floor and nothing has been acknowledged yet. */
  public boolean beforeFirstStart(long scn) {
    return !hasCommit() && scn < startFloorScn();
  }

  public boolean hasCommit() {
    return lastCommitKey != null;
  }

  /** The SCN-only form: the redo byte address is dropped (legacy callers and tests). */
  public Position withResumeScn(long scn) {
    return new Position(
        version,
        scn,
        lastCommitScn,
        lastCommitKey,
        lastCommitThread,
        eventIndex,
        journalGeneration,
        schemaEpoch,
        identity,
        released,
        snapshot,
        extras,
        null,
        0,
        lastCommitRsId,
        lastCommitSsn,
        threads);
  }

  /** The resume point: SCN floor for log selection plus the inclusive redo byte address. */
  public Position withResume(RedoRecordId point) {
    return new Position(
        version,
        point.scn(),
        lastCommitScn,
        lastCommitKey,
        lastCommitThread,
        eventIndex,
        journalGeneration,
        schemaEpoch,
        identity,
        released,
        snapshot,
        extras,
        point.hasRba() ? point.rsId() : null,
        point.hasRba() ? point.ssn() : 0,
        lastCommitRsId,
        lastCommitSsn,
        threads);
  }

  /**
   * ADR-0026: the resume point of a restart, per redo thread. With at most one thread in {@code
   * point} and no per-thread block this is exactly {@link #withResume(RedoRecordId)} with the
   * legacy point, so a single-instance offset is unchanged. Otherwise each thread of {@code point}
   * gets a mark (keeping its last acknowledged commit; a thread that had none takes the legacy last
   * commit when that commit is on it), threads already in the block but not in {@code point} keep
   * theirs, and the legacy keys carry the lowest resume SCN of the marks without a redo byte
   * address, which no single address could give for several threads.
   */
  public Position withResume(ResumePoint point) {
    if (!perThread() && !point.perThread()) {
      return withResume(point.legacy());
    }
    if (point.byThread().isEmpty()) {
      throw new IllegalArgumentException(
          "A per-thread position needs a resume point per thread, not " + point.legacy());
    }
    java.util.SortedMap<Integer, ThreadMark> marks = new java.util.TreeMap<>(threads);
    for (Map.Entry<Integer, RedoRecordId> e : point.byThread().entrySet()) {
      int thread = e.getKey();
      ThreadMark had = marks.get(thread);
      marks.put(
          thread,
          had != null && had.hasCommit()
              ? new ThreadMark(e.getValue(), had.commit(), had.commitKey())
              : seeded(thread, e.getValue()));
    }
    if (hasCommit() && !marks.containsKey(lastCommitThread)) {
      // the legacy last commit's thread has no point yet: it is read again from the lowest SCN
      marks.put(lastCommitThread, seeded(lastCommitThread, lowestResume(marks)));
    }
    return new Position(
        version,
        lowestResume(marks).scn(),
        lastCommitScn,
        lastCommitKey,
        lastCommitThread,
        eventIndex,
        journalGeneration,
        schemaEpoch,
        identity,
        released,
        snapshot,
        extras,
        null,
        0,
        lastCommitRsId,
        lastCommitSsn,
        marks);
  }

  /** A thread's first mark: its last commit is the legacy last commit when that is on it. */
  private ThreadMark seeded(int thread, RedoRecordId resume) {
    return hasCommit() && thread == lastCommitThread
        ? new ThreadMark(resume, lastCommitId(), lastCommitKey)
        : new ThreadMark(resume, null, null);
  }

  /** The lowest resume SCN of {@code marks}, without a redo byte address. */
  private static RedoRecordId lowestResume(java.util.SortedMap<Integer, ThreadMark> marks) {
    long lowest = Long.MAX_VALUE;
    for (ThreadMark m : marks.values()) {
      lowest = Math.min(lowest, m.resume().scn());
    }
    return new RedoRecordId(lowest, null, 0);
  }

  /** Where mining restarts: the resume SCN with the redo byte address when the position has one. */
  public RedoRecordId resumePoint() {
    return new RedoRecordId(resumeScn, resumeRsId, resumeSsn);
  }

  /** ADR-0026: the resume point per redo thread; the legacy point alone for one thread. */
  public ResumePoint resume() {
    if (!perThread()) {
      return ResumePoint.single(resumePoint());
    }
    java.util.SortedMap<Integer, RedoRecordId> by = new java.util.TreeMap<>();
    threads.forEach((t, m) -> by.put(t, m.resume()));
    return new ResumePoint(resumePoint(), by);
  }

  /**
   * Where the redo of {@code thread} is read again from: its mark's resume record in a per-thread
   * position, else the legacy point.
   */
  public RedoRecordId resumePointFor(int thread) {
    ThreadMark m = threads.get(thread);
    return m == null ? resumePoint() : m.resume();
  }

  /** The last acknowledged commit as a redo record, or null before any commit. */
  public RedoRecordId lastCommitId() {
    return lastCommitKey == null
        ? null
        : new RedoRecordId(lastCommitScn, lastCommitRsId, lastCommitSsn);
  }

  /**
   * After the framework acknowledged {@code acknowledged} events of the given commit (SCN only).
   */
  public Position withCommit(long commitScn, int thread, TxKey key, int acknowledged) {
    return withCommit(new RedoRecordId(commitScn, null, 0), thread, key, acknowledged);
  }

  /** After the framework acknowledged {@code acknowledged} events of the commit at {@code id}. */
  public Position withCommit(RedoRecordId id, int thread, TxKey key, int acknowledged) {
    Map<String, Object> kept = extras;
    if (extras.containsKey(START_FLOOR_SCN)) {
      // ADR-0019: with a commit acknowledged the first-start floor no longer applies
      kept = new java.util.LinkedHashMap<>(extras);
      kept.remove(START_FLOOR_SCN);
      kept.remove(START_OPEN_SCN);
      kept.remove(START_OPEN_XIDS);
    }
    java.util.SortedMap<Integer, ThreadMark> marks = threads;
    if (perThread()) {
      // ADR-0026: the commit is also the last acknowledged one of its own thread
      marks = new java.util.TreeMap<>(threads);
      ThreadMark had = marks.get(thread);
      marks.put(
          thread,
          new ThreadMark(
              had != null ? had.resume() : new RedoRecordId(resumeScn, null, 0), id, key));
    }
    return new Position(
        version,
        resumeScn,
        id.scn(),
        key,
        thread,
        acknowledged,
        journalGeneration,
        schemaEpoch,
        identity,
        released,
        snapshot,
        kept,
        resumeRsId,
        resumeSsn,
        id.hasRba() ? id.rsId() : null,
        id.hasRba() ? id.ssn() : 0,
        marks);
  }

  /** {@code extras} with {@code key} set to {@code value}; null removes it. */
  public Position withExtra(String key, Object value) {
    Map<String, Object> next = new java.util.LinkedHashMap<>(extras);
    if (value == null) {
      next.remove(key);
    } else {
      next.put(key, value);
    }
    return new Position(
        version,
        resumeScn,
        lastCommitScn,
        lastCommitKey,
        lastCommitThread,
        eventIndex,
        journalGeneration,
        schemaEpoch,
        identity,
        released,
        snapshot,
        next,
        resumeRsId,
        resumeSsn,
        lastCommitRsId,
        lastCommitSsn,
        threads);
  }

  /** PRD-02 SNAP-3: the snapshot block, null when no snapshot was ever started. */
  public Position withSnapshot(Map<String, Object> block) {
    return new Position(
        version,
        resumeScn,
        lastCommitScn,
        lastCommitKey,
        lastCommitThread,
        eventIndex,
        journalGeneration,
        schemaEpoch,
        identity,
        released,
        block,
        extras,
        resumeRsId,
        resumeSsn,
        lastCommitRsId,
        lastCommitSsn,
        threads);
  }

  public Position withJournalGeneration(long generation) {
    return new Position(
        version,
        resumeScn,
        lastCommitScn,
        lastCommitKey,
        lastCommitThread,
        eventIndex,
        generation,
        schemaEpoch,
        identity,
        released,
        snapshot,
        extras,
        resumeRsId,
        resumeSsn,
        lastCommitRsId,
        lastCommitSsn,
        threads);
  }

  public Position withReleased(List<String> xids) {
    return new Position(
        version,
        resumeScn,
        lastCommitScn,
        lastCommitKey,
        lastCommitThread,
        eventIndex,
        journalGeneration,
        schemaEpoch,
        identity,
        xids,
        snapshot,
        extras,
        resumeRsId,
        resumeSsn,
        lastCommitRsId,
        lastCommitSsn,
        threads);
  }

  /** ADR-0026: the per-thread block; empty for a single-thread position. */
  public Position withThreads(java.util.SortedMap<Integer, ThreadMark> marks) {
    return new Position(
        version,
        resumeScn,
        lastCommitScn,
        lastCommitKey,
        lastCommitThread,
        eventIndex,
        journalGeneration,
        schemaEpoch,
        identity,
        released,
        snapshot,
        extras,
        resumeRsId,
        resumeSsn,
        lastCommitRsId,
        lastCommitSsn,
        marks);
  }

  /** True when the position carries a mark per redo thread (ADR-0026). */
  public boolean perThread() {
    return !threads.isEmpty();
  }
}
