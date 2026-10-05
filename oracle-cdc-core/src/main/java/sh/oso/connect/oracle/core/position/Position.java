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
    long lastCommitSsn) {

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
  }

  /** The starting position for a fresh connector on this database. */
  public static Position initial(long startScn, DatabaseIdentity identity) {
    return new Position(
        CURRENT_VERSION, startScn, 0, null, 0, 0, 0, 0, identity, List.of(), null, Map.of());
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
        lastCommitSsn);
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
        lastCommitSsn);
  }

  /** Where mining restarts: the resume SCN with the redo byte address when the position has one. */
  public RedoRecordId resumePoint() {
    return new RedoRecordId(resumeScn, resumeRsId, resumeSsn);
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
        extras,
        resumeRsId,
        resumeSsn,
        id.hasRba() ? id.rsId() : null,
        id.hasRba() ? id.ssn() : 0);
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
        lastCommitSsn);
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
        lastCommitSsn);
  }
}
