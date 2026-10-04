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
package sh.oso.connect.oracle.core.mining.event;

import java.time.Instant;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;

/**
 * Semantic events the engine consumes (ADR-0009). Real rows become events through {@link
 * LogMinerRowAdapter}; unit tests script them directly with the fake, so the engine is never
 * coupled to V$LOGMNR_CONTENTS quirks.
 */
public sealed interface MiningEvent {

  RedoRecordId id();

  default long scn() {
    return id().scn();
  }

  /** START row: a transaction began. Zero XIDs never create buffer entries (CORE-TX-1). */
  record TxStart(
      TxKey tx,
      RedoRecordId id,
      int thread,
      String username,
      String clientId,
      long sessionNo,
      long serialNo,
      Instant timestamp)
      implements MiningEvent {}

  /** A row change on a captured object; {@code undo} marks a ROLLBACK=1 row (CORE-TX-2). */
  record Dml(
      TxKey tx,
      RedoRecordId id,
      int thread,
      Operation op,
      TableId table,
      long dataObj,
      long dataObjd,
      long dataObjv,
      String rowId,
      String sqlRedo,
      String sqlUndo,
      boolean undo,
      int status,
      String info,
      String username,
      Instant timestamp)
      implements MiningEvent {}

  record Commit(TxKey tx, RedoRecordId id, int thread, Instant timestamp) implements MiningEvent {}

  record Rollback(TxKey tx, RedoRecordId id, int thread, Instant timestamp)
      implements MiningEvent {}

  /** DDL by a captured owner; {@code objectName} may be null when the segment no longer exists. */
  record Ddl(
      TxKey tx,
      RedoRecordId id,
      int thread,
      String pdb,
      String owner,
      String objectName,
      long dataObj,
      long dataObjv,
      String sql,
      String username,
      int status,
      String info,
      Instant timestamp)
      implements MiningEvent {}

  /** OPERATION UNSUPPORTED (255) for a captured object: a stop or DLQ condition (CORE-MINE-10). */
  record Unsupported(
      TxKey tx,
      RedoRecordId id,
      TableId table,
      long dataObj,
      int status,
      String info,
      String sqlRedo)
      implements MiningEvent {}

  /** MISSING_SCN: LogMiner found a hole in the redo it was given; always a stop (CORE-MINE-10). */
  record MissingScn(RedoRecordId id, int thread, String info) implements MiningEvent {}

  /** Any other operation code that reached the client; counted and ignored. */
  record Other(TxKey tx, RedoRecordId id, Operation op, String operation) implements MiningEvent {}

  /** Fake-only: the boundary between two logs, used by scheduler tests. */
  record LogBoundary(RedoRecordId id, int thread, long sequence) implements MiningEvent {}
}
