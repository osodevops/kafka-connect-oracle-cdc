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

  /** The transaction the row belongs to, or null for rows that name none. */
  default TxKey txOrNull() {
    return null;
  }

  /** The same row attributed to {@code tx}; rows that name no transaction are returned as is. */
  default MiningEvent withTx(TxKey tx) {
    return this;
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
      implements MiningEvent {
    @Override
    public TxKey txOrNull() {
      return tx;
    }

    @Override
    public MiningEvent withTx(TxKey t) {
      return new TxStart(t, id, thread, username, clientId, sessionNo, serialNo, timestamp);
    }
  }

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
      implements MiningEvent {
    @Override
    public TxKey txOrNull() {
      return tx;
    }

    @Override
    public MiningEvent withTx(TxKey t) {
      return new Dml(
          t, id, thread, op, table, dataObj, dataObjd, dataObjv, rowId, sqlRedo, sqlUndo, undo,
          status, info, username, timestamp);
    }
  }

  record Commit(TxKey tx, RedoRecordId id, int thread, Instant timestamp) implements MiningEvent {
    @Override
    public TxKey txOrNull() {
      return tx;
    }

    @Override
    public MiningEvent withTx(TxKey t) {
      return new Commit(t, id, thread, timestamp);
    }
  }

  record Rollback(TxKey tx, RedoRecordId id, int thread, Instant timestamp) implements MiningEvent {
    @Override
    public TxKey txOrNull() {
      return tx;
    }

    @Override
    public MiningEvent withTx(TxKey t) {
      return new Rollback(t, id, thread, timestamp);
    }
  }

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
      implements MiningEvent {
    @Override
    public TxKey txOrNull() {
      return tx;
    }

    @Override
    public MiningEvent withTx(TxKey t) {
      return new Ddl(
          t,
          id,
          thread,
          pdb,
          owner,
          objectName,
          dataObj,
          dataObjv,
          sql,
          username,
          status,
          info,
          timestamp);
    }
  }

  /** OPERATION UNSUPPORTED (255) for a captured object: a stop or DLQ condition (CORE-MINE-10). */
  record Unsupported(
      TxKey tx,
      RedoRecordId id,
      TableId table,
      long dataObj,
      int status,
      String info,
      String sqlRedo)
      implements MiningEvent {
    @Override
    public TxKey txOrNull() {
      return tx;
    }

    @Override
    public MiningEvent withTx(TxKey t) {
      return new Unsupported(t, id, table, dataObj, status, info, sqlRedo);
    }
  }

  /** MISSING_SCN: LogMiner found a hole in the redo it was given; always a stop (CORE-MINE-10). */
  record MissingScn(RedoRecordId id, int thread, String info) implements MiningEvent {}

  /** Any other operation code that reached the client; counted and ignored. */
  record Other(TxKey tx, RedoRecordId id, Operation op, String operation) implements MiningEvent {
    @Override
    public TxKey txOrNull() {
      return tx;
    }

    @Override
    public MiningEvent withTx(TxKey t) {
      return new Other(t, id, op, operation);
    }
  }

  /** Fake-only: the boundary between two logs, used by scheduler tests. */
  record LogBoundary(RedoRecordId id, int thread, long sequence) implements MiningEvent {}
}
