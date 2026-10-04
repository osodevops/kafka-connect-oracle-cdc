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
package sh.oso.connect.oracle.core.mining;

import java.time.Instant;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;

/**
 * One V$LOGMNR_CONTENTS row, every column the engine depends on (reference/logminer-columns.md).
 */
public record LogMinerRow(
    long scn,
    long startScn,
    long commitScn,
    Instant timestamp,
    Instant commitTimestamp,
    int thread,
    Xid xid,
    String operation,
    int operationCode,
    boolean rollback,
    int status,
    String info,
    String segOwner,
    String segName,
    String tableName,
    String username,
    long sessionNo,
    long serialNo,
    String clientId,
    String rowId,
    String rsId,
    long ssn,
    boolean csf,
    long dataObj,
    long dataObjd,
    long dataObjv,
    int srcConId,
    String srcConName,
    long srcConDbid,
    int conId,
    String sqlRedo,
    String sqlUndo) {

  public Operation op() {
    return Operation.fromCode(operationCode);
  }

  public RedoRecordId id() {
    return new RedoRecordId(scn, rsId == null ? "" : rsId, ssn);
  }

  public TxKey txKey() {
    return new TxKey(srcConId, xid == null ? Xid.ZERO : xid);
  }

  /** The same row with continued SQL text appended (CSF continuation). */
  LogMinerRow withSql(String redo, String undo) {
    return new LogMinerRow(
        scn,
        startScn,
        commitScn,
        timestamp,
        commitTimestamp,
        thread,
        xid,
        operation,
        operationCode,
        rollback,
        status,
        info,
        segOwner,
        segName,
        tableName,
        username,
        sessionNo,
        serialNo,
        clientId,
        rowId,
        rsId,
        ssn,
        false,
        dataObj,
        dataObjd,
        dataObjv,
        srcConId,
        srcConName,
        srcConDbid,
        conId,
        redo,
        undo);
  }
}
