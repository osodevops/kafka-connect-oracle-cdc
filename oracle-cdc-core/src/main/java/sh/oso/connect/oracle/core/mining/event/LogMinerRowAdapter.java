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

import java.sql.SQLException;
import java.util.Map;
import sh.oso.connect.oracle.core.mining.LogMinerRow;
import sh.oso.connect.oracle.core.mining.RowCursor;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.TableId;

/**
 * Maps V$LOGMNR_CONTENTS rows to {@link MiningEvent}s. Joins CSF continuation rows into one event.
 * Names the table by (SRC_CON_ID, DATA_OBJ#) through the resolved id map first, because rows
 * written into a segment that was later dropped carry no name under the online catalog
 * (reference/object-id-stability.md), and falls back to the row's own SEG_OWNER and TABLE_NAME.
 */
public final class LogMinerRowAdapter {

  private final Map<sh.oso.connect.oracle.core.mining.ObjectKey, TableId> tablesByObject;
  private LogMinerRow pending;
  private StringBuilder redo;
  private StringBuilder undo;

  public LogMinerRowAdapter(
      Map<sh.oso.connect.oracle.core.mining.ObjectKey, TableId> tablesByObject) {
    this.tablesByObject = Map.copyOf(tablesByObject);
  }

  /** Returns the event for this row, or null when the row is a continuation still being joined. */
  public MiningEvent accept(LogMinerRow row) {
    if (pending != null) {
      redo.append(nullToEmpty(row.sqlRedo()));
      undo.append(nullToEmpty(row.sqlUndo()));
      if (row.csf()) {
        return null;
      }
      LogMinerRow whole = pendingWithSql(pending, redo.toString(), undo.toString());
      pending = null;
      redo = null;
      undo = null;
      return toEvent(whole);
    }
    if (row.csf()) {
      pending = row;
      redo = new StringBuilder(nullToEmpty(row.sqlRedo()));
      undo = new StringBuilder(nullToEmpty(row.sqlUndo()));
      return null;
    }
    return toEvent(row);
  }

  /** A CSF chain was cut by the end of the step: nothing is emitted for it. */
  public boolean hasPending() {
    return pending != null;
  }

  public void reset() {
    pending = null;
    redo = null;
    undo = null;
  }

  private static LogMinerRow pendingWithSql(LogMinerRow r, String sqlRedo, String sqlUndo) {
    return new LogMinerRow(
        r.scn(),
        r.startScn(),
        r.commitScn(),
        r.timestamp(),
        r.commitTimestamp(),
        r.thread(),
        r.xid(),
        r.operation(),
        r.operationCode(),
        r.rollback(),
        r.status(),
        r.info(),
        r.segOwner(),
        r.segName(),
        r.tableName(),
        r.username(),
        r.sessionNo(),
        r.serialNo(),
        r.clientId(),
        r.rowId(),
        r.rsId(),
        r.ssn(),
        false,
        r.dataObj(),
        r.dataObjd(),
        r.dataObjv(),
        r.srcConId(),
        r.srcConName(),
        r.srcConDbid(),
        r.conId(),
        sqlRedo,
        sqlUndo);
  }

  MiningEvent toEvent(LogMinerRow r) {
    Operation op = r.op();
    switch (op) {
      case START:
        return new MiningEvent.TxStart(
            r.txKey(),
            r.id(),
            r.thread(),
            r.username(),
            r.clientId(),
            r.sessionNo(),
            r.serialNo(),
            r.timestamp());
      case COMMIT:
        return new MiningEvent.Commit(r.txKey(), r.id(), r.thread(), r.timestamp());
      case ROLLBACK:
        return new MiningEvent.Rollback(r.txKey(), r.id(), r.thread(), r.timestamp());
      case DDL:
        return new MiningEvent.Ddl(
            r.txKey(),
            r.id(),
            r.thread(),
            pdbOf(r),
            r.segOwner(),
            r.tableName(),
            r.dataObj(),
            r.dataObjv(),
            r.sqlRedo(),
            r.username(),
            r.status(),
            r.info(),
            r.timestamp());
      case UNSUPPORTED:
        return new MiningEvent.Unsupported(
            r.txKey(), r.id(), table(r), r.dataObj(), r.status(), r.info(), r.sqlRedo());
      case MISSING_SCN:
        return new MiningEvent.MissingScn(r.id(), r.thread(), r.info());
      default:
        if (op.isRowChange()) {
          return new MiningEvent.Dml(
              r.txKey(),
              r.id(),
              r.thread(),
              op,
              table(r),
              r.dataObj(),
              r.dataObjd(),
              r.dataObjv(),
              r.rowId(),
              r.sqlRedo(),
              r.sqlUndo(),
              r.rollback(),
              r.status(),
              r.info(),
              r.username(),
              r.timestamp());
        }
        return new MiningEvent.Other(r.txKey(), r.id(), op, r.operation());
    }
  }

  private TableId table(LogMinerRow r) {
    TableId resolved =
        tablesByObject.get(
            new sh.oso.connect.oracle.core.mining.ObjectKey(r.srcConId(), r.dataObj()));
    if (resolved != null) {
      return resolved;
    }
    String owner = r.segOwner() == null ? "" : r.segOwner();
    String name =
        r.tableName() != null
            ? r.tableName()
            : r.segName() != null ? r.segName() : "OBJ# " + r.dataObj();
    return new TableId(pdbOf(r), owner, name);
  }

  private static String pdbOf(LogMinerRow r) {
    return r.srcConId() > 1 ? r.srcConName() : null;
  }

  private static String nullToEmpty(String s) {
    return s == null ? "" : s;
  }

  /** Wraps a row cursor as an event cursor. */
  public EventCursor adapt(RowCursor rows) {
    return new EventCursor() {
      private MiningEvent current;

      @Override
      public boolean next() throws SQLException {
        while (rows.next()) {
          MiningEvent e = accept(rows.row());
          if (e != null) {
            current = e;
            return true;
          }
        }
        current = null;
        return false;
      }

      @Override
      public MiningEvent event() {
        return current;
      }

      @Override
      public void close() throws SQLException {
        reset();
        rows.close();
      }
    };
  }
}
