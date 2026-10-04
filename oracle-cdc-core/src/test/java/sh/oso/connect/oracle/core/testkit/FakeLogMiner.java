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
package sh.oso.connect.oracle.core.testkit;

import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import sh.oso.connect.oracle.core.mining.event.EventCursor;
import sh.oso.connect.oracle.core.mining.event.EventSource;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;

/**
 * Scripted {@link EventSource} (CORE-TEST-1, ADR-0009). Events are appended in redo order with
 * ascending SCNs; a cursor over a range replays the events inside it. Faults fire when the cursor
 * reaches a given event index, once or always, so step-retry logic can be driven without Oracle.
 */
public final class FakeLogMiner implements EventSource {

  public static final TableId DEFAULT_TABLE = new TableId("FREEPDB1", "APP", "ORDERS");

  private final List<MiningEvent> events = new ArrayList<>();
  private final Map<Integer, Throwable> faults = new HashMap<>();
  private final Map<Integer, Boolean> faultAlways = new HashMap<>();
  private long nextScn = 1000;
  private long rba = 1;
  public int opened;
  public boolean closed;

  public FakeLogMiner startAt(long scn) {
    this.nextScn = scn;
    return this;
  }

  public long nextScn() {
    return nextScn;
  }

  private RedoRecordId id() {
    return new RedoRecordId(nextScn++, String.format("0x%06x.%08x.%04x", 1, rba++, 0), 0);
  }

  public FakeLogMiner add(MiningEvent e) {
    events.add(e);
    return this;
  }

  public TxKey tx(long usn, long slot, long sqn) {
    return new TxKey(3, new Xid(usn, slot, sqn));
  }

  public FakeLogMiner start(TxKey tx, String user) {
    return add(new MiningEvent.TxStart(tx, id(), 1, user, null, 10, 1, Instant.EPOCH));
  }

  public FakeLogMiner insert(TxKey tx, TableId t, String sql) {
    return dml(tx, Operation.INSERT, t, sql, false);
  }

  public FakeLogMiner update(TxKey tx, TableId t, String sql) {
    return dml(tx, Operation.UPDATE, t, sql, false);
  }

  public FakeLogMiner delete(TxKey tx, TableId t, String sql) {
    return dml(tx, Operation.DELETE, t, sql, false);
  }

  /** A ROLLBACK=1 row undoing the latest earlier row with the same ROW_ID. */
  public FakeLogMiner undo(TxKey tx, Operation op, TableId t, String sql, String rowId) {
    return add(
        new MiningEvent.Dml(
            tx,
            id(),
            1,
            op,
            t,
            100,
            100,
            1,
            rowId,
            sql,
            null,
            true,
            0,
            null,
            "APP",
            Instant.EPOCH));
  }

  public FakeLogMiner dml(TxKey tx, Operation op, TableId t, String sql, boolean undo) {
    return add(
        new MiningEvent.Dml(
            tx,
            id(),
            1,
            op,
            t,
            100,
            100,
            1,
            "AAAR" + rba,
            sql,
            null,
            undo,
            0,
            null,
            "APP",
            Instant.EPOCH));
  }

  public FakeLogMiner dmlWithRowId(TxKey tx, Operation op, TableId t, String sql, String rowId) {
    return add(
        new MiningEvent.Dml(
            tx,
            id(),
            1,
            op,
            t,
            100,
            100,
            1,
            rowId,
            sql,
            null,
            false,
            0,
            null,
            "APP",
            Instant.EPOCH));
  }

  public FakeLogMiner commit(TxKey tx) {
    return add(new MiningEvent.Commit(tx, id(), 1, Instant.EPOCH));
  }

  public FakeLogMiner rollback(TxKey tx) {
    return add(new MiningEvent.Rollback(tx, id(), 1, Instant.EPOCH));
  }

  public FakeLogMiner ddl(TxKey tx, TableId t, long dataObj, String sql) {
    return add(
        new MiningEvent.Ddl(
            tx,
            id(),
            1,
            t.pdb(),
            t.schema(),
            t.table(),
            dataObj,
            2,
            sql,
            "APP",
            0,
            null,
            Instant.EPOCH));
  }

  public FakeLogMiner logBoundary(long sequence) {
    return add(new MiningEvent.LogBoundary(id(), 1, sequence));
  }

  public FakeLogMiner missingScn() {
    return add(new MiningEvent.MissingScn(id(), 1, "missing redo"));
  }

  /** Throws {@code t} when a cursor reaches event {@code index} (0-based, in the full script). */
  public FakeLogMiner faultAt(int index, Throwable t, boolean always) {
    faults.put(index, t);
    faultAlways.put(index, always);
    return this;
  }

  public List<MiningEvent> events() {
    return List.copyOf(events);
  }

  @Override
  public EventCursor open(long startScn, long endScn) {
    opened++;
    return new EventCursor() {
      private int i = -1;
      private MiningEvent current;

      @Override
      public boolean next() throws SQLException {
        while (++i < events.size()) {
          MiningEvent e = events.get(i);
          if (e.scn() < startScn) {
            continue;
          }
          if (e.scn() >= endScn) {
            break;
          }
          Throwable fault = faults.get(i);
          if (fault != null) {
            if (!faultAlways.get(i)) {
              faults.remove(i);
            }
            if (fault instanceof SQLException sql) {
              throw sql;
            }
            if (fault instanceof RuntimeException rt) {
              throw rt;
            }
            throw new IllegalStateException(fault);
          }
          current = e;
          return true;
        }
        current = null;
        return false;
      }

      @Override
      public MiningEvent event() {
        return current;
      }

      @Override
      public void close() {}
    };
  }

  @Override
  public void close() {
    closed = true;
  }
}
