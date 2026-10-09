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
  public int recycled;
  private SQLException openFault;

  /** P1-17: what the redo dictionary makes of a lag row, by the row's position in the redo. */
  private final Map<RedoRecordId, MiningEvent> withRedoDictionary = new HashMap<>();

  /** Opens with the redo dictionary so far. */
  public int replays;

  /** Thrown by every redo-dictionary open, like a database without a usable build. */
  public RuntimeException redoDictionaryFault;

  /** The next {@link #open} throws {@code e} once, like a mining connection that was dropped. */
  public FakeLogMiner failNextOpen(SQLException e) {
    this.openFault = e;
    return this;
  }

  public FakeLogMiner startAt(long scn) {
    this.nextScn = scn;
    return this;
  }

  public long nextScn() {
    return nextScn;
  }

  private Long lateScn;
  private boolean zeroRsId;

  /** The redo thread stamped on the events scripted from now on; 1 until {@link #onThread}. */
  private int thread = 1;

  /**
   * Events scripted from now on carry redo thread {@code thread}, as rows of another RAC instance
   * would. The redo byte addresses keep one counter across threads, so the cursor still sees every
   * event in script order.
   */
  public FakeLogMiner onThread(int thread) {
    this.thread = thread;
    return this;
  }

  /** With {@link #perThreadRba()}: the next redo byte address of each thread. */
  private java.util.Map<Integer, Long> rbaByThread;

  /**
   * From now on each thread numbers its redo byte addresses from one, as each RAC instance writes
   * its own redo: two threads then produce the same RS_ID strings, which are different records.
   */
  public FakeLogMiner perThreadRba() {
    this.rbaByThread = new java.util.HashMap<>();
    return this;
  }

  private RedoRecordId id() {
    long scn = lateScn != null ? lateScn : nextScn++;
    lateScn = null;
    if (zeroRsId) {
      zeroRsId = false;
      return new RedoRecordId(scn, " 0x000000.00000000.0000 ", 0);
    }
    long next = rbaByThread == null ? rba++ : rbaByThread.merge(thread, 1L, Long::sum);
    return new RedoRecordId(scn, String.format(" 0x%06x.%08x.%04x ", 1, next, 0), 0);
  }

  /**
   * The next event carries an all-zero RS_ID, which is no redo byte address: LogMiner returned the
   * ROLLBACK row of a rolled-back transaction that way on a GitHub runner (7 October 2026).
   */
  public FakeLogMiner zeroRsId() {
    this.zeroRsId = true;
    return this;
  }

  /**
   * The next event carries {@code scn}, an SCN already passed, but the next redo byte address: a
   * private redo strand bound late (ADR-0014). The SCN counter is not advanced.
   */
  public FakeLogMiner late(long scn) {
    this.lateScn = scn;
    return this;
  }

  public FakeLogMiner add(MiningEvent e) {
    events.add(e);
    return this;
  }

  public TxKey tx(long usn, long slot, long sqn) {
    return new TxKey(3, new Xid(usn, slot, sqn));
  }

  public FakeLogMiner start(TxKey tx, String user) {
    return add(new MiningEvent.TxStart(tx, id(), thread, user, null, 10, 1, Instant.EPOCH));
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
            thread,
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
            thread,
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
            thread,
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

  /**
   * P1-17: a row written before a later DDL on its table. The online catalog gives it STATUS 2 and
   * {@code generic} SQL; mined with the redo dictionary it is {@code real} with STATUS 0.
   */
  public FakeLogMiner lagged(
      TxKey tx, Operation op, TableId t, String generic, String real, String rowId) {
    dmlWithRowId(tx, op, t, real, rowId);
    MiningEvent.Dml exact = (MiningEvent.Dml) events.remove(events.size() - 1);
    events.add(
        new MiningEvent.Dml(
            exact.tx(),
            exact.id(),
            exact.thread(),
            exact.op(),
            exact.table(),
            exact.dataObj(),
            exact.dataObjd(),
            exact.dataObjv(),
            exact.rowId(),
            generic,
            null,
            false,
            2,
            null,
            exact.username(),
            exact.timestamp()));
    // still generic with the redo dictionary too: LogMiner keeps STATUS 2
    withRedoDictionary.put(
        exact.id(), real.equals(generic) ? events.get(events.size() - 1) : exact);
    return this;
  }

  public FakeLogMiner commit(TxKey tx) {
    return add(new MiningEvent.Commit(tx, id(), thread, Instant.EPOCH));
  }

  public FakeLogMiner rollback(TxKey tx) {
    return add(new MiningEvent.Rollback(tx, id(), thread, Instant.EPOCH));
  }

  public FakeLogMiner ddl(TxKey tx, TableId t, long dataObj, String sql) {
    return add(
        new MiningEvent.Ddl(
            tx,
            id(),
            thread,
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
    return add(new MiningEvent.LogBoundary(id(), thread, sequence));
  }

  public FakeLogMiner missingScn() {
    return add(new MiningEvent.MissingScn(id(), thread, "missing redo"));
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
  public void recycle() {
    recycled++;
  }

  /** The SCN-cursor form, for tests that reason in SCNs only. */
  public EventCursor open(long startScn, long endScn) throws SQLException {
    return open(sh.oso.connect.oracle.core.mining.step.StepCursor.at(startScn), endScn);
  }

  @Override
  public EventCursor open(sh.oso.connect.oracle.core.mining.step.StepCursor from, long endScn)
      throws SQLException {
    opened++;
    if (openFault != null) {
      SQLException e = openFault;
      openFault = null;
      throw e;
    }
    return cursor(from, endScn, Map.of());
  }

  @Override
  public EventSource redoDictionary() {
    return new EventSource() {
      @Override
      public EventCursor open(sh.oso.connect.oracle.core.mining.step.StepCursor from, long endScn)
          throws SQLException {
        replays++;
        if (redoDictionaryFault != null) {
          throw redoDictionaryFault;
        }
        return cursor(from, endScn, withRedoDictionary);
      }

      @Override
      public void close() {}
    };
  }

  private EventCursor cursor(
      sh.oso.connect.oracle.core.mining.step.StepCursor from,
      long endScn,
      Map<RedoRecordId, MiningEvent> replaced) {
    return new EventCursor() {
      private int i = -1;
      private MiningEvent current;

      @Override
      public boolean next() throws SQLException {
        while (++i < events.size()) {
          MiningEvent e = events.get(i);
          // with a redo byte address the log is read in append order whatever the SCN; without
          // one the fake behaves like an SCN window
          // an all-zero RS_ID is no address: the query returns those rows by SCN within the step;
          // a thread without a mark of its own (nor a thread-less one) is read by SCN (ADR-0026)
          sh.oso.connect.oracle.core.model.RedoRecordId mark = from.markFor(e.thread());
          boolean skip =
              from.hasRba()
                  ? e.id().zeroRsId() || mark == null || !mark.hasRba()
                      ? e.scn() < from.scn()
                      : from.alreadyApplied(e.thread(), e.id())
                  : e.scn() < from.scn();
          if (skip) {
            continue;
          }
          if (e.scn() >= endScn) {
            if (from.hasRba()) {
              continue; // a later row below the bound may still follow
            }
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
          current = replaced.getOrDefault(e.id(), e);
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
