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
package sh.oso.connect.oracle.core.buffer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;

class HeapTransactionBufferTest {

  private static final TableId T = new TableId("FREEPDB1", "APP", "ORDERS");
  private long scn = 100;

  private RedoRecordId id() {
    return new RedoRecordId(scn++, "0x01", 0);
  }

  private static TxKey key(int con, long sqn) {
    return new TxKey(con, new Xid(7, 1, sqn));
  }

  private RowChange change(TxKey k, Operation op, String rowId, String name) {
    Map<String, Object> img = Map.of("ID", 1, "NAME", name);
    return new RowChange(
        T,
        op,
        op == Operation.INSERT ? null : img,
        op == Operation.DELETE ? null : img,
        false,
        rowId,
        id(),
        k,
        Instant.EPOCH);
  }

  private MiningEvent.TxStart start(TxKey k, String user) {
    return new MiningEvent.TxStart(k, id(), 1, user, "client", 10, 1, Instant.EPOCH);
  }

  private MiningEvent.Commit commit(TxKey k) {
    return new MiningEvent.Commit(k, id(), 1, Instant.ofEpochSecond(42));
  }

  @Test
  void insertAndCommitEmitsOneTransactionInRedoOrderWithStartInfo() {
    HeapTransactionBuffer b = new HeapTransactionBuffer();
    TxKey k = key(3, 1);
    b.start(start(k, "APP"));
    assertThat(b.openTransactions()).isZero();
    assertThat(b.pendingStarts()).isEqualTo(1);
    b.add(k, change(k, Operation.INSERT, "AAA1", "a"));
    b.add(k, change(k, Operation.UPDATE, "AAA1", "b"));
    assertThat(b.openTransactions()).isEqualTo(1);
    assertThat(b.oldestFirstCaptured()).isPresent();
    Optional<CommittedTransaction> tx = b.commit(commit(k));
    assertThat(tx).isPresent();
    CommittedTransaction c = tx.get();
    assertThat(c.events())
        .extracting(RowChange::op)
        .containsExactly(Operation.INSERT, Operation.UPDATE);
    assertThat(c.username()).isEqualTo("APP");
    assertThat(c.clientId()).isEqualTo("client");
    assertThat(c.startId()).isNotNull();
    assertThat(c.firstCaptured().scn()).isLessThan(c.commitScn());
    assertThat(c.commitTimestamp()).isEqualTo(Instant.ofEpochSecond(42));
    assertThat(c.size()).isEqualTo(2);
    assertThat(b.openTransactions()).isZero();
    assertThat(b.pendingStarts()).isZero();
    assertThat(b.oldestFirstCaptured()).isEmpty();
    BufferMetricsSnapshot m = b.metrics();
    assertThat(m.committedTransactions()).isEqualTo(1);
    assertThat(m.bufferedEvents()).isZero();
    assertThat(m.estimatedBytes()).isZero();
    assertThat(m.oldestOpenScn()).isEqualTo(-1);
  }

  @Test
  void savepointRollbackUndoesTheLatestChangeWithTheSameRowid() {
    HeapTransactionBuffer b = new HeapTransactionBuffer();
    TxKey k = key(3, 2);
    b.add(k, change(k, Operation.INSERT, "R10", "ten"));
    // savepoint
    b.add(k, change(k, Operation.INSERT, "R11", "eleven"));
    b.add(k, change(k, Operation.UPDATE, "R10", "ten!"));
    b.add(k, change(k, Operation.DELETE, "R1", "one"));
    // rollback to savepoint: undo rows arrive in reverse order
    b.undo(k, id(), "R1"); // insert (undo) of the delete
    b.undo(k, id(), "R10"); // update (undo) of the update, not of the earlier insert
    b.undo(k, id(), "R11"); // delete (undo) of the insert
    b.add(k, change(k, Operation.INSERT, "R12", "twelve"));
    CommittedTransaction c = b.commit(commit(k)).orElseThrow();
    assertThat(c.events()).extracting(RowChange::rowId).containsExactly("R10", "R12");
    assertThat(c.events().get(0).op()).isEqualTo(Operation.INSERT);
    assertThat(c.events().get(0).after()).containsEntry("NAME", "ten");
    BufferMetricsSnapshot m = b.metrics();
    assertThat(m.undoneEvents()).isEqualTo(3);
    assertThat(m.unmatchedUndo()).isZero();
  }

  @Test
  void fullRollbackDiscardsTheEntryAndItsStart() {
    HeapTransactionBuffer b = new HeapTransactionBuffer();
    TxKey k = key(3, 3);
    b.start(start(k, "APP"));
    b.add(k, change(k, Operation.INSERT, "R1", "x"));
    b.undo(k, id(), "R1");
    b.rollback(new MiningEvent.Rollback(k, id(), 1, Instant.EPOCH));
    assertThat(b.openTransactions()).isZero();
    assertThat(b.pendingStarts()).isZero();
    assertThat(b.commit(commit(k))).isEmpty();
    assertThat(b.metrics().rolledBackTransactions()).isEqualTo(1);
    assertThat(b.metrics().committedTransactions()).isZero();
  }

  @Test
  void interleavedTransactionsAndTheSameXidInTwoPdbsStaySeparate() {
    HeapTransactionBuffer b = new HeapTransactionBuffer();
    TxKey a = key(3, 5);
    TxKey bb = key(4, 5); // same XID, other PDB (ADR-0002)
    TxKey c = key(3, 6);
    b.add(a, change(a, Operation.INSERT, "A1", "a1"));
    b.add(bb, change(bb, Operation.INSERT, "B1", "b1"));
    b.add(c, change(c, Operation.INSERT, "C1", "c1"));
    b.add(a, change(a, Operation.INSERT, "A2", "a2"));
    assertThat(b.openTransactions()).isEqualTo(3);
    assertThat(b.oldestFirstCaptured().orElseThrow().scn()).isEqualTo(100);
    CommittedTransaction tb = b.commit(commit(bb)).orElseThrow();
    assertThat(tb.events()).extracting(RowChange::rowId).containsExactly("B1");
    assertThat(b.oldestFirstCaptured().orElseThrow().scn()).isEqualTo(100);
    CommittedTransaction ta = b.commit(commit(a)).orElseThrow();
    assertThat(ta.events()).extracting(RowChange::rowId).containsExactly("A1", "A2");
    assertThat(b.oldestFirstCaptured().orElseThrow().scn()).isEqualTo(102);
    assertThat(b.commit(commit(c)).orElseThrow().key()).isEqualTo(c);
    assertThat(b.metrics().committedTransactions()).isEqualTo(3);
  }

  /**
   * Regression for <a href="https://github.com/debezium/dbz/issues/2683">dbz#2683</a>: an all-zero
   * XID START row opens no transaction (the engine-level case is {@code
   * IgnoresAllZeroXidStartRowsTest} in the regression corpus).
   */
  @Test
  @org.junit.jupiter.api.Tag("dbz-2683")
  void startRowsNeverCreateEntriesAndZeroXidsAreIgnored() {
    HeapTransactionBuffer b = new HeapTransactionBuffer();
    TxKey k = key(3, 9);
    b.start(start(k, "SYS"));
    assertThat(b.commit(commit(k))).isEmpty();
    assertThat(b.pendingStarts()).isZero();
    TxKey zero = new TxKey(3, Xid.ZERO);
    b.start(start(zero, "SYS"));
    assertThat(b.pendingStarts()).isZero();
    assertThat(b.metrics().ignoredZeroXidStarts()).isEqualTo(1);
    assertThatThrownBy(() -> b.add(zero, change(zero, Operation.INSERT, "Z", "z")))
        .isInstanceOf(IllegalArgumentException.class);
    assertThat(b.openTransactions()).isZero();
  }

  @Test
  void undoWithoutAMatchIsCountedNotFatal() {
    HeapTransactionBuffer b = new HeapTransactionBuffer();
    TxKey k = key(3, 11);
    b.undo(k, id(), "R9");
    assertThat(b.openTransactions()).isZero();
    b.add(k, change(k, Operation.INSERT, "R1", "x"));
    b.undo(k, id(), "R9");
    b.undo(k, id(), null);
    assertThat(b.metrics().unmatchedUndo()).isEqualTo(3);
    assertThat(b.commit(commit(k)).orElseThrow().size()).isEqualTo(1);
  }

  @Test
  void estimatesBytesAndFillsThreadFromCommitWhenNoStartWasSeen() {
    HeapTransactionBuffer b = new HeapTransactionBuffer();
    TxKey k = key(3, 12);
    RowChange big =
        new RowChange(
            T,
            Operation.INSERT,
            null,
            Map.of("NOTE", "x".repeat(10_000)),
            false,
            "R1",
            id(),
            k,
            Instant.EPOCH);
    b.add(k, big);
    assertThat(b.metrics().estimatedBytes()).isGreaterThan(20_000);
    CommittedTransaction c = b.commit(commit(k)).orElseThrow();
    assertThat(c.thread()).isEqualTo(1);
    assertThat(c.username()).isNull();
    assertThat(c.startId()).isNull();
    assertThat(b.metrics().estimatedBytes()).isZero();
  }
}
