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

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import sh.oso.connect.oracle.core.errors.BufferExhaustedException;
import sh.oso.connect.oracle.core.errors.OracleCdcCorruptionException;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;

/** CORE-TX-3: heap budget, largest-first spill, spill cap, and identical results either way. */
class SpillingBufferTest {

  static final TableId T = new TableId("FREEPDB1", "APP", "T");
  static final long MIB = 1 << 20;

  static TxKey tx(int n) {
    return new TxKey(3, new Xid(n, n % 7, 1000 + n));
  }

  static RowChange change(TxKey tx, long scn, String rowId, int payload) {
    return new RowChange(
        T,
        Operation.INSERT,
        null,
        Map.of("ID", BigDecimal.valueOf(scn), "V", "x".repeat(payload)),
        false,
        rowId,
        new RedoRecordId(scn, "rs", 0),
        tx,
        null);
  }

  static MiningEvent.TxStart start(TxKey tx, long scn) {
    return new MiningEvent.TxStart(
        tx, new RedoRecordId(scn, "rs", 0), 1, "APP", "client", 1, 1, Instant.EPOCH);
  }

  static MiningEvent.Commit commit(TxKey tx, long scn) {
    return new MiningEvent.Commit(tx, new RedoRecordId(scn, "rs", 0), 1, Instant.EPOCH);
  }

  static MiningEvent.Rollback rollback(TxKey tx, long scn) {
    return new MiningEvent.Rollback(tx, new RedoRecordId(scn, "rs", 0), 1, Instant.EPOCH);
  }

  @Test
  void oneMillionEventsStayWithinASixteenMebibyteBudget(@TempDir Path dir) throws Exception {
    try (SpillStore store = new SpillStore(dir, 4L * 1024 * MIB)) {
      HeapTransactionBuffer b = new HeapTransactionBuffer(16 * MIB, store);
      TxKey big = tx(1);
      TxKey small = tx(2);
      b.start(start(big, 10));
      b.start(start(small, 11));
      long peakHeap = 0;
      for (int i = 0; i < 1_000_000; i++) {
        b.add(big, change(big, 100 + i, "big" + i, 16));
        if (i % 1000 == 0) {
          b.add(small, change(small, 100 + i, "small" + i, 16));
        }
        if (i % 10_000 == 0) {
          peakHeap = Math.max(peakHeap, b.metrics().estimatedBytes());
          assertThat(b.metrics().estimatedBytes()).isLessThanOrEqualTo(16 * MIB);
        }
      }
      BufferMetricsSnapshot m = b.metrics();
      assertThat(m.spilledTransactions()).isEqualTo(1);
      assertThat(m.spilledBytes()).isEqualTo(store.totalBytes()).isGreaterThan(16 * MIB);
      assertThat(m.bufferedEvents()).isEqualTo(1_001_000);
      assertThat(b.spills()).isEqualTo(1);
      assertThat(b.largest(1).get(0).key()).isEqualTo(big);
      assertThat(b.oldestFirstCaptured().get().scn()).isEqualTo(100);
      // commit: the events are read back lazily in redo order; heap stays small
      Optional<CommittedTransaction> c = b.commit(commit(big, 5_000_000));
      assertThat(c).isPresent();
      assertThat(c.get().size()).isEqualTo(1_000_000);
      assertThat(c.get().events()).isInstanceOf(SpillStore.SpilledChanges.class);
      long expected = 100;
      for (int i = 0; i < c.get().size(); i++) {
        RowChange r = c.get().events().get(i);
        assertThat(r.id().scn()).isEqualTo(expected++);
      }
      assertThat(c.get().username()).isEqualTo("APP");
      assertThat(Files.list(dir).count()).as("file kept until released").isEqualTo(1);
      b.release(big);
      assertThat(Files.list(dir).count()).isZero();
      assertThat(store.totalBytes()).isZero();
      assertThat(b.metrics().spilledTransactions()).isZero();
      assertThat(b.commit(commit(small, 5_000_001)).get().size()).isEqualTo(1000);
      System.out.println(
          "spill: peak heap estimate "
              + peakHeap
              + " bytes, "
              + m.spilledBytes()
              + " bytes spilled");
    }
  }

  @Test
  void spillsTheLargestTransactionFirstAndKeepsAppendingToIt(@TempDir Path dir) throws Exception {
    try (SpillStore store = new SpillStore(dir, 64 * MIB)) {
      HeapTransactionBuffer b = new HeapTransactionBuffer(64 * 1024, store);
      TxKey a = tx(1);
      TxKey c = tx(2);
      for (int i = 0; i < 100; i++) {
        b.add(a, change(a, 100 + i, "a" + i, 200)); // the big one
        if (i % 10 == 0) {
          b.add(c, change(c, 200 + i, "c" + i, 10));
        }
      }
      assertThat(b.metrics().spilledTransactions()).isEqualTo(1);
      assertThat(b.largest(2).get(0).key()).isEqualTo(a);
      assertThat(b.largest(2).get(0).spilledBytes()).isPositive();
      assertThat(b.largest(2).get(1).key()).isEqualTo(c);
      assertThat(b.largest(2).get(1).spilledBytes()).isZero();
      long disk = store.totalBytes();
      b.add(a, change(a, 999, "a999", 200));
      assertThat(store.totalBytes()).as("later changes go straight to disk").isGreaterThan(disk);
      assertThat(b.metrics().estimatedBytes()).isLessThanOrEqualTo(64 * 1024);
      // rollback of a spilled transaction removes its file
      b.rollback(rollback(a, 2000));
      assertThat(store.openFiles()).isZero();
      assertThat(b.metrics().spilledTransactions()).isZero();
      assertThat(b.commit(commit(c, 2001)).get().size()).isEqualTo(10);
    }
  }

  @Test
  void exceedingTheSpillCapStopsWithTheLargestTransactionsNamed(@TempDir Path dir)
      throws Exception {
    try (SpillStore store = new SpillStore(dir, 64 * MIB)) {
      HeapTransactionBuffer b = new HeapTransactionBuffer(MIB, store);
      // a tiny cap: 70 MiB default minimum is enforced by config, not by the store
      HeapTransactionBuffer tiny =
          new HeapTransactionBuffer(32 * 1024, new SpillStore(dir.resolve("tiny"), 200 * 1024));
      for (int t = 1; t <= 12; t++) {
        tiny.start(start(tx(t), t));
      }
      assertThatThrownBy(
              () -> {
                for (int i = 0; i < 100_000; i++) {
                  TxKey k = tx(1 + (i % 12));
                  tiny.add(k, change(k, 1000 + i, "r" + i, 64 + (i % 12) * 16));
                }
              })
          .isInstanceOf(BufferExhaustedException.class)
          .hasMessageContaining("cdc.buffer.spill.max.bytes")
          .hasMessageContaining("Largest open transactions")
          .hasMessageContaining("user APP")
          .hasMessageContaining("spilled bytes")
          .satisfies(
              e -> assertThat(((BufferExhaustedException) e).code().code()).isEqualTo("CDC-4001"));
      assertThat(b.metrics().spilledTransactions()).isZero();
    }
  }

  @Test
  void aFullDiskIsATypedStopNamingTheSpillDirectory(@TempDir Path dir) throws Exception {
    SpillStore failing =
        new SpillStore(dir, 64 * MIB) {
          @Override
          java.io.OutputStream openForAppend(Path p) {
            return new java.io.OutputStream() {
              @Override
              public void write(int x) throws java.io.IOException {
                throw new java.io.IOException("No space left on device");
              }
            };
          }
        };
    HeapTransactionBuffer b = new HeapTransactionBuffer(4 * 1024, failing);
    TxKey a = tx(1);
    assertThatThrownBy(
            () -> {
              for (int i = 0; i < 1000; i++) {
                b.add(a, change(a, 100 + i, "a" + i, 100));
              }
            })
        .isInstanceOf(BufferExhaustedException.class)
        .hasMessageContaining("No space left on device")
        .hasMessageContaining(dir.toString());
  }

  @Test
  void aDamagedSpillFileIsACorruptionStopAtCommit(@TempDir Path dir) throws Exception {
    try (SpillStore store = new SpillStore(dir, 64 * MIB)) {
      HeapTransactionBuffer b = new HeapTransactionBuffer(4 * 1024, store);
      TxKey a = tx(1);
      for (int i = 0; i < 200; i++) {
        b.add(a, change(a, 100 + i, "a" + i, 100));
      }
      b.undo(a, new RedoRecordId(400, "rs", 0), "a150"); // forces the resolve passes to read
      Path file = dir.resolve(SpillStore.fileName(a));
      assertThat(Files.exists(file)).isTrue();
      // the file is still open for append; damage an early byte on disk after a flush
      b.add(a, change(a, 401, "a401", 60_000)); // large enough to flush the buffered stream
      byte[] bytes = Files.readAllBytes(file);
      bytes[30] ^= 0x7f;
      Files.write(file, bytes);
      assertThatThrownBy(() -> b.commit(commit(a, 500)))
          .isInstanceOf(OracleCdcCorruptionException.class)
          .hasMessageContaining("damaged");
    }
  }

  /** Random operations against an unlimited heap buffer and a two-kilobyte budget agree exactly. */
  @Test
  void spillingAndHeapOnlyBuffersCommitIdenticalTransactions(@TempDir Path dir) throws Exception {
    Random rnd = new Random(20261005);
    try (SpillStore store = new SpillStore(dir, 64 * MIB)) {
      HeapTransactionBuffer heap = new HeapTransactionBuffer();
      HeapTransactionBuffer spilling = new HeapTransactionBuffer(2048, store);
      List<TxKey> live = new ArrayList<>();
      int next = 1;
      long scn = 1000;
      int commits = 0;
      for (int step = 0; step < 20_000; step++) {
        int roll = rnd.nextInt(100);
        if (live.size() < 6 && roll < 15) {
          TxKey k = tx(next++);
          live.add(k);
          MiningEvent.TxStart s = start(k, scn++);
          heap.start(s);
          spilling.start(s);
        } else if (!live.isEmpty() && roll < 80) {
          TxKey k = live.get(rnd.nextInt(live.size()));
          RowChange c = change(k, scn++, "r" + rnd.nextInt(40), rnd.nextInt(300));
          heap.add(k, c);
          spilling.add(k, c);
        } else if (!live.isEmpty() && roll < 90) {
          TxKey k = live.get(rnd.nextInt(live.size()));
          RedoRecordId id = new RedoRecordId(scn++, "rs", 0);
          String rowId = "r" + rnd.nextInt(40);
          heap.undo(k, id, rowId);
          spilling.undo(k, id, rowId);
        } else if (!live.isEmpty() && roll < 97) {
          TxKey k = live.remove(rnd.nextInt(live.size()));
          MiningEvent.Commit c = commit(k, scn++);
          Optional<CommittedTransaction> h = heap.commit(c);
          Optional<CommittedTransaction> s = spilling.commit(c);
          assertThat(s.isPresent()).isEqualTo(h.isPresent());
          if (h.isPresent()) {
            commits++;
            assertThat(s.get().size()).isEqualTo(h.get().size());
            for (int i = 0; i < h.get().size(); i++) {
              assertThat(s.get().events().get(i)).isEqualTo(h.get().events().get(i));
            }
            assertThat(s.get().firstCaptured()).isEqualTo(h.get().firstCaptured());
            assertThat(s.get().username()).isEqualTo(h.get().username());
            spilling.release(k);
            heap.release(k);
          }
        } else if (!live.isEmpty()) {
          TxKey k = live.remove(rnd.nextInt(live.size()));
          MiningEvent.Rollback r = rollback(k, scn++);
          heap.rollback(r);
          spilling.rollback(r);
        }
        assertThat(spilling.oldestFirstCaptured()).isEqualTo(heap.oldestFirstCaptured());
        assertThat(spilling.openTransactions()).isEqualTo(heap.openTransactions());
      }
      assertThat(commits).isGreaterThan(100);
      assertThat(spilling.spills()).isGreaterThan(10);
      BufferMetricsSnapshot hm = heap.metrics();
      BufferMetricsSnapshot sm = spilling.metrics();
      assertThat(sm.committedTransactions()).isEqualTo(hm.committedTransactions());
      assertThat(sm.rolledBackTransactions()).isEqualTo(hm.rolledBackTransactions());
      // undo counts for open spilled transactions resolve at commit, so compare after draining
      for (TxKey k : List.copyOf(live)) {
        MiningEvent.Commit c = commit(k, scn++);
        heap.commit(c);
        spilling.commit(c);
        spilling.release(k);
      }
      // an undo inside a rolled-back spilled transaction is counted without resolution, so the
      // split between matched and unmatched can differ; the total of undo rows seen cannot
      assertThat(spilling.metrics().undoneEvents() + spilling.metrics().unmatchedUndo())
          .isEqualTo(heap.metrics().undoneEvents() + heap.metrics().unmatchedUndo());
      assertThat(spilling.metrics().bufferedEvents()).isEqualTo(heap.metrics().bufferedEvents());
      assertThat(store.openFiles()).isZero();
    }
  }
}
