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
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import sh.oso.connect.oracle.core.errors.JournalCorruptionException;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;

/** CORE-TX-4, CORE-TX-5, ADR-0003: journal chunks, resume bound, tombstones and restore. */
class JournalingBufferTest {

  static final TableId T = new TableId("FREEPDB1", "APP", "T");

  /** Records chunks and tombstones; a fake journal topic for restore tests. */
  static final class RecordingSink implements JournalSink {
    final List<JournalChunk> chunks = new ArrayList<>();
    final List<String> tombstones = new ArrayList<>();

    @Override
    public void chunk(JournalChunk c) {
      chunks.add(c);
    }

    @Override
    public void tombstones(TxKey key, List<JournalChunk.Ref> refs) {
      StringBuilder sb = new StringBuilder(key.toString());
      for (JournalChunk.Ref r : refs) {
        sb.append(" ").append(r.chunk()).append("@").append(r.generation());
      }
      tombstones.add(sb.toString());
    }

    List<JournalChunk> of(TxKey key) {
      return chunks.stream().filter(c -> c.key().equals(key)).toList();
    }
  }

  static final class Clock implements java.util.function.Supplier<Instant> {
    Instant now = Instant.parse("2026-10-05T07:00:00Z");

    @Override
    public Instant get() {
      return now;
    }

    void advance(Duration d) {
      now = now.plus(d);
    }
  }

  static TxKey tx(int n) {
    return new TxKey(3, new Xid(n, 1, 1000 + n));
  }

  static RowChange change(TxKey tx, long scn, String rowId) {
    return new RowChange(
        T,
        Operation.INSERT,
        null,
        Map.of("ID", BigDecimal.valueOf(scn), "V", "v" + scn),
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

  static HeapTransactionBuffer buffer(
      JournalPolicy policy, RecordingSink sink, Clock clock, long gen) {
    return new HeapTransactionBuffer(Long.MAX_VALUE, null, policy, sink, gen, clock);
  }

  @Test
  void crossingTheEventThresholdWritesTheHistoryThenEveryStepAppendsAChunk() {
    RecordingSink sink = new RecordingSink();
    Clock clock = new Clock();
    HeapTransactionBuffer b = buffer(new JournalPolicy(null, 5, 1 << 20), sink, clock, 7);
    TxKey a = tx(1);
    b.start(start(a, 100));
    for (int i = 1; i <= 4; i++) {
      b.add(a, change(a, 100 + i, "r" + i));
    }
    b.flushJournal(clock.get());
    assertThat(sink.chunks).as("below threshold").isEmpty();
    assertThat(b.oldestFirstCaptured().get().scn()).isEqualTo(101);
    b.undo(a, new RedoRecordId(105, "rs", 0), "r4");
    b.add(a, change(a, 106, "r5"));
    b.add(a, change(a, 107, "r6"));
    b.flushJournal(clock.get());
    assertThat(sink.chunks).hasSize(1);
    JournalChunk c0 = sink.chunks.get(0);
    assertThat(c0.key()).isEqualTo(a);
    assertThat(c0.chunk()).isZero();
    assertThat(c0.generation()).isEqualTo(7);
    assertThat(c0.events()).as("surviving heap changes only; the undo was applied").isEqualTo(5);
    assertThat(c0.undos()).isZero();
    assertThat(c0.first().scn()).isEqualTo(101);
    assertThat(c0.last().scn()).isEqualTo(107);
    assertThat(c0.firstCaptured().scn()).isEqualTo(101);
    assertThat(c0.startId().scn()).isEqualTo(100);
    assertThat(c0.username()).isEqualTo("APP");
    assertThat(b.oldestFirstCaptured().get().scn())
        .as("resume bound moves to the last journaled record")
        .isEqualTo(107);
    assertThat(b.metrics().journaledTransactions()).isEqualTo(1);
    // the next step: two changes and an undo become chunk 1 with an undo frame
    b.add(a, change(a, 110, "r7"));
    b.undo(a, new RedoRecordId(111, "rs", 0), "r7");
    b.add(a, change(a, 112, "r8"));
    b.flushJournal(clock.get());
    assertThat(sink.chunks).hasSize(2);
    JournalChunk c1 = sink.chunks.get(1);
    assertThat(c1.chunk()).isEqualTo(1);
    assertThat(c1.events()).isEqualTo(2);
    assertThat(c1.undos()).isEqualTo(1);
    assertThat(c1.first().scn()).isEqualTo(110);
    assertThat(c1.last().scn()).isEqualTo(112);
    assertThat(b.oldestFirstCaptured().get().scn()).isEqualTo(112);
    b.flushJournal(clock.get());
    assertThat(sink.chunks).as("nothing pending, nothing written").hasSize(2);
    // commit: events in order, tombstones only once released
    Optional<CommittedTransaction> c = b.commit(commit(a, 200));
    assertThat(c.get().events())
        .extracting(r -> r.id().scn())
        .containsExactly(101L, 102L, 103L, 106L, 107L, 112L);
    assertThat(sink.tombstones).isEmpty();
    b.release(a);
    assertThat(sink.tombstones).containsExactly(a + " 0@7 1@7");
    assertThat(b.chunksWritten()).isEqualTo(2);
  }

  @Test
  void ageThresholdJournalsEvenWithoutNewRedoAndRollbackTombstones() {
    RecordingSink sink = new RecordingSink();
    Clock clock = new Clock();
    HeapTransactionBuffer b =
        buffer(new JournalPolicy(Duration.ofMinutes(5), Long.MAX_VALUE, 1 << 20), sink, clock, 1);
    TxKey a = tx(1);
    b.add(a, change(a, 100, "r1"));
    clock.advance(Duration.ofMinutes(4));
    b.flushJournal(clock.get());
    assertThat(sink.chunks).isEmpty();
    clock.advance(Duration.ofMinutes(1));
    b.flushJournal(clock.get()); // the idle path calls this too
    assertThat(sink.chunks).hasSize(1);
    b.rollback(new MiningEvent.Rollback(a, new RedoRecordId(300, "rs", 0), 1, Instant.EPOCH));
    assertThat(sink.tombstones).containsExactly(a + " 0@1");
    assertThat(b.metrics().journaledTransactions()).isZero();
  }

  @Test
  void chunksSplitAtTheConfiguredSize() {
    RecordingSink sink = new RecordingSink();
    Clock clock = new Clock();
    HeapTransactionBuffer b = buffer(new JournalPolicy(null, 1, 600), sink, clock, 1);
    TxKey a = tx(1);
    for (int i = 0; i < 20; i++) {
      b.add(a, change(a, 100 + i, "r" + i));
    }
    b.flushJournal(clock.get());
    assertThat(sink.chunks.size()).isGreaterThan(3);
    int total = 0;
    for (int i = 0; i < sink.chunks.size(); i++) {
      assertThat(sink.chunks.get(i).chunk()).isEqualTo(i);
      total += sink.chunks.get(i).events();
    }
    assertThat(total).isEqualTo(20);
    assertThat(sink.chunks.get(sink.chunks.size() - 1).last().scn()).isEqualTo(119);
  }

  @Test
  void aSpilledTransactionIsJournaledFromItsFile(@TempDir Path dir) throws Exception {
    try (SpillStore store = new SpillStore(dir, 1 << 30)) {
      RecordingSink sink = new RecordingSink();
      Clock clock = new Clock();
      HeapTransactionBuffer b =
          new HeapTransactionBuffer(
              1024, store, new JournalPolicy(null, 50, 1 << 20), sink, 1, clock);
      TxKey a = tx(1);
      for (int i = 0; i < 60; i++) {
        b.add(a, change(a, 100 + i, "r" + i));
        if (i == 30) {
          b.undo(a, new RedoRecordId(1000, "rs", 0), "r29");
        }
      }
      assertThat(b.metrics().spilledTransactions()).isEqualTo(1);
      b.flushJournal(clock.get());
      assertThat(sink.chunks).hasSize(1);
      JournalChunk c0 = sink.chunks.get(0);
      assertThat(c0.events() + c0.undos()).as("every spilled frame, undo included").isEqualTo(61);
      assertThat(c0.undos()).isEqualTo(1);
      List<JournalFrames.Frame> frames = JournalFrames.decode(c0.payload(), "test");
      assertThat(frames.get(31).isUndo()).isTrue();
      assertThat(frames.get(31).undoRowId()).isEqualTo("r29");
      assertThat(frames.get(31).undoId().scn()).isEqualTo(1000);
      Optional<CommittedTransaction> c = b.commit(commit(a, 500));
      assertThat(c.get().size()).isEqualTo(59);
      b.release(a);
      assertThat(sink.tombstones).hasSize(1);
      assertThat(store.openFiles()).isZero();
    }
  }

  @Test
  void restoreRebuildsTheEntryAndLaterChunksContinueTheSequence() {
    RecordingSink sink = new RecordingSink();
    Clock clock = new Clock();
    HeapTransactionBuffer first = buffer(new JournalPolicy(null, 3, 1 << 20), sink, clock, 1);
    TxKey a = tx(1);
    first.start(start(a, 100));
    first.add(a, change(a, 101, "r1"));
    first.add(a, change(a, 102, "r2"));
    first.add(a, change(a, 103, "r3"));
    first.flushJournal(clock.get());
    first.add(a, change(a, 104, "r4"));
    first.undo(a, new RedoRecordId(105, "rs", 0), "r2");
    first.flushJournal(clock.get());
    assertThat(sink.chunks).hasSize(2);
    // a new task generation reloads the two chunks, then mines on
    RecordingSink sink2 = new RecordingSink();
    HeapTransactionBuffer second = buffer(new JournalPolicy(null, 3, 1 << 20), sink2, clock, 2);
    second.restore(sink.of(a));
    assertThat(second.openTransactions()).isEqualTo(1);
    assertThat(second.oldestFirstCaptured().get().scn())
        .as("resume bound is the last journaled record")
        .isEqualTo(105);
    assertThat(second.metrics().journaledTransactions()).isEqualTo(1);
    assertThat(second.largest(1).get(0).journaled()).isTrue();
    assertThat(second.largest(1).get(0).firstScn()).isEqualTo(101);
    second.add(a, change(a, 106, "r5"));
    second.flushJournal(clock.get());
    assertThat(sink2.chunks).hasSize(1);
    assertThat(sink2.chunks.get(0).chunk()).as("continues after the restored chunks").isEqualTo(2);
    assertThat(sink2.chunks.get(0).generation()).isEqualTo(2);
    CommittedTransaction c = second.commit(commit(a, 200)).get();
    assertThat(c.events()).extracting(r -> r.id().scn()).containsExactly(101L, 103L, 104L, 106L);
    assertThat(c.startId().scn()).isEqualTo(100);
    assertThat(c.username()).isEqualTo("APP");
    assertThat(c.firstCaptured().scn()).isEqualTo(101);
    second.release(a);
    assertThat(sink2.tombstones)
        .as("restored chunks are tombstoned under the generation that wrote them")
        .containsExactly(a + " 0@1 1@1 2@2");
  }

  @Test
  void aMissingChunkIsAJournalCorruptionStop() {
    RecordingSink sink = new RecordingSink();
    Clock clock = new Clock();
    HeapTransactionBuffer first = buffer(new JournalPolicy(null, 1, 100), sink, clock, 1);
    TxKey a = tx(1);
    for (int i = 0; i < 6; i++) {
      first.add(a, change(a, 100 + i, "r" + i));
    }
    first.flushJournal(clock.get());
    assertThat(sink.chunks.size()).isGreaterThan(2);
    List<JournalChunk> withGap = new ArrayList<>(sink.chunks);
    withGap.remove(1);
    HeapTransactionBuffer second = buffer(JournalPolicy.NEVER, new RecordingSink(), clock, 2);
    assertThatThrownBy(() -> second.restore(withGap))
        .isInstanceOf(JournalCorruptionException.class)
        .hasMessageContaining("missing chunk 1");
    assertThat(second.openTransactions()).isZero();
  }

  /** Random operations with and without journaling commit identical transactions. */
  @Test
  void journalingNeverChangesWhatIsCommitted() {
    Random rnd = new Random(5);
    RecordingSink sink = new RecordingSink();
    Clock clock = new Clock();
    HeapTransactionBuffer plain = new HeapTransactionBuffer();
    HeapTransactionBuffer journaling = buffer(new JournalPolicy(null, 4, 400), sink, clock, 1);
    List<TxKey> live = new ArrayList<>();
    int next = 1;
    long scn = 1000;
    int commits = 0;
    for (int step = 0; step < 5000; step++) {
      int roll = rnd.nextInt(100);
      if (live.size() < 5 && roll < 15) {
        TxKey k = tx(next++);
        live.add(k);
        plain.start(start(k, scn));
        journaling.start(start(k, scn++));
      } else if (!live.isEmpty() && roll < 80) {
        TxKey k = live.get(rnd.nextInt(live.size()));
        RowChange c = change(k, scn++, "r" + rnd.nextInt(20));
        plain.add(k, c);
        journaling.add(k, c);
      } else if (!live.isEmpty() && roll < 90) {
        TxKey k = live.get(rnd.nextInt(live.size()));
        RedoRecordId id = new RedoRecordId(scn++, "rs", 0);
        String rowId = "r" + rnd.nextInt(20);
        plain.undo(k, id, rowId);
        journaling.undo(k, id, rowId);
      } else if (!live.isEmpty()) {
        TxKey k = live.remove(rnd.nextInt(live.size()));
        MiningEvent.Commit c = commit(k, scn++);
        Optional<CommittedTransaction> p = plain.commit(c);
        Optional<CommittedTransaction> j = journaling.commit(c);
        assertThat(j.isPresent()).isEqualTo(p.isPresent());
        if (p.isPresent()) {
          commits++;
          assertThat(j.get().events()).containsExactlyElementsOf(p.get().events());
          journaling.release(k);
        }
      }
      if (step % 7 == 0) {
        journaling.flushJournal(clock.get());
      }
      assertThat(journaling.openTransactions()).isEqualTo(plain.openTransactions());
    }
    assertThat(commits).isGreaterThan(50);
    assertThat(sink.chunks.size()).isGreaterThan(20);
    assertThat(sink.tombstones).isNotEmpty();
  }
}
