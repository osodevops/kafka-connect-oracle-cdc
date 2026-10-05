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
import static sh.oso.connect.oracle.core.buffer.JournalingBufferTest.T;
import static sh.oso.connect.oracle.core.buffer.JournalingBufferTest.commit;
import static sh.oso.connect.oracle.core.buffer.JournalingBufferTest.start;
import static sh.oso.connect.oracle.core.buffer.JournalingBufferTest.tx;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.RowIds;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;

/**
 * Undo of changes that carry a synthetic ROWID (ADR-0015), with the savepoint sequences of
 * reference/lob-redo-shapes.md: Oracle undoes newest first, and its undo rows name the real ROWID
 * while the changes they undo only had the placeholder. The savepoint bugs with LOB columns fixed
 * in Debezium 3.7 (dbz#1422, dbz#1735, dbz#1917) are these invariants; the engine-level suite is
 * {@code RollsBackToSavepointExactlyEngineIT} in the regression corpus.
 */
@org.junit.jupiter.api.Tag("dbz-1422")
@org.junit.jupiter.api.Tag("dbz-1735")
@org.junit.jupiter.api.Tag("dbz-1917")
class LobUndoBufferTest {

  static final TableId OTHER = new TableId("FREEPDB1", "APP", "OTHER");
  static final String R6 = "AAAR5FAAYAAAAANAAB";
  static final String R7 = "AAAR5FAAYAAAAANAAC";

  @TempDir Path dir;
  long scn = 1000;

  @Test
  void anUndoTargetsTheNewestLobGroupNotAnOlderChangeWithTheRealRowid() {
    for (Kind kind : Kind.values()) {
      List<RowChange> out =
          run(
              kind,
              (b) -> {
                b.add(K, upd(R6, "kept6"));
                b.add(K, upd(RowIds.lobGroup(RowIds.PLACEHOLDER, "g1"), "kept6"));
                assertThat(b.undo(K, id(), R6, T, Operation.UPDATE))
                    .isEqualTo(RowIds.lobGroup(RowIds.PLACEHOLDER, "g1"));
              });
      assertThat(out).as(kind.name()).hasSize(2);
      assertThat(out.get(0).rowId()).isEqualTo(R6);
      // downgraded, not removed: LogMiner shows no call boundaries for LOB writes
      assertThat(RowIds.isInert(out.get(1).rowId())).as(kind.name()).isTrue();
      assertThat(out.get(1).after()).isEqualTo(out.get(1).before());
    }
  }

  @Test
  void aRowPieceWithItsLobWritesIsRemovedWhole() {
    for (Kind kind : Kind.values()) {
      List<RowChange> out =
          run(
              kind,
              (b) -> {
                b.add(K, upd(R7, "y7"));
                b.add(K, upd(RowIds.rowPiece(RowIds.PLACEHOLDER, "p1"), "x6"));
                b.undo(K, id(), R6, T, Operation.UPDATE);
              });
      assertThat(out).as(kind.name()).extracting(RowChange::rowId).containsExactly(R7);
    }
  }

  @Test
  void anInsertWhoseLobsWereAllOutOfRowIsUndoneByTheDeleteOfItsRealRowid() {
    for (Kind kind : Kind.values()) {
      List<RowChange> out =
          run(
              kind,
              (b) -> {
                b.add(K, ins(RowIds.rowPiece(RowIds.PLACEHOLDER, "i1")));
                b.undo(K, id(), "AAAR5FAAYAAAAAOAAD", T, Operation.DELETE);
                b.add(K, ins("AAAR5FAAYAAAAAOAAD"));
              });
      assertThat(out)
          .as(kind.name())
          .extracting(RowChange::rowId)
          .containsExactly("AAAR5FAAYAAAAAOAAD");
    }
  }

  @Test
  void undosOfSeveralRowsPopNewestFirst() {
    for (Kind kind : Kind.values()) {
      List<RowChange> out =
          run(
              kind,
              (b) -> {
                b.add(K, upd(RowIds.lobGroup(RowIds.PLACEHOLDER, "a"), "y6"));
                b.add(K, upd(RowIds.lobGroup(RowIds.PLACEHOLDER, "b"), "y7"));
                b.undo(K, id(), R7, T, Operation.UPDATE);
                b.undo(K, id(), R6, T, Operation.UPDATE);
                b.add(K, upd(R6, "z6"));
              });
      assertThat(out).as(kind.name()).hasSize(3);
      assertThat(out.subList(0, 2)).allMatch(c -> RowIds.isInert(c.rowId()));
      assertThat(out.get(0).before()).containsEntry("NAME", "y6");
      assertThat(out.get(2).rowId()).isEqualTo(R6);
    }
  }

  @Test
  void anUndoThatDoesNotFitTheNewestChangeFallsBackToTheRowid() {
    for (Kind kind : Kind.values()) {
      List<RowChange> out =
          run(
              kind,
              (b) -> {
                b.add(K, upd(R6, "a"));
                b.add(K, upd(RowIds.lobGroup(R7, "g"), "b")); // real ROWID R7 inside
                // another table, another operation, another real ROWID: none of them fit
                assertThat(b.undo(K, id(), R6, OTHER, Operation.UPDATE)).isEqualTo(R6);
              });
      assertThat(out)
          .as(kind.name())
          .extracting(RowChange::rowId)
          .containsExactly(RowIds.lobGroup(R7, "g"));
      List<RowChange> out2 =
          run(
              kind,
              (b) -> {
                b.add(K, upd(R6, "a"));
                b.add(K, upd(RowIds.lobGroup(R7, "g"), "b"));
                b.undo(K, id(), R6, T, Operation.UPDATE); // R6 is not the group's real ROWID
                b.undo(K, id(), R7, T, Operation.INSERT); // INSERT does not reverse an UPDATE
              });
      assertThat(out2)
          .as(kind.name())
          .extracting(RowChange::rowId)
          .containsExactly(RowIds.lobGroup(R7, "g"));
    }
  }

  @Test
  void aRestoredEntryStillResolvesAnUndoToItsNewestLobGroup() {
    JournalingBufferTest.RecordingSink sink = new JournalingBufferTest.RecordingSink();
    JournalingBufferTest.Clock clock = new JournalingBufferTest.Clock();
    HeapTransactionBuffer first =
        JournalingBufferTest.buffer(new JournalPolicy(null, 1, 1 << 16), sink, clock, 1);
    first.start(start(K, scn++));
    String group = RowIds.lobGroup(RowIds.PLACEHOLDER, "g1");
    first.add(K, upd(R6, "kept6"));
    first.add(K, upd(group, "kept6"));
    first.flushJournal(clock.get());
    assertThat(sink.of(K)).isNotEmpty();
    HeapTransactionBuffer second = JournalingBufferTest.buffer(JournalPolicy.NEVER, sink, clock, 2);
    second.restore(sink.of(K));
    assertThat(second.undo(K, id(), R6, T, Operation.UPDATE)).isEqualTo(group);
    List<RowChange> out = second.commit(commit(K, scn++)).orElseThrow().events();
    assertThat(out).extracting(RowChange::rowId).containsExactly(R6, RowIds.inert(group));
  }

  enum Kind {
    HEAP,
    SPILLED,
    JOURNAL_RESTORED
  }

  static final TxKey K = tx(1);

  /** Runs {@code body} on a buffer of the given kind and returns what the commit releases. */
  private List<RowChange> run(Kind kind, Consumer<TransactionBuffer> body) {
    try {
      switch (kind) {
        case HEAP:
          {
            HeapTransactionBuffer b = new HeapTransactionBuffer();
            b.start(start(K, scn++));
            body.accept(b);
            return new ArrayList<>(b.commit(commit(K, scn++)).orElseThrow().events());
          }
        case SPILLED:
          {
            try (SpillStore store = new SpillStore(dir.resolve("s" + scn), 64L << 20)) {
              HeapTransactionBuffer b = new HeapTransactionBuffer(1, store);
              b.start(start(K, scn++));
              body.accept(b);
              CommittedTransaction tx = b.commit(commit(K, scn++)).orElseThrow();
              List<RowChange> out = new ArrayList<>(tx.events());
              b.release(K);
              return out;
            }
          }
        default:
          {
            // journal everything, then rebuild the entry in a fresh buffer before the commit
            JournalingBufferTest.RecordingSink sink = new JournalingBufferTest.RecordingSink();
            JournalingBufferTest.Clock clock = new JournalingBufferTest.Clock();
            HeapTransactionBuffer first =
                JournalingBufferTest.buffer(new JournalPolicy(null, 1, 1 << 16), sink, clock, 1);
            first.start(start(K, scn++));
            body.accept(first);
            first.flushJournal(clock.get());
            HeapTransactionBuffer second =
                JournalingBufferTest.buffer(JournalPolicy.NEVER, sink, clock, 2);
            second.restore(sink.of(K));
            return new ArrayList<>(second.commit(commit(K, scn++)).orElseThrow().events());
          }
      }
    } catch (java.io.IOException e) {
      throw new java.io.UncheckedIOException(e);
    }
  }

  private RedoRecordId id() {
    return new RedoRecordId(scn++, "rs", 0);
  }

  private RowChange upd(String rowId, String name) {
    Map<String, Object> image = Map.of("ID", BigDecimal.valueOf(6), "NAME", name);
    return new RowChange(T, Operation.UPDATE, image, image, false, rowId, id(), K, null);
  }

  private RowChange ins(String rowId) {
    return new RowChange(
        T,
        Operation.INSERT,
        null,
        Map.of("ID", BigDecimal.valueOf(scn), "C", "x"),
        false,
        rowId,
        id(),
        K,
        null);
  }
}
