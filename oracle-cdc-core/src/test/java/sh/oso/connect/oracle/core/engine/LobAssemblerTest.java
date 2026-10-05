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
package sh.oso.connect.oracle.core.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.SCHEMA;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.erase;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.insertEmpty;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.trim;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.update;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.where;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.write;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.writeBytes;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.decode.RowDecoder;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.RowIds;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;
import sh.oso.connect.oracle.core.testkit.LobRedoShapes;

/** The row sequences of reference/lob-redo-shapes.md, folded the way Oracle undoes them. */
class LobAssemblerTest {

  static final TxKey K = new TxKey(3, new Xid(1, 2, 3));
  static final String R6 = "AAAR5FAAYAAAAANAAB";
  int seq;

  @Test
  void aLargeInsertFoldsItsWritesAndTakesTheLocatorUpdatesRowid() {
    LobAssembler a = new LobAssembler(LobAssembler.Mode.INLINE, 1 << 20);
    List<RowChange> out = new ArrayList<>();
    out.addAll(row(a, Operation.INSERT, insertEmpty(6, "row6"), RowIds.PLACEHOLDER));
    String big = "a".repeat(1022) + "z";
    out.addAll(lob(a, write("C", where(6, "row6"), 1, big.substring(0, 1022)), RowIds.PLACEHOLDER));
    out.addAll(lob(a, write("C", where(6, "row6"), 1023, "z"), RowIds.PLACEHOLDER));
    assertThat(out).isEmpty();
    assertThat(a.oldestPending()).isPresent();
    out.addAll(row(a, Operation.UPDATE, update("\"NC\" = NULL, \"B\" = NULL", 6, "row6"), R6));
    assertThat(out).hasSize(1);
    RowChange c = out.get(0);
    assertThat(c.op()).isEqualTo(Operation.INSERT);
    assertThat(c.rowId()).isEqualTo(R6);
    assertThat(c.after())
        .containsEntry("C", big)
        .containsEntry("NC", null)
        .containsEntry("B", null)
        .containsEntry("NAME", "row6");
    assertThat(a.merged()).isEqualTo(1);
    assertThat(a.pendingCount()).isZero();
  }

  @Test
  void anInsertWhoseLobsAreAllOutOfRowKeepsASyntheticRowid() {
    LobAssembler a = new LobAssembler(LobAssembler.Mode.INLINE, 1 << 20);
    row(a, Operation.INSERT, insertEmpty(11, "eleven"), RowIds.PLACEHOLDER);
    lob(a, write("C", where(11, "eleven"), 1, "cc"), RowIds.PLACEHOLDER);
    lob(a, write("NC", where(11, "eleven"), 1, "nn"), RowIds.PLACEHOLDER);
    lob(a, writeBytes(where(11, "eleven"), 1, new byte[] {1, 2}), RowIds.PLACEHOLDER);
    RowChange c = a.flush(K).orElseThrow();
    assertThat(c.op()).isEqualTo(Operation.INSERT);
    assertThat(RowIds.isSynthetic(c.rowId())).isTrue();
    assertThat(RowIds.isLobGroup(c.rowId())).isFalse();
    assertThat(RowIds.real(c.rowId())).isNull();
    assertThat(c.after()).containsEntry("C", "cc").containsEntry("NC", "nn");
    assertThat((byte[]) c.after().get("B")).containsExactly(1, 2);
  }

  @Test
  void aScalarUpdateAndItsLobWritesAreOneChange() {
    LobAssembler a = new LobAssembler(LobAssembler.Mode.INLINE, 1 << 20);
    row(a, Operation.UPDATE, update("\"NAME\" = 'both'", 6, "kept6"), RowIds.PLACEHOLDER);
    lob(a, write("C", where(6, "both"), 1, "new value"), RowIds.PLACEHOLDER);
    lob(a, trim("C", where(6, "both"), 9), RowIds.PLACEHOLDER);
    RowChange c = a.flush(K).orElseThrow();
    assertThat(c.op()).isEqualTo(Operation.UPDATE);
    assertThat(c.before()).containsEntry("NAME", "kept6");
    assertThat(c.after()).containsEntry("NAME", "both").containsEntry("C", "new value");
    assertThat(c.after()).doesNotContainKeys("NC", "B"); // unchanged LOBs are unavailable
    assertThat(RowIds.isSynthetic(c.rowId())).isTrue();
  }

  @Test
  void lobWritesWithoutARowPieceAreAnUpdateKnownOnlyOnceTrimmed() {
    LobAssembler a = new LobAssembler(LobAssembler.Mode.INLINE, 1 << 20);
    lob(a, write("C", where(6, "kept6"), 1, "abcd"), RowIds.PLACEHOLDER);
    lob(a, write("C", where(6, "kept6"), 5, "ef"), RowIds.PLACEHOLDER);
    lob(a, trim("C", where(6, "kept6"), 6), RowIds.PLACEHOLDER);
    // the next row of another statement closes the group
    List<RowChange> out =
        row(a, Operation.UPDATE, update("\"NAME\" = 'z'", 7, "row7"), "AAAR5FAAYAAAAANAAC");
    assertThat(out).hasSize(2);
    RowChange g = out.get(0);
    assertThat(g.op()).isEqualTo(Operation.UPDATE);
    assertThat(RowIds.isLobGroup(g.rowId())).isTrue();
    assertThat(g.before()).isEqualTo(LobRedoShapes.row(6, "kept6"));
    assertThat(g.after()).containsEntry("C", "abcdef");
    assertThat(g.partial()).isFalse();
    assertThat(out.get(1).rowId()).isEqualTo("AAAR5FAAYAAAAANAAC");

    // no trim: the tail beyond the last write is unknown
    lob(a, write("C", where(6, "kept6"), 1, "abcd"), RowIds.PLACEHOLDER);
    assertThat(a.flush(K).orElseThrow().after()).doesNotContainKey("C");
  }

  @Test
  void anAppendToAnUnknownValueIsUnavailableAndKeepsTheRealRowid() {
    LobAssembler a = new LobAssembler(LobAssembler.Mode.INLINE, 1 << 20);
    lob(a, write("C", where(7, "row7"), 9002, "hello"), "AAAR5FAAYAAAAANAAC");
    RowChange c = a.flush(K).orElseThrow();
    assertThat(c.after()).doesNotContainKey("C");
    assertThat(RowIds.real(c.rowId())).isEqualTo("AAAR5FAAYAAAAANAAC");
    assertThat(RowIds.isLobGroup(c.rowId())).isTrue();
  }

  @Test
  void anotherRowOrARewriteFromTheStartBeginsANewChange() {
    LobAssembler a = new LobAssembler(LobAssembler.Mode.INLINE, 1 << 20);
    List<RowChange> out = new ArrayList<>();
    out.addAll(lob(a, write("C", where(6, "y6"), 1, "six"), RowIds.PLACEHOLDER));
    out.addAll(lob(a, trim("C", where(6, "y6"), 3), RowIds.PLACEHOLDER));
    out.addAll(lob(a, write("C", where(7, "y7"), 1, "seven"), RowIds.PLACEHOLDER));
    out.addAll(lob(a, trim("C", where(7, "y7"), 5), RowIds.PLACEHOLDER));
    out.addAll(lob(a, write("C", where(7, "y7"), 1, "again"), RowIds.PLACEHOLDER));
    a.flush(K).ifPresent(out::add);
    assertThat(out).hasSize(3);
    assertThat(out).extracting(c -> c.after().get("C")).containsExactly("six", "seven", null);
    assertThat(out).extracting(RowChange::rowId).doesNotHaveDuplicates();
    // a real-ROWID write does not join a placeholder group
    lob(a, write("C", where(7, "y7"), 1, "x"), RowIds.PLACEHOLDER);
    assertThat(lob(a, write("C", where(7, "y7"), 2, "y"), "AAAR5FAAYAAAAANAAC")).hasSize(1);
  }

  @Test
  void aColumnSetByTheRowPieceIsAKnownBaseForItsWrites() {
    LobAssembler a = new LobAssembler(LobAssembler.Mode.INLINE, 1 << 20);
    row(a, Operation.UPDATE, update("\"NC\" = EMPTY_CLOB()", 6, "both"), RowIds.PLACEHOLDER);
    lob(a, write("NC", where(6, "both"), 1, "é€'é"), RowIds.PLACEHOLDER);
    lob(a, write("NC", where(6, "both"), 5, "€"), RowIds.PLACEHOLDER);
    assertThat(a.flush(K).orElseThrow().after()).containsEntry("NC", "é€'é€");
  }

  @Test
  void writesPastTheEndPadAndEraseAndTrimEditAKnownValue() {
    LobAssembler a = new LobAssembler(LobAssembler.Mode.INLINE, 1 << 20);
    row(a, Operation.INSERT, insertEmpty(1, "a"), RowIds.PLACEHOLDER);
    lob(a, write("C", where(1, "a"), 3, "xy"), RowIds.PLACEHOLDER);
    lob(a, writeBytes(where(1, "a"), 2, new byte[] {9}), RowIds.PLACEHOLDER);
    lob(a, write("NC", where(1, "a"), 1, "abcdef"), RowIds.PLACEHOLDER);
    lob(a, erase("NC", where(1, "a"), 2, 2), RowIds.PLACEHOLDER);
    lob(a, trim("NC", where(1, "a"), 5), RowIds.PLACEHOLDER);
    RowChange c = a.flush(K).orElseThrow();
    assertThat(c.after()).containsEntry("C", "  xy").containsEntry("NC", "a  de");
    assertThat((byte[]) c.after().get("B")).containsExactly(0, 9);
  }

  @Test
  void textPositionsCountCharactersNotUtf16Units() {
    LobAssembler a = new LobAssembler(LobAssembler.Mode.INLINE, 1 << 20);
    row(a, Operation.INSERT, insertEmpty(1, "a"), RowIds.PLACEHOLDER);
    lob(a, write("C", where(1, "a"), 1, "x\ud83d\ude00y"), RowIds.PLACEHOLDER);
    lob(a, write("C", where(1, "a"), 4, "z"), RowIds.PLACEHOLDER);
    lob(a, write("NC", where(1, "a"), 1, "a\ud83d\ude00b\ud83d\ude00c"), RowIds.PLACEHOLDER);
    lob(a, write("NC", where(1, "a"), 2, "Q"), RowIds.PLACEHOLDER);
    lob(a, erase("NC", where(1, "a"), 1, 4), RowIds.PLACEHOLDER);
    lob(a, trim("NC", where(1, "a"), 4), RowIds.PLACEHOLDER);
    RowChange c = a.flush(K).orElseThrow();
    assertThat(c.after()).containsEntry("C", "x\ud83d\ude00yz").containsEntry("NC", "aQb ");
    // a lone write ending inside the old value keeps what follows it
    row(a, Operation.INSERT, insertEmpty(2, "b"), RowIds.PLACEHOLDER);
    lob(
        a,
        write("C", where(2, "b"), 1, "\ud83d\ude00\ud83d\ude00\ud83d\ude00"),
        RowIds.PLACEHOLDER);
    lob(a, write("C", where(2, "b"), 2, "m"), RowIds.PLACEHOLDER);
    assertThat(a.flush(K).orElseThrow().after()).containsEntry("C", "\ud83d\ude00m\ud83d\ude00");
  }

  @Test
  void anErasePastTheKnownPrefixOrAGapLosesTheValue() {
    LobAssembler a = new LobAssembler(LobAssembler.Mode.INLINE, 1 << 20);
    lob(a, write("C", where(6, "s"), 1, "abc"), RowIds.PLACEHOLDER);
    lob(a, erase("C", where(6, "s"), 10, 2), RowIds.PLACEHOLDER);
    lob(a, trim("C", where(6, "s"), 2), RowIds.PLACEHOLDER);
    assertThat(a.flush(K).orElseThrow().after()).doesNotContainKey("C");
    lob(a, write("C", where(6, "s"), 1, "abc"), RowIds.PLACEHOLDER);
    lob(a, trim("C", where(6, "s"), 9), RowIds.PLACEHOLDER);
    assertThat(a.flush(K).orElseThrow().after()).doesNotContainKey("C");
  }

  @Test
  void valuesOverTheLimitAreUnavailableAndReportedOnce() {
    LobAssembler a = new LobAssembler(LobAssembler.Mode.INLINE, 4);
    row(a, Operation.INSERT, insertEmpty(1, "a"), RowIds.PLACEHOLDER);
    lob(a, write("C", where(1, "a"), 1, "abcde"), RowIds.PLACEHOLDER);
    lob(a, write("NC", where(1, "a"), 1, "€€"), RowIds.PLACEHOLDER); // 2 chars, 6 bytes
    RowChange c = a.flush(K).orElseThrow();
    assertThat(c.after()).doesNotContainKeys("C", "NC");
    LobAssembler.Oversize o = a.takeOversize(K).orElseThrow();
    assertThat(o.column()).isEqualTo("C");
    assertThat(o.table()).isEqualTo("FREEPDB1.APP.DOCS");
    assertThat(a.takeOversize(K)).isEmpty();
  }

  @Test
  void skipModeKeepsNoContentButStillGroupsTheStatement() {
    LobAssembler a = new LobAssembler(LobAssembler.Mode.SKIP, 4);
    row(a, Operation.INSERT, insertEmpty(1, "a"), RowIds.PLACEHOLDER);
    lob(a, write("C", where(1, "a"), 1, "abcdefgh"), RowIds.PLACEHOLDER);
    RowChange c = a.flush(K).orElseThrow();
    assertThat(c.after()).doesNotContainKey("C").containsEntry("NC", "");
    assertThat(a.takeOversize(K)).isEmpty();
    assertThat(a.fragments()).isEqualTo(1);
  }

  @Test
  void otherChangesPassThroughAndDiscardForgetsTheTransaction() {
    LobAssembler a = new LobAssembler();
    List<RowChange> del =
        row(a, Operation.DELETE, "delete from \"APP\".\"DOCS\" where " + where(1, "a"), R6);
    assertThat(del).hasSize(1);
    assertThat(del.get(0).rowId()).isEqualTo(R6);
    row(a, Operation.INSERT, insertEmpty(1, "a"), RowIds.PLACEHOLDER);
    a.discard(K);
    assertThat(a.flush(K)).isEmpty();
    assertThat(a.oldestPending()).isEmpty();
    // an insert not followed by its locator update is released by the next row, synthesised
    row(a, Operation.INSERT, insertEmpty(1, "a"), RowIds.PLACEHOLDER);
    List<RowChange> out = row(a, Operation.INSERT, insertEmpty(2, "b"), RowIds.PLACEHOLDER);
    assertThat(out).hasSize(1);
    assertThat(out.get(0).after()).containsEntry("ID", BigDecimal.ONE);
    assertThat(RowIds.isSynthetic(out.get(0).rowId())).isTrue();
  }

  private List<RowChange> row(LobAssembler a, Operation op, String sql, String rowId) {
    MiningEvent.Dml d = dml(op, sql, rowId);
    return a.accept(K, RowDecoder.decode(d, SCHEMA), SCHEMA);
  }

  private List<RowChange> lob(LobAssembler a, String sql, String rowId) {
    MiningEvent.Dml d = dml(Operation.LOB_WRITE, sql, rowId);
    return a.acceptLob(K, RowDecoder.decodeLob(d, SCHEMA), SCHEMA);
  }

  private MiningEvent.Dml dml(Operation op, String sql, String rowId) {
    seq++;
    return new MiningEvent.Dml(
        K,
        new RedoRecordId(100 + seq, "0x0f." + seq + ".10", 0),
        1,
        op,
        LobRedoShapes.DOCS,
        100,
        100,
        1,
        rowId,
        sql,
        null,
        false,
        op.isLobOp() ? 2 : 0,
        null,
        "APP",
        Instant.EPOCH);
  }
}
