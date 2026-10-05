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

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import sh.oso.connect.oracle.core.errors.OracleCdcCorruptionException;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;

class SpillStoreTest {

  static final TableId T = new TableId("FREEPDB1", "APP", "T");
  static final TxKey TX = new TxKey(3, new Xid(1, 2, 3));

  static RowChange change(int i, String rowId) {
    return new RowChange(
        T,
        Operation.INSERT,
        null,
        Map.of("ID", BigDecimal.valueOf(i), "V", "value-" + i),
        false,
        rowId,
        new RedoRecordId(1000 + i, "rs" + i, 0),
        TX,
        null);
  }

  @Test
  void appendsFramesAndReadsSurvivorsInOrder(@TempDir Path dir) throws Exception {
    try (SpillStore store = new SpillStore(dir, 1 << 20)) {
      SpillStore.SpillFile f = store.create(TX);
      for (int i = 0; i < 100; i++) {
        f.append(change(i, "row" + i));
      }
      assertThat(f.changeFrames()).isEqualTo(100);
      assertThat(store.totalBytes()).isEqualTo(f.bytes()).isPositive();
      SpillStore.Resolved r = f.resolve();
      assertThat(r.undone()).isZero();
      assertThat(r.unmatchedUndo()).isZero();
      List<RowChange> list = r.changes();
      assertThat(list).hasSize(100);
      for (int i = 0; i < 100; i++) {
        assertThat(list.get(i).after().get("ID")).isEqualTo(BigDecimal.valueOf(i));
      }
      // a backward jump rewinds and still answers correctly
      assertThat(list.get(5).rowId()).isEqualTo("row5");
      assertThat(list.get(99).rowId()).isEqualTo("row99");
      assertThatThrownBy(() -> list.get(100)).isInstanceOf(IndexOutOfBoundsException.class);
      f.delete();
      assertThat(store.totalBytes()).isZero();
      assertThat(Files.list(dir).count()).isZero();
    }
  }

  @Test
  void undoFramesRemoveTheLatestEarlierChangeWithTheSameRowId(@TempDir Path dir) throws Exception {
    try (SpillStore store = new SpillStore(dir, 1 << 20)) {
      SpillStore.SpillFile f = store.create(TX);
      f.append(change(1, "A"));
      f.append(change(2, "B"));
      f.append(change(3, "A")); // second version of A
      f.appendUndo(new RedoRecordId(900, "u", 0), "A"); // removes change 3, not change 1
      f.append(change(4, "C"));
      f.appendUndo(new RedoRecordId(900, "u", 0), "Z"); // nothing to match
      f.appendUndo(new RedoRecordId(900, "u", 0), "B");
      SpillStore.Resolved r = f.resolve();
      assertThat(r.undone()).isEqualTo(2);
      assertThat(r.unmatchedUndo()).isEqualTo(1);
      assertThat(r.changes())
          .extracting(c -> c.after().get("ID"))
          .containsExactly(BigDecimal.valueOf(1), BigDecimal.valueOf(4));
    }
  }

  @Test
  void aDamagedFrameIsATypedCorruptionStop(@TempDir Path dir) throws Exception {
    try (SpillStore store = new SpillStore(dir, 1 << 20)) {
      SpillStore.SpillFile f = store.create(TX);
      f.append(change(1, "A"));
      f.append(change(2, "B"));
      f.appendUndo(new RedoRecordId(900, "u", 0), "B");
      Path p = dir.resolve(SpillStore.fileName(TX));
      // resolve seals the file; flip a payload byte of the first frame afterwards
      SpillStore.Resolved r = f.resolve();
      assertThat(r.changes()).hasSize(1);
      byte[] bytes = Files.readAllBytes(p);
      bytes[9 + 20] ^= 0x55;
      Files.write(p, bytes);
      assertThatThrownBy(() -> r.changes().get(0))
          .isInstanceOf(OracleCdcCorruptionException.class)
          .hasMessageContaining("CRC");
      // a truncated frame is detected too
      Files.write(p, java.util.Arrays.copyOf(bytes, 12));
      assertThatThrownBy(() -> r.changes().get(0))
          .isInstanceOf(OracleCdcCorruptionException.class)
          .hasMessageContaining("truncated");
      // and so is an absurd frame length
      bytes[0] = (byte) 0x7f;
      Files.write(p, bytes);
      assertThatThrownBy(() -> r.changes().get(0))
          .isInstanceOf(OracleCdcCorruptionException.class)
          .hasMessageContaining("length");
    }
  }

  private static long frameLength(byte[] b, int off) {
    return ((b[off] & 0xffL) << 24)
        | ((b[off + 1] & 0xffL) << 16)
        | ((b[off + 2] & 0xffL) << 8)
        | (b[off + 3] & 0xffL);
  }

  @Test
  void staleFilesFromAnEarlierRunAreRemovedOnOpen(@TempDir Path dir) throws Exception {
    Files.writeString(dir.resolve("1-2.3.4" + SpillStore.SUFFIX), "stale");
    Files.writeString(dir.resolve("keep.txt"), "mine");
    try (SpillStore store = new SpillStore(dir, 1 << 20)) {
      assertThat(Files.exists(dir.resolve("1-2.3.4" + SpillStore.SUFFIX))).isFalse();
      assertThat(Files.exists(dir.resolve("keep.txt"))).isTrue();
      assertThat(store.openFiles()).isZero();
    }
  }

  @Test
  void closeDeletesEveryOpenFile(@TempDir Path dir) throws Exception {
    SpillStore store = new SpillStore(dir, 1 << 20);
    store.create(TX).append(change(1, "A"));
    store.create(new TxKey(3, new Xid(5, 5, 5))).append(change(2, "B"));
    assertThat(store.openFiles()).isEqualTo(2);
    store.close();
    assertThat(Files.list(dir).count()).isZero();
    assertThat(store.totalBytes()).isZero();
  }

  @Test
  void aFullDiskSurfacesAsAnIoExceptionToTheBuffer(@TempDir Path dir) throws Exception {
    SpillStore store =
        new SpillStore(dir, 1 << 20) {
          @Override
          OutputStream openForAppend(Path p) {
            return new OutputStream() {
              int written;

              @Override
              public void write(int b) throws IOException {
                if (++written > 100) {
                  throw new IOException("No space left on device");
                }
              }
            };
          }
        };
    SpillStore.SpillFile f = store.create(TX);
    assertThatThrownBy(
            () -> {
              for (int i = 0; i < 10; i++) {
                f.append(change(i, "r"));
              }
            })
        .isInstanceOf(IOException.class)
        .hasMessageContaining("No space");
    store.close();
  }
}
