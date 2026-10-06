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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;
import sh.oso.connect.oracle.core.errors.OracleCdcCorruptionException;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TxKey;

/**
 * Append-only spill files for transactions that no longer fit the heap budget (CORE-TX-3). One file
 * per transaction under the spill directory; every frame carries a CRC32, so a damaged file is a
 * typed stop, never a silently shortened transaction. Spill files are not durable state: the
 * position re-mines open transactions after a restart, and stale files from an earlier run are
 * removed when the store opens.
 */
public class SpillStore implements AutoCloseable {

  static final String SUFFIX = ".spill";
  private static final byte FRAME_CHANGE = 1;
  private static final byte FRAME_UNDO = 2;
  private static final int MAX_FRAME = 1 << 30;

  private final Path dir;
  private final long maxBytes;
  private final Map<TxKey, SpillFile> files = new LinkedHashMap<>();
  private long totalBytes;

  public SpillStore(Path dir, long maxBytes) throws IOException {
    this.dir = dir;
    this.maxBytes = maxBytes;
    Files.createDirectories(dir);
    try (DirectoryStream<Path> stale = Files.newDirectoryStream(dir, "*" + SUFFIX)) {
      for (Path p : stale) {
        Files.deleteIfExists(p);
      }
    }
  }

  public Path dir() {
    return dir;
  }

  public long maxBytes() {
    return maxBytes;
  }

  /** Bytes currently on disk across every spilled transaction. */
  public long totalBytes() {
    return totalBytes;
  }

  public int openFiles() {
    return files.size();
  }

  /** Opens the spill file for a transaction; fails when one already exists. */
  SpillFile create(TxKey key) throws IOException {
    if (files.containsKey(key)) {
      throw new IllegalStateException("transaction already spilled: " + key);
    }
    Path p = dir.resolve(fileName(key));
    SpillFile f = new SpillFile(key, p, openForAppend(p));
    files.put(key, f);
    return f;
  }

  /** Overridable for tests that simulate a full disk. */
  OutputStream openForAppend(Path p) throws IOException {
    return new BufferedOutputStream(
        Files.newOutputStream(
            p,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE),
        1 << 16);
  }

  InputStream openForRead(Path p) throws IOException {
    return new BufferedInputStream(Files.newInputStream(p), 1 << 16);
  }

  static String fileName(TxKey key) {
    return key.srcConId()
        + "-"
        + key.xid().usn()
        + "."
        + key.xid().slot()
        + "."
        + key.xid().sqn()
        + SUFFIX;
  }

  @Override
  public void close() {
    for (SpillFile f : List.copyOf(files.values())) {
      f.delete();
    }
  }

  /** The spilled changes of one transaction: a file with CHANGE and UNDO frames in redo order. */
  final class SpillFile {
    final TxKey key;
    final Path path;
    private OutputStream out;
    private DataOutputStream data;
    private long bytes;
    private int changeFrames;
    private int undoFrames;
    private boolean sealed;

    SpillFile(TxKey key, Path path, OutputStream out) {
      this.key = key;
      this.path = path;
      this.out = out;
      this.data = new DataOutputStream(out);
    }

    long bytes() {
      return bytes;
    }

    int changeFrames() {
      return changeFrames;
    }

    int undoFrames() {
      return undoFrames;
    }

    void append(RowChange c) throws IOException {
      frame(FRAME_CHANGE, RowChangeCodec.encode(c));
      changeFrames++;
    }

    void appendUndo(RedoRecordId undoId, String rowId) throws IOException {
      frame(FRAME_UNDO, encodeUndo(undoId, rowId));
      undoFrames++;
    }

    /**
     * Every frame in file order, for journaling a spilled transaction. The file keeps accepting
     * appends afterwards.
     */
    List<JournalFrames.Frame> frames() throws IOException {
      data.flush();
      List<JournalFrames.Frame> out = new ArrayList<>(changeFrames + undoFrames);
      try (FrameReader r = new FrameReader(openForRead(path), path)) {
        while (r.next()) {
          if (r.kind == FRAME_CHANGE) {
            out.add(JournalFrames.Frame.of(RowChangeCodec.decode(r.payload)));
          } else {
            Undo u = decodeUndo(r.payload);
            out.add(JournalFrames.Frame.undo(u.id, u.rowId));
          }
        }
      }
      return out;
    }

    private void frame(byte kind, byte[] payload) throws IOException {
      if (sealed) {
        throw new IllegalStateException("spill file sealed: " + path);
      }
      CRC32 crc = new CRC32();
      crc.update(kind);
      crc.update(payload);
      data.writeInt(payload.length);
      data.writeInt((int) crc.getValue());
      data.writeByte(kind);
      data.write(payload);
      long added = 9L + payload.length;
      bytes += added;
      totalBytes += added;
    }

    /**
     * Flushes, applies the UNDO frames the way the heap entry would (each removes the latest
     * earlier change with the same ROWID) and returns the survivors as a lazily read list.
     */
    Resolved resolve() throws IOException {
      seal();
      BitSet removed = new BitSet();
      BitSet downgraded = new BitSet();
      int unmatched = 0;
      if (undoFrames > 0) {
        // pass 1: which ROWIDs are undone at all
        Map<String, Integer> undone = new HashMap<>();
        try (FrameReader r = new FrameReader(openForRead(path), path)) {
          while (r.next()) {
            if (r.kind == FRAME_UNDO) {
              undone.merge(decodeUndo(r.payload).rowId, 1, Integer::sum);
            }
          }
        }
        // pass 2: resolve each UNDO against the latest earlier surviving change with that ROWID
        Map<String, java.util.ArrayDeque<Integer>> latest = new HashMap<>();
        try (FrameReader r = new FrameReader(openForRead(path), path)) {
          int index = 0;
          while (r.next()) {
            if (r.kind == FRAME_CHANGE) {
              String rowId = RowChangeCodec.decode(r.payload).rowId();
              if (rowId != null && undone.containsKey(rowId)) {
                latest.computeIfAbsent(rowId, k -> new java.util.ArrayDeque<>()).push(index);
              }
            } else {
              String rowId = decodeUndo(r.payload).rowId;
              removed.set(index);
              java.util.ArrayDeque<Integer> stack = latest.get(rowId);
              if (stack == null || stack.isEmpty()) {
                unmatched++;
              } else if (sh.oso.connect.oracle.core.model.RowIds.isLobGroup(rowId)) {
                downgraded.set(stack.pop()); // ADR-0015: kept, its LOB values unavailable
              } else {
                removed.set(stack.pop());
              }
            }
            index++;
          }
        }
      }
      int survivors = changeFrames + undoFrames - removed.cardinality();
      return new Resolved(
          new SpilledChanges(this, removed, downgraded, survivors),
          undoFrames - unmatched,
          unmatched);
    }

    private void seal() throws IOException {
      if (!sealed) {
        data.flush();
        out.close();
        sealed = true;
      }
    }

    void delete() {
      try {
        if (!sealed) {
          out.close();
          sealed = true;
        }
      } catch (IOException ignore) {
        // the file is deleted next
      }
      try {
        Files.deleteIfExists(path);
      } catch (IOException ignore) {
        // best effort; the store's directory is cleaned at the next open
      }
      if (files.remove(key) != null) {
        totalBytes -= bytes;
      }
    }
  }

  /** The outcome of resolving a spilled transaction at commit. */
  record Resolved(List<RowChange> changes, int undone, int unmatchedUndo) {}

  private record Undo(RedoRecordId id, String rowId) {}

  private static byte[] encodeUndo(RedoRecordId id, String rowId) {
    java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream(64);
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeLong(id == null ? -1 : id.scn());
      out.writeUTF(id == null || id.rsId() == null ? "" : id.rsId());
      out.writeLong(id == null ? 0 : id.ssn());
      out.writeUTF(rowId);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
    return bytes.toByteArray();
  }

  private static Undo decodeUndo(byte[] payload) throws IOException {
    try (DataInputStream in = new DataInputStream(new java.io.ByteArrayInputStream(payload))) {
      long scn = in.readLong();
      String rsId = in.readUTF();
      long ssn = in.readLong();
      String rowId = in.readUTF();
      return new Undo(scn < 0 ? null : new RedoRecordId(scn, rsId, ssn), rowId);
    }
  }

  /** Sequential frame reader with CRC verification. */
  private static final class FrameReader implements AutoCloseable {
    private final DataInputStream in;
    private final Path path;
    byte kind;
    byte[] payload;
    private int frameNo;

    FrameReader(InputStream in, Path path) {
      this.in = new DataInputStream(in);
      this.path = path;
    }

    boolean next() throws IOException {
      int len;
      try {
        len = in.readInt();
      } catch (EOFException e) {
        return false;
      }
      if (len < 0 || len > MAX_FRAME) {
        throw corrupt("frame " + frameNo + " has length " + len);
      }
      int expected;
      byte k;
      byte[] p = new byte[len];
      try {
        expected = in.readInt();
        k = in.readByte();
        in.readFully(p);
      } catch (EOFException e) {
        throw corrupt("frame " + frameNo + " is truncated");
      }
      CRC32 crc = new CRC32();
      crc.update(k);
      crc.update(p);
      if ((int) crc.getValue() != expected) {
        throw corrupt("frame " + frameNo + " failed its CRC check");
      }
      kind = k;
      payload = p;
      frameNo++;
      return true;
    }

    private OracleCdcCorruptionException corrupt(String what) {
      return new OracleCdcCorruptionException(
          "Spill file " + path + " is damaged: " + what + ".",
          "The spilled copy of an uncommitted transaction cannot be trusted. Check the spill"
              + " volume (cdc.buffer.spill.dir), then restart the task: it re-mines the open"
              + " transactions from the position.");
    }

    @Override
    public void close() throws IOException {
      in.close();
    }
  }

  /**
   * The surviving changes of a spilled transaction, read from the file on demand. Sequential {@code
   * get(i)} is O(1) amortised, and asking for the change just read again returns it without going
   * back (the sink and the envelope both read event i); any other backward jump re-reads from the
   * start. The size is known from the resolve pass, so callers that iterate by index behave exactly
   * as with a heap list.
   */
  final class SpilledChanges extends AbstractList<RowChange> implements CommittedTransaction.Lazy {
    private final SpillFile file;
    private final BitSet removed;
    private final BitSet downgraded;
    private final int size;
    private FrameReader reader;
    private int nextLogical; // logical index of the next survivor the reader will deliver
    private int opens;
    private int lastIndex = -1;
    private RowChange last;

    SpilledChanges(SpillFile file, BitSet removed, BitSet downgraded, int size) {
      this.file = file;
      this.removed = removed;
      this.downgraded = downgraded;
      this.size = size;
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public RowChange get(int index) {
      if (index < 0 || index >= size) {
        throw new IndexOutOfBoundsException(index);
      }
      if (index == lastIndex) {
        return last;
      }
      try {
        if (reader == null || index < nextLogical) {
          rewind();
        }
        while (true) {
          if (!reader.next()) {
            throw new OracleCdcCorruptionException(
                "Spill file " + file.path + " ended before survivor " + index + " of " + size + ".",
                "Restart the task; it re-mines the open transactions from the position.");
          }
          int f = reader.frameNo - 1; // the frame just delivered
          if (reader.kind != FRAME_CHANGE || removed.get(f)) {
            continue;
          }
          int logical = nextLogical++;
          if (logical == index) {
            RowChange c = RowChangeCodec.decode(reader.payload);
            last = downgraded.get(f) ? TransactionEntry.inert(c) : c;
            lastIndex = index;
            return last;
          }
        }
      } catch (IOException e) {
        closeReader(); // a later call starts over rather than continuing a broken stream
        throw new OracleCdcCorruptionException(
            "Reading spill file " + file.path + " failed: " + e.getMessage(),
            "Check the spill volume (cdc.buffer.spill.dir) and restart the task.",
            e);
      } catch (OracleCdcCorruptionException e) {
        closeReader();
        throw e;
      }
    }

    private void rewind() throws IOException {
      if (reader != null) {
        reader.close();
      }
      reader = new FrameReader(openForRead(file.path), file.path);
      nextLogical = 0;
      opens++;
    }

    /** How many times the file was opened for reading: once for a reader that never goes back. */
    int opens() {
      return opens;
    }

    /** Closes the reader; the buffer deletes the file when the transaction is released. */
    void closeReader() {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException ignore) {
          // nothing else to do
        }
        reader = null;
      }
    }
  }
}
