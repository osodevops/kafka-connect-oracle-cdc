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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import sh.oso.connect.oracle.core.errors.JournalCorruptionException;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;

/** The frames inside a journal chunk payload: a change, or an undo marker for a ROWID. */
public final class JournalFrames {

  /** A change or an undo; exactly one of {@code change} and {@code undoRowId} is set. */
  public record Frame(RowChange change, RedoRecordId undoId, String undoRowId) {
    public static Frame of(RowChange c) {
      return new Frame(c, null, null);
    }

    public static Frame undo(RedoRecordId id, String rowId) {
      return new Frame(null, id, rowId);
    }

    public boolean isUndo() {
      return change == null;
    }

    public RedoRecordId id() {
      return change != null ? change.id() : undoId;
    }
  }

  private static final byte CHANGE = 1;
  private static final byte UNDO = 2;

  private JournalFrames() {}

  public static byte[] encode(List<Frame> frames) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream(1024);
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      out.writeInt(frames.size());
      for (Frame f : frames) {
        if (f.isUndo()) {
          out.writeByte(UNDO);
          out.writeLong(f.undoId().scn());
          writeString(out, f.undoId().rsId());
          out.writeLong(f.undoId().ssn());
          writeString(out, f.undoRowId());
        } else {
          out.writeByte(CHANGE);
          byte[] b = RowChangeCodec.encode(f.change());
          out.writeInt(b.length);
          out.write(b);
        }
      }
    } catch (IOException e) {
      throw new IllegalStateException("in-memory encode failed", e);
    }
    return bytes.toByteArray();
  }

  /** Approximate encoded size of a frame, for chunk splitting. */
  public static int sizeOf(Frame f) {
    return f.isUndo() ? 64 : (int) SizeEstimate.of(f.change());
  }

  public static List<Frame> decode(byte[] payload, String what) {
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload))) {
      int n = in.readInt();
      if (n < 0 || n > payload.length) {
        throw new JournalCorruptionException(
            what + " declares " + n + " frames in " + payload.length + " bytes.",
            "The journal topic holds a damaged chunk. Reset the connector offsets to a position"
                + " before the transaction and remove the chunk, or resnapshot.");
      }
      List<Frame> out = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        byte kind = in.readByte();
        if (kind == UNDO) {
          RedoRecordId id = new RedoRecordId(in.readLong(), readString(in), in.readLong());
          out.add(Frame.undo(id, readString(in)));
        } else if (kind == CHANGE) {
          int len = in.readInt();
          if (len < 0 || len > payload.length) {
            throw new JournalCorruptionException(
                what + " frame " + i + " has length " + len + ".",
                "The journal topic holds a damaged chunk; see the runbook.");
          }
          byte[] b = new byte[len];
          in.readFully(b);
          out.add(Frame.of(RowChangeCodec.decode(b)));
        } else {
          throw new JournalCorruptionException(
              what + " frame " + i + " has unknown kind " + kind + ".",
              "The journal topic holds a damaged chunk; see the runbook.");
        }
      }
      return out;
    } catch (IOException e) {
      throw new JournalCorruptionException(
          what + " cannot be decoded: " + e.getMessage(),
          "The journal topic holds a damaged chunk; see the runbook.",
          e);
    }
  }

  private static void writeString(DataOutputStream out, String s) throws IOException {
    if (s == null) {
      out.writeInt(-1);
      return;
    }
    byte[] b = s.getBytes(StandardCharsets.UTF_8);
    out.writeInt(b.length);
    out.write(b);
  }

  private static String readString(DataInputStream in) throws IOException {
    int n = in.readInt();
    if (n < 0) {
      return null;
    }
    byte[] b = new byte[n];
    in.readFully(b);
    return new String(b, StandardCharsets.UTF_8);
  }
}
