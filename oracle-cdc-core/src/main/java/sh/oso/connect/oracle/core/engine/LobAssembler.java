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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import sh.oso.connect.oracle.core.decode.LobFragment;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.RowIds;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.TableSchema;

/**
 * Turns LogMiner's row pieces and LOB rows into one change per statement and row (PRD-00
 * CORE-DEC-6, ADR-0015, reference/lob-redo-shapes.md).
 *
 * <p>A statement that writes an out-of-row LOB appears as a row piece carrying the placeholder
 * ROWID (an INSERT with EMPTY_CLOB or EMPTY_BLOB values, or an UPDATE of the other columns), then
 * the LOB_WRITE, LOB_TRIM and LOB_ERASE rows for that row, and for an INSERT often a locator UPDATE
 * with the real ROWID. Oracle undoes such a statement with a single row, so the pieces are folded
 * into one change while the transaction's next row keeps matching. LOB rows without a row piece (an
 * UPDATE of an existing out-of-row LOB, or DBMS_LOB calls) become a change of their own.
 *
 * <p>A LOB value is published only when it is known completely: the base was empty or set by the
 * row piece, or the writes cover it from the first character and a trim fixes its length. Anything
 * else, and anything above {@code maxBytes}, leaves the column out of the image, which the
 * connector renders according to {@code cdc.lob.mode}. In {@code skip} mode no content is kept.
 *
 * <p>A held change counts as open work for the resume position (CORE-POS-2).
 */
public final class LobAssembler {

  /** What the assembler keeps of LOB content. */
  public enum Mode {
    /** No content is kept; LOB columns written by redo are unavailable. */
    SKIP,
    /** Content is assembled up to the size limit. */
    INLINE,
    /** As INLINE; the engine reselects what stays unavailable at commit. */
    RESELECT
  }

  /** The first value of a transaction that went over the limit. */
  public record Oversize(String table, String column, long bytes) {}

  private final Mode mode;
  private final long maxBytes;
  private final Map<TxKey, Open> open = new HashMap<>();
  private final Map<TxKey, Oversize> oversize = new HashMap<>();
  private long counter;
  private long merged;
  private long fragments;

  public LobAssembler(Mode mode, long maxBytes) {
    this.mode = Objects.requireNonNull(mode, "mode");
    this.maxBytes = maxBytes;
  }

  /** The pre-CORE-DEC-6 behaviour: values kept inline up to 1 MiB. */
  public LobAssembler() {
    this(Mode.INLINE, 1L << 20);
  }

  /** A decoded row change; returns the changes ready for the buffer, in order. */
  public List<RowChange> accept(TxKey key, RowChange change, TableSchema schema) {
    List<RowChange> out = new ArrayList<>(2);
    Open held = open.remove(key);
    if (held != null) {
      if (held.piece != null
          && held.piece.op() == Operation.INSERT
          && RowIds.isPlaceholder(held.piece.rowId())
          && isLocatorUpdate(held.piece, change, held.lobColumns)) {
        merged++;
        out.add(finish(held, change));
        return out;
      }
      out.add(finish(held, null));
    }
    if ((change.op() == Operation.INSERT || change.op() == Operation.UPDATE)
        && RowIds.isPlaceholder(change.rowId())) {
      open.put(key, new Open(key, change, change.after(), null, change.id(), schema));
    } else {
      out.add(change);
    }
    return out;
  }

  /** A decoded LOB row; returns the changes it closed, in order. */
  public List<RowChange> acceptLob(TxKey key, LobFragment f, TableSchema schema) {
    fragments++;
    List<RowChange> out = new ArrayList<>(1);
    Open held = open.get(key);
    if (held == null || !held.takes(f)) {
      if (held != null) {
        open.remove(key);
        out.add(finish(held, null));
      }
      held =
          new Open(
              key,
              null,
              f.where(),
              RowIds.isPlaceholder(f.rowId()) ? null : f.rowId(),
              f.id(),
              schema);
      open.put(key, held);
    }
    if (held.timestamp == null) {
      held.timestamp = f.timestamp();
    }
    held.apply(f, key);
    return out;
  }

  /** Releases the held change (before a commit or an undo of the same transaction). */
  public Optional<RowChange> flush(TxKey key) {
    Open held = open.remove(key);
    return held == null ? Optional.empty() : Optional.of(finish(held, null));
  }

  /** Forgets the transaction (rollback, discard, orphan release). */
  public void discard(TxKey key) {
    open.remove(key);
    oversize.remove(key);
  }

  /** The first value of the transaction that went over the limit, if any; cleared at commit. */
  public Optional<Oversize> takeOversize(TxKey key) {
    return Optional.ofNullable(oversize.remove(key));
  }

  /** Earliest held change, so the resume point never passes it (CORE-POS-2). */
  public Optional<RedoRecordId> oldestPending() {
    return open.values().stream().map(o -> o.id).min(RedoRecordId::compareTo);
  }

  public int pendingCount() {
    return open.size();
  }

  /** Locator updates folded into their INSERT. */
  public long merged() {
    return merged;
  }

  /** LOB rows applied. */
  public long fragments() {
    return fragments;
  }

  private RowChange finish(Open o, RowChange locator) {
    Map<String, Object> after = new LinkedHashMap<>(o.image);
    for (Map.Entry<String, Lob> e : o.lobs.entrySet()) {
      Lob lob = e.getValue();
      Object v = lob.value();
      long size = v instanceof String str ? str.getBytes(StandardCharsets.UTF_8).length : 0;
      if (size > maxBytes) {
        oversize.putIfAbsent(o.tx, new Oversize(o.schema.table().fqn(), e.getKey(), size));
        v = null;
      }
      if (v == null) {
        after.remove(e.getKey()); // unavailable: the connector renders it per cdc.lob.mode
      } else {
        after.put(e.getKey(), v);
      }
    }
    if (locator != null) {
      after.putAll(locator.after());
    }
    String unique = unique(o.id);
    if (o.piece == null) {
      int nonLob = o.schema.columns().size() - o.lobColumns.size();
      return new RowChange(
          o.schema.table(),
          Operation.UPDATE,
          o.image,
          after,
          o.image.size() < nonLob,
          RowIds.lobGroup(o.realRowId, unique),
          o.id,
          o.tx,
          o.timestamp);
    }
    String rowId =
        locator != null && !RowIds.isPlaceholder(locator.rowId())
            ? locator.rowId()
            : RowIds.rowPiece(o.piece.rowId(), unique);
    return new RowChange(
        o.piece.table(),
        o.piece.op(),
        o.piece.before(),
        after,
        o.piece.partial(),
        rowId,
        o.piece.id(),
        o.piece.tx(),
        o.piece.timestamp());
  }

  private String unique(RedoRecordId id) {
    String at = id == null ? "" : id.rsId() == null ? Long.toString(id.scn()) : id.rsId().trim();
    return at + "." + (counter++);
  }

  /** The UPDATE right after the insert, same table, before image equal on every non-LOB column. */
  static boolean isLocatorUpdate(
      RowChange insert, RowChange next, java.util.Set<String> lobColumns) {
    if (next.op() != Operation.UPDATE
        || !next.table().equals(insert.table())
        || next.before() == null
        || insert.after() == null) {
      return false;
    }
    for (String col : next.after().keySet()) {
      if (!lobColumns.contains(col)
          && !Objects.equals(next.after().get(col), next.before().get(col))) {
        return false;
      }
    }
    for (Map.Entry<String, Object> e : next.before().entrySet()) {
      if (lobColumns.contains(e.getKey())) {
        continue;
      }
      if (!insert.after().containsKey(e.getKey())
          || !valuesEqual(insert.after().get(e.getKey()), e.getValue())) {
        return false;
      }
    }
    return true;
  }

  static boolean valuesEqual(Object a, Object b) {
    if (a instanceof java.math.BigDecimal x && b instanceof java.math.BigDecimal y) {
      return x.compareTo(y) == 0;
    }
    if (a instanceof byte[] x && b instanceof byte[] y) {
      return Arrays.equals(x, y);
    }
    return Objects.equals(a, b);
  }

  /** A change being assembled: a row piece with its LOB rows, or LOB rows alone. */
  private final class Open {
    final TxKey tx;
    final RowChange piece;
    final Map<String, Object> image;
    final String realRowId;
    final RedoRecordId id;
    java.time.Instant timestamp;
    final TableSchema schema;
    final java.util.Set<String> lobColumns = new java.util.HashSet<>();
    final Map<String, Lob> lobs = new LinkedHashMap<>();

    Open(
        TxKey tx,
        RowChange piece,
        Map<String, Object> image,
        String realRowId,
        RedoRecordId id,
        TableSchema schema) {
      this.tx = tx;
      this.piece = piece;
      this.image = image == null ? Map.of() : image;
      this.realRowId = realRowId;
      this.id = id;
      this.timestamp = piece == null ? null : piece.timestamp();
      // LOB groups take the time of their first row

      this.schema = schema;
      for (ColumnSpec c : schema.columns()) {
        if (c.type().isLob()) {
          lobColumns.add(c.name());
        }
      }
    }

    /** Whether a LOB row continues this change rather than starting a new statement. */
    boolean takes(LobFragment f) {
      if (!f.table().equals(schema.table())) {
        return false;
      }
      if (piece != null) {
        // only a row piece that LogMiner could not give a ROWID is followed by its LOB rows
        if (!RowIds.isPlaceholder(piece.rowId()) || !RowIds.isPlaceholder(f.rowId())) {
          return false;
        }
      } else if (!Objects.equals(realRowId, RowIds.isPlaceholder(f.rowId()) ? null : f.rowId())) {
        return false;
      }
      for (Map.Entry<String, Object> e : f.where().entrySet()) {
        if (!image.containsKey(e.getKey()) || !valuesEqual(image.get(e.getKey()), e.getValue())) {
          return false;
        }
      }
      // a write from the first position into a column this change already wrote is a new call
      Lob lob = lobs.get(f.column());
      return lob == null
          || !lob.written
          || !(f.edits().get(0) instanceof LobFragment.Write w && w.offset() == 1);
    }

    void apply(LobFragment f, TxKey key) {
      Lob lob = lobs.get(f.column());
      if (lob == null) {
        boolean known =
            piece != null && piece.after() != null && piece.after().containsKey(f.column());
        Object base = known ? baseOf(piece.after().get(f.column()), f.binary()) : null;
        lob = new Lob(f.binary(), base);
        if (mode == Mode.SKIP) {
          lob.lose(); // nothing is kept; the column is unavailable
        }
        lobs.put(f.column(), lob);
      }
      for (LobFragment.Edit e : f.edits()) {
        lob.apply(e);
        if (!lob.lost && lob.length > maxBytes) {
          // at least one byte per character: over the limit whatever the encoding
          oversize.putIfAbsent(key, new Oversize(schema.table().fqn(), f.column(), lob.length));
          lob.lose();
        }
      }
    }
  }

  private static Object baseOf(Object v, boolean binary) {
    if (v == null) {
      return binary ? new byte[0] : "";
    }
    return v;
  }

  /**
   * One LOB column's value: the whole value when the base is known, otherwise the prefix written
   * from the first position, which a trim can complete. {@code length} counts bytes for a BLOB and
   * characters (code points) for text, as DBMS_LOB offsets do.
   */
  static final class Lob {
    final boolean binary;
    boolean known;
    boolean lost;
    boolean written;
    boolean supplementary; // text holds surrogate pairs: positions need converting
    StringBuilder text;
    byte[] bytes;
    int length;

    /** {@code base} is the known value, or null when it is unknown (or not kept, in skip mode). */
    Lob(boolean binary, Object base) {
      this.binary = binary;
      if (base == null) {
        start(binary, "", new byte[0]);
        known = false;
      } else {
        start(
            binary, base instanceof String s ? s : "", base instanceof byte[] b ? b : new byte[0]);
        known = true;
      }
    }

    private void start(boolean binary, String s, byte[] b) {
      if (binary) {
        bytes = Arrays.copyOf(b, Math.max(16, b.length));
        length = b.length;
      } else {
        text = new StringBuilder(s);
        supplementary = s.length() != s.codePointCount(0, s.length());
        length = s.codePointCount(0, s.length());
      }
    }

    /** The UTF-16 index of character {@code cp} of the text. */
    private int at(int cp) {
      return supplementary ? text.offsetByCodePoints(0, cp) : cp;
    }

    void lose() {
      lost = true;
      text = null;
      bytes = null;
    }

    void apply(LobFragment.Edit e) {
      if (e instanceof LobFragment.Write w) {
        written = true;
        if (lost) {
          return;
        }
        long pos = w.offset() - 1;
        if (pos < 0 || (!known && pos > length) || pos + w.length() > Integer.MAX_VALUE) {
          lose(); // a gap before data we never saw
          return;
        }
        if (pos > length) {
          pad((int) pos);
        }
        write((int) pos, w.data());
      } else if (e instanceof LobFragment.Trim t) {
        if (lost) {
          return;
        }
        if (t.length() <= length) {
          if (!binary) {
            text.setLength(at((int) t.length()));
          }
          length = (int) t.length();
          known = true; // the prefix up to the new end is ours
        } else if (!known) {
          lose();
        }
      } else if (e instanceof LobFragment.Erase er) {
        written = true;
        if (lost) {
          return;
        }
        long from = er.offset() - 1;
        long to = Math.min(from + er.amount(), length);
        if (from < 0 || (!known && from + er.amount() > length)) {
          lose();
          return;
        }
        if (binary) {
          for (long i = from; i < to; i++) {
            bytes[(int) i] = 0;
          }
        } else if (to > from) {
          text.replace(at((int) from), at((int) to), " ".repeat((int) (to - from)));
        }
      }
    }

    private void pad(int to) {
      if (binary) {
        ensure(to);
        Arrays.fill(bytes, length, to, (byte) 0);
      } else {
        text.append(" ".repeat(to - length));
      }
      length = to;
    }

    private void write(int pos, Object data) {
      if (binary) {
        byte[] d = (byte[]) data;
        ensure(pos + d.length);
        System.arraycopy(d, 0, bytes, pos, d.length);
        length = Math.max(length, pos + d.length);
      } else {
        String d = (String) data;
        int n = d.codePointCount(0, d.length());
        int from = at(pos);
        int to = pos + n >= length ? text.length() : at(pos + n);
        text.replace(from, to, d);
        supplementary |= n != d.length();
        length = Math.max(length, pos + n);
      }
    }

    private void ensure(int size) {
      if (size > bytes.length) {
        bytes = Arrays.copyOf(bytes, Math.max(size, bytes.length * 2));
      }
    }

    /** The complete value, or null when it is unavailable. */
    Object value() {
      if (lost || !known) {
        return null;
      }
      return binary ? Arrays.copyOf(bytes, length) : text.toString();
    }
  }
}
