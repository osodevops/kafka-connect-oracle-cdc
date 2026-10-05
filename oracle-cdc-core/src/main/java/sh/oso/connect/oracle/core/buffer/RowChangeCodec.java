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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;

/**
 * Binary form of a {@link RowChange} for the spill store (CORE-TX-3). Every Java type the decoder
 * produces has a tag; a value of any other type is a programming error and is refused rather than
 * stringified, because the spilled copy must decode to exactly what was buffered.
 */
final class RowChangeCodec {

  private static final byte T_NULL = 0;
  private static final byte T_STRING = 1;
  private static final byte T_DECIMAL = 2;
  private static final byte T_FLOAT = 3;
  private static final byte T_DOUBLE = 4;
  private static final byte T_BYTES = 5;
  private static final byte T_LOCAL_DATE_TIME = 6;
  private static final byte T_OFFSET_DATE_TIME = 7;
  private static final byte T_INSTANT = 8;
  private static final byte T_DURATION = 9;
  private static final byte T_PERIOD = 10;
  private static final byte T_LONG = 11;
  private static final byte T_INT = 12;
  private static final byte T_BOOLEAN = 13;
  private static final byte T_LOCAL_DATE = 14;
  private static final byte T_BIG_INTEGER = 15;
  private static final byte T_SHORT = 16;
  private static final byte T_BYTE = 17;

  private RowChangeCodec() {}

  static byte[] encode(RowChange c) {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream(256);
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      writeString(out, c.table().pdb());
      writeString(out, c.table().schema());
      writeString(out, c.table().table());
      out.writeByte(c.op().ordinal());
      writeImage(out, c.before());
      writeImage(out, c.after());
      out.writeBoolean(c.partial());
      writeString(out, c.rowId());
      writeId(out, c.id());
      out.writeInt(c.tx().srcConId());
      out.writeLong(c.tx().xid().usn());
      out.writeLong(c.tx().xid().slot());
      out.writeLong(c.tx().xid().sqn());
      if (c.timestamp() == null) {
        out.writeBoolean(false);
      } else {
        out.writeBoolean(true);
        out.writeLong(c.timestamp().getEpochSecond());
        out.writeInt(c.timestamp().getNano());
      }
    } catch (IOException e) {
      throw new IllegalStateException("in-memory encode failed", e);
    }
    return bytes.toByteArray();
  }

  static RowChange decode(byte[] bytes) throws IOException {
    try (DataInputStream in = new DataInputStream(new java.io.ByteArrayInputStream(bytes))) {
      TableId table = new TableId(readString(in), readString(in), readString(in));
      Operation op = Operation.values()[in.readUnsignedByte()];
      Map<String, Object> before = readImage(in);
      Map<String, Object> after = readImage(in);
      boolean partial = in.readBoolean();
      String rowId = readString(in);
      RedoRecordId id = readId(in);
      TxKey tx = new TxKey(in.readInt(), new Xid(in.readLong(), in.readLong(), in.readLong()));
      Instant ts = null;
      if (in.readBoolean()) {
        ts = Instant.ofEpochSecond(in.readLong(), in.readInt());
      }
      return new RowChange(table, op, before, after, partial, rowId, id, tx, ts);
    }
  }

  private static void writeId(DataOutputStream out, RedoRecordId id) throws IOException {
    if (id == null) {
      out.writeBoolean(false);
      return;
    }
    out.writeBoolean(true);
    out.writeLong(id.scn());
    writeString(out, id.rsId());
    out.writeLong(id.ssn());
  }

  private static RedoRecordId readId(DataInputStream in) throws IOException {
    if (!in.readBoolean()) {
      return null;
    }
    return new RedoRecordId(in.readLong(), readString(in), in.readLong());
  }

  private static void writeImage(DataOutputStream out, Map<String, Object> image)
      throws IOException {
    if (image == null) {
      out.writeInt(-1);
      return;
    }
    out.writeInt(image.size());
    for (Map.Entry<String, Object> e : image.entrySet()) {
      writeString(out, e.getKey());
      writeValue(out, e.getValue());
    }
  }

  private static Map<String, Object> readImage(DataInputStream in) throws IOException {
    int n = in.readInt();
    if (n < 0) {
      return null;
    }
    Map<String, Object> m = new LinkedHashMap<>();
    for (int i = 0; i < n; i++) {
      String k = readString(in);
      m.put(k, readValue(in));
    }
    return m;
  }

  static void writeValue(DataOutputStream out, Object v) throws IOException {
    if (v == null) {
      out.writeByte(T_NULL);
    } else if (v instanceof String s) {
      out.writeByte(T_STRING);
      writeString(out, s);
    } else if (v instanceof BigDecimal d) {
      out.writeByte(T_DECIMAL);
      writeString(out, d.toString());
    } else if (v instanceof Float f) {
      out.writeByte(T_FLOAT);
      out.writeFloat(f);
    } else if (v instanceof Double d) {
      out.writeByte(T_DOUBLE);
      out.writeDouble(d);
    } else if (v instanceof byte[] b) {
      out.writeByte(T_BYTES);
      out.writeInt(b.length);
      out.write(b);
    } else if (v instanceof LocalDateTime t) {
      out.writeByte(T_LOCAL_DATE_TIME);
      out.writeLong(t.toEpochSecond(ZoneOffset.UTC));
      out.writeInt(t.getNano());
    } else if (v instanceof OffsetDateTime t) {
      out.writeByte(T_OFFSET_DATE_TIME);
      out.writeLong(t.toEpochSecond());
      out.writeInt(t.getNano());
      out.writeInt(t.getOffset().getTotalSeconds());
    } else if (v instanceof Instant t) {
      out.writeByte(T_INSTANT);
      out.writeLong(t.getEpochSecond());
      out.writeInt(t.getNano());
    } else if (v instanceof Duration d) {
      out.writeByte(T_DURATION);
      out.writeLong(d.getSeconds());
      out.writeInt(d.getNano());
    } else if (v instanceof Period p) {
      out.writeByte(T_PERIOD);
      out.writeInt(p.getYears());
      out.writeInt(p.getMonths());
      out.writeInt(p.getDays());
    } else if (v instanceof Long l) {
      out.writeByte(T_LONG);
      out.writeLong(l);
    } else if (v instanceof Integer i) {
      out.writeByte(T_INT);
      out.writeInt(i);
    } else if (v instanceof Boolean b) {
      out.writeByte(T_BOOLEAN);
      out.writeBoolean(b);
    } else if (v instanceof LocalDate d) {
      out.writeByte(T_LOCAL_DATE);
      out.writeLong(d.toEpochDay());
    } else if (v instanceof BigInteger i) {
      out.writeByte(T_BIG_INTEGER);
      byte[] b = i.toByteArray();
      out.writeInt(b.length);
      out.write(b);
    } else if (v instanceof Short s) {
      out.writeByte(T_SHORT);
      out.writeShort(s);
    } else if (v instanceof Byte b) {
      out.writeByte(T_BYTE);
      out.writeByte(b);
    } else {
      throw new IllegalArgumentException(
          "cannot spill a value of type " + v.getClass().getName() + "; add it to RowChangeCodec");
    }
  }

  static Object readValue(DataInputStream in) throws IOException {
    byte tag = in.readByte();
    switch (tag) {
      case T_NULL:
        return null;
      case T_STRING:
        return readString(in);
      case T_DECIMAL:
        return new BigDecimal(readString(in));
      case T_FLOAT:
        return in.readFloat();
      case T_DOUBLE:
        return in.readDouble();
      case T_BYTES:
        return readBytes(in);
      case T_LOCAL_DATE_TIME:
        return LocalDateTime.ofEpochSecond(in.readLong(), in.readInt(), ZoneOffset.UTC);
      case T_OFFSET_DATE_TIME:
        {
          long s = in.readLong();
          int n = in.readInt();
          ZoneOffset off = ZoneOffset.ofTotalSeconds(in.readInt());
          return OffsetDateTime.ofInstant(Instant.ofEpochSecond(s, n), off);
        }
      case T_INSTANT:
        return Instant.ofEpochSecond(in.readLong(), in.readInt());
      case T_DURATION:
        return Duration.ofSeconds(in.readLong(), in.readInt());
      case T_PERIOD:
        return Period.of(in.readInt(), in.readInt(), in.readInt());
      case T_LONG:
        return in.readLong();
      case T_INT:
        return in.readInt();
      case T_BOOLEAN:
        return in.readBoolean();
      case T_LOCAL_DATE:
        return LocalDate.ofEpochDay(in.readLong());
      case T_BIG_INTEGER:
        return new BigInteger(readBytes(in));
      case T_SHORT:
        return in.readShort();
      case T_BYTE:
        return in.readByte();
      default:
        throw new IOException("unknown value tag " + tag);
    }
  }

  private static byte[] readBytes(DataInputStream in) throws IOException {
    int n = in.readInt();
    if (n < 0 || n > 1 << 30) {
      throw new IOException("bad length " + n);
    }
    byte[] b = new byte[n];
    in.readFully(b);
    return b;
  }

  private static void writeString(DataOutputStream out, String s) throws IOException {
    if (s == null) {
      out.writeInt(-1);
      return;
    }
    byte[] b = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    out.writeInt(b.length);
    out.write(b);
  }

  private static String readString(DataInputStream in) throws IOException {
    int n = in.readInt();
    if (n < 0) {
      return null;
    }
    if (n > 1 << 30) {
      throw new IOException("bad length " + n);
    }
    byte[] b = new byte[n];
    in.readFully(b);
    return new String(b, java.nio.charset.StandardCharsets.UTF_8);
  }
}
