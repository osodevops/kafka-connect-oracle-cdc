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
package sh.oso.connect.oracle.core.snapshot;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.HexFormat;
import java.util.Set;
import sh.oso.connect.oracle.core.schema.OracleType;

/**
 * Key values of chunk bounds as strings, so a bound can live in the offset's snapshot block and be
 * bound again after a restart. The prefix carries the binding: {@code n:} number, {@code s:} and
 * {@code N:} character and national character, {@code t:} DATE or TIMESTAMP, {@code x:} RAW, {@code
 * r:} ROWID.
 */
public final class BoundCodec {

  /** Key types whose ranges the connector can bind and compare in SQL. */
  static final Set<OracleType> RANGE_TYPES =
      Set.of(
          OracleType.NUMBER,
          OracleType.FLOAT,
          OracleType.VARCHAR2,
          OracleType.CHAR,
          OracleType.NVARCHAR2,
          OracleType.NCHAR,
          OracleType.DATE,
          OracleType.TIMESTAMP,
          OracleType.RAW);

  private BoundCodec() {}

  /** Reads key column {@code index} of the current row in the form {@link #encode} takes. */
  static String read(ResultSet rs, int index, OracleType type) throws SQLException {
    switch (type) {
      case NUMBER:
      case FLOAT:
        BigDecimal n = rs.getBigDecimal(index);
        return n == null ? null : "n:" + n.toPlainString();
      case VARCHAR2:
      case CHAR:
        String s = rs.getString(index);
        return s == null ? null : "s:" + s;
      case NVARCHAR2:
      case NCHAR:
        String ns = rs.getNString(index);
        return ns == null ? null : "N:" + ns;
      case DATE:
      case TIMESTAMP:
        Timestamp t = rs.getTimestamp(index);
        return t == null ? null : "t:" + t.toLocalDateTime();
      case RAW:
        byte[] b = rs.getBytes(index);
        return b == null ? null : "x:" + HexFormat.of().formatHex(b);
      default:
        throw new IllegalArgumentException("not a range key type: " + type);
    }
  }

  static String rowId(String rowId) {
    return "r:" + rowId;
  }

  /** Binds an encoded value to parameter {@code index}. */
  static void bind(PreparedStatement ps, int index, String encoded) throws SQLException {
    String v = encoded.substring(2);
    switch (encoded.charAt(0)) {
      case 'n' -> ps.setBigDecimal(index, new BigDecimal(v));
      case 's', 'r' -> ps.setString(index, v);
      case 'N' -> ps.setNString(index, v);
      case 't' -> ps.setTimestamp(index, Timestamp.valueOf(LocalDateTime.parse(v)));
      case 'x' -> ps.setBytes(index, HexFormat.of().parseHex(v));
      default -> throw new IllegalArgumentException("unknown bound encoding: " + encoded);
    }
  }
}
