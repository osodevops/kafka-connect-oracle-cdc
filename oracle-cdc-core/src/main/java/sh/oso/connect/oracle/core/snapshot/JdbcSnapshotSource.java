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
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import sh.oso.connect.oracle.core.decode.OracleTypeCodec;
import sh.oso.connect.oracle.core.decode.SqlLiteral;
import sh.oso.connect.oracle.core.errors.DecodeException;
import sh.oso.connect.oracle.core.errors.LobTooLargeException;
import sh.oso.connect.oracle.core.jdbc.ConnectionRole;
import sh.oso.connect.oracle.core.jdbc.SessionInitializer;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.KeySource;
import sh.oso.connect.oracle.core.schema.TableSchema;

/**
 * {@link SnapshotSource} over a connection of its own. Switches to the table's PDB (the capture
 * user holds SET CONTAINER, SELECT ANY TABLE and FLASHBACK ANY TABLE), sorts and compares in binary
 * so planned key ranges and range predicates agree, and selects temporal and interval columns as
 * text in the mining session's formats so {@link OracleTypeCodec} decodes them exactly as it
 * decodes redo. JDBC-bound; proven by the engine tier.
 */
public final class JdbcSnapshotSource implements SnapshotSource {

  private static final int DEFAULT_ROWS_PER_BLOCK = 50;

  private final Connection c;
  private final int fetchSize;
  private final boolean lobs;
  private final long lobMaxBytes;
  private final boolean lobOversizeFail;
  private String container;

  /**
   * {@code lobs}: read CLOB, NCLOB and BLOB columns (cdc.lob.mode other than skip); a value above
   * {@code lobMaxBytes} fails the snapshot when {@code lobOversizeFail}, else stays unavailable.
   */
  public JdbcSnapshotSource(
      Connection c, int fetchSize, boolean lobs, long lobMaxBytes, boolean lobOversizeFail)
      throws SQLException {
    this.c = c;
    this.fetchSize = Math.max(1, fetchSize);
    this.lobs = lobs;
    this.lobMaxBytes = lobMaxBytes;
    this.lobOversizeFail = lobOversizeFail;
    binarySort();
  }

  @Override
  public Kind kind(TableSchema schema) throws SQLException {
    boolean keyed =
        schema.keySource() != KeySource.ROWID
            && schema.keySource() != KeySource.NONE
            && !schema.keyColumns().isEmpty();
    if (keyed) {
      boolean rangeable = true;
      for (String k : schema.keyColumns()) {
        ColumnSpec col = schema.column(k);
        // a NULL key value matches no range predicate: such a key cannot partition the table
        rangeable &= col != null && !col.nullable() && BoundCodec.RANGE_TYPES.contains(col.type());
      }
      if (rangeable) {
        return Kind.KEY;
      }
    }
    TableId t = schema.table();
    use(t);
    try (PreparedStatement ps =
        c.prepareStatement("SELECT iot_type FROM dba_tables WHERE owner = ? AND table_name = ?")) {
      ps.setString(1, t.schema());
      ps.setString(2, t.table());
      try (ResultSet rs = ps.executeQuery()) {
        // ADR-0004: an index-organised table's ROWIDs are logical, so no ROWID ranges
        return rs.next() && rs.getString(1) != null ? Kind.ALL : Kind.ROWID;
      }
    }
  }

  @Override
  public List<ChunkRange> plan(
      TableSchema schema, Kind kind, List<String> lower, int count, int chunkRows)
      throws SQLException {
    use(schema.table());
    return switch (kind) {
      case ALL -> List.of(ChunkRange.ALL);
      case KEY -> planKeys(schema, lower, count, chunkRows);
      case ROWID -> planRowIds(schema, lower, count, chunkRows);
    };
  }

  private List<ChunkRange> planKeys(
      TableSchema schema, List<String> lower, int count, int chunkRows) throws SQLException {
    List<ChunkRange> out = new ArrayList<>();
    List<String> lo = lower;
    while (out.size() < count) {
      List<String> hi = nextKey(schema, lo, chunkRows);
      out.add(new ChunkRange(lo, hi));
      if (hi == null) {
        break;
      }
      lo = hi;
    }
    return out;
  }

  /** The key {@code rows} rows past {@code lower}: where the next chunk starts, or null. */
  private List<String> nextKey(TableSchema schema, List<String> lower, int rows)
      throws SQLException {
    List<String> keys = schema.keyColumns();
    List<String> params = new ArrayList<>();
    StringBuilder sql = new StringBuilder("SELECT ");
    for (int i = 0; i < keys.size(); i++) {
      sql.append(i == 0 ? "" : ", ").append(quote(keys.get(i)));
    }
    sql.append(" FROM ").append(name(schema.table()));
    if (lower != null) {
      sql.append(" WHERE ").append(bound(keys, lower, true, params));
    }
    sql.append(" ORDER BY ");
    for (int i = 0; i < keys.size(); i++) {
      sql.append(i == 0 ? "" : ", ").append(quote(keys.get(i)));
    }
    sql.append(" OFFSET ? ROWS FETCH NEXT 1 ROWS ONLY");
    try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
      int p = 1;
      for (String v : params) {
        BoundCodec.bind(ps, p++, v);
      }
      ps.setInt(p, rows);
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return null;
        }
        List<String> out = new ArrayList<>();
        for (int i = 0; i < keys.size(); i++) {
          out.add(BoundCodec.read(rs, i + 1, schema.column(keys.get(i)).type()));
        }
        return out;
      }
    }
  }

  /**
   * ADR-0004: ROWID ranges from the table's extents, cut where the blocks so far hold about {@code
   * chunkRows} rows at the table's average rows per block. ROWIDs order by data object, file and
   * block, so the ranges cover every ROWID, including extents added after planning.
   */
  private List<ChunkRange> planRowIds(
      TableSchema schema, List<String> lower, int count, int chunkRows) throws SQLException {
    TableId t = schema.table();
    long rowsPerBlock = DEFAULT_ROWS_PER_BLOCK;
    try (PreparedStatement ps =
        c.prepareStatement(
            "SELECT num_rows, blocks FROM dba_tables WHERE owner = ? AND table_name = ?")) {
      ps.setString(1, t.schema());
      ps.setString(2, t.table());
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next() && rs.getLong(1) > 0 && rs.getLong(2) > 0) {
          rowsPerBlock = Math.max(1, rs.getLong(1) / rs.getLong(2));
        }
      }
    }
    long targetBlocks = Math.max(1, chunkRows / rowsPerBlock);
    String extents =
        "SELECT ROWIDTOCHAR(rid), blocks FROM (SELECT DBMS_ROWID.ROWID_CREATE(1,"
            + " o.data_object_id, e.relative_fno, e.block_id, 0) rid, e.blocks FROM dba_extents e"
            + " JOIN dba_objects o ON o.owner = e.owner AND o.object_name = e.segment_name AND"
            + " NVL(o.subobject_name, '-') = NVL(e.partition_name, '-') AND o.object_type LIKE"
            + " 'TABLE%' WHERE e.owner = ? AND e.segment_name = ? AND e.segment_type LIKE 'TABLE%')"
            + (lower == null ? "" : " WHERE rid > CHARTOROWID(?)")
            + " ORDER BY rid";
    List<ChunkRange> out = new ArrayList<>();
    try (PreparedStatement ps = c.prepareStatement(extents)) {
      ps.setString(1, t.schema());
      ps.setString(2, t.table());
      if (lower != null) {
        BoundCodec.bind(ps, 3, lower.get(0));
      }
      List<String> lo = lower;
      long blocks = 0;
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          if (blocks >= targetBlocks) {
            List<String> hi = List.of(BoundCodec.rowId(rs.getString(1)));
            out.add(new ChunkRange(lo, hi));
            if (out.size() == count) {
              return out; // the next batch starts at hi
            }
            lo = hi;
            blocks = 0;
          }
          blocks += rs.getLong(2);
        }
      }
      out.add(new ChunkRange(lo, null)); // the rest of the table, however it has grown
    }
    return out;
  }

  @Override
  public long currentScn() throws SQLException {
    try (Statement st = c.createStatement();
        ResultSet rs = st.executeQuery("SELECT current_scn FROM v$database")) {
      rs.next();
      return rs.getLong(1);
    }
  }

  @Override
  public List<SnapshotRow> read(
      TableSchema schema, Kind kind, ChunkRange range, long scn, String where) throws SQLException {
    use(schema.table());
    boolean rowIds = schema.keySource() == KeySource.ROWID || schema.keyColumns().isEmpty();
    List<ColumnSpec> cols = new ArrayList<>();
    StringBuilder sql = new StringBuilder("SELECT ");
    sql.append(rowIds ? "ROWIDTOCHAR(ROWID)" : "NULL");
    for (ColumnSpec col : schema.columns()) {
      if (col.type().isLob() && !lobs) {
        continue; // cdc.lob.mode=skip: unavailable, as in the stream
      }
      sql.append(", ").append(expression(col));
      cols.add(col);
    }
    sql.append(" FROM ").append(name(schema.table())).append(" AS OF SCN ?");
    List<String> params = new ArrayList<>();
    List<String> predicates = new ArrayList<>();
    if (kind == Kind.KEY) {
      if (range.lower() != null) {
        predicates.add(bound(schema.keyColumns(), range.lower(), true, params));
      }
      if (range.upper() != null) {
        predicates.add(bound(schema.keyColumns(), range.upper(), false, params));
      }
    } else if (kind == Kind.ROWID) {
      if (range.lower() != null) {
        predicates.add("ROWID >= CHARTOROWID(?)");
        params.add(range.lower().get(0));
      }
      if (range.upper() != null) {
        predicates.add("ROWID < CHARTOROWID(?)");
        params.add(range.upper().get(0));
      }
    }
    if (where != null && !where.isBlank()) {
      predicates.add("(" + where + ")");
    }
    if (!predicates.isEmpty()) {
      sql.append(" WHERE ").append(String.join(" AND ", predicates));
    }
    List<SnapshotRow> out = new ArrayList<>();
    try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
      ps.setFetchSize(fetchSize);
      ps.setLong(1, scn);
      int p = 2;
      for (String v : params) {
        BoundCodec.bind(ps, p++, v);
      }
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          Map<String, Object> values = new LinkedHashMap<>();
          for (int i = 0; i < cols.size(); i++) {
            ColumnSpec col = cols.get(i);
            Object v = value(rs, i + 2, col);
            if (v != OVERSIZE) {
              values.put(col.name(), v);
            }
          }
          out.add(new SnapshotRow(values, rowIds ? rs.getString(1) : null));
        }
      }
    }
    return out;
  }

  /** A LOB above cdc.lob.max.bytes under the placeholder action: the column stays unavailable. */
  private static final Object OVERSIZE = new Object();

  private static String expression(ColumnSpec col) {
    String q = quote(col.name());
    return switch (col.type()) {
      case DATE, TIMESTAMP, TIMESTAMP_TZ, TIMESTAMP_LTZ, INTERVAL_YM, INTERVAL_DS ->
          "TO_CHAR(" + q + ")";
      case XMLTYPE -> "XMLSERIALIZE(CONTENT " + q + " AS CLOB)"; // text, as the stream has it
      case VARCHAR2,
          CHAR,
          NVARCHAR2,
          NCHAR,
          LONG,
          ROWID,
          UROWID,
          NUMBER,
          FLOAT,
          BINARY_FLOAT,
          BINARY_DOUBLE,
          RAW,
          LONG_RAW,
          CLOB,
          NCLOB,
          BLOB ->
          q;
      default ->
          throw new DecodeException(
              "Column "
                  + col.name()
                  + " has type "
                  + col.typeText()
                  + " which the snapshot reader does not support",
              "Exclude the table, or set cdc.on.decode.error=dlq to skip its snapshot with an ops"
                  + " event (DOC-5).");
    };
  }

  private Object value(ResultSet rs, int i, ColumnSpec col) throws SQLException {
    switch (col.type()) {
      case NVARCHAR2:
      case NCHAR:
        return rs.getNString(i);
      case NUMBER:
      case FLOAT:
        BigDecimal n = rs.getBigDecimal(i);
        // redo literals carry no negative scale; neither does a value read here
        return n == null || n.scale() >= 0 ? n : n.setScale(0);
      case BINARY_FLOAT:
        float f = rs.getFloat(i);
        return rs.wasNull() ? null : f;
      case BINARY_DOUBLE:
        double d = rs.getDouble(i);
        return rs.wasNull() ? null : d;
      case DATE:
        return literal(
            rs.getString(i), SqlLiteral.Kind.TO_DATE, SessionInitializer.DATE_FORMAT, col);
      case TIMESTAMP:
        return literal(
            rs.getString(i),
            SqlLiteral.Kind.TO_TIMESTAMP,
            SessionInitializer.TIMESTAMP_FORMAT,
            col);
      case TIMESTAMP_TZ:
      case TIMESTAMP_LTZ:
        return literal(
            rs.getString(i),
            SqlLiteral.Kind.TO_TIMESTAMP_TZ,
            SessionInitializer.TIMESTAMP_TZ_FORMAT,
            col);
      case INTERVAL_YM:
        return literal(rs.getString(i), SqlLiteral.Kind.TO_YMINTERVAL, null, col);
      case INTERVAL_DS:
        return literal(rs.getString(i), SqlLiteral.Kind.TO_DSINTERVAL, null, col);
      case RAW:
      case LONG_RAW:
        return rs.getBytes(i);
      case CLOB:
      case XMLTYPE:
        return lob(rs.getString(i), col);
      case NCLOB:
        return lob(rs.getNString(i), col);
      case BLOB:
        return lob(rs.getBytes(i), col);
      default:
        return rs.getString(i);
    }
  }

  private static Object literal(String text, SqlLiteral.Kind kind, String format, ColumnSpec col) {
    return text == null ? null : OracleTypeCodec.decode(col, new SqlLiteral(kind, text, format));
  }

  private Object lob(Object v, ColumnSpec col) {
    if (v == null) {
      return null;
    }
    long bytes =
        v instanceof byte[] b ? b.length : ((String) v).getBytes(StandardCharsets.UTF_8).length;
    if (bytes <= lobMaxBytes) {
      return v;
    }
    if (lobOversizeFail) {
      throw new LobTooLargeException(
          "Snapshot of column "
              + col.name()
              + " read "
              + bytes
              + " bytes, above cdc.lob.max.bytes="
              + lobMaxBytes
              + ".",
          "Raise cdc.lob.max.bytes, or set cdc.lob.oversize.action=placeholder to publish the"
              + " placeholder for such values.");
    }
    return OVERSIZE;
  }

  /**
   * Lexicographic comparison of the key tuple with a bound: at or after it for a lower bound,
   * before it for an upper bound. Oracle has no ordered row-value comparison, so it is spelled out.
   */
  static String bound(List<String> keys, List<String> values, boolean lower, List<String> params) {
    List<String> terms = new ArrayList<>();
    for (int i = 0; i < keys.size(); i++) {
      StringBuilder t = new StringBuilder("(");
      for (int j = 0; j < i; j++) {
        t.append(quote(keys.get(j))).append(" = ? AND ");
        params.add(values.get(j));
      }
      String op = lower ? (i == keys.size() - 1 ? ">=" : ">") : "<";
      t.append(quote(keys.get(i))).append(' ').append(op).append(" ?)");
      params.add(values.get(i));
      terms.add(t.toString());
    }
    return "(" + String.join(" OR ", terms) + ")";
  }

  private void use(TableId t) throws SQLException {
    String pdb = t.pdb();
    if (pdb != null && container == null) {
      container = currentContainer(c); // a PDB-local session is already there (ADR-0027)
    }
    if (pdb != null && !pdb.equals(container)) {
      try (Statement st = c.createStatement()) {
        st.execute("ALTER SESSION SET CONTAINER = " + quote(pdb));
      }
      // the formats and sort order belong to the session; set them again in the new container
      SessionInitializer.apply(c, ConnectionRole.SNAPSHOT);
      binarySort();
      container = pdb;
    }
  }

  /** The session's container name, upper case. */
  static String currentContainer(Connection c) throws SQLException {
    try (Statement st = c.createStatement();
        java.sql.ResultSet rs =
            st.executeQuery("SELECT SYS_CONTEXT('USERENV', 'CON_NAME') FROM dual")) {
      return rs.next() ? rs.getString(1) : null;
    }
  }

  private void binarySort() throws SQLException {
    try (Statement st = c.createStatement()) {
      st.execute("ALTER SESSION SET NLS_SORT = BINARY");
      st.execute("ALTER SESSION SET NLS_COMP = BINARY");
    }
  }

  private static String name(TableId t) {
    return quote(t.schema()) + "." + quote(t.table());
  }

  static String quote(String identifier) {
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }

  @Override
  public void close() throws SQLException {
    c.close();
  }
}
