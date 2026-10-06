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
package sh.oso.connect.oracle.bench.check;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Assertion one of ADR-0012: the materialised rows of each table equal {@code SELECT * ... AS OF
 * SCN} at the check SCN, compared column by column through the {@link Normaliser}. ORA-01555,
 * ORA-08181 and ORA-01466 mark the run inconclusive rather than failed.
 */
public final class StateChecker {

  /** Differences for one table, with the first examples. */
  public static final class TableDiff {
    public long databaseRows;
    public long kafkaRows;
    public final List<String> missingInKafka = new ArrayList<>();
    public final List<String> extraInKafka = new ArrayList<>();
    public final List<String> mismatched = new ArrayList<>();
    public long missingCount;
    public long extraCount;
    public long mismatchCount;

    public boolean clean() {
      return missingCount == 0 && extraCount == 0 && mismatchCount == 0;
    }
  }

  private final Connection c;
  private final String owner;

  public StateChecker(Connection connection, String owner) {
    this.c = connection;
    this.owner = owner;
  }

  /** Compares one table; {@code kafkaRows} are keyed by the record key JSON. */
  public TableDiff compare(String table, Map<String, JsonNode> kafkaRows, long checkScn)
      throws SQLException {
    TableDiff diff = new TableDiff();
    List<String> keyColumns = primaryKey(table);
    String sql = "SELECT * FROM " + owner + "." + table + " AS OF SCN " + checkScn;
    Map<String, Map<String, String>> db = new TreeMap<>();
    List<Normaliser.Column> columns;
    try (PreparedStatement ps = c.prepareStatement(sql);
        ResultSet rs = ps.executeQuery()) {
      ResultSetMetaData md = rs.getMetaData();
      columns = new ArrayList<>();
      for (int i = 1; i <= md.getColumnCount(); i++) {
        columns.add(
            new Normaliser.Column(
                md.getColumnName(i),
                md.getColumnType(i),
                md.getColumnTypeName(i),
                md.getPrecision(i),
                md.getScale(i)));
      }
      while (rs.next()) {
        Map<String, String> row = new LinkedHashMap<>();
        for (int i = 1; i <= columns.size(); i++) {
          row.put(columns.get(i - 1).name(), Normaliser.fromDatabase(rs, i, columns.get(i - 1)));
        }
        db.put(keyOf(row, keyColumns), row);
      }
    }
    diff.databaseRows = db.size();
    diff.kafkaRows = kafkaRows.size();
    Map<String, Map<String, String>> kafka = new TreeMap<>();
    for (JsonNode after : kafkaRows.values()) {
      Map<String, String> row = new LinkedHashMap<>();
      for (Normaliser.Column col : columns) {
        row.put(col.name(), Normaliser.fromRecord(after.get(col.name()), col));
      }
      kafka.put(keyOf(row, keyColumns), row);
    }
    for (Map.Entry<String, Map<String, String>> e : db.entrySet()) {
      Map<String, String> k = kafka.remove(e.getKey());
      if (k == null) {
        diff.missingCount++;
        if (diff.missingInKafka.size() < 5) {
          diff.missingInKafka.add(e.getKey());
        }
        continue;
      }
      for (Normaliser.Column col : columns) {
        String a = e.getValue().get(col.name());
        String b = k.get(col.name());
        if (a == null ? b != null : !a.equals(b)) {
          diff.mismatchCount++;
          if (diff.mismatched.size() < 5) {
            diff.mismatched.add(e.getKey() + "." + col.name() + ": db=" + a + " kafka=" + b);
          }
        }
      }
    }
    for (String extra : kafka.keySet()) {
      diff.extraCount++;
      if (diff.extraInKafka.size() < 5) {
        diff.extraInKafka.add(extra);
      }
    }
    return diff;
  }

  List<String> primaryKey(String table) throws SQLException {
    List<String> cols = new ArrayList<>();
    DatabaseMetaData md = c.getMetaData();
    try (ResultSet rs = md.getPrimaryKeys(null, owner, table)) {
      Map<Integer, String> bySeq = new TreeMap<>();
      while (rs.next()) {
        bySeq.put(rs.getInt("KEY_SEQ"), rs.getString("COLUMN_NAME"));
      }
      cols.addAll(bySeq.values());
    }
    return cols;
  }

  static String keyOf(Map<String, String> row, List<String> keyColumns) {
    if (keyColumns.isEmpty()) {
      return row.toString(); // keyless: the whole row is the identity
    }
    StringBuilder sb = new StringBuilder();
    for (String k : keyColumns) {
      sb.append(k).append('=').append(row.get(k)).append(';');
    }
    return sb.toString();
  }

  /**
   * The {@code AS OF SCN} read cannot be answered: the undo is gone (ORA-01555, ORA-08181), or the
   * SCN is too close to or before a change of the table's definition (ORA-01466). The run is then
   * inconclusive; nothing is known about the records.
   */
  public static boolean isSnapshotTooOld(SQLException e) {
    return e.getErrorCode() == 1555 || e.getErrorCode() == 8181 || e.getErrorCode() == 1466;
  }
}
