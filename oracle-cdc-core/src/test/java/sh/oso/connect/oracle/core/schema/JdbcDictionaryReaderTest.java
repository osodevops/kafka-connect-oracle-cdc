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
package sh.oso.connect.oracle.core.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.model.TableId;

/**
 * ADR-0016 amendment: the read at start ({@link JdbcDictionaryReader#readAll}) gives, table for
 * table, what the per-table reads give, with a bounded number of statements per container and each
 * chunk's last DDL times read after its layouts. The connection is a scripted fake that serves the
 * dictionary views from rows and binds; the SQL itself is proven against Oracle Database Free by
 * the engine and connector suites.
 */
class JdbcDictionaryReaderTest {

  static final TableId A = new TableId("FREEPDB1", "APP", "A");
  static final TableId B = new TableId("FREEPDB1", "APP", "B");
  static final TableId C = new TableId("FREEPDB1", "SALES", "C");
  static final TableId MISSING = new TableId("FREEPDB1", "APP", "MISSING");
  static final TableId A2 = new TableId("FREEPDB2", "APP", "A");
  static final Instant DDL = Instant.parse("2026-10-06T08:00:00Z");

  /** Rows of the fake views; the first three cells are container, owner and table. */
  final List<Object[]> tabCols = new ArrayList<>();

  final List<Object[]> logGroups = new ArrayList<>();
  final List<Object[]> primaryKeys = new ArrayList<>();
  final List<Object[]> uniques = new ArrayList<>();
  final List<Object[]> objects = new ArrayList<>();
  final Map<String, Integer> containers = Map.of("FREEPDB1", 3, "FREEPDB2", 4);
  final List<String> statements = new ArrayList<>();

  JdbcDictionaryReaderTest() {
    // A: key, nullable text, a number without precision; all-column logging; a two-column unique
    // index besides the primary key
    tabCols.add(new Object[] {3, "APP", "A", "ID", 1, "NUMBER", 22, 9, 0, "N"});
    tabCols.add(new Object[] {3, "APP", "A", "NAME", 2, "VARCHAR2", 40, null, null, "Y"});
    tabCols.add(new Object[] {3, "APP", "A", "AMOUNT", 3, "NUMBER", 22, null, null, "Y"});
    logGroups.add(new Object[] {3, "APP", "A", "ALL COLUMN LOGGING"});
    primaryKeys.add(new Object[] {3, "APP", "A", "ID", 1});
    uniques.add(new Object[] {3, "APP", "A", "A_UK", "NAME", 1});
    uniques.add(new Object[] {3, "APP", "A", "A_UK", "AMOUNT", 2});
    objects.add(new Object[] {3, "APP", "A", DDL});
    // B: no key, primary key logging, two unique indexes
    tabCols.add(new Object[] {3, "APP", "B", "CODE", 1, "CHAR", 4, null, null, "N"});
    tabCols.add(new Object[] {3, "APP", "B", "SEQ", 2, "NUMBER", 22, 5, 0, "N"});
    logGroups.add(new Object[] {3, "APP", "B", "PRIMARY KEY LOGGING"});
    uniques.add(new Object[] {3, "APP", "B", "B_U1", "CODE", 1});
    uniques.add(new Object[] {3, "APP", "B", "B_U2", "SEQ", 1});
    objects.add(new Object[] {3, "APP", "B", DDL.plusSeconds(60)});
    // C: another owner; a timestamp column; no DDL time
    tabCols.add(new Object[] {3, "SALES", "C", "ID", 1, "NUMBER", 22, 9, 0, "N"});
    tabCols.add(new Object[] {3, "SALES", "C", "AT", 2, "TIMESTAMP(6)", 11, null, 6, "Y"});
    primaryKeys.add(new Object[] {3, "SALES", "C", "ID", 1});
    // A in another container, with another layout
    tabCols.add(new Object[] {4, "APP", "A", "ID", 1, "NUMBER", 22, 9, 0, "N"});
    primaryKeys.add(new Object[] {4, "APP", "A", "ID", 1});
    objects.add(new Object[] {4, "APP", "A", DDL.minusSeconds(60)});
    // a table of a captured owner the read did not ask for
    tabCols.add(new Object[] {3, "APP", "OTHER", "X", 1, "NUMBER", 22, null, null, "Y"});
  }

  @Test
  void theReadAtStartGivesWhatThePerTableReadsGiveInABoundedNumberOfStatements() throws Exception {
    JdbcDictionaryReader reader = new JdbcDictionaryReader(this::connection, 2);
    List<TableId> tables = List.of(A, B, C, MISSING, A2);

    Map<TableId, DictionaryReader.Layout> all = reader.readAll(tables);
    List<String> bulk = List.copyOf(statements);

    assertThat(all).containsOnlyKeys(A, B, C, A2);
    for (TableId t : List.of(A, B, C, A2)) {
      DictionaryReader.Layout one =
          new DictionaryReader.Layout(
              reader.read(t).orElseThrow(),
              reader.keyCandidates(t),
              reader.lastDdlTime(t).orElse(null));
      assertThat(all.get(t)).as(t.fqn()).isEqualTo(one);
    }
    assertThat(reader.read(MISSING)).isEmpty();
    assertThat(all.get(A).columns().supplementalAllColumns()).isTrue();
    assertThat(all.get(B).columns().supplementalPrimaryKey()).isTrue();
    assertThat(all.get(B).keys().notNullUniqueIndexes())
        .containsExactly(List.of("CODE"), List.of("SEQ"));
    assertThat(all.get(A).keys().notNullUniqueIndexes()).containsExactly(List.of("NAME", "AMOUNT"));
    assertThat(all.get(C).lastDdlTime()).isNull();
    assertThat(all.get(A2).columns().columns()).hasSize(1);

    // FREEPDB1: its container id, then two chunks of two tables, five statements each, the last
    // DDL times last; FREEPDB2: its id and one chunk
    String chunk = "tab_cols log_groups constraints indexes objects";
    assertThat(String.join(" ", bulk))
        .isEqualTo(String.join(" ", "containers", chunk, chunk, "containers", chunk));
  }

  /** The dictionary view a statement reads, by its FROM clause. */
  static String view(String sql) {
    for (String v : List.of("containers", "tab_cols", "log_groups", "constraints", "indexes")) {
      if (sql.contains("FROM v$" + v) || sql.contains("FROM cdb_" + v)) {
        return v;
      }
    }
    return sql.contains("FROM cdb_objects") ? "objects" : sql;
  }

  Connection connection() {
    return (Connection)
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {Connection.class},
            (p, m, a) -> m.getName().equals("prepareStatement") ? statement((String) a[0]) : null);
  }

  PreparedStatement statement(String sql) {
    statements.add(view(sql));
    Map<Integer, Object> binds = new HashMap<>();
    return (PreparedStatement)
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {PreparedStatement.class},
            (p, m, a) ->
                switch (m.getName()) {
                  case "setInt", "setString", "setLong" -> {
                    binds.put((Integer) a[0], a[1]);
                    yield null;
                  }
                  case "executeQuery" -> results(rows(sql, binds));
                  default -> null;
                });
  }

  /** What the view returns for these binds, in the statement's order and shape. */
  List<Object[]> rows(String sql, Map<Integer, Object> binds) {
    String view = view(sql);
    if (view.equals("containers")) {
      return List.<Object[]>of(
          new Object[] {
            containers.get(((String) binds.get(1)).toUpperCase(java.util.Locale.ROOT))
          });
    }
    int con = (Integer) binds.get(1);
    boolean bulk = sql.contains(") IN (");
    List<String> wanted = new ArrayList<>(); // owner and table pairs
    for (int i = 2; binds.containsKey(i) && (bulk || i <= 3); i++) {
      wanted.add((String) binds.get(i));
    }
    List<Object[]> source =
        switch (view) {
          case "tab_cols" -> tabCols;
          case "log_groups" -> logGroups;
          case "constraints" -> primaryKeys;
          case "indexes" -> uniques;
          default -> objects;
        };
    List<Object[]> matched = new ArrayList<>();
    for (Object[] r : source) {
      if ((Integer) r[0] != con) {
        continue;
      }
      for (int i = 0; i < wanted.size(); i += 2) {
        if (wanted.get(i).equals(r[1]) && wanted.get(i + 1).equals(r[2])) {
          matched.add(r);
        }
      }
    }
    // ORDER BY owner, table, then index name and column position, or column id
    matched.sort(
        Comparator.comparing((Object[] r) -> (String) r[1])
            .thenComparing(r -> (String) r[2])
            .thenComparing(r -> view.equals("indexes") ? (String) r[3] : "")
            .thenComparing(
                r ->
                    switch (view) {
                      case "tab_cols" -> (Integer) r[4];
                      case "constraints" -> (Integer) r[4];
                      case "indexes" -> (Integer) r[5];
                      default -> 0;
                    }));
    List<Object[]> out = new ArrayList<>();
    if (view.equals("log_groups") && !bulk) {
      long n = matched.stream().filter(r -> r[3].equals(binds.get(4))).count();
      return List.<Object[]>of(new Object[] {(int) n});
    }
    for (Object[] r : matched) {
      Object[] cells =
          switch (view) {
            case "tab_cols" -> slice(r, 3, 10);
            case "log_groups" -> slice(r, 3, 4);
            case "constraints" -> slice(r, 3, 4);
            case "indexes" -> slice(r, 3, 5);
            default -> new Object[] {Timestamp.from((Instant) r[3])};
          };
      out.add(bulk ? prefixed(r[1], r[2], cells) : cells);
    }
    return out;
  }

  static Object[] slice(Object[] r, int from, int to) {
    return java.util.Arrays.copyOfRange(r, from, to);
  }

  static Object[] prefixed(Object owner, Object table, Object[] cells) {
    Object[] out = new Object[cells.length + 2];
    out[0] = owner;
    out[1] = table;
    System.arraycopy(cells, 0, out, 2, cells.length);
    return out;
  }

  ResultSet results(List<Object[]> rows) {
    int[] at = {-1};
    return (ResultSet)
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {ResultSet.class},
            (p, m, a) -> {
              switch (m.getName()) {
                case "next":
                  return ++at[0] < rows.size();
                case "close":
                  return null;
                default:
                  break;
              }
              Object v = rows.get(at[0])[(Integer) a[0] - 1];
              return switch (m.getName()) {
                case "getInt" -> v == null ? 0 : ((Number) v).intValue();
                case "getLong" -> v == null ? 0L : ((Number) v).longValue();
                case "getString" -> v == null ? null : v.toString();
                case "getTimestamp" -> (Timestamp) v;
                default -> v; // getObject
              };
            });
  }
}
