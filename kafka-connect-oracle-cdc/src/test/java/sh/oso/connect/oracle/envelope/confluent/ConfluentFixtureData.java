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
package sh.oso.connect.oracle.envelope.confluent;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.KeySource;
import sh.oso.connect.oracle.core.schema.OracleType;
import sh.oso.connect.oracle.core.schema.TableSchema;

/** The deterministic input of ConfluentFormatRefQualIT, independent of the recorded output. */
final class ConfluentFixtureData {
  static final List<String> TABLES =
      List.of("TYPES", "EXOTIC", "COMPOSITE", "NOPK", "LOBS", "NUMS", "LOBS2");

  record Input(Operation op, Map<String, Object> before, Map<String, Object> after, boolean ddl) {}

  record Data(TableSchema schema, List<Map<String, Object>> snapshots, List<Input> changes) {}

  static Data data(String name) {
    List<ColumnSpec> columns = new ArrayList<>();
    List<String> keys =
        name.equals("NOPK")
            ? List.of()
            : name.equals("COMPOSITE") || name.equals("LOBS2") ? List.of("A", "B") : List.of("ID");
    List<Map<String, Object>> seeds = new ArrayList<>();
    List<Input> changes = new ArrayList<>();
    switch (name) {
      case "TYPES" -> {
        number(columns, "ID", 10, 0, false);
        number(columns, "N_PLAIN", -1, -1, true);
        number(columns, "N_20_0", 20, 0, true);
        number(columns, "N_10_2", 10, 2, true);
        number(columns, "N_5", 5, 0, true);
        column(columns, "F_FLOAT", OracleType.FLOAT, true);
        column(columns, "C_CHAR", OracleType.CHAR, true);
        column(columns, "C_VC", OracleType.VARCHAR2, true);
        column(columns, "C_NCHAR", OracleType.NCHAR, true);
        column(columns, "C_NVC", OracleType.NVARCHAR2, true);
        column(columns, "D_DATE", OracleType.DATE, true);
        column(columns, "T_TS0", OracleType.TIMESTAMP, true);
        column(columns, "T_TS3", OracleType.TIMESTAMP, true);
        column(columns, "T_TS6", OracleType.TIMESTAMP, true);
        column(columns, "T_TZ", OracleType.TIMESTAMP_TZ, true);
        column(columns, "T_LTZ", OracleType.TIMESTAMP_LTZ, true);
        column(columns, "R_RAW", OracleType.RAW, true);
        seeds.add(types(1, "one"));
        seeds.add(types(2, null));
        seeds.add(row("ID", decimal("3")));
        Map<String, Object> ten = types(10, "ten");
        insert(changes, ten);
        Map<String, Object> upd = replace(ten, "C_VC", "upd");
        update(changes, ten, upd);
        Map<String, Object> nullNumber = replace(upd, "N_10_2", null);
        update(changes, upd, nullNumber);
        Map<String, Object> eleven = types(11, "eleven");
        Map<String, Object> twelve = types(12, "twelve");
        insert(changes, eleven);
        insert(changes, twelve);
        delete(changes, eleven);
        update(changes, twelve, replace(twelve, "ID", decimal("20")));
        changes.add(
            new Input(
                Operation.INSERT,
                null,
                row("ID", decimal("30"), "C_VC", "after ddl", "EXTRA", "x"),
                true));
        changes.add(
            new Input(Operation.INSERT, null, row("ID", decimal("99"), "C_VC", "last"), true));
      }
      case "EXOTIC" -> {
        number(columns, "ID", 10, 0, false);
        column(columns, "F_BINF", OracleType.BINARY_FLOAT, true);
        column(columns, "F_BIND", OracleType.BINARY_DOUBLE, true);
        column(columns, "I_YM", OracleType.INTERVAL_YM, true);
        column(columns, "I_DS", OracleType.INTERVAL_DS, true);
        column(columns, "T_TS9", OracleType.TIMESTAMP, true);
        number(columns, "N_NEG", 5, -2, true);
        seeds.add(exotic(1));
        seeds.add(row("ID", decimal("2")));
        insert(changes, exotic(10));
      }
      case "COMPOSITE" -> {
        number(columns, "A", 5, 0, false);
        column(columns, "B", OracleType.VARCHAR2, false);
        column(columns, "V", OracleType.VARCHAR2, true);
        seeds.add(row("A", decimal("0"), "B", "s", "V", "seed"));
        Map<String, Object> first = row("A", decimal("1"), "B", "a", "V", "x");
        insert(changes, first);
        Map<String, Object> upd = replace(first, "V", "y");
        update(changes, first, upd);
        update(changes, upd, replace(upd, "B", "b"));
      }
      case "NOPK" -> {
        number(columns, "X", 5, 0, true);
        column(columns, "Y", OracleType.VARCHAR2, true);
        seeds.add(row("X", decimal("0"), "Y", "seed"));
        Map<String, Object> first = row("X", decimal("1"), "Y", "a");
        insert(changes, first);
        Map<String, Object> upd = replace(first, "Y", "b");
        update(changes, first, upd);
        delete(changes, upd);
        insert(changes, row("X", decimal("2"), "Y", "post-trunc"));
      }
      case "LOBS" -> {
        number(columns, "ID", 10, 0, false);
        column(columns, "V", OracleType.VARCHAR2, true);
        column(columns, "C", OracleType.CLOB, true);
        column(columns, "N", OracleType.NCLOB, true);
        column(columns, "B", OracleType.BLOB, true);
        seeds.add(row("ID", decimal("0"), "V", "seed"));
        Map<String, Object> first = row("ID", decimal("1"), "V", "v1");
        insert(changes, first);
        Map<String, Object> upd = replace(first, "V", "v2");
        update(changes, first, upd);
        delete(changes, upd);
      }
      case "LOBS2" -> {
        number(columns, "A", 5, 0, false);
        column(columns, "B", OracleType.VARCHAR2, false);
        column(columns, "C", OracleType.CLOB, true);
        seeds.add(row("A", decimal("0"), "B", "s"));
        Map<String, Object> first = row("A", decimal("1"), "B", "a");
        insert(changes, first);
        Map<String, Object> upd = replace(first, "B", "b");
        update(changes, first, upd);
        delete(changes, upd);
      }
      case "NUMS" -> {
        number(columns, "ID", 10, 0, false);
        for (int width : List.of(2, 3, 4, 9, 18, 19)) number(columns, "N" + width, width, 0, true);
        number(columns, "N3M2", 3, -2, true);
        number(columns, "N17M2", 17, -2, true);
        number(columns, "N15_2", 15, 2, true);
        number(columns, "N16_2", 16, 2, true);
        number(columns, "N38_10", 38, 10, true);
        number(columns, "N_ROUND", -1, -1, true);
        column(columns, "NN", OracleType.VARCHAR2, false);
        seeds.add(nums(1, "0.125"));
        seeds.add(nums(2, "0.135"));
        seeds.add(nums(3, "-0.125"));
        insert(changes, nums(10, "0.12"));
      }
      default -> throw new IllegalArgumentException(name);
    }
    TableSchema schema =
        new TableSchema(
            new TableId("FREEPDB1", "SCHEMA", name),
            columns,
            keys,
            keys.isEmpty() ? KeySource.NONE : KeySource.PRIMARY_KEY,
            true,
            false);
    return new Data(schema, seeds, changes);
  }

  static TableSchema afterDdl(TableSchema original) {
    List<ColumnSpec> columns = new ArrayList<>(original.columns());
    column(columns, "EXTRA", OracleType.VARCHAR2, true);
    return new TableSchema(
            original.table(), columns, original.keyColumns(), original.keySource(), true, false)
        .withVersion(2, 500);
  }

  static Map<String, Object> types(int id, String vc) {
    return row(
        "ID",
        decimal(Integer.toString(id)),
        "N_PLAIN",
        decimal("12345.678901234567"),
        "N_20_0",
        decimal("12345678901234567890"),
        "N_10_2",
        decimal("-1234.56"),
        "N_5",
        decimal("42"),
        "F_FLOAT",
        decimal("3.142"),
        "C_CHAR",
        "ab   ",
        "C_VC",
        vc,
        "C_NCHAR",
        "xy   ",
        "C_NVC",
        "nvc-" + id,
        "D_DATE",
        LocalDateTime.parse("2024-03-01T10:11:12"),
        "T_TS0",
        LocalDateTime.parse("2024-03-01T10:11:12"),
        "T_TS3",
        LocalDateTime.parse("2024-03-01T10:11:12.123"),
        "T_TS6",
        LocalDateTime.parse("2024-03-01T10:11:12.123456"),
        "T_TZ",
        OffsetDateTime.parse("2024-03-01T10:11:12.123456+02:00"),
        "T_LTZ",
        Instant.parse("2024-03-01T10:11:12.123456Z"),
        "R_RAW",
        new byte[] {0, (byte) 255, 16});
  }

  static Map<String, Object> exotic(int id) {
    return row(
        "ID",
        decimal(Integer.toString(id)),
        "T_TS9",
        LocalDateTime.parse("1969-12-31T23:59:59.123456789"),
        "N_NEG",
        decimal("12300"));
  }

  static Map<String, Object> nums(int id, String rounding) {
    return row(
        "ID",
        decimal(Integer.toString(id)),
        "N2",
        decimal("99"),
        "N3",
        decimal("999"),
        "N4",
        decimal("9999"),
        "N9",
        decimal("999999999"),
        "N18",
        decimal("999999999999999999"),
        "N19",
        decimal("9999999999999999999"),
        "N3M2",
        decimal("12300"),
        "N17M2",
        decimal("1234567890123456700"),
        "N15_2",
        decimal("9999999999999.99"),
        "N16_2",
        decimal("99999999999999.99"),
        "N38_10",
        decimal("1234567890123456789012345678.1234567890"),
        "N_ROUND",
        decimal(rounding),
        "NN",
        "required");
  }

  static Map<String, Object> row(Object... entries) {
    Map<String, Object> out = new LinkedHashMap<>();
    for (int i = 0; i < entries.length; i += 2) out.put((String) entries[i], entries[i + 1]);
    return out;
  }

  static BigDecimal decimal(String value) {
    return new BigDecimal(value);
  }

  static Map<String, Object> replace(Map<String, Object> row, String name, Object value) {
    Map<String, Object> out = new LinkedHashMap<>(row);
    out.put(name, value);
    return out;
  }

  static void number(
      List<ColumnSpec> columns, String name, int precision, int scale, boolean nullable) {
    columns.add(
        new ColumnSpec(
            name, columns.size() + 1, OracleType.NUMBER, "NUMBER", 0, precision, scale, nullable));
  }

  static void column(List<ColumnSpec> columns, String name, OracleType type, boolean nullable) {
    columns.add(new ColumnSpec(name, columns.size() + 1, type, type.name(), 40, -1, -1, nullable));
  }

  static void insert(List<Input> changes, Map<String, Object> row) {
    changes.add(new Input(Operation.INSERT, null, row, false));
  }

  static void update(List<Input> changes, Map<String, Object> before, Map<String, Object> after) {
    changes.add(new Input(Operation.UPDATE, before, after, false));
  }

  static void delete(List<Input> changes, Map<String, Object> row) {
    changes.add(new Input(Operation.DELETE, row, null, false));
  }
}
