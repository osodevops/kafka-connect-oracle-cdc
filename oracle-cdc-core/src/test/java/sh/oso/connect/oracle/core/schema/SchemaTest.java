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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.errors.DecodeException;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.testkit.InMemorySchemaStore;

class SchemaTest {

  static final TableId T = new TableId("FREEPDB1", "APP", "ORDERS");

  static TableSchema schema() {
    return new TableSchema(
        T,
        List.of(
            ColumnSpec.of("ID", 1, OracleType.NUMBER),
            ColumnSpec.of("CODE", 2, OracleType.VARCHAR2),
            ColumnSpec.of("NAME", 3, OracleType.VARCHAR2)),
        List.of(),
        KeySource.NONE,
        true,
        false);
  }

  @Test
  void dictionaryTypeTextParses() {
    assertThat(OracleType.fromDictionary("TIMESTAMP(6) WITH LOCAL TIME ZONE"))
        .isEqualTo(OracleType.TIMESTAMP_LTZ);
    assertThat(OracleType.fromDictionary("TIMESTAMP(9) WITH TIME ZONE"))
        .isEqualTo(OracleType.TIMESTAMP_TZ);
    assertThat(OracleType.fromDictionary("TIMESTAMP(6)")).isEqualTo(OracleType.TIMESTAMP);
    assertThat(OracleType.fromDictionary("INTERVAL DAY(5) TO SECOND(6)"))
        .isEqualTo(OracleType.INTERVAL_DS);
    assertThat(OracleType.fromDictionary("INTERVAL YEAR(4) TO MONTH"))
        .isEqualTo(OracleType.INTERVAL_YM);
    assertThat(OracleType.fromDictionary("VECTOR(3, FLOAT32)")).isEqualTo(OracleType.VECTOR);
    assertThat(OracleType.fromDictionary("LONG RAW")).isEqualTo(OracleType.LONG_RAW);
    assertThat(OracleType.fromDictionary("SYS.XMLTYPE")).isEqualTo(OracleType.XMLTYPE);
    assertThat(OracleType.fromDictionary("SDO_GEOMETRY")).isEqualTo(OracleType.UNKNOWN);
    assertThat(OracleType.fromDictionary(null)).isEqualTo(OracleType.UNKNOWN);
    assertThat(OracleType.BOOLEAN.unsupportedByLogMiner()).isTrue();
    assertThat(OracleType.NUMBER.unsupportedByLogMiner()).isFalse();
    assertThat(OracleType.NCLOB.isLob()).isTrue();
  }

  @Test
  void keySelectionOrder() {
    KeySelector fail = new KeySelector(Map.of(), KeySelector.MissingKeyPolicy.FAIL);
    KeySelector.Candidates pk = new KeySelector.Candidates(List.of("ID"), List.of(List.of("CODE")));
    assertThat(fail.select(schema(), pk).keySource()).isEqualTo(KeySource.PRIMARY_KEY);
    KeySelector.Candidates uq =
        new KeySelector.Candidates(List.of(), List.of(List.of("CODE"), List.of("NAME")));
    TableSchema u = fail.select(schema(), uq);
    assertThat(u.keySource()).isEqualTo(KeySource.UNIQUE_INDEX);
    assertThat(u.keyColumns()).containsExactly("CODE");
    KeySelector.Candidates none = new KeySelector.Candidates(List.of(), List.of());
    assertThatThrownBy(() -> fail.select(schema(), none))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining("cdc.key.missing=fail");
    assertThat(
            new KeySelector(Map.of(), KeySelector.MissingKeyPolicy.ROWID)
                .select(schema(), none)
                .keySource())
        .isEqualTo(KeySource.ROWID);
    assertThat(
            new KeySelector(Map.of(), KeySelector.MissingKeyPolicy.NONE)
                .select(schema(), none)
                .keySource())
        .isEqualTo(KeySource.NONE);
    KeySelector override =
        new KeySelector(
            Map.of("FREEPDB1.APP.ORDERS", List.of("NAME")), KeySelector.MissingKeyPolicy.FAIL);
    assertThat(override.select(schema(), pk).keyColumns()).containsExactly("NAME");
    assertThat(override.select(schema(), pk).keySource()).isEqualTo(KeySource.OVERRIDE);
    KeySelector bad =
        new KeySelector(
            Map.of("FREEPDB1.APP.ORDERS", List.of("NOPE")), KeySelector.MissingKeyPolicy.FAIL);
    assertThatThrownBy(() -> bad.select(schema(), pk)).isInstanceOf(DecodeException.class);
    assertThat(KeySelector.MissingKeyPolicy.parse("RowId"))
        .isEqualTo(KeySelector.MissingKeyPolicy.ROWID);
    assertThat(KeySelector.MissingKeyPolicy.parse(null))
        .isEqualTo(KeySelector.MissingKeyPolicy.FAIL);
  }

  @Test
  void registryUsesStoreThenDictionaryAndCaches() throws Exception {
    InMemorySchemaStore store = new InMemorySchemaStore();
    int[] reads = {0};
    DictionaryReader dict =
        new DictionaryReader() {
          public Optional<TableSchema> read(TableId table) {
            reads[0]++;
            return table.equals(T) ? Optional.of(schema()) : Optional.empty();
          }

          public KeySelector.Candidates keyCandidates(TableId table) {
            return new KeySelector.Candidates(List.of("ID"), List.of());
          }
        };
    SchemaRegistry reg =
        new SchemaRegistry(
            store, dict, new KeySelector(Map.of(), KeySelector.MissingKeyPolicy.FAIL));
    TableSchema s = reg.current(T);
    assertThat(s.keyColumns()).containsExactly("ID");
    assertThat(store.saves).isEqualTo(1);
    reg.current(T);
    assertThat(reads[0]).isEqualTo(1);
    reg.invalidate(T);
    reg.current(T);
    assertThat(reads[0]).isEqualTo(1); // the store answers before the dictionary
    assertThatThrownBy(() -> reg.current(new TableId("FREEPDB1", "APP", "MISSING")))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining("not in the dictionary");
    TableSchema replaced = schema().withKey(List.of("CODE"), KeySource.OVERRIDE);
    reg.put(replaced);
    assertThat(reg.current(T).keySource()).isEqualTo(KeySource.OVERRIDE);
    assertThat(schema().column("NAME").position()).isEqualTo(3);
    assertThat(schema().column("ZZZ")).isNull();
  }
}
