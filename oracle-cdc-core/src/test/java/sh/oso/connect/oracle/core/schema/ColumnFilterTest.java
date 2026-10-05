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

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.PatternSyntaxException;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.errors.DecodeException;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;

/** PRD-01 SRC-SEL-2: which columns cdc.columns.exclude keeps out, and the layouts it projects. */
class ColumnFilterTest {

  static final TableId CUSTOMERS = new TableId("FREEPDB1", "APP", "CUSTOMERS");
  static final TableId ORDERS = new TableId("FREEPDB1", "APP", "ORDERS");
  static final TableId NON_CDB = new TableId(null, "APP", "CUSTOMERS");

  static TableSchema customers(List<String> key, KeySource source) {
    return new TableSchema(
        CUSTOMERS,
        List.of(
            ColumnSpec.of("ID", 1, OracleType.NUMBER),
            ColumnSpec.of("NAME", 2, OracleType.VARCHAR2),
            ColumnSpec.of("SSN", 3, OracleType.VARCHAR2),
            ColumnSpec.of("NOTES", 4, OracleType.CLOB)),
        key,
        source,
        true,
        false,
        3,
        900);
  }

  @Test
  void patternsMatchTheWholeQualifiedNameCaseInsensitivelyByDefault() {
    ColumnFilter f = ColumnFilter.of(List.of("FREEPDB1\\.APP\\.CUSTOMERS\\.SSN"), false);
    assertThat(f.isEmpty()).isFalse();
    assertThat(f.excludes(CUSTOMERS, "SSN")).isTrue();
    assertThat(f.excludes(CUSTOMERS, "ssn")).isTrue();
    assertThat(f.excludes(CUSTOMERS, "SSN_HASH")).as("a whole match, not a prefix").isFalse();
    assertThat(f.excludes(ORDERS, "SSN")).isFalse();
    assertThat(f.excludes(NON_CDB, "SSN")).as("a non-CDB name has no PDB").isFalse();
    assertThat(ColumnFilter.of(List.of("APP\\.CUSTOMERS\\.SSN"), false).excludes(NON_CDB, "SSN"))
        .isTrue();
  }

  @Test
  void caseSensitivePatternsFollowTheTablePatterns() {
    ColumnFilter f = ColumnFilter.of(List.of("FREEPDB1\\.APP\\.CUSTOMERS\\.Ssn"), true);
    assertThat(f.excludes(CUSTOMERS, "Ssn")).isTrue();
    assertThat(f.excludes(CUSTOMERS, "SSN")).isFalse();
  }

  @Test
  void blankEntriesMeanNothingAndBadPatternsAreRejected() {
    assertThat(ColumnFilter.of(List.of("", "  "), false)).isSameAs(ColumnFilter.none());
    assertThat(ColumnFilter.none().excludes(CUSTOMERS, "SSN")).isFalse();
    assertThat(ColumnFilter.none().mayExclude(CUSTOMERS)).isFalse();
    assertThat(ColumnFilter.none().toString()).contains("none");
    assertThat(ColumnFilter.of(List.of(".*\\.SSN"), false).toString()).contains(".*\\.SSN");
    assertThatThrownBy(() -> ColumnFilter.of(List.of("APP\\.(SSN"), false))
        .isInstanceOf(PatternSyntaxException.class);
  }

  @Test
  void aTableMayHaveExcludedColumnsWhenAPatternCouldMatchPastItsName() {
    assertThat(ColumnFilter.of(List.of(".*\\.SSN"), false).mayExclude(ORDERS)).isTrue();
    assertThat(
            ColumnFilter.of(List.of("FREEPDB1\\.APP\\.CUSTOMERS\\.(SSN|DOB)"), false)
                .mayExclude(CUSTOMERS))
        .isTrue();
    ColumnFilter one = ColumnFilter.of(List.of("FREEPDB1\\.APP\\.CUSTOMERS\\.SSN"), false);
    assertThat(one.mayExclude(ORDERS)).isFalse();
    assertThat(one.mayExclude(new TableId("FREEPDB1", "APP", "CUST"))).isFalse();
    assertThat(one.mayExclude(new TableId("FREEPDB1", "APP", "CUSTOMERS_ARCHIVE"))).isFalse();
    assertThat(one.mayExclude(CUSTOMERS)).isTrue();
  }

  @Test
  void aProjectedLayoutKeepsTheKeyAndEveryOtherColumnInOrder() {
    ColumnFilter f = ColumnFilter.of(List.of(".*\\.CUSTOMERS\\.(SSN|NOTES)"), false);
    TableSchema full = customers(List.of("ID"), KeySource.PRIMARY_KEY);
    TableSchema projected = f.project(full);
    assertThat(projected.columns()).extracting(ColumnSpec::name).containsExactly("ID", "NAME");
    assertThat(projected.keyColumns()).containsExactly("ID");
    assertThat(projected.version()).isEqualTo(3);
    assertThat(projected.validFromScn()).isEqualTo(900);
    assertThat(projected.exact()).isTrue();
    assertThat(f.project(full)).as("cached").isSameAs(projected);
    assertThat(f.project(projected)).as("nothing left to drop").isSameAs(projected);
    assertThat(ColumnFilter.none().project(full)).isSameAs(full);
    assertThat(ColumnFilter.of(List.of(".*\\.ORDERS\\..*"), false).project(full)).isSameAs(full);
  }

  @Test
  void aKeyColumnCannotBeExcluded() {
    ColumnFilter f = ColumnFilter.of(List.of(".*\\.CUSTOMERS\\.(ID|SSN)"), false);
    TableSchema byId = customers(List.of("ID"), KeySource.PRIMARY_KEY);
    assertThat(f.excludedKeyColumns(byId)).containsExactly("ID");
    assertThatThrownBy(() -> f.project(byId))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining("CDC-3001")
        .hasMessageContaining("column ID of FREEPDB1.APP.CUSTOMERS")
        .hasMessageContaining("PRIMARY_KEY")
        .hasMessageContaining("cdc.key.columns");
    TableSchema composite = customers(List.of("ID", "SSN"), KeySource.UNIQUE_INDEX);
    assertThatThrownBy(() -> f.project(composite)).hasMessageContaining("columns ID, SSN");
    // keyed by ROWID or not keyed: nothing of the key is a column
    assertThat(f.project(customers(List.of(), KeySource.ROWID)).columns())
        .extracting(ColumnSpec::name)
        .containsExactly("NAME", "NOTES");
  }

  @Test
  void imagesAndChangesLoseOnlyTheExcludedColumns() {
    ColumnFilter f = ColumnFilter.of(List.of(".*\\.SSN"), false);
    Map<String, Object> image = new LinkedHashMap<>();
    image.put("ID", 1);
    image.put("SSN", "123-45-6789");
    image.put("NAME", null);
    assertThat(f.project(CUSTOMERS, image)).containsExactly(Map.entry("ID", 1), entry("NAME"));
    Map<String, Object> clean = Map.of("ID", 1);
    assertThat(f.project(CUSTOMERS, clean)).isSameAs(clean);
    assertThat(f.project(CUSTOMERS, (Map<String, Object>) null)).isNull();

    RowChange update =
        new RowChange(
            CUSTOMERS,
            Operation.UPDATE,
            image,
            image,
            true,
            "AAAR1",
            new RedoRecordId(10, " 0x000001.00000002.0010 ", 3),
            new TxKey(3, new Xid(1, 2, 3)),
            Instant.EPOCH,
            2);
    RowChange projected = f.project(update);
    assertThat(projected.before()).doesNotContainKey("SSN").containsKeys("ID", "NAME");
    assertThat(projected.after()).doesNotContainKey("SSN");
    assertThat(projected.partial()).isTrue();
    assertThat(projected.schemaVersion()).isEqualTo(2);
    assertThat(projected.id()).isEqualTo(update.id());
    RowChange insert =
        new RowChange(
            CUSTOMERS,
            Operation.INSERT,
            null,
            clean,
            false,
            "AAAR2",
            update.id(),
            update.tx(),
            Instant.EPOCH);
    assertThat(f.project(insert)).isSameAs(insert);
    assertThat(ColumnFilter.none().project(update)).isSameAs(update);
  }

  @Test
  void aQualifiedNameCanBeCheckedWithoutATable() {
    ColumnFilter f = ColumnFilter.of(List.of("FREEPDB1\\.APP\\.CUSTOMERS\\.SSN"), false);
    assertThat(f.excludesName("FREEPDB1.APP.CUSTOMERS.SSN")).isTrue();
    assertThat(f.excludesName("FREEPDB1.APP.CUSTOMERS.ID")).isFalse();
  }

  private static Map.Entry<String, Object> entry(String key) {
    return new java.util.AbstractMap.SimpleEntry<>(key, null);
  }
}
