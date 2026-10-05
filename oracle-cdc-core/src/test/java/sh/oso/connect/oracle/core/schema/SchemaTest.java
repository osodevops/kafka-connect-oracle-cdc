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
    reg.forget(T);
    assertThat(reg.known(T)).isFalse();
    reg.current(T);
    assertThat(reads[0]).isEqualTo(2); // forgotten by cache and store: the dictionary again
    assertThat(reg.known(T)).isTrue();
    assertThatThrownBy(() -> reg.current(new TableId("FREEPDB1", "APP", "MISSING")))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining("not in the dictionary");
    TableSchema replaced = schema().withKey(List.of("CODE"), KeySource.OVERRIDE);
    reg.put(replaced);
    assertThat(reg.current(T).keySource()).isEqualTo(KeySource.OVERRIDE);
    assertThat(schema().column("NAME").position()).isEqualTo(3);
    assertThat(schema().column("ZZZ")).isNull();
  }

  @Test
  void aDdlAddsAVersionOnlyWhenTheLayoutChanged() throws Exception {
    InMemorySchemaStore store = new InMemorySchemaStore();
    TableSchema[] dictionary = {schema()};
    DictionaryReader dict =
        new DictionaryReader() {
          public Optional<TableSchema> read(TableId table) {
            return Optional.ofNullable(dictionary[0]);
          }

          public KeySelector.Candidates keyCandidates(TableId table) {
            return new KeySelector.Candidates(List.of("ID"), List.of());
          }
        };
    SchemaRegistry reg =
        new SchemaRegistry(
            store, dict, new KeySelector(Map.of(), KeySelector.MissingKeyPolicy.FAIL));
    TableSchema v1 = reg.current(T);
    assertThat(v1.version()).isEqualTo(1);
    assertThat(reg.applyDdl(T, 500)).as("a comment or an index changes nothing").isEmpty();
    List<ColumnSpec> wider = new java.util.ArrayList<>(schema().columns());
    wider.add(ColumnSpec.of("ADDED", wider.size() + 1, OracleType.VARCHAR2));
    dictionary[0] = new TableSchema(T, wider, List.of(), KeySource.NONE, true, false);
    TableSchema v2 = reg.applyDdl(T, 600).orElseThrow();
    assertThat(v2.version()).isEqualTo(2);
    assertThat(v2.validFromScn()).isEqualTo(600);
    assertThat(v2.column("ADDED")).isNotNull();
    assertThat(v2.keyColumns()).containsExactly("ID");
    assertThat(reg.current(T)).isEqualTo(v2);
    assertThat(store.schemas.get(T)).isEqualTo(v2);
    assertThat(v2.sameLayout(v1)).isFalse();
    dictionary[0] = null; // dropped
    assertThat(reg.applyDdl(T, 700)).isEmpty();
    assertThat(reg.known(T)).isFalse();
  }

  @Test
  void aStoredVersionThatDiffersFromTheDictionaryStopsUnlessItsDdlIsAhead() throws Exception {
    InMemorySchemaStore store = new InMemorySchemaStore();
    TableSchema stored = schema();
    store.save(stored);
    TableSchema[] dictionary = {stored};
    java.time.Instant resumeTime = java.time.Instant.parse("2026-10-05T10:00:00Z");
    java.time.Instant[] lastDdl = {resumeTime.minusSeconds(3600)};
    DictionaryReader dict =
        new DictionaryReader() {
          public Optional<TableSchema> read(TableId table) {
            return Optional.ofNullable(dictionary[0]);
          }

          public KeySelector.Candidates keyCandidates(TableId table) {
            return new KeySelector.Candidates(List.of(), List.of());
          }

          @Override
          public Optional<java.time.Instant> lastDdlTime(TableId table) {
            return Optional.ofNullable(lastDdl[0]);
          }

          @Override
          public Optional<java.time.Instant> timeOfScn(long scn) {
            return scn == 1000 ? Optional.of(resumeTime) : Optional.empty();
          }
        };
    SchemaRegistry reg =
        new SchemaRegistry(
            store, dict, new KeySelector(Map.of(), KeySelector.MissingKeyPolicy.NONE));

    reg.validate(T, 1000); // the same layout
    reg.validate(new TableId("FREEPDB1", "APP", "NEVER_SEEN"), 1000); // nothing stored

    List<ColumnSpec> wider = new java.util.ArrayList<>(schema().columns());
    wider.add(ColumnSpec.of("ADDED", wider.size() + 1, OracleType.VARCHAR2));
    dictionary[0] = new TableSchema(T, wider, List.of(), KeySource.NONE, true, false);
    assertThatThrownBy(() -> reg.validate(T, 1000))
        .as("the table changed before the resume point: a DDL was missed")
        .isInstanceOf(sh.oso.connect.oracle.core.errors.SchemaMismatchException.class)
        .hasMessageContaining("CDC-6003")
        .hasMessageContaining("APP.ORDERS");
    assertThatThrownBy(() -> reg.validate(T, 2000))
        .as("the resume point's time is unknown: nothing explains the difference")
        .isInstanceOf(sh.oso.connect.oracle.core.errors.SchemaMismatchException.class);

    lastDdl[0] = resumeTime.plusSeconds(60);
    reg.validate(T, 1000); // the DDL is ahead in the redo and will be applied there
    lastDdl[0] = resumeTime.minusSeconds(5);
    reg.validate(T, 1000); // within SCN_TO_TIMESTAMP's precision: counted as ahead

    dictionary[0] = null;
    lastDdl[0] = null;
    reg.validate(T, 1000); // dropped since: the DROP is ahead
  }

  @Test
  void replayedRowsDecodeOnlyWithAnExactVersionValidAtTheirScn() throws Exception {
    InMemorySchemaStore store = new InMemorySchemaStore();
    TableSchema[] dictionary = {schema()};
    java.time.Instant[] lastDdl = {java.time.Instant.parse("2026-10-05T09:00:00Z")};
    Map<Long, java.time.Instant> times = new java.util.HashMap<>();
    DictionaryReader dict =
        new DictionaryReader() {
          public Optional<TableSchema> read(TableId table) {
            return Optional.ofNullable(dictionary[0]);
          }

          public KeySelector.Candidates keyCandidates(TableId table) {
            return new KeySelector.Candidates(List.of("ID"), List.of());
          }

          @Override
          public Optional<java.time.Instant> lastDdlTime(TableId table) {
            return Optional.ofNullable(lastDdl[0]);
          }

          @Override
          public Optional<java.time.Instant> timeOfScn(long scn) {
            return Optional.ofNullable(times.get(scn));
          }

          @Override
          public Optional<Long> scnAt(java.time.Instant time) {
            return Optional.of(400L);
          }
        };
    SchemaRegistry reg =
        new SchemaRegistry(
            store, dict, new KeySelector(Map.of(), KeySelector.MissingKeyPolicy.FAIL));

    // first read: valid from the table's last DDL (as an SCN, with the mapping's slack)
    TableSchema v1 = reg.current(T);
    assertThat(v1.validFromScn()).isEqualTo(400);
    assertThatThrownBy(() -> reg.at(T, 399))
        .isInstanceOf(sh.oso.connect.oracle.core.errors.DictionaryUnavailableException.class)
        .hasMessageContaining("CDC-6001")
        .hasMessageContaining("first read the table after that DDL");
    assertThat(reg.at(T, 400)).isEqualTo(v1);

    // a DDL at 600 that is the table's last: exact
    List<ColumnSpec> wider = new java.util.ArrayList<>(schema().columns());
    wider.add(ColumnSpec.of("ADDED", wider.size() + 1, OracleType.VARCHAR2));
    dictionary[0] = new TableSchema(T, wider, List.of(), KeySource.NONE, true, false);
    times.put(600L, java.time.Instant.parse("2026-10-05T10:00:00Z"));
    lastDdl[0] = java.time.Instant.parse("2026-10-05T10:00:01Z");
    TableSchema v2 = reg.applyDdl(T, 600).orElseThrow();
    assertThat(v2.exact()).isTrue();

    // a DDL at 700, but the dictionary already shows one a minute later: the layout is a guess
    List<ColumnSpec> wider2 = new java.util.ArrayList<>(wider);
    wider2.add(ColumnSpec.of("MORE", wider2.size() + 1, OracleType.VARCHAR2));
    dictionary[0] = new TableSchema(T, wider2, List.of(), KeySource.NONE, true, false);
    times.put(700L, java.time.Instant.parse("2026-10-05T10:05:00Z"));
    lastDdl[0] = java.time.Instant.parse("2026-10-05T10:06:00Z");
    TableSchema v3 = reg.applyDdl(T, 700).orElseThrow();
    assertThat(v3.exact()).isFalse();
    assertThat(reg.at(T, 650)).isEqualTo(v2);
    assertThatThrownBy(() -> reg.at(T, 750)).hasMessageContaining("was read after a further DDL");

    // that later DDL, mined: the same layout, now confirmed, becomes an exact version
    times.put(800L, java.time.Instant.parse("2026-10-05T10:06:00Z"));
    TableSchema v4 = reg.applyDdl(T, 800).orElseThrow();
    assertThat(v4.exact()).isTrue();
    assertThat(v4.sameLayout(v3)).isTrue();
    assertThat(reg.at(T, 900)).isEqualTo(v4);
    assertThat(reg.applyDdl(T, 900)).as("nothing new").isEmpty();

    // a buffered row renders with the version it was decoded with
    assertThat(reg.version(T, 2)).isEqualTo(v2);
    assertThat(reg.version(T, 0)).isEqualTo(v4);
    assertThatThrownBy(() -> reg.version(T, 9))
        .isInstanceOf(sh.oso.connect.oracle.core.errors.SchemaMismatchException.class);
  }
}
