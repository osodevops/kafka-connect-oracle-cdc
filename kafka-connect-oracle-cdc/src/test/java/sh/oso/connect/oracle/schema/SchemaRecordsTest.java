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
package sh.oso.connect.oracle.schema;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.KeySource;
import sh.oso.connect.oracle.core.schema.OracleType;
import sh.oso.connect.oracle.core.schema.TableSchema;

class SchemaRecordsTest {

  static final TableId T = new TableId("FREEPDB1", "APP", "ORDERS");

  static TableSchema v1() {
    return new TableSchema(
        T,
        List.of(
            new ColumnSpec("ID", 1, OracleType.NUMBER, "NUMBER(10)", 22, 10, 0, false),
            new ColumnSpec("NOTE", 2, OracleType.VARCHAR2, "VARCHAR2(40)", 40, 0, 0, true)),
        List.of("ID"),
        KeySource.PRIMARY_KEY,
        true,
        false);
  }

  static TableSchema v2() {
    return new TableSchema(
            T,
            List.of(
                new ColumnSpec("ID", 1, OracleType.NUMBER, "NUMBER(10)", 22, 10, 0, false),
                new ColumnSpec("NOTE", 2, OracleType.VARCHAR2, "VARCHAR2(40)", 40, 0, 0, true),
                new ColumnSpec("ADDED", 3, OracleType.TIMESTAMP, "TIMESTAMP(6)", 11, 0, 6, true)),
            List.of("ID"),
            KeySource.PRIMARY_KEY,
            true,
            false)
        .withVersion(2, 5000);
  }

  @Test
  void versionsSurviveTheWorkersJsonConverterAndAStructRoundTrip() {
    SchemaRecords.Writer w =
        new SchemaRecords.Writer("cdc.cdc.schema", "cdc", Map.of("server", "cdc"));
    SourceRecord r = w.versions(T, List.of(v1(), v2()), Map.of("scn", 1L));
    assertThat(r.topic()).isEqualTo("cdc.cdc.schema");
    assertThat(r.sourceOffset()).isEqualTo(Map.of("scn", 1L));

    // as written by the task
    assertThat(SchemaRecords.table(r.key(), "cdc")).isEqualTo(T);
    assertThat(SchemaRecords.versions(T, r.value())).containsExactly(v1(), v2());

    // as read back from the topic: maps once schemas are off, structs when they are on
    for (boolean schemas : List.of(true, false)) {
      JsonConverter keys = new JsonConverter();
      JsonConverter values = new JsonConverter();
      keys.configure(Map.of("schemas.enable", String.valueOf(schemas)), true);
      values.configure(Map.of("schemas.enable", String.valueOf(schemas)), false);
      SchemaAndValue k =
          keys.toConnectData(r.topic(), keys.fromConnectData(r.topic(), r.keySchema(), r.key()));
      SchemaAndValue v =
          values.toConnectData(
              r.topic(), values.fromConnectData(r.topic(), r.valueSchema(), r.value()));
      assertThat(SchemaRecords.table(k.value(), "cdc")).as("schemas %s", schemas).isEqualTo(T);
      assertThat(SchemaRecords.table(k.value(), "other")).isNull();
      List<TableSchema> back = SchemaRecords.versions(T, v.value());
      assertThat(back).as("schemas %s", schemas).containsExactly(v1(), v2());
      assertThat(back.get(1).version()).isEqualTo(2);
      assertThat(back.get(1).validFromScn()).isEqualTo(5000);
    }
  }

  @Test
  void aRemovedTableIsATombstoneAndANonCdbTableHasNoPdb() {
    TableId plain = new TableId(null, "APP", "ORDERS");
    SourceRecord r =
        new SchemaRecords.Writer("t", "cdc", Map.of("server", "cdc"))
            .removed(plain, Map.of("scn", 2L));
    assertThat(r.value()).isNull();
    assertThat(SchemaRecords.table(r.key(), "cdc")).isEqualTo(plain);
    assertThat(SchemaRecords.versions(plain, r.value())).isEmpty();
  }
}
