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

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.schema.TableSchema;

class ConfluentFormatOperationsTest {
  static List<SourceRecord> records(
      String profile,
      TableSchema schema,
      Operation op,
      Map<String, Object> before,
      Map<String, Object> after) {
    RowChange change =
        new RowChange(
            schema.table(),
            op,
            before,
            after,
            false,
            "R1",
            new RedoRecordId(101, "r", 0),
            ConfluentFormatFixtureTest.TX,
            Instant.ofEpochSecond(1));
    CommittedTransaction tx =
        new CommittedTransaction(
            ConfluentFormatFixtureTest.TX,
            change.id(),
            new RedoRecordId(100, "start", 0),
            new RedoRecordId(200, "commit", 0),
            Instant.ofEpochSecond(2),
            1,
            "SCHEMA",
            null,
            List.of(change));
    return ConfluentFormatFixtureTest.format(profile)
        .records(tx, 0, 1, schema, ConfluentFormatFixtureTest.OFFSET);
  }

  @Test
  void aLobOnlyChangeEmitsNoTableRecordButAChangeToBothKindsKeepsItsRow() {
    TableSchema schema = ConfluentFixtureData.data("LOBS").schema();
    Map<String, Object> before =
        ConfluentFixtureData.row("ID", ConfluentFixtureData.decimal("1"), "V", "v1", "C", "old");
    Map<String, Object> after = ConfluentFixtureData.replace(before, "C", "new");
    for (String rowId :
        List.of(
            sh.oso.connect.oracle.core.model.RowIds.lobGroup("R1", "1"),
            sh.oso.connect.oracle.core.model.RowIds.rowPiece("R1", "1"))) {
      RowChange change =
          new RowChange(
              schema.table(),
              Operation.UPDATE,
              before,
              after,
              false,
              rowId,
              new RedoRecordId(101, "r", 0),
              ConfluentFormatFixtureTest.TX,
              Instant.EPOCH);
      CommittedTransaction tx =
          new CommittedTransaction(
              ConfluentFormatFixtureTest.TX,
              change.id(),
              null,
              new RedoRecordId(200, "c", 0),
              Instant.EPOCH,
              1,
              "SCHEMA",
              null,
              List.of(change));
      assertThat(
              ConfluentFormatFixtureTest.format("default")
                  .records(tx, 0, 1, schema, ConfluentFormatFixtureTest.OFFSET))
          .isEmpty();
    }
    RowChange mixed =
        new RowChange(
            schema.table(),
            Operation.UPDATE,
            before,
            ConfluentFixtureData.replace(after, "V", "v2"),
            false,
            sh.oso.connect.oracle.core.model.RowIds.rowPiece("R1", "2"),
            new RedoRecordId(101, "r", 0),
            ConfluentFormatFixtureTest.TX,
            Instant.EPOCH);
    CommittedTransaction tx =
        new CommittedTransaction(
            ConfluentFormatFixtureTest.TX,
            mixed.id(),
            null,
            new RedoRecordId(200, "c", 0),
            Instant.EPOCH,
            1,
            "SCHEMA",
            null,
            List.of(mixed));
    assertThat(
            ConfluentFormatFixtureTest.format("default")
                .records(tx, 0, 1, schema, ConfluentFormatFixtureTest.OFFSET))
        .hasSize(1);
  }

  @Test
  void enabledDeleteTombstonesReplaceTheDeleteIncludingOnAKeylessTable() {
    for (String table : List.of("TYPES", "NOPK")) {
      ConfluentFixtureData.Data data = ConfluentFixtureData.data(table);
      List<SourceRecord> records =
          records("tombstones", data.schema(), Operation.DELETE, data.snapshots().get(0), null);
      assertThat(records).hasSize(1);
      assertThat(records.get(0).value()).isNull();
      if (table.equals("NOPK")) assertThat(records.get(0).key()).isNull();
    }
  }

  @Test
  void keyChangeHasTwoTombstonesWhenEnabledAndDeleteTombstoneInsertOtherwise() {
    TableSchema schema = ConfluentFixtureData.data("COMPOSITE").schema();
    Map<String, Object> before =
        ConfluentFixtureData.row("A", ConfluentFixtureData.decimal("1"), "B", "a", "V", "x");
    Map<String, Object> after = ConfluentFixtureData.replace(before, "B", "b");
    List<SourceRecord> enabled = records("tombstones", schema, Operation.UPDATE, before, after);
    assertThat(enabled).hasSize(3);
    assertThat(enabled.get(0).value()).isNull();
    assertThat(enabled.get(1).value()).isNull();
    assertThat(enabled.get(0).key()).isEqualTo(enabled.get(1).key());
    assertThat(((Struct) enabled.get(2).value()).getString("op_type")).isEqualTo("I");
    List<SourceRecord> defaults = records("default", schema, Operation.UPDATE, before, after);
    assertThat(((Struct) defaults.get(0).value()).getString("op_type")).isEqualTo("D");
    assertThat(defaults.get(1).value()).isNull();
    assertThat(((Struct) defaults.get(2).value()).getString("op_type")).isEqualTo("I");
    assertThat(defaults.get(0).headers()).isNotSameAs(defaults.get(1).headers());
  }

  @Test
  void metadataUsesRowScnAndCommitScnAndBeforeAppearsOnlyOnUpdate() {
    TableSchema schema = ConfluentFixtureData.data("COMPOSITE").schema();
    Map<String, Object> before =
        ConfluentFixtureData.row("A", ConfluentFixtureData.decimal("1"), "B", "a", "V", "x");
    Map<String, Object> after = ConfluentFixtureData.replace(before, "V", "y");
    Struct update =
        (Struct) records("all-fields", schema, Operation.UPDATE, before, after).get(0).value();
    assertThat(update.getString("scn")).isEqualTo("101");
    assertThat(update.getString("cscn")).isEqualTo("200");
    assertThat(update.getStruct("before").getString("V")).isEqualTo("x");
    assertThat(
            records("all-fields", schema, Operation.INSERT, null, after)
                .get(0)
                .valueSchema()
                .field("before"))
        .isNull();
    assertThat(
            records("all-fields", schema, Operation.DELETE, before, null)
                .get(0)
                .valueSchema()
                .field("before"))
        .isNull();
  }
}
