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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfig;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.errors.DecodeException;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.RowIds;
import sh.oso.connect.oracle.core.schema.KeySource;
import sh.oso.connect.oracle.core.schema.TableSchema;
import sh.oso.connect.oracle.core.snapshot.SnapshotRow;
import sh.oso.connect.oracle.envelope.TopicRouter;

class ConfluentLobFixtureTest {
  static ConfluentFormat format(Map<String, String> extra) {
    Map<String, String> props = new java.util.HashMap<>(extra);
    props.put(CoreConfig.LOB_MODE, "topic");
    props.put(
        OracleCdcSourceConnectorConfig.LOB_TOPIC_TEMPLATE,
        "lob.${pdb}.${schema}.${table}.${column}");
    OracleCdcSourceConnectorConfig config = ConfluentFormatConfigTest.config(props);
    return new ConfluentFormat(
        config,
        new TopicRouter(config.topicTemplate(true), config.topicPrefix(), "FREE"),
        "FREE",
        config.confluentSettings(),
        Clock.fixed(Instant.EPOCH, ZoneOffset.UTC));
  }

  static List<SourceRecord> change(
      ConfluentFormat format,
      TableSchema schema,
      Operation operation,
      Map<String, Object> before,
      Map<String, Object> after,
      Set<String> touched,
      String rowId) {
    RowChange change =
        new RowChange(
                schema.table(),
                operation,
                before,
                after,
                false,
                rowId,
                new RedoRecordId(100, "r", 0),
                ConfluentFormatFixtureTest.TX,
                Instant.EPOCH)
            .withTouchedLobs(touched);
    CommittedTransaction tx =
        new CommittedTransaction(
            change.tx(),
            change.id(),
            null,
            new RedoRecordId(200, "c", 0),
            Instant.EPOCH,
            1,
            "SCHEMA",
            null,
            List.of(change));
    return format.records(tx, 0, 1, schema, ConfluentFormatFixtureTest.OFFSET);
  }

  @Test
  void snapshotInsertColumnOnlyUpdateAndDeleteMatchTheLobFixtures() throws Exception {
    ConfluentFormat format = format(Map.of());
    TableSchema schema = ConfluentFixtureData.data("LOBS").schema();
    Map<String, Object> seed =
        ConfluentFixtureData.row(
            "ID",
            ConfluentFixtureData.decimal("0"),
            "V",
            "seed",
            "C",
            "small clob",
            "N",
            "small nclob",
            "B",
            new byte[] {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef});
    Map<String, Object> inserted =
        ConfluentFixtureData.row(
            "ID",
            ConfluentFixtureData.decimal("1"),
            "V",
            "v1",
            "C",
            "clob one",
            "N",
            "nclob one",
            "B",
            new byte[] {1, 2});
    String large = "large clob " + "x".repeat(3000 - "large clob ".length());
    Map<String, Object> updated = ConfluentFixtureData.replace(inserted, "C", large);
    List<SourceRecord> all =
        new ArrayList<>(
            format.snapshotRecords(
                schema.table(),
                new SnapshotRow(seed, null),
                schema,
                100,
                0,
                "true",
                ConfluentFormatFixtureTest.OFFSET));
    all.addAll(
        change(format, schema, Operation.INSERT, null, inserted, Set.of("C", "N", "B"), "R1"));
    List<SourceRecord> only =
        change(
            format,
            schema,
            Operation.UPDATE,
            inserted,
            updated,
            Set.of("C"),
            RowIds.lobGroup("R1", "1"));
    assertThat(only).hasSize(1);
    assertThat(only.get(0).topic()).endsWith(".C");
    all.addAll(only);
    List<SourceRecord> nonLob =
        change(
            format,
            schema,
            Operation.UPDATE,
            updated,
            ConfluentFixtureData.replace(updated, "V", "v2"),
            Set.of(),
            "R1");
    assertThat(nonLob).hasSize(1);
    assertThat(nonLob.get(0).topic()).doesNotStartWith("lob.");
    all.addAll(change(format, schema, Operation.DELETE, updated, null, Set.of(), "R1"));
    for (String column : List.of("C", "N", "B")) {
      String topic = "lob.FREEPDB1.SCHEMA.LOBS." + column;
      List<SourceRecord> actual = all.stream().filter(r -> r.topic().equals(topic)).toList();
      List<JsonNode> expected =
          ConfluentFormatFixtureTest.read(
              ConfluentFormatFixtureTest.FIXTURES.resolve("lob-topics").resolve(topic + ".jsonl"));
      assertThat(actual).hasSize(expected.size());
      for (int i = 0; i < actual.size(); i++)
        assertThat(ConfluentFormatFixtureTest.serialise(actual.get(i)))
            .isEqualTo(ConfluentFormatFixtureTest.normalise(expected.get(i)));
    }
  }

  @Test
  void compositeLobKeysMatchTheFixtureIndependentOfTheTableKeyTemplate() throws Exception {
    TableSchema schema = ConfluentFixtureData.data("LOBS2").schema();
    JsonNode expected =
        ConfluentFormatFixtureTest.read(
                ConfluentFormatFixtureTest.FIXTURES.resolve(
                    "lob-topics/lob.FREEPDB1.SCHEMA.LOBS2.C.jsonl"))
            .get(0);
    Map<String, Object> seed =
        ConfluentFixtureData.row(
            "A", ConfluentFixtureData.decimal("0"), "B", "s", "C", "composite seed");
    for (String keyTemplate :
        List.of(
            ConfluentSettings.KEY_STRUCT_OR_VALUE,
            ConfluentSettings.KEY_STRUCT,
            ConfluentSettings.KEY_CONCATENATED)) {
      List<SourceRecord> records =
          format(Map.of(ConfluentFormatConfig.KEY_TEMPLATE, keyTemplate))
              .snapshotRecords(
                  schema.table(),
                  new SnapshotRow(seed, null),
                  schema,
                  100,
                  0,
                  "true",
                  ConfluentFormatFixtureTest.OFFSET);
      assertThat(records).hasSize(2);
      assertThat(ConfluentFormatFixtureTest.serialise(records.get(1)))
          .isEqualTo(ConfluentFormatFixtureTest.normalise(expected));
    }
  }

  @Test
  void keyChangesRemoveOldLobStateAndPublishTheNewKeyAsTheDocumentedDifference() {
    ConfluentFormat format = format(Map.of());
    TableSchema schema = ConfluentFixtureData.data("LOBS2").schema();
    Map<String, Object> old =
        ConfluentFixtureData.row("A", ConfluentFixtureData.decimal("1"), "B", "a", "C", "complete");
    Map<String, Object> newer = ConfluentFixtureData.replace(old, "B", "b");
    List<SourceRecord> records =
        change(format, schema, Operation.UPDATE, old, newer, Set.of(), "R1");
    assertThat(records).hasSize(5);
    assertThat(records.get(3).value()).isNull();
    assertThat(((Struct) ((Struct) records.get(3).key()).get("primary_key")).getString("B"))
        .isEqualTo("a");
    assertThat(((Struct) ((Struct) records.get(4).key()).get("primary_key")).getString("B"))
        .isEqualTo("b");
    assertThat((byte[]) records.get(4).value())
        .isEqualTo("complete".getBytes(java.nio.charset.StandardCharsets.UTF_8));
    assertThat(records.get(3).headers()).isNotSameAs(records.get(4).headers());
    assertThat(records.get(3).headers().lastWithName("cdc.scn")).isNotNull();
    assertThat(records.get(4).headers().lastWithName("cdc.scn")).isNotNull();
  }

  @Test
  void aSmallLobAssignmentAlsoSuppressesItsTableRecord() {
    ConfluentFormat format = format(Map.of());
    TableSchema schema = ConfluentFixtureData.data("LOBS2").schema();
    Map<String, Object> old =
        ConfluentFixtureData.row("A", ConfluentFixtureData.decimal("1"), "B", "a", "C", "old");
    List<SourceRecord> records =
        change(
            format,
            schema,
            Operation.UPDATE,
            old,
            ConfluentFixtureData.replace(old, "C", "new"),
            Set.of("C"),
            "R1");
    assertThat(records).hasSize(1);
    assertThat(records.get(0).topic()).startsWith("lob.");
  }

  @Test
  @org.junit.jupiter.api.Tag("dbz-2504")
  void anAmbiguousRollbackPublishesItsFinalReselectedLobValueWithoutATableRow() {
    TableSchema schema = ConfluentFixtureData.data("LOBS2").schema();
    Map<String, Object> row =
        ConfluentFixtureData.row("A", ConfluentFixtureData.decimal("1"), "B", "a", "C", "kept");
    List<SourceRecord> records =
        change(
            format(Map.of()),
            schema,
            Operation.UPDATE,
            row,
            row,
            Set.of("C"),
            RowIds.inert(RowIds.lobGroup("R1", "1")));
    assertThat(records).hasSize(1);
    assertThat(records.get(0).topic()).startsWith("lob.");
    assertThat((byte[]) records.get(0).value())
        .isEqualTo("kept".getBytes(java.nio.charset.StandardCharsets.UTF_8));
  }

  @Test
  void malformedLobTextStopsInsteadOfPublishingReplacementCharacters() {
    TableSchema schema = ConfluentFixtureData.data("LOBS2").schema();
    Map<String, Object> row =
        ConfluentFixtureData.row("A", ConfluentFixtureData.decimal("1"), "B", "a", "C", "\uD800");
    assertThatThrownBy(
            () -> change(format(Map.of()), schema, Operation.INSERT, null, row, Set.of(), "R1"))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining("not valid UTF-8");
  }

  @Test
  void incompleteAndKeylessLobStateStopsTyped() {
    TableSchema schema = ConfluentFixtureData.data("LOBS2").schema();
    ConfluentFormat format = format(Map.of());
    Map<String, Object> missing =
        ConfluentFixtureData.row("A", ConfluentFixtureData.decimal("1"), "B", "a");
    assertThatThrownBy(
            () -> change(format, schema, Operation.INSERT, null, missing, Set.of(), "R1"))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining("unavailable");
    assertThatThrownBy(
            () ->
                format.snapshotRecords(
                    schema.table(),
                    new SnapshotRow(missing, null),
                    schema.withKey(List.of(), KeySource.NONE),
                    100,
                    0,
                    "true",
                    ConfluentFormatFixtureTest.OFFSET))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining("stable column key");
    assertThatThrownBy(
            () ->
                format.snapshotRecords(
                    schema.table(),
                    new SnapshotRow(missing, null),
                    schema.withKey(List.of("A"), KeySource.ROWID),
                    100,
                    0,
                    "true",
                    ConfluentFormatFixtureTest.OFFSET))
        .isInstanceOf(DecodeException.class);
  }
}
