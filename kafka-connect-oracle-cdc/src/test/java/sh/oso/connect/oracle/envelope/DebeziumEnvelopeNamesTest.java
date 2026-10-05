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
package sh.oso.connect.oracle.envelope;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfig;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfigTest;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.errors.ErrorCode;
import sh.oso.connect.oracle.core.errors.NameCollisionException;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.KeySource;
import sh.oso.connect.oracle.core.schema.OracleType;
import sh.oso.connect.oracle.core.schema.TableSchema;
import sh.oso.connect.oracle.core.snapshot.SnapshotRow;

/**
 * ADR-0020: cdc.schema.name.adjustment.mode and cdc.field.name.adjustment.mode in the envelope.
 * Names change only when a mode is set; values are read from the column each field belongs to.
 */
class DebeziumEnvelopeNamesTest {

  /** A quoted lower-case table name with a space and a #, as SAP-style schemas have. */
  static final TableId AWKWARD = new TableId("FREEPDB1", "APP", "order lines#");

  static TableSchema awkward(KeySource source) {
    return new TableSchema(
        AWKWARD,
        List.of(
            new ColumnSpec("ID#", 1, OracleType.NUMBER, "NUMBER", 0, 9, 0, false),
            new ColumnSpec("lower name", 2, OracleType.VARCHAR2, "VARCHAR2", 100, -1, -1, true),
            new ColumnSpec("AMOUNT$", 3, OracleType.NUMBER, "NUMBER", 0, 10, 2, true),
            new ColumnSpec("1ST", 4, OracleType.VARCHAR2, "VARCHAR2", 10, -1, -1, true),
            new ColumnSpec("PLAIN_COL", 5, OracleType.VARCHAR2, "VARCHAR2", 10, -1, -1, true)),
        source == KeySource.PRIMARY_KEY ? List.of("ID#") : List.of(),
        source,
        true,
        false);
  }

  static Map<String, Object> image(int id, String name, String amount) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("ID#", new BigDecimal(id));
    m.put("lower name", name);
    m.put("AMOUNT$", amount == null ? null : new BigDecimal(amount));
    m.put("1ST", "q" + id);
    m.put("PLAIN_COL", "p");
    return m;
  }

  static RowChange change(
      TableId table, Operation op, Map<String, Object> before, Map<String, Object> after) {
    return new RowChange(
        table,
        op,
        before,
        after,
        false,
        "AAAR",
        new RedoRecordId(801, "0x01", 0),
        DebeziumEnvelopeTest.K,
        Instant.ofEpochSecond(100));
  }

  static OracleCdcSourceConnectorConfig config(String prefix, String schemaMode, String fieldMode) {
    Map<String, String> p = OracleCdcSourceConnectorConfigTest.minimal();
    p.put(OracleCdcSourceConnectorConfig.TOPIC_PREFIX, prefix);
    if (schemaMode != null) {
      p.put(OracleCdcSourceConnectorConfig.SCHEMA_NAME_ADJUSTMENT_MODE, schemaMode);
    }
    if (fieldMode != null) {
      p.put(OracleCdcSourceConnectorConfig.FIELD_NAME_ADJUSTMENT_MODE, fieldMode);
    }
    return new OracleCdcSourceConnectorConfig(p);
  }

  static DebeziumEnvelope envelope(OracleCdcSourceConnectorConfig cfg) {
    return new DebeziumEnvelope(
        cfg, new TopicRouter(cfg.topicTemplate(true), cfg.topicPrefix(), "FREE"), "FREE");
  }

  static DebeziumEnvelope envelope(String schemaMode, String fieldMode) {
    return envelope(config("my-cdc", schemaMode, fieldMode));
  }

  static List<SourceRecord> insertUpdateDelete(DebeziumEnvelope e, TableSchema schema) {
    CommittedTransaction tx =
        DebeziumEnvelopeTest.tx(
            change(schema.table(), Operation.INSERT, null, image(1, "a", "10.50")),
            change(schema.table(), Operation.UPDATE, image(1, "a", "10.50"), image(1, "b", "11")),
            change(schema.table(), Operation.DELETE, image(1, "b", "11"), null));
    List<SourceRecord> out = new java.util.ArrayList<>();
    for (int i = 0; i < 3; i++) {
      out.addAll(e.records(tx, i, i + 1, schema, DebeziumEnvelopeTest.offset()));
    }
    return out;
  }

  static List<String> fieldNames(org.apache.kafka.connect.data.Schema s) {
    return s.fields().stream().map(Field::name).toList();
  }

  @Test
  void noneLeavesTheRecordsExactlyAsTheyWereWithoutTheSettings() {
    List<SourceRecord> unset =
        insertUpdateDelete(envelope(null, null), awkward(KeySource.PRIMARY_KEY));
    List<SourceRecord> none =
        insertUpdateDelete(envelope("none", "NONE"), awkward(KeySource.PRIMARY_KEY));
    assertThat(none).isEqualTo(unset);
    JsonConverter json = new JsonConverter();
    json.configure(Map.of("schemas.enable", "true"), false);
    for (int i = 0; i < unset.size(); i++) {
      SourceRecord a = unset.get(i);
      SourceRecord b = none.get(i);
      assertThat(json.fromConnectData(b.topic(), b.valueSchema(), b.value()))
          .isEqualTo(json.fromConnectData(a.topic(), a.valueSchema(), a.value()));
    }
    SourceRecord c = unset.get(0);
    assertThat(c.valueSchema().name()).isEqualTo("my-cdc.FREEPDB1.APP.order lines#.Envelope");
    assertThat(c.keySchema().name()).isEqualTo("my-cdc.FREEPDB1.APP.order lines#.Key");
    assertThat(fieldNames(c.valueSchema().field("after").schema()))
        .containsExactly("ID#", "lower name", "AMOUNT$", "1ST", "PLAIN_COL");
    assertThat(fieldNames(c.keySchema())).containsExactly("ID#");
  }

  @Test
  void avroAdjustsSchemaAndFieldNamesAndKeepsEveryValueOnItsColumn() {
    List<SourceRecord> out =
        insertUpdateDelete(envelope("avro", "avro"), awkward(KeySource.PRIMARY_KEY));
    assertThat(out).hasSize(4); // create, update, delete, tombstone
    SourceRecord c = out.get(0);
    assertThat(c.topic())
        .as("topic names follow cdc.topic.template, not the adjustment")
        .isEqualTo("my-cdc.FREEPDB1.APP.order_lines_");
    assertThat(c.valueSchema().name()).isEqualTo("my_cdc.FREEPDB1.APP.order_lines_.Envelope");
    assertThat(c.keySchema().name()).isEqualTo("my_cdc.FREEPDB1.APP.order_lines_.Key");
    org.apache.kafka.connect.data.Schema row = c.valueSchema().field("after").schema();
    assertThat(row.name()).isEqualTo("my_cdc.FREEPDB1.APP.order_lines_.Value");
    assertThat(fieldNames(row))
        .containsExactly("ID_", "lower_name", "AMOUNT_", "_1ST", "PLAIN_COL");
    assertThat(fieldNames(c.keySchema())).containsExactly("ID_");
    assertThat(((Struct) c.key()).get("ID_")).isEqualTo(1);
    Struct after = ((Struct) c.value()).getStruct("after");
    assertThat(after.get("ID_")).isEqualTo(1);
    assertThat(after.get("lower_name")).isEqualTo("a");
    assertThat(after.get("AMOUNT_")).isEqualTo(new BigDecimal("10.50"));
    assertThat(after.get("_1ST")).isEqualTo("q1");
    assertThat(after.get("PLAIN_COL")).isEqualTo("p");
    Struct update = (Struct) out.get(1).value();
    assertThat(update.getString("op")).isEqualTo("u");
    assertThat(update.getStruct("before").get("lower_name")).isEqualTo("a");
    assertThat(update.getStruct("after").get("lower_name")).isEqualTo("b");
    assertThat(update.getStruct("after").get("AMOUNT_")).isEqualTo(new BigDecimal("11.00"));
    Struct delete = (Struct) out.get(2).value();
    assertThat(delete.getStruct("before").get("ID_")).isEqualTo(1);
    assertThat(((Struct) out.get(3).key()).get("ID_")).as("tombstone key").isEqualTo(1);
    assertThat(out.get(3).value()).isNull();
    // fixed names, the source block and the transaction block are untouched
    Struct v = (Struct) c.value();
    assertThat(v.schema().field("source").schema().name())
        .isEqualTo("io.debezium.connector.oracle.Source");
    assertThat(v.getStruct("source").getString("table")).isEqualTo("order lines#");
    assertThat(v.getStruct("source").getString("name")).isEqualTo("my-cdc");
    assertThat(v.schema().field("transaction").schema().name()).isEqualTo("event.block");
    assertThat(fieldNames(v.schema().field("transaction").schema()))
        .containsExactly("id", "total_order", "data_collection_order");
    assertThat(fieldNames(v.schema()))
        .containsExactly(
            "before", "after", "source", "op", "ts_ms", "ts_us", "ts_ns", "transaction");
  }

  @Test
  void avroUnicodeEscapesTheNamesAndTheUnderscore() {
    List<SourceRecord> out =
        insertUpdateDelete(
            envelope("avro_unicode", "avro_unicode"), awkward(KeySource.PRIMARY_KEY));
    SourceRecord c = out.get(0);
    assertThat(c.valueSchema().name())
        .isEqualTo("my_u002dcdc.FREEPDB1.APP.order_u0020lines_u0023.Envelope");
    org.apache.kafka.connect.data.Schema row = c.valueSchema().field("after").schema();
    assertThat(fieldNames(row))
        .containsExactly(
            "ID_u0023", "lower_u0020name", "AMOUNT_u0024", "_u0031ST", "PLAIN_u005fCOL");
    Struct after = ((Struct) c.value()).getStruct("after");
    assertThat(after.get("lower_u0020name")).isEqualTo("a");
    assertThat(after.get("PLAIN_u005fCOL")).isEqualTo("p");
    assertThat(((Struct) c.key()).get("ID_u0023")).isEqualTo(1);
  }

  @Test
  void eitherSettingWorksOnItsOwn() {
    SourceRecord schemasOnly =
        insertUpdateDelete(envelope("avro", null), awkward(KeySource.PRIMARY_KEY)).get(0);
    assertThat(schemasOnly.valueSchema().name())
        .isEqualTo("my_cdc.FREEPDB1.APP.order_lines_.Envelope");
    assertThat(fieldNames(schemasOnly.keySchema())).containsExactly("ID#");
    SourceRecord fieldsOnly =
        insertUpdateDelete(envelope(null, "avro"), awkward(KeySource.PRIMARY_KEY)).get(0);
    assertThat(fieldsOnly.valueSchema().name())
        .isEqualTo("my-cdc.FREEPDB1.APP.order lines#.Envelope");
    assertThat(fieldNames(fieldsOnly.keySchema())).containsExactly("ID_");
  }

  @Test
  void partialBeforeImagesAndRowidKeysUseTheMapping() {
    DebeziumEnvelope e = envelope("avro", "avro");
    Map<String, Object> partial = new LinkedHashMap<>();
    partial.put("ID#", new BigDecimal(5));
    partial.put("AMOUNT$", new BigDecimal("3.00"));
    Map<String, Object> after = image(5, "x", "4");
    List<SourceRecord> out =
        e.records(
            DebeziumEnvelopeTest.tx(change(AWKWARD, Operation.UPDATE, partial, after)),
            0,
            1,
            awkward(KeySource.PRIMARY_KEY),
            DebeziumEnvelopeTest.offset());
    Struct before = ((Struct) out.get(0).value()).getStruct("before");
    assertThat(before.get("ID_")).isEqualTo(5);
    assertThat(before.get("AMOUNT_")).isEqualTo(new BigDecimal("3.00"));
    assertThat(before.get("lower_name")).as("absent from the partial image").isNull();
    Map<String, Object> noKey = new LinkedHashMap<>();
    noKey.put("lower name", "x");
    List<SourceRecord> del =
        e.records(
            DebeziumEnvelopeTest.tx(change(AWKWARD, Operation.DELETE, noKey, null)),
            0,
            1,
            awkward(KeySource.PRIMARY_KEY),
            DebeziumEnvelopeTest.offset());
    assertThat(del).as("no key column in the image, so no key and no tombstone").hasSize(1);
    assertThat(del.get(0).key()).isNull();
    SourceRecord byRowid =
        e.records(
                DebeziumEnvelopeTest.tx(
                    change(AWKWARD, Operation.INSERT, null, image(6, "y", "1"))),
                0,
                1,
                awkward(KeySource.ROWID),
                DebeziumEnvelopeTest.offset())
            .get(0);
    assertThat(byRowid.keySchema().name()).isEqualTo("my_cdc.FREEPDB1.APP.order_lines_.Key");
    assertThat(((Struct) byRowid.key()).getString("ROWID")).isEqualTo("AAAR");
  }

  @Test
  void snapshotRecordsUseTheSameNames() {
    DebeziumEnvelope e = envelope("avro", "avro");
    SourceRecord r =
        e.snapshotRecord(
            AWKWARD,
            new SnapshotRow(image(7, "z", "2.5"), "AAAS"),
            awkward(KeySource.PRIMARY_KEY),
            900,
            1000,
            "true",
            DebeziumEnvelopeTest.offset());
    Struct after = ((Struct) r.value()).getStruct("after");
    assertThat(after.schema().name()).isEqualTo("my_cdc.FREEPDB1.APP.order_lines_.Value");
    assertThat(after.get("lower_name")).isEqualTo("z");
    assertThat(after.get("AMOUNT_")).isEqualTo(new BigDecimal("2.50"));
    assertThat(((Struct) r.key()).get("ID_")).isEqualTo(7);
  }

  static TableSchema clashing(String first, String second) {
    return new TableSchema(
        new TableId("FREEPDB1", "APP", "T"),
        List.of(
            new ColumnSpec("ID", 1, OracleType.NUMBER, "NUMBER", 0, 9, 0, false),
            new ColumnSpec(first, 2, OracleType.VARCHAR2, "VARCHAR2", 10, -1, -1, true),
            new ColumnSpec(second, 3, OracleType.VARCHAR2, "VARCHAR2", 10, -1, -1, true)),
        List.of("ID"),
        KeySource.PRIMARY_KEY,
        true,
        false);
  }

  @Test
  void twoColumnsThatAdjustToOneFieldStopTheTaskBeforeTheTablesFirstRecord() {
    DebeziumEnvelope e = envelope("avro", "avro");
    TableSchema schema = clashing("A#", "A$");
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("ID", BigDecimal.ONE);
    row.put("A#", "hash");
    row.put("A$", "dollar");
    CommittedTransaction tx =
        DebeziumEnvelopeTest.tx(change(schema.table(), Operation.INSERT, null, row));
    assertThatThrownBy(() -> e.records(tx, 0, 1, schema, DebeziumEnvelopeTest.offset()))
        .isInstanceOfSatisfying(
            NameCollisionException.class,
            x -> assertThat(x.code()).isEqualTo(ErrorCode.NAME_COLLISION))
        .hasMessageStartingWith(
            "[CDC-6004] Columns A# and A$ of table FREEPDB1.APP.T both become field A_ under"
                + " cdc.field.name.adjustment.mode=avro.")
        .hasMessageContaining("avro_unicode")
        .hasMessageEndingWith("/runbooks/name-collision");
    assertThatThrownBy(() -> e.schemas(clashing("B_", "B$")))
        .as("a column already named like the adjusted one")
        .isInstanceOf(NameCollisionException.class)
        .hasMessageContaining("Columns B_ and B$");
    assertThat(fieldNames(envelope("avro", "avro_unicode").schemas(schema).value()))
        .as("avro_unicode keeps them apart")
        .containsExactly("ID", "A_u0023", "A_u0024");
    assertThat(fieldNames(envelope("avro", null).schemas(schema).value()))
        .as("schema adjustment alone leaves field names alone")
        .containsExactly("ID", "A#", "A$");
  }

  @Test
  void fieldAdjustmentAppliesToTheColumnsLeftAfterExclusion() {
    Map<String, String> p = OracleCdcSourceConnectorConfigTest.minimal();
    p.put(OracleCdcSourceConnectorConfig.FIELD_NAME_ADJUSTMENT_MODE, "avro");
    p.put(OracleCdcSourceConnectorConfig.COLUMNS_EXCLUDE, "FREEPDB1\\.APP\\.T\\.A\\$");
    DebeziumEnvelope e = envelope(new OracleCdcSourceConnectorConfig(p));
    TableSchema schema = clashing("A#", "A$");
    Map<String, Object> row = new LinkedHashMap<>();
    row.put("ID", BigDecimal.ONE);
    row.put("A#", "hash");
    row.put("A$", "dollar"); // an image restored from an older journal may still carry it
    SourceRecord r =
        e.records(
                DebeziumEnvelopeTest.tx(change(schema.table(), Operation.INSERT, null, row)),
                0,
                1,
                schema,
                DebeziumEnvelopeTest.offset())
            .get(0);
    Struct after = ((Struct) r.value()).getStruct("after");
    assertThat(fieldNames(after.schema())).containsExactly("ID", "A_");
    assertThat(after.get("A_")).isEqualTo("hash");
  }

  @Test
  void twoTablesOnOneTopicOnlyThroughSanitisingStopTheTaskBeforeTheSecondTablesRecord() {
    DebeziumEnvelope e = envelope(null, null);
    TableSchema hash = clashing("X", "Y");
    TableSchema dollar =
        new TableSchema(
            new TableId("FREEPDB1", "APP", "T$"),
            hash.columns(),
            hash.keyColumns(),
            hash.keySource(),
            true,
            false);
    TableSchema hashTable =
        new TableSchema(
            new TableId("FREEPDB1", "APP", "T#"),
            hash.columns(),
            hash.keyColumns(),
            hash.keySource(),
            true,
            false);
    Map<String, Object> row = Map.of("ID", BigDecimal.ONE, "X", "x", "Y", "y");
    assertThat(
            e.records(
                    DebeziumEnvelopeTest.tx(change(hashTable.table(), Operation.INSERT, null, row)),
                    0,
                    1,
                    hashTable,
                    DebeziumEnvelopeTest.offset())
                .get(0)
                .topic())
        .isEqualTo("my-cdc.FREEPDB1.APP.T_");
    assertThatThrownBy(
            () ->
                e.records(
                    DebeziumEnvelopeTest.tx(change(dollar.table(), Operation.INSERT, null, row)),
                    0,
                    1,
                    dollar,
                    DebeziumEnvelopeTest.offset()))
        .isInstanceOf(NameCollisionException.class)
        .hasMessageContaining("Tables FREEPDB1.APP.T# and FREEPDB1.APP.T$");
  }
}
