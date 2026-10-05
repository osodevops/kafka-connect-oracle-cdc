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

import java.math.BigDecimal;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfig;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfigTest;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.KeySource;
import sh.oso.connect.oracle.core.schema.OracleType;
import sh.oso.connect.oracle.core.schema.TableSchema;

class DebeziumEnvelopeTest {

  static final TableId T = new TableId("FREEPDB1", "APP", "ORDERS");
  static final TxKey K = new TxKey(3, new Xid(7, 1, 42));

  static TableSchema schema(KeySource source) {
    return new TableSchema(
        T,
        List.of(
            new ColumnSpec("ID", 1, OracleType.NUMBER, "NUMBER", 0, 9, 0, false),
            new ColumnSpec("NAME", 2, OracleType.VARCHAR2, "VARCHAR2", 100, -1, -1, true),
            new ColumnSpec("AMOUNT", 3, OracleType.NUMBER, "NUMBER", 0, 10, 2, true)),
        source == KeySource.PRIMARY_KEY ? List.of("ID") : List.of(),
        source,
        true,
        false);
  }

  static Map<String, Object> row(int id, String name, String amount) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put("ID", new BigDecimal(id));
    m.put("NAME", name);
    m.put("AMOUNT", amount == null ? null : new BigDecimal(amount));
    return m;
  }

  static RowChange change(
      Operation op, Map<String, Object> before, Map<String, Object> after, long scn) {
    return new RowChange(
        T,
        op,
        before,
        after,
        false,
        "AAAR",
        new RedoRecordId(scn, " 0x01 ", 0),
        K,
        Instant.ofEpochSecond(100));
  }

  static CommittedTransaction tx(RowChange... changes) {
    return new CommittedTransaction(
        K,
        changes[0].id(),
        null,
        new RedoRecordId(900, "0x9", 0),
        Instant.ofEpochSecond(200),
        1,
        "APP",
        null,
        List.of(changes));
  }

  static DebeziumEnvelope envelope(boolean tombstones) {
    Map<String, String> p = OracleCdcSourceConnectorConfigTest.minimal();
    p.put(OracleCdcSourceConnectorConfig.TOMBSTONES_ON_DELETE, Boolean.toString(tombstones));
    OracleCdcSourceConnectorConfig cfg = new OracleCdcSourceConnectorConfig(p);
    return new DebeziumEnvelope(
        cfg, new TopicRouter(cfg.topicTemplate(true), "cdc", "FREE"), "FREE");
  }

  static Position offset() {
    return Position.initial(500, new DatabaseIdentity(1, 1));
  }

  @Test
  void createUpdateDeleteWithTombstoneHeadersAndSource() {
    DebeziumEnvelope e = envelope(true);
    CommittedTransaction tx =
        tx(
            change(Operation.INSERT, null, row(1, "a", "10.50"), 801),
            change(Operation.UPDATE, row(1, "a", "10.50"), row(1, "b", "10.50"), 802),
            change(Operation.DELETE, row(1, "b", "10.50"), null, 803));
    Position off = offset().withCommit(900, 1, K, 1);
    List<SourceRecord> c = e.records(tx, 0, 1, schema(KeySource.PRIMARY_KEY), off);
    assertThat(c).hasSize(1);
    SourceRecord r = c.get(0);
    assertThat(r.topic()).isEqualTo("cdc.FREEPDB1.APP.ORDERS");
    assertThat(((Struct) r.key()).getInt32("ID")).isEqualTo(1);
    assertThat(r.keySchema().name()).isEqualTo("cdc.FREEPDB1.APP.ORDERS.Key");
    Struct v = (Struct) r.value();
    assertThat(v.schema().name()).isEqualTo("cdc.FREEPDB1.APP.ORDERS.Envelope");
    assertThat(v.getString("op")).isEqualTo("c");
    assertThat(v.getStruct("before")).isNull();
    assertThat(v.getStruct("after").getString("NAME")).isEqualTo("a");
    assertThat(v.getStruct("after").get("AMOUNT")).isEqualTo(new BigDecimal("10.50"));
    assertThat(v.getInt64("ts_ms")).isEqualTo(200_000L);
    Struct source = v.getStruct("source");
    assertThat(source.getString("connector")).isEqualTo("oracle-cdc");
    assertThat(source.getString("name")).isEqualTo("cdc");
    assertThat(source.getString("db")).isEqualTo("FREEPDB1");
    assertThat(source.getString("schema")).isEqualTo("APP");
    assertThat(source.getString("table")).isEqualTo("ORDERS");
    assertThat(source.getString("txId")).isEqualTo("7.1.42");
    assertThat(source.getString("scn")).isEqualTo("801");
    assertThat(source.getString("commit_scn")).isEqualTo("900");
    assertThat(source.getString("snapshot")).isEqualTo("false");
    assertThat(source.getString("user_name")).isEqualTo("APP");
    assertThat(v.getStruct("transaction").getString("id")).isEqualTo("7.1.42");
    assertThat(v.getStruct("transaction").getInt64("total_order")).isEqualTo(1L);
    assertThat(r.headers().lastWithName("cdc.scn").value()).isEqualTo(801L);
    assertThat(r.headers().lastWithName("cdc.commit_scn").value()).isEqualTo(900L);
    assertThat(r.headers().lastWithName("cdc.xid").value()).isEqualTo("7.1.42");
    assertThat(r.headers().lastWithName("cdc.event_index").value()).isEqualTo(0);
    assertThat(r.headers().lastWithName("cdc.event_count").value()).isEqualTo(3);
    assertThat(r.sourcePartition()).isEqualTo(Map.of("server", "cdc"));
    Position encoded = sh.oso.connect.oracle.core.position.PositionCodec.read(r.sourceOffset());
    assertThat(encoded.resumeScn()).isEqualTo(500);
    assertThat(encoded.eventIndex()).isEqualTo(1);
    assertThat(encoded.lastCommitKey()).isEqualTo(K);

    List<SourceRecord> u = e.records(tx, 1, 2, schema(KeySource.PRIMARY_KEY), off);
    assertThat(u).hasSize(1);
    Struct uv = (Struct) u.get(0).value();
    assertThat(uv.getString("op")).isEqualTo("u");
    assertThat(uv.getStruct("before").getString("NAME")).isEqualTo("a");
    assertThat(uv.getStruct("after").getString("NAME")).isEqualTo("b");
    assertThat(uv.getStruct("transaction").getInt64("data_collection_order")).isEqualTo(2L);

    List<SourceRecord> d = e.records(tx, 2, 3, schema(KeySource.PRIMARY_KEY), off);
    assertThat(d).hasSize(2);
    assertThat(((Struct) d.get(0).value()).getString("op")).isEqualTo("d");
    assertThat(((Struct) d.get(0).value()).getStruct("after")).isNull();
    assertThat(d.get(1).value()).isNull();
    assertThat(d.get(1).valueSchema()).isNull();
    assertThat(d.get(1).key()).isEqualTo(d.get(0).key());
    assertThat(envelope(false).records(tx, 2, 3, schema(KeySource.PRIMARY_KEY), off)).hasSize(1);
  }

  @Test
  void keyChangeBecomesDeleteTombstoneCreateAndRowidKeys() {
    DebeziumEnvelope e = envelope(true);
    CommittedTransaction tx = tx(change(Operation.UPDATE, row(1, "a", "1"), row(2, "a", "1"), 801));
    List<SourceRecord> out = e.records(tx, 0, 1, schema(KeySource.PRIMARY_KEY), offset());
    assertThat(out).hasSize(3);
    assertThat(((Struct) out.get(0).value()).getString("op")).isEqualTo("d");
    assertThat(((Struct) out.get(0).key()).getInt32("ID")).isEqualTo(1);
    assertThat(out.get(1).value()).isNull();
    assertThat(((Struct) out.get(2).value()).getString("op")).isEqualTo("c");
    assertThat(((Struct) out.get(2).key()).getInt32("ID")).isEqualTo(2);

    List<SourceRecord> rowid = e.records(tx, 0, 1, schema(KeySource.ROWID), offset());
    assertThat(rowid).hasSize(1);
    assertThat(((Struct) rowid.get(0).key()).getString("ROWID")).isEqualTo("AAAR");
    assertThat(((Struct) rowid.get(0).value()).getString("op")).isEqualTo("u");

    List<SourceRecord> none = e.records(tx, 0, 1, schema(KeySource.NONE), offset());
    assertThat(none).hasSize(1);
    assertThat(none.get(0).key()).isNull();
    assertThat(none.get(0).keySchema()).isNull();
  }

  @Test
  void partialBeforeImagesOmitAbsentColumnsAndNullKeyWhenMissing() {
    DebeziumEnvelope e = envelope(true);
    Map<String, Object> partial = new LinkedHashMap<>();
    partial.put("ID", new BigDecimal(5));
    partial.put("AMOUNT", new BigDecimal("3.00"));
    Map<String, Object> after = new LinkedHashMap<>(partial);
    after.put("AMOUNT", new BigDecimal("4.00"));
    RowChange c =
        new RowChange(
            T,
            Operation.UPDATE,
            partial,
            after,
            true,
            "AAAR",
            new RedoRecordId(1, "0x", 0),
            K,
            Instant.EPOCH);
    List<SourceRecord> out = e.records(tx(c), 0, 1, schema(KeySource.PRIMARY_KEY), offset());
    Struct before = ((Struct) out.get(0).value()).getStruct("before");
    assertThat(before.getString("NAME")).isNull();
    assertThat(before.get("AMOUNT")).isEqualTo(new BigDecimal("3.00"));
    Map<String, Object> noKey = new LinkedHashMap<>();
    noKey.put("NAME", "x");
    RowChange nk =
        new RowChange(
            T,
            Operation.DELETE,
            noKey,
            null,
            true,
            "AAAR",
            new RedoRecordId(2, "0x", 0),
            K,
            Instant.EPOCH);
    List<SourceRecord> del = e.records(tx(nk), 0, 1, schema(KeySource.PRIMARY_KEY), offset());
    assertThat(del).as("no key, so no tombstone").hasSize(1);
    assertThat(del.get(0).key()).isNull();
  }

  static DebeziumEnvelope lobEnvelope(String mode) {
    Map<String, String> p = OracleCdcSourceConnectorConfigTest.minimal();
    p.put(sh.oso.connect.oracle.core.config.CoreConfig.LOB_MODE, mode);
    OracleCdcSourceConnectorConfig cfg = new OracleCdcSourceConnectorConfig(p);
    return new DebeziumEnvelope(
        cfg, new TopicRouter(cfg.topicTemplate(true), "cdc", "FREE"), "FREE");
  }

  static final TableSchema DOCS = sh.oso.connect.oracle.core.testkit.LobRedoShapes.SCHEMA;

  static RowChange lobUpdate(Map<String, Object> after, String rowId) {
    Map<String, Object> before = Map.of("ID", java.math.BigDecimal.ONE, "NAME", "a");
    return new RowChange(
        DOCS.table(),
        Operation.UPDATE,
        before,
        after,
        false,
        rowId,
        new RedoRecordId(800, "0x8", 0),
        K,
        Instant.ofEpochSecond(100));
  }

  @Test
  void lobColumnsAreLeftOutInSkipModeAndUnavailableValuesCarryThePlaceholder() {
    Map<String, Object> after =
        new java.util.HashMap<>(Map.of("ID", java.math.BigDecimal.ONE, "NAME", "b", "C", "text"));
    RowChange c =
        lobUpdate(
            after, sh.oso.connect.oracle.core.model.RowIds.lobGroup("AAAR5FAAYAAAAANAAB", "1"));

    Struct skipped =
        (Struct) lobEnvelope("skip").records(tx(c), 0, 1, DOCS, offset()).get(0).value();
    assertThat(skipped.getStruct("after").schema().field("C")).isNull();
    assertThat(skipped.getStruct("after").getString("NAME")).isEqualTo("b");
    // a synthetic ROWID publishes only the real ROWID inside it
    assertThat(skipped.getStruct("source").getString("row_id")).isEqualTo("AAAR5FAAYAAAAANAAB");

    Struct inline =
        (Struct) lobEnvelope("inline").records(tx(c), 0, 1, DOCS, offset()).get(0).value();
    Struct a = inline.getStruct("after");
    assertThat(a.getString("C")).isEqualTo("text");
    assertThat(a.getString("NC")).isEqualTo("__cdc_unavailable_value");
    assertThat(new String(a.getBytes("B"), java.nio.charset.StandardCharsets.UTF_8))
        .isEqualTo("__cdc_unavailable_value");
    assertThat(inline.getStruct("before").getString("C")).isEqualTo("__cdc_unavailable_value");
    assertThat(inline.getStruct("source").getString("reselect")).isNull();

    Struct reselect =
        (Struct) lobEnvelope("reselect").records(tx(c), 0, 1, DOCS, offset()).get(0).value();
    assertThat(reselect.getStruct("source").getString("reselect")).isEqualTo("failed");
    RowChange complete =
        lobUpdate(
            Map.of(
                "ID",
                java.math.BigDecimal.ONE,
                "NAME",
                "b",
                "C",
                "t",
                "NC",
                "n",
                "B",
                new byte[] {1}),
            sh.oso.connect.oracle.core.model.RowIds.rowPiece(
                sh.oso.connect.oracle.core.model.RowIds.PLACEHOLDER, "2"));
    Struct ok =
        (Struct) lobEnvelope("reselect").records(tx(complete), 0, 1, DOCS, offset()).get(0).value();
    assertThat(ok.getStruct("source").getString("reselect")).isNull();
    assertThat(ok.getStruct("source").getString("row_id")).isNull();
  }

  @Test
  void anExcludedColumnHasNoFieldInRecordsOrTheirSchemas() {
    Map<String, String> p = OracleCdcSourceConnectorConfigTest.minimal();
    p.put(OracleCdcSourceConnectorConfig.COLUMNS_EXCLUDE, "FREEPDB1\\.APP\\.ORDERS\\.NAME");
    OracleCdcSourceConnectorConfig cfg = new OracleCdcSourceConnectorConfig(p);
    DebeziumEnvelope e =
        new DebeziumEnvelope(cfg, new TopicRouter(cfg.topicTemplate(true), "cdc", "FREE"), "FREE");
    // a change still carrying the column (written to a journal before the filter was configured)
    CommittedTransaction tx =
        tx(change(Operation.UPDATE, row(1, "secret", "1.00"), row(1, "secret2", "2.00"), 801));
    SourceRecord r =
        e.records(tx, 0, 1, schema(KeySource.PRIMARY_KEY), offset().withCommit(900, 1, K, 1))
            .get(0);
    Struct v = (Struct) r.value();
    org.apache.kafka.connect.data.Schema value = r.valueSchema().field("after").schema();
    assertThat(value.fields())
        .extracting(org.apache.kafka.connect.data.Field::name)
        .containsExactly("ID", "AMOUNT");
    assertThat(v.getStruct("before").schema().field("NAME")).isNull();
    assertThat(v.getStruct("after").get("AMOUNT")).isEqualTo(new BigDecimal("2.00"));
    assertThat(String.valueOf(v)).doesNotContain("secret");
    assertThat(((Struct) r.key()).get("ID")).isEqualTo(1);

    SourceRecord snap =
        e.snapshotRecord(
            T,
            new sh.oso.connect.oracle.core.snapshot.SnapshotRow(row(2, "secret", "3.00"), null),
            schema(KeySource.PRIMARY_KEY),
            700,
            1L,
            "first",
            offset());
    assertThat(snap.valueSchema().field("after").schema().field("NAME")).isNull();
    assertThat(String.valueOf(snap.value())).doesNotContain("secret");
  }
}
