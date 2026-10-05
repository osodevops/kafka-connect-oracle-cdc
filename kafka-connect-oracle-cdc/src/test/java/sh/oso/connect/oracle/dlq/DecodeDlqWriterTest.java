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
package sh.oso.connect.oracle.dlq;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.buffer.TransactionBuffer;
import sh.oso.connect.oracle.core.errors.DecodeException;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.position.PositionCodec;

class DecodeDlqWriterTest {

  static final TxKey TX = new TxKey(3, new Xid(5, 1, 735));
  static final TableId T = new TableId("FREEPDB1", "APP", "ORDERS");
  final DecodeDlqWriter w = new DecodeDlqWriter("cdc.cdc.dlq", "cdc", Map.of("server", "cdc"));
  final Position p = Position.initial(5000, new DatabaseIdentity(77, 1));

  @Test
  void decodeErrorCarriesTheRawRedoAndTheException() {
    MiningEvent.Dml d =
        new MiningEvent.Dml(
            TX,
            new RedoRecordId(4990, " 0x000001.00000010.0010 ", 2),
            1,
            Operation.UPDATE,
            T,
            77,
            78,
            2,
            "AAAr",
            "update \"APP\".\"ORDERS\" set \"AMOUNT\" = '1' where ROWID = 'AAAr'",
            "update ... undo",
            false,
            0,
            null,
            "APP",
            Instant.EPOCH);
    SourceRecord r =
        w.decodeError(d, new DecodeException("unknown column AMOUNT", "refresh"), 9, p, 1234L);
    assertThat(r.topic()).isEqualTo("cdc.cdc.dlq");
    assertThat(PositionCodec.read(r.sourceOffset())).isEqualTo(p);
    assertThat(((Struct) r.key()).getString("xid")).isEqualTo("5.1.735");
    Struct v = (Struct) r.value();
    assertThat(v.getString("kind")).isEqualTo("decode-error");
    assertThat(v.getString("table")).isEqualTo("ORDERS");
    assertThat(v.getInt64("scn")).isEqualTo(4990L);
    assertThat(v.getString("rs_id")).isEqualTo(" 0x000001.00000010.0010 ");
    assertThat(v.getString("operation")).isEqualTo("UPDATE");
    assertThat(v.getString("sql_redo")).contains("AMOUNT");
    assertThat(v.getString("sql_undo")).isEqualTo("update ... undo");
    assertThat(v.getInt64("schema_version")).isEqualTo(9L);
    assertThat(v.getString("exception")).isEqualTo(DecodeException.class.getName());
    assertThat(v.getString("message")).contains("unknown column");
    assertThat(v.getString("user")).isEqualTo("APP");
    assertThat(v.get("events")).isNull();
  }

  @Test
  void unsupportedRowAndDiscardedTransactionHaveTheirOwnKinds() {
    MiningEvent.Unsupported u =
        new MiningEvent.Unsupported(
            TX,
            new RedoRecordId(4991, " 0x000001.00000011.0010 ", 0),
            T,
            77,
            2,
            "JSON column",
            "insert into \"APP\".\"ORDERS\" ...");
    Struct uv = (Struct) w.unsupported(u, p, 1L).value();
    assertThat(uv.getString("kind")).isEqualTo("unsupported-row");
    assertThat(uv.getInt32("status")).isEqualTo(2);
    assertThat(uv.getString("info")).isEqualTo("JSON column");
    assertThat(uv.getString("sql_redo")).startsWith("insert into");
    TransactionBuffer.OpenTransaction t =
        new TransactionBuffer.OpenTransaction(
            TX, "APP", "c", 4000, 4800, Instant.EPOCH, 11, 7, 42, 1000, 0, false);
    SourceRecord dr = w.discarded(t, Duration.ofMinutes(90), p, 2L);
    Struct dv = (Struct) dr.value();
    assertThat(dv.getString("kind")).isEqualTo("transaction-discarded");
    assertThat(dv.getInt64("first_scn")).isEqualTo(4000L);
    assertThat(dv.getInt64("last_scn")).isEqualTo(4800L);
    assertThat(dv.getInt32("events")).isEqualTo(42);
    assertThat(dv.getInt64("age_ms")).isEqualTo(Duration.ofMinutes(90).toMillis());
    assertThat(dv.get("sql_redo")).isNull();
  }
}
