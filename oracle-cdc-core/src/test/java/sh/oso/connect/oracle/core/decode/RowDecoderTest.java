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
package sh.oso.connect.oracle.core.decode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.errors.DecodeException;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.KeySource;
import sh.oso.connect.oracle.core.schema.OracleType;
import sh.oso.connect.oracle.core.schema.TableSchema;

class RowDecoderTest {

  private static final TableId T = new TableId("FREEPDB1", "APP", "ORDERS");

  private static TableSchema schema(boolean allColumns) {
    return new TableSchema(
        T,
        List.of(
            ColumnSpec.of("ID", 1, OracleType.NUMBER),
            ColumnSpec.of("NAME", 2, OracleType.VARCHAR2),
            ColumnSpec.of("AMOUNT", 3, OracleType.NUMBER)),
        List.of("ID"),
        KeySource.PRIMARY_KEY,
        allColumns,
        !allColumns);
  }

  private static MiningEvent.Dml dml(Operation op, String sql, int status) {
    return new MiningEvent.Dml(
        new TxKey(3, new Xid(1, 2, 3)),
        new RedoRecordId(500, "0x01", 0),
        1,
        op,
        T,
        1001,
        1001,
        1,
        "AAAR",
        sql,
        null,
        false,
        status,
        null,
        "APP",
        Instant.EPOCH);
  }

  @Test
  void insertUpdateDeleteWithFullBeforeImages() {
    RowChange ins =
        RowDecoder.decode(
            dml(
                Operation.INSERT,
                "insert into \"APP\".\"ORDERS\"(\"ID\",\"NAME\",\"AMOUNT\") values"
                    + " ('1','a','10.5')",
                0),
            schema(true));
    assertThat(ins.before()).isNull();
    assertThat(ins.after())
        .containsExactly(
            java.util.Map.entry("ID", new BigDecimal("1")),
            java.util.Map.entry("NAME", "a"),
            java.util.Map.entry("AMOUNT", new BigDecimal("10.5")));
    assertThat(ins.partial()).isFalse();
    assertThat(ins.rowId()).isEqualTo("AAAR");
    assertThat(ins.tx()).isEqualTo(new TxKey(3, new Xid(1, 2, 3)));

    RowChange upd =
        RowDecoder.decode(
            dml(
                Operation.UPDATE,
                "update \"APP\".\"ORDERS\" set \"AMOUNT\" = '11' where \"ID\" = '1' and \"NAME\" ="
                    + " 'a' and \"AMOUNT\" = '10.5'",
                0),
            schema(true));
    assertThat(upd.before()).containsEntry("AMOUNT", new BigDecimal("10.5"));
    assertThat(upd.after())
        .containsEntry("AMOUNT", new BigDecimal("11"))
        .containsEntry("NAME", "a");
    assertThat(upd.after().keySet()).containsExactly("ID", "NAME", "AMOUNT");
    assertThat(upd.partial()).isFalse();

    RowChange del =
        RowDecoder.decode(
            dml(
                Operation.DELETE,
                "delete from \"APP\".\"ORDERS\" where \"ID\" = '1' and \"NAME\" IS NULL and"
                    + " \"AMOUNT\" = '11'",
                0),
            schema(true));
    assertThat(del.after()).isNull();
    assertThat(del.before()).containsEntry("NAME", null).containsKey("NAME");
    assertThat(del.partial()).isFalse();
  }

  @Test
  void primaryKeyOnlyLoggingYieldsPartialBeforeImages() {
    RowChange upd =
        RowDecoder.decode(
            dml(
                Operation.UPDATE,
                "update \"APP\".\"ORDERS\" set \"AMOUNT\" = '11' where \"ID\" = '1' and \"AMOUNT\""
                    + " = '10.5'",
                0),
            schema(false));
    assertThat(upd.partial()).isTrue();
    assertThat(upd.before().keySet()).containsExactly("ID", "AMOUNT");
    assertThat(upd.after().keySet()).containsExactly("ID", "AMOUNT");
    RowChange undo =
        RowDecoder.decode(
            dml(
                Operation.DELETE,
                "delete from \"APP\".\"ORDERS\" where \"ID\" = '1' and ROWID = 'AAAX'",
                0),
            schema(false));
    assertThat(undo.rowId()).isEqualTo("AAAX");
    assertThat(RowDecoder.fullBeforeImagePossible(schema(false), Operation.UPDATE)).isFalse();
    assertThat(RowDecoder.fullBeforeImagePossible(schema(false), Operation.INSERT)).isTrue();
  }

  @Test
  void refusesUnknownColumnsStatusTwoAndMismatchedOperations() {
    assertThatThrownBy(
            () ->
                RowDecoder.decode(
                    dml(
                        Operation.INSERT,
                        "insert into \"APP\".\"ORDERS\"(\"ID\",\"COL 2\") values ('1','x')",
                        0),
                    schema(true)))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining("COL 2");
    assertThatThrownBy(
            () -> RowDecoder.decode(dml(Operation.INSERT, "insert ...", 2), schema(true)))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining("STATUS 2");
    assertThatThrownBy(
            () ->
                RowDecoder.decode(
                    dml(Operation.UPDATE, "insert into \"APP\".\"ORDERS\"(\"ID\") values ('1')", 0),
                    schema(true)))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining("OPERATION_CODE");
  }
}
