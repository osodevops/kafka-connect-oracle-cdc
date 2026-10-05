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
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.SCHEMA;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.where;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.write;

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
import sh.oso.connect.oracle.core.schema.ColumnFilter;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.KeySource;
import sh.oso.connect.oracle.core.schema.OracleType;
import sh.oso.connect.oracle.core.schema.TableSchema;

/**
 * PRD-01 SRC-SEL-2 in the decoder: an excluded column is dropped once the parser has named it, so
 * its literal is never converted, never fails a row and never appears in a message.
 */
class ColumnFilterDecodeTest {

  static final TableId T = new TableId("FREEPDB1", "APP", "CUSTOMERS");
  static final ColumnFilter SSN = ColumnFilter.of(List.of(".*\\.CUSTOMERS\\.SSN"), false);
  static final String SECRET = "123-45-6789";

  static TableSchema schema() {
    return new TableSchema(
        T,
        List.of(
            ColumnSpec.of("ID", 1, OracleType.NUMBER),
            ColumnSpec.of("NAME", 2, OracleType.VARCHAR2),
            ColumnSpec.of("SSN", 3, OracleType.NUMBER)),
        List.of("ID"),
        KeySource.PRIMARY_KEY,
        true,
        false);
  }

  static MiningEvent.Dml dml(Operation op, String sql) {
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
        0,
        null,
        "APP",
        Instant.EPOCH);
  }

  @Test
  void anExcludedColumnIsAbsentFromBothImagesAndNeverConverted() {
    // SSN is a NUMBER column holding text: converting it would fail with the value in the message
    String insert =
        "insert into \"APP\".\"CUSTOMERS\"(\"ID\",\"NAME\",\"SSN\") values ('1','a','"
            + SECRET
            + "')";
    assertThatThrownBy(() -> RowDecoder.decode(dml(Operation.INSERT, insert), schema()))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining(SECRET);
    RowChange ins = RowDecoder.decode(dml(Operation.INSERT, insert), schema(), SSN);
    assertThat(ins.after()).containsOnlyKeys("ID", "NAME").containsEntry("NAME", "a");
    assertThat(ins.partial()).isFalse();

    RowChange upd =
        RowDecoder.decode(
            dml(
                Operation.UPDATE,
                "update \"APP\".\"CUSTOMERS\" set \"SSN\" = '"
                    + SECRET
                    + "', \"NAME\" = 'b' where \"ID\" = '1' and \"NAME\" = 'a' and \"SSN\" ="
                    + " 'old'"),
            schema(),
            SSN);
    assertThat(upd.before()).containsOnlyKeys("ID", "NAME");
    assertThat(upd.after()).containsOnlyKeys("ID", "NAME").containsEntry("NAME", "b");
    assertThat(upd.partial()).as("every published column is in the before image").isFalse();
  }

  @Test
  void anImageIsPartialOnlyWhenAPublishedColumnIsMissing() {
    String noSsn =
        "update \"APP\".\"CUSTOMERS\" set \"NAME\" = 'b' where \"ID\" = '1' and \"NAME\" = 'a'";
    assertThat(RowDecoder.decode(dml(Operation.UPDATE, noSsn), schema()).partial()).isTrue();
    assertThat(RowDecoder.decode(dml(Operation.UPDATE, noSsn), schema(), SSN).partial()).isFalse();
    assertThat(RowDecoder.nonLobColumns(schema(), SSN)).isEqualTo(2);
    String keyOnly = "delete from \"APP\".\"CUSTOMERS\" where \"ID\" = '1'";
    RowChange del = RowDecoder.decode(dml(Operation.DELETE, keyOnly), schema(), SSN);
    assertThat(del.partial()).isTrue();
    assertThat(del.before()).containsEntry("ID", BigDecimal.ONE);
  }

  @Test
  void aColumnTheSchemaDoesNotKnowStillStopsTheRowEvenWhenAPatternWouldExcludeIt() {
    ColumnFilter generic = ColumnFilter.of(List.of(".*\\.CUSTOMERS\\.COL 3"), false);
    assertThatThrownBy(
            () ->
                RowDecoder.decode(
                    dml(
                        Operation.INSERT,
                        "insert into \"APP\".\"CUSTOMERS\"(\"ID\",\"COL 3\") values ('1','x')"),
                    schema(),
                    generic))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining("COL 3");
  }

  @Test
  void aParseFailureOnATableWithExcludedColumnsDoesNotQuoteTheStatement() {
    String broken =
        "insert into \"APP\".\"CUSTOMERS\"(\"ID\",\"SSN\") values ('1','" + SECRET + "' junk";
    assertThatThrownBy(() -> RowDecoder.decode(dml(Operation.INSERT, broken), schema()))
        .isInstanceOf(DecodeException.class);
    assertThatThrownBy(() -> RowDecoder.decode(dml(Operation.INSERT, broken), schema(), SSN))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining("CDC-3001")
        .hasMessageContaining("FREEPDB1.APP.CUSTOMERS")
        .hasMessageContaining("withheld")
        .hasNoCause()
        .satisfies(e -> assertThat(e.getMessage()).doesNotContain(SECRET));
    // a filter that cannot match the table leaves the parser's message as it was (a pattern that
    // starts with .* reads every name to its end, so it counts as possibly matching any table)
    ColumnFilter elsewhere = ColumnFilter.of(List.of("FREEPDB1\\.APP\\.ORDERS\\..*"), false);
    assertThatThrownBy(() -> RowDecoder.decode(dml(Operation.INSERT, broken), schema(), elsewhere))
        .isInstanceOf(DecodeException.class)
        .satisfies(e -> assertThat(e.getMessage()).doesNotContain("withheld"));
  }

  @Test
  void aLobRowOfAnExcludedColumnKeepsItsShapeButNoData() {
    ColumnFilter clob = ColumnFilter.of(List.of(".*\\.DOCS\\.(C|NAME)"), false);
    // the declared amount does not match the buffer: never checked for an excluded column
    String lying =
        write("C", where(1, "a"), 3, "abc").replace("write(loc_c, 3,", "write(loc_c, 4,");
    assertThatThrownBy(() -> RowDecoder.decodeLob(lob(lying), SCHEMA))
        .isInstanceOf(DecodeException.class);
    LobFragment f = RowDecoder.decodeLob(lob(lying), SCHEMA, clob);
    assertThat(f.column()).isEqualTo("C");
    assertThat(f.edits()).containsExactly(new LobFragment.Write(3, ""));
    assertThat(f.where()).containsOnlyKeys("ID");

    // a published LOB column of the same table still carries its data, the row image filtered
    LobFragment nc = RowDecoder.decodeLob(lob(write("NC", where(1, "a"), 1, "xy")), SCHEMA, clob);
    assertThat(nc.edits()).containsExactly(new LobFragment.Write(1, "xy"));
    assertThat(nc.where()).containsOnlyKeys("ID");

    String broken = write("C", where(1, "a"), 1, SECRET).replace("BEGIN", "BEGAN");
    assertThatThrownBy(() -> RowDecoder.decodeLob(lob(broken), SCHEMA, clob))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining("withheld")
        .satisfies(e -> assertThat(e.getMessage()).doesNotContain(SECRET));
  }

  @Test
  void aBlobWriteOfAnExcludedColumnCarriesNoBytes() {
    ColumnFilter blob = ColumnFilter.of(List.of(".*\\.DOCS\\.B"), false);
    LobFragment b =
        RowDecoder.decodeLob(
            lob(
                sh.oso.connect.oracle.core.testkit.LobRedoShapes.writeBytes(
                    where(1, "a"), 5, new byte[] {1, 2, 3})),
            SCHEMA,
            blob);
    assertThat(b.binary()).isTrue();
    assertThat(b.edits()).hasSize(1);
    LobFragment.Write w = (LobFragment.Write) b.edits().get(0);
    assertThat(w.offset()).isEqualTo(5);
    assertThat((byte[]) w.data()).isEmpty();
  }

  static MiningEvent.Dml lob(String sql) {
    return LobRedoParserTest.dml(sql);
  }
}
