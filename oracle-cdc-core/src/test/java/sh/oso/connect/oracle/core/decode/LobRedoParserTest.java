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
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.erase;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.trim;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.where;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.write;
import static sh.oso.connect.oracle.core.testkit.LobRedoShapes.writeBytes;

import java.math.BigDecimal;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.errors.DecodeException;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowIds;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;
import sh.oso.connect.oracle.core.testkit.LobRedoShapes;

class LobRedoParserTest {

  @Test
  void writeTrimAndEraseBlocksParseIntoTheirCalls() {
    LobRedo w = SqlRedoParser.parseLob(write("C", where(2, "two"), 1023, "it's"));
    assertThat(w.owner()).isEqualTo("APP");
    assertThat(w.table()).isEqualTo("DOCS");
    assertThat(w.column()).isEqualTo("C");
    assertThat(w.where()).extracting(ColumnValue::column).containsExactly("ID", "NAME");
    assertThat(w.ops()).hasSize(1);
    LobRedo.Write op = (LobRedo.Write) w.ops().get(0);
    assertThat(op.amount()).isEqualTo(4);
    assertThat(op.offset()).isEqualTo(1023);
    assertThat(op.data().value()).isEqualTo("it's");

    LobRedo b = SqlRedoParser.parseLob(writeBytes(where(2, "two"), 1, new byte[] {(byte) 0xca, 1}));
    assertThat(((LobRedo.Write) b.ops().get(0)).data().kind()).isEqualTo(SqlLiteral.Kind.HEXTORAW);

    assertThat(SqlRedoParser.parseLob(trim("C", where(2, "two"), 7000)).ops())
        .containsExactly(new LobRedo.Trim(7000));
    assertThat(SqlRedoParser.parseLob(erase("NC", where(2, "two"), 10, 1)).ops())
        .containsExactly(new LobRedo.Erase(10, 1));
  }

  @Test
  void malformedBlocksAreDecodeErrors() {
    assertThatThrownBy(() -> SqlRedoParser.parseLob(null)).isInstanceOf(DecodeException.class);
    assertThatThrownBy(() -> SqlRedoParser.parseLob("DECLARE x CLOB;"))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining("BEGIN");
    String noBuffer = write("C", where(1, "a"), 1, "x").replace(" buf_c := 'x'; ", "");
    assertThatThrownBy(() -> SqlRedoParser.parseLob(noBuffer))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining("never assigned");
    String other = trim("C", where(1, "a"), 3).replace("dbms_lob.trim", "dbms_lob.append");
    assertThatThrownBy(() -> SqlRedoParser.parseLob(other))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining("dbms_lob.append");
    String locatorOnly =
        "DECLARE \n loc_c CLOB; \nBEGIN\n select \"C\" into loc_c from \"APP\".\"DOCS\" where"
            + " \"ID\" = '1' for update;\nEND;";
    assertThatThrownBy(() -> SqlRedoParser.parseLob(locatorOnly))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining("no dbms_lob call");
    String badAssign = write("C", where(1, "a"), 1, "x").replace(":=", "=");
    assertThatThrownBy(() -> SqlRedoParser.parseLob(badAssign)).isInstanceOf(DecodeException.class);
  }

  @Test
  void decodedFragmentsCarryTypedDataAndTheRowImage() {
    LobFragment f = RowDecoder.decodeLob(dml(write("NC", where(6, "both"), 1, "é€'")), SCHEMA);
    assertThat(f.table()).isEqualTo(LobRedoShapes.DOCS);
    assertThat(f.binary()).isFalse();
    assertThat(f.where()).containsEntry("ID", new BigDecimal(6)).containsEntry("NAME", "both");
    assertThat(f.edits()).containsExactly(new LobFragment.Write(1, "é€'"));
    LobFragment b =
        RowDecoder.decodeLob(dml(writeBytes(where(6, "both"), 5, new byte[] {1, 2, 3})), SCHEMA);
    assertThat(b.binary()).isTrue();
    assertThat((byte[]) ((LobFragment.Write) b.edits().get(0)).data()).containsExactly(1, 2, 3);
  }

  @Test
  void aBufferThatDoesNotMatchItsAmountOrTheWrongTableStops() {
    String lying =
        write("C", where(1, "a"), 1, "abc").replace("write(loc_c, 3,", "write(loc_c, 4,");
    assertThatThrownBy(() -> RowDecoder.decodeLob(dml(lying), SCHEMA))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining("declares 4 characters");
    String elsewhere = write("C", where(1, "a"), 1, "x").replace("\"DOCS\"", "\"OTHER\"");
    assertThatThrownBy(() -> RowDecoder.decodeLob(dml(elsewhere), SCHEMA))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining("APP.OTHER");
    String notLob = write("C", where(1, "a"), 1, "x").replace("select \"C\"", "select \"NAME\"");
    assertThatThrownBy(() -> RowDecoder.decodeLob(dml(notLob), SCHEMA))
        .isInstanceOf(DecodeException.class)
        .hasMessageContaining("not a LOB column");
  }

  static MiningEvent.Dml dml(String sql) {
    return new MiningEvent.Dml(
        new TxKey(3, new Xid(1, 1, 1)),
        new RedoRecordId(100, "0x1.2.3", 0),
        1,
        Operation.LOB_WRITE,
        LobRedoShapes.DOCS,
        100,
        100,
        1,
        RowIds.PLACEHOLDER,
        sql,
        null,
        false,
        2,
        "LOB sql_redo not re-executable",
        "APP",
        Instant.EPOCH);
  }
}
