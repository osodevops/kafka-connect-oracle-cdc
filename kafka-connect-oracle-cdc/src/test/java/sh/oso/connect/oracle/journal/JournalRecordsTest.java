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
package sh.oso.connect.oracle.journal;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.buffer.JournalChunk;
import sh.oso.connect.oracle.core.buffer.JournalFrames;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.position.PositionCodec;

class JournalRecordsTest {

  static final TxKey TX = new TxKey(3, new Xid(5, 1, 735));
  static final TableId T = new TableId("FREEPDB1", "APP", "T");

  static JournalChunk chunk() {
    RowChange c =
        new RowChange(
            T,
            Operation.INSERT,
            null,
            Map.of("ID", BigDecimal.ONE, "V", "x"),
            false,
            "AAAr",
            new RedoRecordId(101, " 0x0001.00000002.0010 ", 0),
            TX,
            null);
    List<JournalFrames.Frame> frames =
        List.of(
            JournalFrames.Frame.of(c),
            JournalFrames.Frame.undo(new RedoRecordId(102, " 0x0001.00000003.0010 ", 1), "AAAr"));
    return new JournalChunk(
        TX,
        2,
        9,
        new RedoRecordId(101, " 0x0001.00000002.0010 ", 0),
        new RedoRecordId(102, " 0x0001.00000003.0010 ", 1),
        1,
        1,
        new RedoRecordId(90, "first", 0),
        new RedoRecordId(80, "start", 0),
        1,
        "APP",
        "client",
        JournalFrames.encode(frames));
  }

  static JsonConverter converter(boolean schemas, boolean isKey) {
    JsonConverter c = new JsonConverter();
    c.configure(Map.of("schemas.enable", Boolean.toString(schemas)), isKey);
    return c;
  }

  @Test
  void chunkRecordRoundTripsThroughTheJsonConverterInBothModes() {
    JournalRecords r = new JournalRecords("cdc.cdc.txjournal", "cdc", Map.of("server", "cdc"));
    Position p = Position.initial(5000, new DatabaseIdentity(77, 1));
    SourceRecord rec = r.chunk(chunk(), p);
    assertThat(rec.topic()).isEqualTo("cdc.cdc.txjournal");
    assertThat(PositionCodec.read(rec.sourceOffset())).isEqualTo(p);
    for (boolean schemas : new boolean[] {true, false}) {
      JsonConverter keys = converter(schemas, true);
      JsonConverter values = converter(schemas, false);
      byte[] k = keys.fromConnectData(rec.topic(), rec.keySchema(), rec.key());
      byte[] v = values.fromConnectData(rec.topic(), rec.valueSchema(), rec.value());
      SchemaAndValue kk = keys.toConnectData(rec.topic(), k);
      SchemaAndValue vv = values.toConnectData(rec.topic(), v);
      JournalRecords.ChunkKey key = JournalRecords.parseKey(kk.value(), "cdc");
      assertThat(key).isEqualTo(new JournalRecords.ChunkKey(TX, 2, 9));
      assertThat(JournalRecords.parseKey(kk.value(), "other"))
          .as("another connector's record")
          .isNull();
      JournalChunk back = JournalRecords.parseValue(key, vv.value());
      JournalChunk orig = chunk();
      assertThat(back.first()).isEqualTo(orig.first());
      assertThat(back.last()).isEqualTo(orig.last());
      assertThat(back.events()).isEqualTo(1);
      assertThat(back.undos()).isEqualTo(1);
      assertThat(back.firstCaptured()).isEqualTo(orig.firstCaptured());
      assertThat(back.startId()).isEqualTo(orig.startId());
      assertThat(back.thread()).isEqualTo(1);
      assertThat(back.username()).isEqualTo("APP");
      assertThat(back.clientId()).isEqualTo("client");
      assertThat(back.payload()).containsExactly(orig.payload());
      List<JournalFrames.Frame> frames = JournalFrames.decode(back.payload(), "t");
      assertThat(frames).hasSize(2);
      assertThat(frames.get(0).change().after()).containsEntry("V", "x");
      assertThat(frames.get(1).undoRowId()).isEqualTo("AAAr");
    }
  }

  @Test
  void tombstoneHasTheSameKeyAndNoValue() {
    JournalRecords r = new JournalRecords("t", "cdc", Map.of("server", "cdc"));
    SourceRecord rec = r.tombstone(new JournalRecords.ChunkKey(TX, 2, 9), null);
    assertThat(rec.value()).isNull();
    assertThat(rec.valueSchema()).isNull();
    assertThat(rec.sourceOffset()).isNull();
    JsonConverter keys = converter(true, true);
    byte[] k = keys.fromConnectData("t", rec.keySchema(), rec.key());
    assertThat(JournalRecords.parseKey(keys.toConnectData("t", k).value(), "cdc"))
        .isEqualTo(new JournalRecords.ChunkKey(TX, 2, 9));
  }
}
