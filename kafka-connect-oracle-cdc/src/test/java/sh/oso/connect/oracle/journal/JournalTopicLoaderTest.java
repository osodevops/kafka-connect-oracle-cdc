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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.buffer.JournalChunk;
import sh.oso.connect.oracle.core.buffer.JournalFrames;
import sh.oso.connect.oracle.core.errors.JournalCorruptionException;
import sh.oso.connect.oracle.core.model.Operation;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;

class JournalTopicLoaderTest {

  static final String TOPIC = "cdc.cdc.txjournal";
  static final TableId T = new TableId("FREEPDB1", "APP", "T");
  final JournalRecords records = new JournalRecords(TOPIC, "cdc", Map.of("server", "cdc"));
  final JsonConverter keys = JournalRecordsTest.converter(true, true);
  final JsonConverter values = JournalRecordsTest.converter(true, false);
  final List<ConsumerRecord<byte[], byte[]>> topic = new ArrayList<>();

  static TxKey tx(int n) {
    return new TxKey(3, new Xid(n, 1, 100 + n));
  }

  static RowChange change(TxKey tx, long scn) {
    return new RowChange(
        T,
        Operation.INSERT,
        null,
        Map.of("ID", BigDecimal.valueOf(scn)),
        false,
        "r" + scn,
        new RedoRecordId(scn, "rs", 0),
        tx,
        null);
  }

  static JournalChunk chunk(TxKey tx, int n, long gen, long... scns) {
    List<JournalFrames.Frame> frames = new ArrayList<>();
    for (long s : scns) {
      frames.add(JournalFrames.Frame.of(change(tx, s)));
    }
    return new JournalChunk(
        tx,
        n,
        gen,
        new RedoRecordId(scns[0], "rs", 0),
        new RedoRecordId(scns[scns.length - 1], "rs", 0),
        scns.length,
        0,
        new RedoRecordId(scns[0], "rs", 0),
        null,
        1,
        "APP",
        null,
        JournalFrames.encode(frames));
  }

  void publish(SourceRecord r) {
    byte[] k = keys.fromConnectData(TOPIC, r.keySchema(), r.key());
    byte[] v = r.value() == null ? null : values.fromConnectData(TOPIC, r.valueSchema(), r.value());
    topic.add(new ConsumerRecord<>(TOPIC, 0, topic.size(), k, v));
  }

  Position position(long resume, long generation) {
    return Position.initial(resume, new DatabaseIdentity(1, 1)).withJournalGeneration(generation);
  }

  @Test
  void restoresContiguousChunksAndDropsReminedFramesAndStaleGenerations() {
    TxKey a = tx(1);
    TxKey b = tx(2);
    publish(records.chunk(chunk(a, 0, 1, 100, 101, 102), null));
    publish(records.chunk(chunk(a, 1, 1, 150, 151), null));
    publish(records.chunk(chunk(a, 2, 1, 200, 260), null)); // straddles the resume SCN 250
    publish(records.chunk(chunk(a, 3, 1, 300), null)); // entirely re-mined: stale
    publish(records.chunk(chunk(a, 2, 2, 200, 201), null)); // newer generation never acknowledged
    publish(records.chunk(chunk(b, 0, 1, 120), null));
    publish(records.tombstone(new JournalRecords.ChunkKey(b, 0, 1), null)); // b ended
    JournalTopicLoader.Loaded loaded =
        new JournalTopicLoader(keys, values, "cdc").load(topic, position(250, 1));
    assertThat(loaded.restore().keySet()).containsExactly(a);
    List<JournalChunk> chunks = loaded.restore().get(a);
    assertThat(chunks).extracting(JournalChunk::chunk).containsExactly(0, 1, 2);
    assertThat(chunks.get(2).events()).as("frame 260 dropped").isEqualTo(1);
    assertThat(chunks.get(2).last().scn()).isEqualTo(200);
    assertThat(loaded.chunks()).isEqualTo(3);
    assertThat(loaded.stale())
        .containsExactlyInAnyOrder(
            new JournalRecords.ChunkKey(a, 3, 1), new JournalRecords.ChunkKey(a, 2, 2));
  }

  @Test
  void aGapInChunkNumbersIsACorruptionStop() {
    TxKey a = tx(1);
    publish(records.chunk(chunk(a, 0, 1, 100), null));
    publish(records.chunk(chunk(a, 2, 1, 120), null));
    assertThatThrownBy(
            () -> new JournalTopicLoader(keys, values, "cdc").load(topic, position(500, 1)))
        .isInstanceOf(JournalCorruptionException.class)
        .hasMessageContaining("missing chunk 1");
  }

  @Test
  void recordsOfAnotherConnectorAndUnreadableRecordsAreHandled() {
    JournalRecords other = new JournalRecords(TOPIC, "other", Map.of("server", "other"));
    publish(other.chunk(chunk(tx(1), 0, 1, 100), null));
    JournalTopicLoader.Loaded loaded =
        new JournalTopicLoader(keys, values, "cdc").load(topic, position(500, 1));
    assertThat(loaded.restore()).isEmpty();
    assertThat(loaded.stale()).isEmpty();
    topic.add(new ConsumerRecord<>(TOPIC, 0, 5, "garbage".getBytes(), "x".getBytes()));
    assertThatThrownBy(
            () -> new JournalTopicLoader(keys, values, "cdc").load(topic, position(500, 1)))
        .isInstanceOf(JournalCorruptionException.class)
        .hasMessageContaining("cdc.journal.converter");
  }

  @Test
  void aWholeChunkBeforeTheResumeScnIsKeptUntouched() {
    TxKey a = tx(1);
    JournalChunk c = chunk(a, 0, 1, 100, 101);
    // ids with the same redo byte address placeholder fall back to SCN order (RedoRecordId)
    assertThat(JournalTopicLoader.trim(c, new RedoRecordId(102, "rs", 0))).isSameAs(c);
    assertThat(JournalTopicLoader.trim(c, new RedoRecordId(100, "rs", 0))).isNull();
    JournalChunk t = JournalTopicLoader.trim(c, new RedoRecordId(101, "rs", 0));
    assertThat(t.events()).isEqualTo(1);
    assertThat(t.last().scn()).isEqualTo(100);
  }

  @Test
  void theReloadBoundaryFollowsRedoOrderWhenBothSidesCarryAnAddress() {
    // ADR-0014: a frame written after the resume point is re-mined even when its SCN is lower
    RedoRecordId resume = new RedoRecordId(500, " 0x000001.00000100.0000 ", 0);
    assertThat(
            JournalTopicLoader.before(new RedoRecordId(400, " 0x000001.00000120.0000 ", 0), resume))
        .as("later in the log, lower SCN: mining produces it again")
        .isFalse();
    assertThat(
            JournalTopicLoader.before(new RedoRecordId(600, " 0x000001.00000090.0000 ", 0), resume))
        .as("earlier in the log, higher SCN: only the journal has it")
        .isTrue();
    assertThat(JournalTopicLoader.before(new RedoRecordId(400, null, 0), resume))
        .as("without an address the SCN decides")
        .isTrue();
  }

  @Test
  void theDefaultReaderAcceptsRecordsWrittenWithOrWithoutTheSchemaEnvelope() {
    TxKey a = tx(1);
    JsonConverter plainKeys = JournalRecordsTest.converter(false, true);
    JsonConverter plainValues = JournalRecordsTest.converter(false, false);
    SourceRecord r = records.chunk(chunk(a, 0, 1, 100, 101), null);
    byte[] k = plainKeys.fromConnectData(TOPIC, r.keySchema(), r.key());
    byte[] v = plainValues.fromConnectData(TOPIC, r.valueSchema(), r.value());
    topic.add(new ConsumerRecord<>(TOPIC, 0, 0, k, v));
    SourceRecord r2 = records.chunk(chunk(a, 1, 1, 102), null);
    topic.add(
        new ConsumerRecord<>(
            TOPIC,
            0,
            1,
            keys.fromConnectData(TOPIC, r2.keySchema(), r2.key()),
            values.fromConnectData(TOPIC, r2.valueSchema(), r2.value())));
    TolerantJsonConverter tk = new TolerantJsonConverter();
    tk.configure(Map.of(), true);
    TolerantJsonConverter tv = new TolerantJsonConverter();
    tv.configure(Map.of(), false);
    JournalTopicLoader.Loaded loaded =
        new JournalTopicLoader(tk, tv, "cdc").load(topic, position(500, 1));
    assertThat(loaded.restore().get(a)).extracting(JournalChunk::chunk).containsExactly(0, 1);
    // pinned to one mode it is strict, like the plain converter
    TolerantJsonConverter strict = new TolerantJsonConverter();
    strict.configure(Map.of("schemas.enable", "true"), true);
    assertThatThrownBy(() -> strict.toConnectData(TOPIC, k))
        .isInstanceOf(org.apache.kafka.connect.errors.DataException.class);
  }
}
