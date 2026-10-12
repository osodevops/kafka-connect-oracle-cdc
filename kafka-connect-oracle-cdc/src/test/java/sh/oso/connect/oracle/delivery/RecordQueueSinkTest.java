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
package sh.oso.connect.oracle.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
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
import sh.oso.connect.oracle.core.position.PositionCodec;
import sh.oso.connect.oracle.core.position.ResumePoint;
import sh.oso.connect.oracle.core.position.ThreadMark;
import sh.oso.connect.oracle.core.schema.ColumnSpec;
import sh.oso.connect.oracle.core.schema.DictionaryReader;
import sh.oso.connect.oracle.core.schema.KeySelector;
import sh.oso.connect.oracle.core.schema.KeySource;
import sh.oso.connect.oracle.core.schema.OracleType;
import sh.oso.connect.oracle.core.schema.SchemaRegistry;
import sh.oso.connect.oracle.core.schema.TableSchema;
import sh.oso.connect.oracle.core.testkit.InMemorySchemaStore;
import sh.oso.connect.oracle.dlq.DecodeDlqWriter;
import sh.oso.connect.oracle.envelope.DebeziumEnvelope;
import sh.oso.connect.oracle.envelope.TopicRouter;
import sh.oso.connect.oracle.heartbeat.HeartbeatEmitter;
import sh.oso.connect.oracle.journal.JournalRecords;
import sh.oso.connect.oracle.ops.OpsEventWriter;

/** The offsets the sink gives each record: what Connect commits once the record is acknowledged. */
class RecordQueueSinkTest {

  static final TableId T = new TableId("FREEPDB1", "APP", "ORDERS");
  static final DatabaseIdentity IDENT = new DatabaseIdentity(77, 1);
  static final TxKey K1 = new TxKey(3, Xid.parse("0001.002.00000003"));
  static final TxKey K2 = new TxKey(3, Xid.parse("0004.005.00000006"));

  static RedoRecordId rba(long scn, long block) {
    return new RedoRecordId(scn, String.format(" 0x%06x.%08x.%04x ", 1, block, 0), 0);
  }

  static RecordQueueSink sink(Position base) {
    OracleCdcSourceConnectorConfig config =
        new OracleCdcSourceConnectorConfig(OracleCdcSourceConnectorConfigTest.minimal());
    InMemorySchemaStore store = new InMemorySchemaStore();
    store.save(
        new TableSchema(
            T,
            List.of(
                ColumnSpec.of("ID", 1, OracleType.NUMBER),
                ColumnSpec.of("NAME", 2, OracleType.VARCHAR2)),
            List.of("ID"),
            KeySource.PRIMARY_KEY,
            true,
            false));
    DictionaryReader none =
        new DictionaryReader() {
          public Optional<TableSchema> read(TableId t) {
            throw new IllegalStateException("the dictionary is not read: " + t);
          }

          public KeySelector.Candidates keyCandidates(TableId t) {
            throw new IllegalStateException("the dictionary is not read: " + t);
          }
        };
    SchemaRegistry registry =
        new SchemaRegistry(
            store, none, new KeySelector(Map.of(), KeySelector.MissingKeyPolicy.FAIL));
    DebeziumEnvelope envelope =
        new DebeziumEnvelope(
            config,
            new TopicRouter(config.topicTemplate(true), config.topicPrefix(), "FREE"),
            "FREE");
    return new RecordQueueSink(
        envelope,
        registry,
        base,
        1000,
        new HeartbeatEmitter(config.heartbeatTopic(), config.topicPrefix(), envelope.partition()),
        new OpsEventWriter(config.opsTopic(), config.topicPrefix(), envelope.partition()),
        new JournalRecords(config.journalTopic(), config.topicPrefix(), envelope.partition()),
        new DecodeDlqWriter(config.dlqTopic(), config.topicPrefix(), envelope.partition()),
        0,
        () -> 0L);
  }

  /** A committed transaction of {@code thread} with one insert per redo address in {@code rows}. */
  static CommittedTransaction tx(TxKey key, int thread, RedoRecordId commit, long... rows) {
    List<RowChange> events = new ArrayList<>();
    for (long block : rows) {
      Map<String, Object> after = Map.of("ID", new java.math.BigDecimal(block), "NAME", "n");
      events.add(
          new RowChange(
              T,
              Operation.INSERT,
              null,
              after,
              false,
              "AAA" + block,
              rba(commit.scn() - rows.length, block),
              key,
              Instant.EPOCH));
    }
    return new CommittedTransaction(
        key,
        events.get(0).id(),
        events.get(0).id(),
        commit,
        Instant.EPOCH,
        thread,
        "APP",
        null,
        events);
  }

  static List<Position> offsets(RecordQueueSink sink) throws InterruptedException {
    List<Position> out = new ArrayList<>();
    for (SourceRecord r : sink.drain(Integer.MAX_VALUE, 0)) {
      out.add(PositionCodec.read(r.sourceOffset()));
    }
    return out;
  }

  @Test
  void withOneThreadTheOffsetsAreThoseOfBefore() throws Exception {
    Position base = Position.initial(100, IDENT);
    RecordQueueSink sink = sink(base);
    CommittedTransaction a = tx(K1, 1, rba(130, 30), 20, 21);
    RedoRecordId candidate = rba(130, 30);
    sink.committed(a, 0, ResumePoint.single(1, candidate));
    List<SourceRecord> records = sink.drain(Integer.MAX_VALUE, 0);
    assertThat(records).hasSize(2);
    // the rule before ADR-0026: the first capture bounds the resume point until the last record
    assertThat(records.get(0).sourceOffset())
        .isEqualTo(
            PositionCodec.write(
                base.withCommit(a.commitId(), 1, K1, 1)
                    .withResume(RecordQueueSink.earlier(candidate, a.firstCaptured()))));
    assertThat(records.get(1).sourceOffset())
        .isEqualTo(
            PositionCodec.write(base.withCommit(a.commitId(), 1, K1, 2).withResume(candidate)))
        .doesNotContainKey("threads");
  }

  @Test
  void anUnfinishedTransactionHoldsBackOnlyItsOwnThread() throws Exception {
    RecordQueueSink sink = sink(Position.initial(100, IDENT));
    // thread 1 commits first; thread 2 is the quiet instance, its addresses far below thread 1's
    CommittedTransaction a = tx(K1, 1, rba(130, 30), 28);
    SortedMap<Integer, RedoRecordId> atA = new TreeMap<>();
    atA.put(1, rba(130, 30));
    atA.put(2, rba(130, 3));
    sink.committed(a, 0, ResumePoint.of(atA));
    CommittedTransaction b = tx(K2, 2, rba(140, 7), 5, 6);
    SortedMap<Integer, RedoRecordId> atB = new TreeMap<>();
    atB.put(1, rba(140, 31));
    atB.put(2, rba(140, 7));
    sink.committed(b, 0, ResumePoint.of(atB));

    List<Position> offsets = offsets(sink);
    assertThat(offsets).hasSize(3).allMatch(Position::perThread);

    Position afterA = offsets.get(0);
    assertThat(afterA.threads())
        .containsExactly(
            Map.entry(1, new ThreadMark(rba(130, 30), a.commitId(), K1)),
            Map.entry(2, new ThreadMark(rba(130, 3), null, null)));

    Position midB = offsets.get(1);
    assertThat(midB.threads().get(2))
        .as("B's first record bounds thread 2 while B is part delivered")
        .isEqualTo(new ThreadMark(rba(138, 5), b.commitId(), K2));
    assertThat(midB.threads().get(1))
        .as("thread 1 is neither held back by B nor loses A")
        .isEqualTo(new ThreadMark(rba(140, 31), a.commitId(), K1));
    assertThat(midB.resumeScn()).isEqualTo(138);
    assertThat(midB.resumeRsId()).isNull();
    assertThat(midB.lastCommitKey()).isEqualTo(K2);
    assertThat(midB.eventIndex()).isEqualTo(1);

    Position afterB = offsets.get(2);
    assertThat(afterB.threads())
        .containsExactly(
            Map.entry(1, new ThreadMark(rba(140, 31), a.commitId(), K1)),
            Map.entry(2, new ThreadMark(rba(140, 7), b.commitId(), K2)));
    assertThat(afterB.eventIndex()).isEqualTo(2);
  }

  @Test
  void aQuietHeartbeatCarriesTheResumePointOfEveryThread() throws Exception {
    RecordQueueSink sink = sink(Position.initial(100, IDENT));
    SortedMap<Integer, RedoRecordId> at = new TreeMap<>();
    at.put(1, rba(150, 40));
    at.put(2, rba(150, 9));
    sink.stepApplied(150, ResumePoint.of(at));
    assertThat(sink.lastResumeCandidate()).isEqualTo(150);
    assertThat(sink.lastResumePoint()).isEqualTo(new RedoRecordId(150, null, 0));
    sink.schemaTopic(
        new sh.oso.connect.oracle.schema.SchemaRecords.Writer("schemas", "cdc", Map.of()));
    sink.schemaRemoved(T); // any record that carries the safe position
    Position p = offsets(sink).get(0);
    assertThat(p.resume().byThread()).isEqualTo(at);
  }
}
