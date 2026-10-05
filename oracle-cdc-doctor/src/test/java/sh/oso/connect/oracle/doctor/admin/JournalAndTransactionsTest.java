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
package sh.oso.connect.oracle.doctor.admin;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.buffer.JournalChunk;
import sh.oso.connect.oracle.core.doctor.MetricsSample;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.position.PositionCodec;
import sh.oso.connect.oracle.doctor.testing.FakeEnv;
import sh.oso.connect.oracle.journal.JournalRecords;
import sh.oso.connect.oracle.journal.TolerantJsonConverter;

/** journal inspect and transactions over journal records written as the task writes them. */
class JournalAndTransactionsTest {

  static final String TOPIC = "cdc.cdc.txjournal";
  static final TxKey A = new TxKey(3, new Xid(7, 1, 2));
  static final TxKey B = new TxKey(3, new Xid(8, 4, 4));
  static final TxKey C = new TxKey(3, new Xid(9, 9, 9));
  static final TxKey BIG = new TxKey(3, new Xid(5, 12, 900));

  private static RedoRecordId id(long scn) {
    return new RedoRecordId(scn, null, 0);
  }

  static JournalChunk chunk(TxKey key, int n, long gen, long first, long last) {
    return new JournalChunk(
        key, n, gen, id(first), id(last), 10, 1, id(4000), null, 1, "BATCH", "etl", new byte[64]);
  }

  private static JsonConverter json(boolean isKey) {
    JsonConverter c = new JsonConverter();
    c.configure(Map.of("schemas.enable", "false"), isKey);
    return c;
  }

  static ConsumerRecord<byte[], byte[]> rec(long offset, SourceRecord r) {
    return new ConsumerRecord<>(
        TOPIC,
        0,
        offset,
        json(true).fromConnectData(TOPIC, r.keySchema(), r.key()),
        r.value() == null ? null : json(false).fromConnectData(TOPIC, r.valueSchema(), r.value()));
  }

  /** A restored, B stale (newer generation), C tombstoned, plus another connector's chunk. */
  static List<ConsumerRecord<byte[], byte[]>> journal(boolean gap) {
    JournalRecords mine = new JournalRecords(TOPIC, "cdc", Map.of());
    JournalRecords other = new JournalRecords(TOPIC, "elsewhere", Map.of());
    List<ConsumerRecord<byte[], byte[]>> out = new ArrayList<>();
    long o = 0;
    out.add(rec(o++, mine.chunk(chunk(A, 0, 1, 4000, 4100), null)));
    out.add(rec(o++, mine.chunk(chunk(A, gap ? 2 : 1, 1, 4100, 4200), null)));
    out.add(rec(o++, mine.chunk(chunk(B, 0, 3, 4300, 4400), null)));
    out.add(rec(o++, mine.chunk(chunk(C, 0, 1, 4500, 4600), null)));
    out.add(rec(o++, mine.tombstone(new JournalRecords.ChunkKey(C, 0, 1), null)));
    out.add(rec(o++, other.chunk(chunk(A, 0, 1, 4000, 4100), null)));
    out.add(rec(o++, mine.chunk(chunk(BIG, 0, 1, 4700, 4800), null)));
    return out;
  }

  static Position position() {
    return Position.initial(5000, OffsetsAdminTest.ID).withJournalGeneration(2);
  }

  private static TolerantJsonConverter tolerant(boolean isKey) {
    TolerantJsonConverter c = new TolerantJsonConverter();
    c.configure(Map.of(), isKey);
    return c;
  }

  @Test
  void inspectSummarisesWhatTheNextStartRestores() {
    JournalInspector.Result r =
        JournalInspector.inspect(
            TOPIC, journal(false), tolerant(true), tolerant(false), "cdc", position());
    assertThat(r.records()).isEqualTo(7);
    assertThat(r.foreign()).isEqualTo(1);
    assertThat(r.tombstones()).isEqualTo(1);
    assertThat(r.problem()).isNull();
    assertThat(r.staleChunks()).isEqualTo(1);
    assertThat(r.transactions())
        .extracting(JournalInspector.Transaction::key)
        .containsExactly(BIG, A, B);
    JournalInspector.Transaction a = r.transactions().get(1);
    assertThat(a.chunks()).containsExactly(0, 1);
    assertThat(a.events()).isEqualTo(20);
    assertThat(a.undos()).isEqualTo(2);
    assertThat(a.lastScn()).isEqualTo(4200);
    assertThat(a.payloadBytes()).isEqualTo(128);
    assertThat(a.nextStart()).isEqualTo("restored (2 chunks)");
    assertThat(r.transactions().get(2).nextStart()).startsWith("not restored");
    assertThat(
            JournalInspector.inspect(
                    TOPIC, journal(false), tolerant(true), tolerant(false), "cdc", null)
                .transactions()
                .get(0)
                .nextStart())
        .isEqualTo("no stored offset");
  }

  @Test
  void inspectCommandReportsAMissingChunk() {
    FakeEnv env = new FakeEnv();
    env.connect.offset = new java.util.LinkedHashMap<>(PositionCodec.write(position()));
    env.kafka.topics.put(TOPIC, journal(false));
    FakeEnv.Run ok =
        env.admin("journal", "inspect", "--connect-url", "http://c:8083", "--name", "orders");
    assertThat(ok.exit()).isZero();
    assertThat(ok.out())
        .contains(
            "7 records (1 of other connectors, 1 tombstones), 4 live chunks for 3 transactions")
        .contains("resume SCN 5000, journal generation 2; 1 chunks would be tombstoned")
        .contains("Consistency: OK.")
        .contains(
            "| 3:7.1.2 | [0, 1] | [1] | 20 | 2 | 4000 | 4200 | BATCH | etl | restored (2 chunks)"
                + " |");

    env.kafka.topics.put(TOPIC, journal(true));
    FakeEnv.Run broken =
        env.admin("journal", "inspect", "--connect-url", "http://c:8083", "--name", "orders");
    assertThat(broken.exit()).isEqualTo(1);
    assertThat(broken.out()).contains("Consistency:").contains("missing chunk 1");

    env.kafka.topics.put(
        TOPIC, List.of(new ConsumerRecord<>(TOPIC, 0, 0, "x".getBytes(), "y".getBytes())));
    FakeEnv.Run unreadable =
        env.admin("journal", "inspect", "--connect-url", "http://c:8083", "--name", "orders");
    assertThat(unreadable.exit()).isEqualTo(1);
    assertThat(unreadable.out()).contains("cannot be read with cdc.journal.converter");
  }

  @Test
  void transactionsListsBufferedAndJournaledWithTheirStateInGvTransaction() {
    FakeEnv env = new FakeEnv();
    env.connect.offset = new java.util.LinkedHashMap<>(PositionCodec.write(position()));
    env.kafka.topics.put(TOPIC, journal(false));
    env.db.active.add(BIG);
    env.db.ages.put(4000L, Duration.ofHours(2));
    env.samples.add(
        new MetricsSample(
            Instant.now(),
            Map.of(),
            List.of(
                new MetricsSample.OpenTransaction(
                    "5.12.900", "BATCH", 600_000, 4700, 2_000_000, 300L << 20, 0, true))));
    FakeEnv.Run r =
        env.admin(
            "transactions",
            "--connect-url",
            "http://c:8083",
            "--name",
            "orders",
            "--jmx-url",
            "service:jmx:rmi:///jndi/rmi://w:9999/jmxrmi");
    assertThat(r.err()).isEmpty();
    assertThat(r.exit()).isZero();
    assertThat(env.metricsUrl).isEqualTo("service:jmx:rmi:///jndi/rmi://w:9999/jmxrmi");
    assertThat(r.out())
        .contains(
            "| 5.12.900 | buffered, journaled | 10 min | 2000000 | 300 MiB | BATCH |  | 4700 | yes"
                + " |")
        .contains("| 3:7.1.2 | journaled | 2 h | 20 | 0 KiB | BATCH | etl | 4000 | no |")
        .contains("https://kafkacdcconnector.com/runbooks/orphan-transaction")
        .doesNotContain("| 3:5.12.900 |");

    FakeEnv bare = new FakeEnv();
    bare.connect.config.remove("cdc.kafka.bootstrap.servers");
    FakeEnv.Run none = bare.admin("transactions", "--connect-url", "u", "--name", "orders");
    assertThat(none.exit()).isEqualTo(64);
    assertThat(none.err()).contains("--jmx-url");
  }
}
