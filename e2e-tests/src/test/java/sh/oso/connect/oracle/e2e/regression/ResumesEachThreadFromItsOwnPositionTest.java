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
package sh.oso.connect.oracle.e2e.regression;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfig;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.engine.CaptureEngine;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.position.PositionCodec;
import sh.oso.connect.oracle.core.testkit.FakeLogMiner;
import sh.oso.connect.oracle.delivery.RecordQueueSink;
import sh.oso.connect.oracle.dlq.DecodeDlqWriter;
import sh.oso.connect.oracle.envelope.DebeziumEnvelope;
import sh.oso.connect.oracle.envelope.TopicRouter;
import sh.oso.connect.oracle.heartbeat.HeartbeatEmitter;
import sh.oso.connect.oracle.journal.JournalRecords;
import sh.oso.connect.oracle.ops.OpsEventWriter;

/**
 * ADR-0026: each RAC instance writes its own redo thread, and a redo byte address says nothing
 * about another thread's. With one resume address and one last commit in the offset, a restart
 * reads a slow thread from the fast thread's address, so the slow thread's later commits, whose
 * addresses sort below it, are never mined again; and it compares every commit with the last one by
 * thread number first, so a later commit of a lower-numbered thread is skipped whole. Found by
 * design review of the per-thread cursor (9 October 2026), before RAC is qualified.
 *
 * <p>The scenario is a qualified two-thread database: transactions on both threads interleave, some
 * stay open while the other thread commits, and the threads' redo byte addresses overlap. The
 * offset of every record in turn is taken as the last one Connect acknowledged, the task restarts
 * from it, and the records delivered before and after the restart together must hold every
 * committed change exactly once.
 */
@Tag("rac-per-thread-restart")
class ResumesEachThreadFromItsOwnPositionTest {

  static final DatabaseIdentity IDENTITY = new DatabaseIdentity(1, 1);

  /** Every committed change, by its NAME column, in the order a single run delivers them. */
  static final List<String> EVERY_CHANGE =
      List.of("a1", "w1", "b1", "d1", "x1", "c1", "c2", "e1", "y1", "y2");

  /**
   * Thread 1 is the busy instance, thread 2 the quiet one; each numbers its redo byte addresses
   * from one, so the quiet thread's later records have lower addresses than the busy thread's
   * earlier ones. C stays open on thread 1 while D and X commit; Y stays open on thread 2 while E
   * commits.
   */
  static void script(FakeLogMiner fake) {
    fake.perThreadRba();
    TxKey a = fake.tx(1, 1, 1);
    TxKey b = fake.tx(1, 2, 1);
    TxKey c = fake.tx(1, 3, 1);
    TxKey d = fake.tx(1, 4, 1);
    TxKey e = fake.tx(1, 5, 1);
    TxKey w = fake.tx(2, 1, 1);
    TxKey x = fake.tx(2, 2, 1);
    TxKey y = fake.tx(2, 3, 1);
    fake.onThread(1).start(a, "APP").insert(a, ScriptedEngine.ORDERS, row(1, "a1")).commit(a);
    fake.onThread(2).start(w, "APP").insert(w, ScriptedEngine.ORDERS, row(101, "w1")).commit(w);
    fake.onThread(1)
        .start(b, "APP")
        .insert(b, ScriptedEngine.ORDERS, row(2, "b1"))
        .commit(b)
        .start(c, "APP")
        .insert(c, ScriptedEngine.ORDERS, row(3, "c1"))
        .start(d, "APP")
        .insert(d, ScriptedEngine.ORDERS, row(4, "d1"))
        .commit(d);
    fake.onThread(2).start(x, "APP").insert(x, ScriptedEngine.ORDERS, row(102, "x1")).commit(x);
    fake.onThread(1).insert(c, ScriptedEngine.ORDERS, row(5, "c2")).commit(c);
    fake.onThread(2).start(y, "APP").insert(y, ScriptedEngine.ORDERS, row(103, "y1"));
    fake.onThread(1).start(e, "APP").insert(e, ScriptedEngine.ORDERS, row(6, "e1")).commit(e);
    fake.onThread(2).insert(y, ScriptedEngine.ORDERS, row(104, "y2")).commit(y);
  }

  static String row(int id, String name) {
    return ScriptedEngine.insert(id, name);
  }

  @Test
  void aRestartFromAnyAcknowledgedRecordDeliversEveryCommittedChangeExactlyOnce() throws Exception {
    ScriptedEngine s = new ScriptedEngine();
    script(s.fake);
    s.safeEnd = s.fake.nextScn();

    List<SourceRecord> first = run(s, Position.initial(1000, IDENTITY));
    assertThat(names(first)).as("one run without a restart").isEqualTo(EVERY_CHANGE);

    for (int acked = 1; acked <= first.size(); acked++) {
      SourceRecord last = first.get(acked - 1);
      Position from = PositionCodec.read(last.sourceOffset());
      assertThat(from.threads())
          .as(
              "the offset of %s holds a mark per qualified thread, agreeing with its legacy keys",
              name(last))
          .containsOnlyKeys(1, 2);
      List<String> delivered = new ArrayList<>(names(first.subList(0, acked)));
      delivered.addAll(names(run(s, from)));
      assertThat(delivered)
          .as(
              "Connect acknowledged up to %s (offset %s), then the task restarted",
              name(last), last.sourceOffset())
          .containsExactlyInAnyOrderElementsOf(EVERY_CHANGE);
    }
  }

  /** One task run from {@code start} to the end of the script; the records it queued, in order. */
  static List<SourceRecord> run(ScriptedEngine s, Position start) throws Exception {
    RecordQueueSink[] sink = new RecordQueueSink[1];
    CaptureEngine e =
        s.engine(3, start, registry -> sink[0] = sink(registry, start))
            .withExpectedThreads(Set.of(1, 2)); // a RAC database the start qualified
    s.runUntilIdle(e);
    List<SourceRecord> out = new ArrayList<>();
    for (SourceRecord r : sink[0].drain(Integer.MAX_VALUE, 0)) {
      if (r.topic().endsWith("ORDERS")) {
        out.add(r);
      }
    }
    return out;
  }

  static RecordQueueSink sink(
      sh.oso.connect.oracle.core.schema.SchemaRegistry registry, Position base) {
    Map<String, String> props = new HashMap<>();
    props.put(CoreConfig.DATABASE_HOST, "oracle");
    props.put(CoreConfig.DATABASE_SERVICE, "FREE");
    props.put(CoreConfig.DATABASE_USER, "c##cdc");
    props.put(CoreConfig.DATABASE_PASSWORD, "CANARY-never-used"); // gitleaks:allow
    props.put(CoreConfig.DATABASE_PDBS, "FREEPDB1");
    props.put(OracleCdcSourceConnectorConfig.TOPIC_PREFIX, "cdc");
    props.put(OracleCdcSourceConnectorConfig.TABLES_INCLUDE, "FREEPDB1\\.APP\\..*");
    OracleCdcSourceConnectorConfig config = new OracleCdcSourceConnectorConfig(props);
    DebeziumEnvelope envelope =
        new DebeziumEnvelope(
            config,
            new TopicRouter(config.topicTemplate(true), config.topicPrefix(), "FREE"),
            "FREE");
    return new RecordQueueSink(
        envelope,
        registry,
        base,
        10_000,
        new HeartbeatEmitter(config.heartbeatTopic(), config.topicPrefix(), envelope.partition()),
        new OpsEventWriter(config.opsTopic(), config.topicPrefix(), envelope.partition()),
        new JournalRecords(config.journalTopic(), config.topicPrefix(), envelope.partition()),
        new DecodeDlqWriter(config.dlqTopic(), config.topicPrefix(), envelope.partition()),
        0, // no heartbeats: only change records are queued
        System::currentTimeMillis);
  }

  static List<String> names(List<SourceRecord> records) {
    return records.stream().map(ResumesEachThreadFromItsOwnPositionTest::name).toList();
  }

  static String name(SourceRecord r) {
    return ((Struct) r.value()).getStruct("after").getString("NAME");
  }
}
