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
package sh.oso.connect.oracle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.position.PositionCodec;

/** The seven slice cases at the task level, with offsets simulated the way Connect commits them. */
class OracleCdcSourceTaskTest {

  @Test
  void multiRowTransactionAndRollbackProduceRecordsInOrderWithOffsets() throws Exception {
    try (TaskHarness h = new TaskHarness()) {
      TxKey a = h.fake.tx(1, 1, 1);
      TxKey b = h.fake.tx(1, 1, 2);
      h.fake
          .start(a, "APP")
          .insert(a, TaskHarness.T, "a1")
          .insert(a, TaskHarness.T, "a2")
          .start(b, "APP")
          .insert(b, TaskHarness.T, "b1")
          .rollback(b)
          .update(a, TaskHarness.T, "a3")
          .commit(a);
      h.safeEnd = h.fake.nextScn();
      h.start();
      List<SourceRecord> records = h.pollUntil(3, 5000);
      assertThat(TaskHarness.sqls(records)).containsExactly("c:a1", "c:a2", "u:a3");
      assertThat(records).allMatch(r -> r.topic().equals("cdc.FREEPDB1.APP.ORDERS"));
      assertThat(h.task().startPosition().resumeScn()).isEqualTo(1000);
      Position last = PositionCodec.read(records.get(2).sourceOffset());
      assertThat(last.lastCommitKey()).isEqualTo(a);
      assertThat(last.eventIndex()).isEqualTo(3);
      assertThat(last.identity()).isEqualTo(TaskHarness.IDENTITY);
      Position first = PositionCodec.read(records.get(0).sourceOffset());
      assertThat(first.eventIndex()).isEqualTo(1);
      assertThat(first.resumeScn())
          .as("not the last record: resume at a's first row")
          .isEqualTo(1001);
      assertThat(last.resumeScn())
          .as("last record: resume may pass the transaction")
          .isEqualTo(last.lastCommitScn());
      assertThat(h.task().poll()).isNull();
      h.acknowledge(records);
      assertThat(h.task().acks().acknowledgedCount()).isEqualTo(3);
    }
  }

  @Test
  void restartAfterAcknowledgedCommitReplaysNothing() throws Exception {
    try (TaskHarness h = new TaskHarness()) {
      TxKey a = h.fake.tx(1, 1, 1);
      h.fake.start(a, "APP").insert(a, TaskHarness.T, "a1").commit(a);
      h.safeEnd = h.fake.nextScn();
      h.start();
      List<SourceRecord> records = h.pollUntil(1, 5000);
      h.acknowledge(records);
      h.restart();
      assertThat(h.sessionsOpened).isEqualTo(2);
      assertThat(h.task().startPosition().lastCommitKey()).isEqualTo(a);
      assertThat(h.pollUntil(1, 500)).isEmpty();
      // new work after the restart flows
      TxKey b = h.fake.tx(1, 1, 2);
      h.fake.start(b, "APP").insert(b, TaskHarness.T, "b1").commit(b);
      h.safeEnd = h.fake.nextScn();
      assertThat(TaskHarness.sqls(h.pollUntil(1, 5000))).containsExactly("c:b1");
    }
  }

  @Test
  void restartMidTransactionReplaysOnlyTheUnacknowledgedSuffixAndDuplicateReplayIsEmpty()
      throws Exception {
    try (TaskHarness h = new TaskHarness()) {
      TxKey a = h.fake.tx(1, 1, 1);
      h.fake
          .start(a, "APP")
          .insert(a, TaskHarness.T, "a1")
          .insert(a, TaskHarness.T, "a2")
          .insert(a, TaskHarness.T, "a3")
          .commit(a);
      h.safeEnd = h.fake.nextScn();
      h.start();
      List<SourceRecord> records = h.pollUntil(3, 5000);
      assertThat(records).hasSize(3);
      h.acknowledge(records.subList(0, 1)); // crash after the first record was committed
      h.restart();
      assertThat(h.task().startPosition().eventIndex()).isEqualTo(1);
      List<SourceRecord> replayed = h.pollUntil(2, 5000);
      assertThat(TaskHarness.sqls(replayed)).containsExactly("c:a2", "c:a3");
      assertThat(replayed.get(0).headers().lastWithName("cdc.event_index").value()).isEqualTo(1);
      h.acknowledge(replayed);
      h.restart();
      assertThat(h.pollUntil(1, 500)).as("everything acknowledged: no duplicates").isEmpty();
    }
  }

  @Test
  void offsetsNeverPointPastAnInterleavedOpenTransaction() throws Exception {
    // dbz#2544: the offset of a record must never let a restart skip a transaction that was still
    // open when that record's transaction committed
    try (TaskHarness h = new TaskHarness()) {
      TxKey early = h.fake.tx(1, 1, 1);
      TxKey quick = h.fake.tx(1, 1, 2);
      h.fake
          .start(early, "APP")
          .insert(early, TaskHarness.T, "e1") // scn 1001
          .start(quick, "APP")
          .insert(quick, TaskHarness.T, "q1")
          .commit(quick) // commit 1004
          .insert(early, TaskHarness.T, "e2")
          .commit(early); // commit 1006
      h.safeEnd = h.fake.nextScn();
      h.start();
      List<SourceRecord> records = h.pollUntil(3, 5000);
      assertThat(TaskHarness.sqls(records)).containsExactly("c:q1", "c:e1", "c:e2");
      Position afterQuick = PositionCodec.read(records.get(0).sourceOffset());
      assertThat(afterQuick.resumeScn())
          .as("resume stays at the open transaction's first row")
          .isEqualTo(1001);
      h.acknowledge(records.subList(0, 1)); // crash right after quick was committed
      h.restart();
      assertThat(TaskHarness.sqls(h.pollUntil(2, 5000))).containsExactly("c:e1", "c:e2");
    }
  }

  @Test
  void rejectsAnOffsetFromAnotherDatabaseAndSurfacesEngineFailures() throws Exception {
    try (TaskHarness h = new TaskHarness()) {
      TxKey a = h.fake.tx(1, 1, 1);
      h.fake.start(a, "APP").insert(a, TaskHarness.T, "a1").commit(a);
      h.safeEnd = h.fake.nextScn();
      h.start();
      List<SourceRecord> records = h.pollUntil(1, 5000);
      Position foreign = PositionCodec.read(records.get(0).sourceOffset());
      Map<String, Object> wrong =
          PositionCodec.write(
              new Position(
                  1,
                  foreign.resumeScn(),
                  0,
                  null,
                  0,
                  0,
                  0,
                  0,
                  new DatabaseIdentity(99, 1),
                  List.of(),
                  null,
                  Map.of()));
      h.task().stop();
      TaskHarness other = new TaskHarness();
      other.fake.start(a, "APP").insert(a, TaskHarness.T, "a1").commit(a);
      java.lang.reflect.Field f = TaskHarness.class.getDeclaredField("committedOffsets");
      f.setAccessible(true);
      @SuppressWarnings("unchecked")
      Map<Map<String, Object>, Map<String, Object>> offsets =
          (Map<Map<String, Object>, Map<String, Object>>) f.get(other);
      offsets.put(Map.of("server", "cdc"), wrong);
      assertThatThrownBy(other::start)
          .isInstanceOf(ConnectException.class)
          .hasMessageContaining("CDC-5001");
    }
    try (TaskHarness h = new TaskHarness()) {
      TxKey a = h.fake.tx(1, 1, 1);
      h.fake.start(a, "APP").insert(a, TaskHarness.T, "a1").commit(a);
      h.safeEnd = h.fake.nextScn();
      h.fake.faultAt(0, new SQLException("ORA-01284: file gone", "72000", 1284), true);
      h.start();
      assertThatThrownBy(() -> h.pollUntil(1, 5000))
          .isInstanceOf(ConnectException.class)
          .hasMessageContaining("CDC-2002");
    }
  }

  @Test
  void startHeartbeatMakesTheStartScnDurableBeforeAnyChange() throws Exception {
    // the Strimzi worker-kill case: a task killed before its first offset flush restarted from a
    // later current SCN and skipped every transaction committed in between
    try (TaskHarness h = new TaskHarness()) {
      h.start();
      List<SourceRecord> first = h.pollUntil(2, 5000, true);
      assertThat(first).hasSize(2);
      SourceRecord hb = first.get(0);
      assertThat(hb.topic()).isEqualTo("cdc.cdc.heartbeat");
      assertThat(((org.apache.kafka.connect.data.Struct) hb.value()).getString("reason"))
          .isEqualTo("start");
      assertThat(PositionCodec.read(hb.sourceOffset()).resumeScn()).isEqualTo(1000);
      // SRC-OPS: the startup event follows with the same position and names the start SCN
      SourceRecord startup = first.get(1);
      assertThat(startup.topic()).isEqualTo("cdc.cdc.ops");
      assertThat(TaskHarness.opsType(startup)).isEqualTo("startup");
      assertThat(TaskHarness.opsDetails(startup))
          .containsEntry("resume_scn", "1000")
          .containsKey("version")
          .doesNotContainKey("last_commit");
      assertThat(PositionCodec.read(startup.sourceOffset()).resumeScn()).isEqualTo(1000);
      h.acknowledge(first);
      // the database moves on while the task is down; changes land at 1001..1003
      TxKey a = h.fake.tx(1, 1, 1);
      h.fake.start(a, "APP").insert(a, TaskHarness.T, "a1").commit(a);
      h.currentScn = 5000;
      h.safeEnd = 5000;
      h.restart();
      assertThat(h.task().startPosition().resumeScn())
          .as("resumes at the durable start SCN")
          .isEqualTo(1000);
      assertThat(TaskHarness.sqls(h.pollUntil(1, 5000))).containsExactly("c:a1");
    }
  }

  @Test
  void quietDatabaseHeartbeatsAdvanceThePositionWithoutRepeatingCommits() throws Exception {
    try (TaskHarness h = new TaskHarness()) {
      h.props.put(OracleCdcSourceConnectorConfig.HEARTBEAT_INTERVAL_MS, "200");
      TxKey a = h.fake.tx(1, 1, 1);
      h.fake.start(a, "APP").insert(a, TaskHarness.T, "a1").commit(a);
      h.safeEnd = h.fake.nextScn() + 500;
      h.start();
      // start heartbeat, startup event, the change, then a quiet heartbeat
      List<SourceRecord> all = h.pollUntil(4, 3000, true);
      List<SourceRecord> heartbeats = all.stream().filter(TaskHarness::isHeartbeat).toList();
      assertThat(heartbeats.size()).isGreaterThanOrEqualTo(2);
      SourceRecord quiet = heartbeats.get(heartbeats.size() - 1);
      assertThat(((org.apache.kafka.connect.data.Struct) quiet.value()).getString("reason"))
          .isEqualTo("quiet");
      Position p = PositionCodec.read(quiet.sourceOffset());
      assertThat(p.resumeScn())
          .as("advanced to the mined end on a quiet database")
          .isEqualTo(h.safeEnd);
      assertThat(p.lastCommitKey()).as("names the last emitted commit").isEqualTo(a);
      h.acknowledge(all);
      h.restart();
      assertThat(h.pollUntil(1, 500)).as("nothing repeats after a quiet heartbeat").isEmpty();
    }
  }

  @Test
  void connectorReturnsOneTaskConfigAndTheComposedDefinition() {
    OracleCdcSourceConnector c = new OracleCdcSourceConnector();
    c.start(OracleCdcSourceConnectorConfigTest.minimal());
    assertThat(c.taskConfigs(3)).hasSize(1);
    assertThat(c.taskClass()).isEqualTo(OracleCdcSourceTask.class);
    assertThat(c.config().names())
        .contains(OracleCdcSourceConnectorConfig.TOPIC_PREFIX, "cdc.database.host");
    assertThat(c.version()).isEqualTo(Version.VERSION);
    c.stop();
  }

  @Test
  void ddlAndIdRefreshAreReportedOnTheOpsTopicWithASafeOffset() throws Exception {
    try (TaskHarness h = new TaskHarness()) {
      TxKey a = h.fake.tx(1, 1, 1);
      TxKey d = h.fake.tx(2, 2, 2);
      h.fake
          .start(a, "APP")
          .insert(a, TaskHarness.T, "a1")
          .commit(a)
          .ddl(d, new TableId("FREEPDB1", "APP", "NEW_T"), 7777, "CREATE TABLE new_t (id NUMBER)");
      h.safeEnd = h.fake.nextScn();
      h.start();
      List<SourceRecord> all = h.pollUntil(5, 5000, true);
      List<String> kinds = all.stream().map(TaskHarness::sql).toList();
      assertThat(kinds)
          .startsWith("<heartbeat>", "<ops:startup>", "c:a1")
          .contains("<ops:ddl-seen>", "<ops:ids-refreshed>");
      SourceRecord ddl =
          all.stream().filter(r -> TaskHarness.sql(r).equals("<ops:ddl-seen>")).findFirst().get();
      assertThat(TaskHarness.opsDetails(ddl))
          .containsEntry("owner", "APP")
          .containsEntry("object", "NEW_T")
          .containsEntry("sql", "CREATE TABLE new_t (id NUMBER)");
      // the ops offset names the last emitted commit: committing it repeats nothing and skips
      // nothing (the same rule as a quiet heartbeat)
      Position p = PositionCodec.read(ddl.sourceOffset());
      assertThat(p.lastCommitKey()).isEqualTo(a);
      assertThat(p.eventIndex()).isEqualTo(1);
      h.acknowledge(all);
      h.restart();
      assertThat(h.pollUntil(1, 500)).isEmpty();
    }
  }

  @Test
  void aStoppingEngineWritesTheStopEventBeforePollRethrows() throws Exception {
    try (TaskHarness h = new TaskHarness()) {
      TxKey a = h.fake.tx(1, 1, 1);
      h.fake.start(a, "APP").insert(a, TaskHarness.T, "a1").commit(a);
      h.safeEnd = h.fake.nextScn();
      h.fake.faultAt(0, new SQLException("ORA-01284: file gone", "72000", 1284), true);
      h.start();
      List<SourceRecord> seen = new java.util.ArrayList<>();
      Throwable failure = null;
      long deadline = System.currentTimeMillis() + 5000;
      while (failure == null && System.currentTimeMillis() < deadline) {
        try {
          List<SourceRecord> batch = h.task().poll();
          if (batch != null) {
            seen.addAll(batch);
          }
        } catch (ConnectException e) {
          failure = e;
        }
      }
      assertThat(failure).isNotNull().hasMessageContaining("CDC-2002");
      List<SourceRecord> stops =
          seen.stream().filter(r -> TaskHarness.sql(r).equals("<ops:stop>")).toList();
      assertThat(stops).as("stop event delivered before the task fails: %s", seen).hasSize(1);
      Map<String, String> details = TaskHarness.opsDetails(stops.get(0));
      assertThat(details.get("code")).isEqualTo("CDC-2002");
      assertThat(details.get("runbook")).contains("/runbooks/");
      assertThat(details.get("exception")).endsWith("Exception");
      assertThat(details).containsKey("operator_action");
    }
  }

  @Test
  void createsTheInternalTopicsWhenBrokerAccessIsConfigured() throws Exception {
    try (TaskHarness h = new TaskHarness()) {
      h.start();
      assertThat(h.topicAdmin.created).as("no broker access, no admin client").isEmpty();
    }
    try (TaskHarness h = new TaskHarness()) {
      h.props.put(OracleCdcSourceConnectorConfig.KAFKA_BOOTSTRAP_SERVERS, "kafka:9092");
      h.props.put("cdc.kafka.security.protocol", "PLAINTEXT");
      h.topicAdmin.existing("cdc.cdc.heartbeat");
      h.start();
      assertThat(h.topicAdmin.created.keySet())
          .containsExactly("cdc.cdc.ops", "cdc.cdc.signals", "cdc.cdc.schema", "cdc.cdc.txjournal");
      assertThat(h.topicAdmin.created.get("cdc.cdc.schema"))
          .containsEntry("cleanup.policy", "compact");
      assertThat(h.topicAdmin.closed).isTrue();
    }
    try (TaskHarness h = new TaskHarness()) {
      h.props.put(OracleCdcSourceConnectorConfig.KAFKA_BOOTSTRAP_SERVERS, "kafka:9092");
      h.topicAdmin.failWith = new java.util.concurrent.TimeoutException("no brokers");
      assertThatThrownBy(h::start)
          .isInstanceOf(ConnectException.class)
          .hasMessageContaining("internal topics");
    }
  }

  @Test
  void aJournaledLongTransactionSurvivesARestartPastItsStart() throws Exception {
    // CORE-TX-4, CORE-TX-5, ADR-0003: a transaction open across a restart whose start the resume
    // position has passed is rebuilt from the journal topic, and its commit arrives exactly once
    try (TaskHarness h = new TaskHarness()) {
      h.props.put(OracleCdcSourceConnectorConfig.KAFKA_BOOTSTRAP_SERVERS, "kafka:9092");
      h.props.put(OracleCdcSourceConnectorConfig.HEARTBEAT_INTERVAL_MS, "100");
      h.props.put(CoreConfig.TXJOURNAL_THRESHOLD_EVENTS, "2");
      h.props.put(CoreConfig.TXJOURNAL_THRESHOLD_MS, "3600000");
      TxKey a = h.fake.tx(1, 1, 1);
      TxKey b = h.fake.tx(2, 2, 2);
      h.fake
          .start(a, "APP")
          .insert(a, TaskHarness.T, "a1")
          .insert(a, TaskHarness.T, "a2")
          .start(b, "APP")
          .insert(b, TaskHarness.T, "b1")
          .commit(b)
          .insert(a, TaskHarness.T, "a3");
      long minedTo = h.fake.nextScn() + 100;
      h.safeEnd = minedTo;
      h.start();
      // b commits; a is journaled (3 events over the threshold of 2) and the quiet heartbeat moves
      // the resume SCN past a's first change
      List<SourceRecord> first = h.pollUntil(5, 5000, true);
      List<String> kinds = first.stream().map(TaskHarness::sql).toList();
      assertThat(kinds).contains("c:b1", "<chunk:0>");
      SourceRecord chunk =
          first.stream().filter(r -> TaskHarness.sql(r).equals("<chunk:0>")).findFirst().get();
      Position chunkOffset = PositionCodec.read(chunk.sourceOffset());
      assertThat(chunkOffset.journalGeneration()).isEqualTo(1);
      assertThat(chunkOffset.resumeScn())
          .as("the chunk's own offset still covers a")
          .isLessThanOrEqualTo(1001);
      List<SourceRecord> more = h.pollUntil(1, 3000, true);
      List<SourceRecord> all = new java.util.ArrayList<>(first);
      all.addAll(more);
      SourceRecord quiet =
          all.stream()
              .filter(TaskHarness::isHeartbeat)
              .filter(
                  r ->
                      ((org.apache.kafka.connect.data.Struct) r.value())
                          .getString("reason")
                          .equals("quiet"))
              .reduce((x, y) -> y)
              .orElseThrow();
      Position p = PositionCodec.read(quiet.sourceOffset());
      assertThat(p.resumeScn())
          .as("resume moved to a's last journaled record, past its start")
          .isGreaterThan(1001);
      assertThat(h.journalTopic).isEmpty();
      h.acknowledge(all);
      assertThat(h.journalTopic).as("the acknowledged chunk is in the topic").hasSize(1);
      // restart: the position is past a's first two changes; the journal supplies them. The
      // remaining redo is added while the task is down, so the restarted engine mines it.
      h.task().stop();
      h.fake.insert(a, TaskHarness.T, "a4").commit(a);
      h.safeEnd = h.fake.nextScn();
      h.start();
      assertThat(h.task().startPosition().resumeScn()).isEqualTo(p.resumeScn());
      assertThat(h.task().startPosition().journalGeneration()).isEqualTo(2);
      List<SourceRecord> second = h.pollUntil(4, 5000);
      assertThat(TaskHarness.sqls(second)).containsExactly("c:a1", "c:a2", "c:a3", "c:a4");
      Position last = PositionCodec.read(second.get(3).sourceOffset());
      assertThat(last.lastCommitKey()).isEqualTo(a);
      assertThat(last.eventIndex()).isEqualTo(4);
      // the journal entries are tombstoned once the commit is consumed
      List<SourceRecord> tail = h.pollUntil(2, 3000, true);
      List<String> tailKinds = tail.stream().map(TaskHarness::sql).toList();
      assertThat(tailKinds).anyMatch(k -> k.startsWith("<tombstone:"));
      h.acknowledge(second);
      h.acknowledge(tail);
      h.restart();
      assertThat(h.pollUntil(1, 500)).as("nothing repeats").isEmpty();
    }
  }
}
