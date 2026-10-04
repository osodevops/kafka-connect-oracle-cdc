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
}
