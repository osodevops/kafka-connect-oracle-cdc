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
import org.junit.jupiter.api.Tag;
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
  void exactlyOnceEndsKafkaTransactionsOnlyAtOracleCommitsAndSplitsLargeOnes() throws Exception {
    try (TaskHarness h = new TaskHarness()) {
      h.exactlyOnce = true;
      h.props.put(OracleCdcSourceConnectorConfig.EOS_SPLIT_MAX_RECORDS, "3");
      TxKey a = h.fake.tx(1, 1, 1);
      TxKey b = h.fake.tx(1, 1, 2);
      h.fake
          .start(a, "APP")
          .insert(a, TaskHarness.T, "a1")
          .insert(a, TaskHarness.T, "a2")
          .commit(a);
      h.fake.start(b, "APP");
      for (int i = 1; i <= 7; i++) {
        h.fake.insert(b, TaskHarness.T, "b" + i);
      }
      h.fake.commit(b);
      h.safeEnd = h.fake.nextScn();
      h.start();
      List<SourceRecord> all = new java.util.ArrayList<>(h.pollUntil(12, 5000, true));
      long deadline = System.currentTimeMillis() + 5000;
      while (System.currentTimeMillis() < deadline
          && !TaskHarness.sqls(all).contains("<ops:transaction-split>")) {
        all.addAll(h.pollUntil(1, 200, true));
      }
      List<String> labels = TaskHarness.sqls(all);
      assertThat(labels)
          .containsSubsequence(
              "c:a1", "c:a2", "c:b1", "c:b3", "c:b6", "c:b7", "<ops:transaction-split>");
      List<String> committed = TaskHarness.sqls(h.kafkaCommits);
      // a Kafka transaction ends at an Oracle commit, or at a forced split of a large transaction
      assertThat(committed.stream().filter(l -> l.startsWith("c:")))
          .isSubsetOf("c:a2", "c:b3", "c:b6", "c:b7")
          .contains("c:b3", "c:b6");
      // everything delivered so far has been committed in a Kafka transaction
      int lastCommitted = all.indexOf(h.kafkaCommits.get(h.kafkaCommits.size() - 1));
      assertThat(lastCommitted).isGreaterThanOrEqualTo(labels.indexOf("c:b7"));
      for (SourceRecord r : all) {
        String l = TaskHarness.sqls(List.of(r)).get(0);
        boolean splitHeader = r.headers().lastWithName("cdc.split") != null;
        assertThat(splitHeader).as(l).isEqualTo(l.startsWith("c:b"));
      }
    }
  }

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

  /**
   * Regression for <a href="https://github.com/debezium/dbz/issues/2544">dbz#2544</a> (an offset
   * ahead of what was delivered): the offset of a record must never let a restart skip a
   * transaction that was still open when that record's transaction committed.
   */
  @Test
  @Tag("dbz-2544")
  void offsetsNeverPointPastAnInterleavedOpenTransaction() throws Exception {
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
  void schemaVersionsAreWrittenToTheSchemaTopicReloadedAndCheckedOnRestart() throws Exception {
    // PRD-03 SCH-1, SCH-6: every version change is one compacted record per table; a restart
    // reads them back instead of the dictionary and stops when the two differ unexplained
    try (TaskHarness h = new TaskHarness()) {
      h.props.put(OracleCdcSourceConnectorConfig.KAFKA_BOOTSTRAP_SERVERS, "kafka:9092");
      String table = "<schema:" + TaskHarness.T.table() + ">";
      TxKey a = h.fake.tx(1, 1, 1);
      h.fake.start(a, "APP").insert(a, TaskHarness.T, "a1").commit(a);
      h.safeEnd = h.fake.nextScn();
      h.start();
      List<SourceRecord> first = h.pollUntil(4, 5000, true);
      assertThat(TaskHarness.sqls(first)).contains(table, "c:a1");
      assertThat(versions(h, first)).containsExactly(1);
      h.acknowledge(first);

      // ALTER TABLE ... ADD: version 2, written with the whole history the restart may need
      h.dictionary.put(
          TaskHarness.T,
          TaskHarness.tableSchema(
              sh.oso.connect.oracle.core.schema.ColumnSpec.of(
                  "NOTE", 3, sh.oso.connect.oracle.core.schema.OracleType.VARCHAR2)));
      TxKey d = h.fake.tx(2, 2, 2);
      TxKey b = h.fake.tx(3, 3, 3);
      long ddlScn = h.fake.nextScn();
      h.fake
          .ddl(d, TaskHarness.T, 4242, "ALTER TABLE orders ADD (note VARCHAR2(10))")
          .start(b, "APP")
          .insert(b, TaskHarness.T, "b1")
          .commit(b);
      h.safeEnd = h.fake.nextScn();
      List<SourceRecord> second = h.pollUntil(4, 5000, true);
      assertThat(TaskHarness.sqls(second))
          .containsSubsequence("<ops:ddl-seen>", table, "<ops:ddl-applied>", "c:b1");
      assertThat(versions(h, second)).containsExactly(1, 2);
      SourceRecord v2 = second.stream().filter(TaskHarness::isSchema).findFirst().get();
      assertThat(PositionCodec.read(v2.sourceOffset()).resumeScn())
          .as("the schema record's offset never passes the DDL")
          .isLessThanOrEqualTo(ddlScn);
      h.acknowledge(second);
      assertThat(h.schemaTopic).hasSize(2);

      // restart: version 2 comes from the topic and matches the dictionary, so nothing is
      // re-read or re-written
      h.restart();
      assertThat(h.pollUntil(3, 1000, true)).noneMatch(TaskHarness::isSchema);

      // a column added while the task was stopped, but the DDL predates the resume point: the
      // DDL was never mined, so the stored version cannot be trusted
      h.task().stop();
      h.dictionary.put(
          TaskHarness.T,
          TaskHarness.tableSchema(
              sh.oso.connect.oracle.core.schema.ColumnSpec.of(
                  "NOTE", 3, sh.oso.connect.oracle.core.schema.OracleType.VARCHAR2),
              sh.oso.connect.oracle.core.schema.ColumnSpec.of(
                  "MISSED", 4, sh.oso.connect.oracle.core.schema.OracleType.VARCHAR2)));
      h.scnTime = java.time.Instant.parse("2026-10-05T10:00:00Z");
      h.lastDdlTime = h.scnTime.minusSeconds(3600);
      assertThatThrownBy(h::start)
          .isInstanceOf(ConnectException.class)
          .hasMessageContaining("CDC-6003");

      // the same difference with the DDL after the resume point: it is ahead in the redo
      h.lastDdlTime = h.scnTime.plusSeconds(60);
      h.start();
      assertThat(h.pollUntil(1, 500)).isEmpty();
    }
  }

  /** A table with the test table's layout under another name. */
  private static sh.oso.connect.oracle.core.schema.TableSchema layoutOf(TableId t) {
    return new sh.oso.connect.oracle.core.schema.TableSchema(
        t,
        TaskHarness.tableSchema().columns(),
        List.of("ID"),
        sh.oso.connect.oracle.core.schema.KeySource.PRIMARY_KEY,
        true,
        false);
  }

  /** The schema topic records among {@code records}, by table name. */
  private static Map<String, SourceRecord> schemaRecords(List<SourceRecord> records) {
    Map<String, SourceRecord> out = new java.util.LinkedHashMap<>();
    for (SourceRecord r : records) {
      if (TaskHarness.isSchema(r)) {
        out.put(((org.apache.kafka.connect.data.Struct) r.key()).getString("table"), r);
      }
    }
    return out;
  }

  @Test
  void everyCapturedTableIsReadAtStartWithoutStartWaitingOnTheRecordQueue() throws Exception {
    // ADR-0016 amendment: version 1 of each captured table is stored at start, valid from the
    // resume SCN, before any of its rows. More tables than the record queue holds (1,000 here)
    // must not leave start() waiting for a poll() that cannot run yet
    try (TaskHarness h = new TaskHarness()) {
      h.props.put(OracleCdcSourceConnectorConfig.KAFKA_BOOTSTRAP_SERVERS, "kafka:9092");
      h.props.put(OracleCdcSourceConnectorConfig.POLL_MAX_RECORDS, "250");
      h.scnTime = java.time.Instant.parse("2026-10-06T09:00:00Z");
      h.lastDdlTime = h.scnTime.minusSeconds(3600);
      List<TableId> tables = new java.util.ArrayList<>();
      for (int i = 0; i < 1200; i++) {
        TableId t = new TableId("FREEPDB1", "APP", "T" + i);
        tables.add(t);
        h.dictionary.put(t, layoutOf(t));
      }
      h.captured = tables;
      org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(
          java.time.Duration.ofSeconds(30), () -> h.start());
      Map<String, SourceRecord> written = new java.util.HashMap<>();
      long deadline = System.currentTimeMillis() + 20_000;
      while (written.size() < tables.size() && System.currentTimeMillis() < deadline) {
        written.putAll(schemaRecords(h.pollUntil(500, 200, true)));
      }
      assertThat(written).hasSize(tables.size());
      List<sh.oso.connect.oracle.core.schema.TableSchema> v =
          sh.oso.connect.oracle.schema.SchemaRecords.versions(
              tables.get(7), written.get("T7").value());
      assertThat(v).extracting(s -> s.validFromScn()).containsExactly(1000L);
      assertThat(PositionCodec.read(written.get("T7").sourceOffset()).resumeScn()).isEqualTo(1000);
    }
  }

  @Test
  void aMissedDdlStillStopsTheStartWhenOtherTablesAreReadThere() throws Exception {
    // SCH-6 runs on the stored versions before the read at start, which only reads tables without
    // one: a DDL the redo never showed still stops the task (CDC-6003)
    try (TaskHarness h = new TaskHarness()) {
      h.props.put(OracleCdcSourceConnectorConfig.KAFKA_BOOTSTRAP_SERVERS, "kafka:9092");
      TableId other = new TableId("FREEPDB1", "APP", "LATER");
      h.dictionary.put(other, layoutOf(other));
      h.start();
      List<SourceRecord> first = h.pollUntil(3, 3000, true);
      assertThat(schemaRecords(first)).containsOnlyKeys(TaskHarness.T.table());
      h.acknowledge(first);
      h.task().stop();

      h.captured = List.of(TaskHarness.T, other);
      h.dictionary.put(
          TaskHarness.T,
          TaskHarness.tableSchema(
              sh.oso.connect.oracle.core.schema.ColumnSpec.of(
                  "MISSED", 3, sh.oso.connect.oracle.core.schema.OracleType.VARCHAR2)));
      h.scnTime = java.time.Instant.parse("2026-10-06T09:00:00Z");
      h.lastDdlTime = h.scnTime.minusSeconds(3600);
      assertThatThrownBy(h::start)
          .isInstanceOf(ConnectException.class)
          .hasMessageContaining("CDC-6003")
          .hasMessageContaining("APP.ORDERS");

      // the same difference with the DDL after the resume point: ahead in the redo. The stored
      // table is left as it is, and the new one is read at start
      h.lastDdlTime = h.scnTime.plusSeconds(60);
      h.start();
      Map<String, SourceRecord> written = schemaRecords(h.pollUntil(3, 3000, true));
      assertThat(written).containsOnlyKeys(other.table());
    }
  }

  @Test
  void aTableThatJoinsTheCapturedSetIsReadWhenItJoinsBeforeAnyOfItsRows() throws Exception {
    // SRC-SEL-4 with the ADR-0016 amendment: the layout is stored at the refresh that adds the
    // table (here after a refresh-tables signal), not at its first change, so a DDL in between
    // cannot strand its earlier rows
    try (TaskHarness h = new TaskHarness()) {
      h.props.put(OracleCdcSourceConnectorConfig.KAFKA_BOOTSTRAP_SERVERS, "kafka:9092");
      TableId fresh = new TableId("FREEPDB1", "APP", "NEW_T");
      h.dictionary.put(fresh, layoutOf(fresh));
      h.onRefresh =
          () -> {
            h.captured = List.of(TaskHarness.T, fresh);
            h.tablesListener.accept(java.util.Set.of(fresh), java.util.Set.of());
          };
      h.start();
      h.signal("{\"id\": \"r\", \"type\": \"refresh-tables\"}");
      List<SourceRecord> all = new java.util.ArrayList<>();
      long deadline = System.currentTimeMillis() + 8000;
      while (all.stream().noneMatch(r -> TaskHarness.sql(r).equals("<ops:table-added>"))
          && System.currentTimeMillis() < deadline) {
        all.addAll(h.pollUntil(1, 200, true));
      }
      assertThat(all.stream().filter(TaskHarness::isOps).map(TaskHarness::opsType))
          .contains("table-added");
      assertThat(schemaRecords(all)).containsKeys(TaskHarness.T.table(), fresh.table());
      assertThat(all).noneMatch(r -> !TaskHarness.isInternal(r));
    }
  }

  /** The version numbers in the last schema topic record among {@code records}. */
  private static List<Integer> versions(TaskHarness h, List<SourceRecord> records) {
    SourceRecord last = null;
    for (SourceRecord r : records) {
      if (TaskHarness.isSchema(r)) {
        last = r;
      }
    }
    assertThat(last).as("a schema topic record").isNotNull();
    return sh.oso.connect.oracle.schema.SchemaRecords.versions(TaskHarness.T, last.value()).stream()
        .map(sh.oso.connect.oracle.core.schema.TableSchema::version)
        .toList();
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
      // internal records are kept: the tombstone often shares a batch with the last change
      List<SourceRecord> second = new java.util.ArrayList<>();
      long until = System.currentTimeMillis() + 5000;
      while (System.currentTimeMillis() < until
          && second.stream().filter(r -> !TaskHarness.isInternal(r)).count() < 4) {
        second.addAll(h.pollUntil(1, 500, true));
      }
      List<SourceRecord> changes = second.stream().filter(r -> !TaskHarness.isInternal(r)).toList();
      assertThat(TaskHarness.sqls(changes)).containsExactly("c:a1", "c:a2", "c:a3", "c:a4");
      Position last = PositionCodec.read(changes.get(3).sourceOffset());
      assertThat(last.lastCommitKey()).isEqualTo(a);
      assertThat(last.eventIndex()).isEqualTo(4);
      // the journal entries are tombstoned once the commit is consumed
      List<SourceRecord> tail = h.pollUntil(2, 3000, true);
      List<String> after = new java.util.ArrayList<>(TaskHarness.sqls(second));
      after.addAll(TaskHarness.sqls(tail));
      assertThat(after).anyMatch(k -> k.startsWith("<tombstone:"));
      h.acknowledge(second);
      h.acknowledge(tail);
      h.restart();
      assertThat(h.pollUntil(1, 500)).as("nothing repeats").isEmpty();
    }
  }

  @Test
  void decodeErrorsGoToTheDlqWithAnOpsEventUnderTheDlqPolicy() throws Exception {
    try (TaskHarness h = new TaskHarness()) {
      h.props.put(CoreConfig.ON_DECODE_ERROR, "dlq");
      TxKey a = h.fake.tx(1, 1, 1);
      h.fake
          .start(a, "APP")
          .insert(a, TaskHarness.T, "a1")
          .insert(a, TaskHarness.T, "bad")
          .insert(a, TaskHarness.T, "a3")
          .commit(a);
      h.safeEnd = h.fake.nextScn();
      h.start();
      List<SourceRecord> all = h.pollUntil(6, 5000, true);
      List<String> kinds = all.stream().map(TaskHarness::sql).toList();
      assertThat(kinds).contains("c:a1", "c:a3", "<ops:decode-error-dlq>").doesNotContain("c:bad");
      SourceRecord dlq =
          all.stream().filter(r -> r.topic().equals("cdc.cdc.dlq")).findFirst().orElseThrow();
      org.apache.kafka.connect.data.Struct v = (org.apache.kafka.connect.data.Struct) dlq.value();
      assertThat(v.getString("kind")).isEqualTo("decode-error");
      assertThat(v.getString("sql_redo")).isEqualTo("bad");
      assertThat(v.getString("xid")).isEqualTo(a.xid().toString());
      assertThat(v.getString("exception")).endsWith("DecodeException");
      // the DLQ record carries the redo; the ops event, read by monitoring, never the value
      SourceRecord event =
          all.stream()
              .filter(r -> TaskHarness.sql(r).equals("<ops:decode-error-dlq>"))
              .findFirst()
              .orElseThrow();
      assertThat(String.valueOf(event.value())).doesNotContain("4111");
      // the DLQ record's offset never passes the open transaction's first change
      assertThat(PositionCodec.read(dlq.sourceOffset()).resumeScn()).isLessThanOrEqualTo(1001);
    }
  }

  @Test
  void aDecodeFailureWithholdsTheRowValueUnlessSensitiveLoggingIsOn() throws Exception {
    for (boolean sensitive : new boolean[] {false, true}) {
      try (TaskHarness h = new TaskHarness()) {
        h.props.put(CoreConfig.LOG_SENSITIVE_DATA, Boolean.toString(sensitive));
        TxKey a = h.fake.tx(1, 1, 1);
        h.fake.start(a, "APP").insert(a, TaskHarness.T, "bad").commit(a);
        h.safeEnd = h.fake.nextScn();
        h.start();
        Throwable t = org.assertj.core.api.Assertions.catchThrowable(() -> h.pollUntil(1, 5000));
        assertThat(t).isInstanceOf(ConnectException.class).hasMessageContaining("CDC-3001");
        if (sensitive) {
          assertThat(t).hasMessageContaining("4111 1111 1111 1111");
        } else {
          assertThat(t.getMessage()).doesNotContain("4111").contains("withheld");
          assertThat(t.getCause().getMessage()).doesNotContain("4111");
        }
      }
    }
  }

  @Test
  void aTransactionOlderThanTheLimitIsDiscardedWithAnOpsEventAndADlqRecord() throws Exception {
    try (TaskHarness h = new TaskHarness()) {
      h.props.put(CoreConfig.TRANSACTION_MAX_AGE_MS, "1000");
      h.props.put(CoreConfig.TRANSACTION_MAX_AGE_ACTION, "discard");
      h.props.put(OracleCdcSourceConnectorConfig.HEARTBEAT_INTERVAL_MS, "200");
      TxKey a = h.fake.tx(1, 1, 1);
      TxKey b = h.fake.tx(2, 2, 2);
      h.fake
          .start(a, "APP")
          .insert(a, TaskHarness.T, "a1")
          .start(b, "APP")
          .insert(b, TaskHarness.T, "b1")
          .commit(b);
      h.safeEnd = h.fake.nextScn() + 50;
      h.start();
      List<SourceRecord> first = h.pollUntil(1, 5000);
      assertThat(TaskHarness.sqls(first)).containsExactly("c:b1");
      // a stays open past the one-second limit: discarded, recorded, and the position moves on
      long deadline = System.currentTimeMillis() + 10_000;
      List<SourceRecord> seen = new java.util.ArrayList<>();
      while (System.currentTimeMillis() < deadline
          && seen.stream()
              .noneMatch(r -> TaskHarness.sql(r).equals("<ops:transaction-discarded>"))) {
        seen.addAll(h.pollUntil(1, 500, true));
      }
      SourceRecord ops =
          seen.stream()
              .filter(r -> TaskHarness.sql(r).equals("<ops:transaction-discarded>"))
              .findFirst()
              .orElseThrow();
      assertThat(TaskHarness.opsDetails(ops))
          .containsEntry("xid", a.xid().toString())
          .containsEntry("events", "1");
      SourceRecord dlq =
          seen.stream().filter(r -> r.topic().equals("cdc.cdc.dlq")).findFirst().orElseThrow();
      assertThat(((org.apache.kafka.connect.data.Struct) dlq.value()).getString("kind"))
          .isEqualTo("transaction-discarded");
      Position p = PositionCodec.read(ops.sourceOffset());
      assertThat(p.released()).containsExactly(a.toString());
      List<SourceRecord> later = h.pollUntil(1, 1500, true);
      SourceRecord quiet =
          later.stream().filter(TaskHarness::isHeartbeat).reduce((x, y) -> y).orElse(null);
      if (quiet != null) {
        assertThat(PositionCodec.read(quiet.sourceOffset()).resumeScn())
            .as("the discarded transaction no longer pins the position")
            .isGreaterThan(1001);
      }
      // its commit arriving afterwards is a typed stop, never a partial emission
      h.fake.insert(a, TaskHarness.T, "a2").commit(a);
      h.safeEnd = h.fake.nextScn() + 50;
      assertThatThrownBy(() -> h.pollUntil(1, 5000))
          .isInstanceOf(ConnectException.class)
          .hasMessageContaining("CDC-7001");
    }
  }

  @Test
  void anInitialSnapshotGoesOutBetweenTheCommitsBeforeAndAfterItsScn() throws Exception {
    // PRD-02 section 3 step 4: commits below the chunk's SCN come first, later ones after it
    try (TaskHarness h = new TaskHarness()) {
      h.props.put(CoreConfig.SNAPSHOT_MODE, "initial");
      h.snapshots.rows(TaskHarness.T, 1, 5).scn = 1003;
      TxKey a = h.fake.tx(1, 1, 1);
      TxKey b = h.fake.tx(2, 2, 2);
      h.fake.start(a, "APP").insert(a, TaskHarness.T, "a1").commit(a); // commit at 1002
      h.fake.start(b, "APP").insert(b, TaskHarness.T, "b1").commit(b); // commit at 1005
      h.safeEnd = 1003; // streaming has reached the snapshot's SCN, not b
      h.start();
      List<SourceRecord> first = h.pollUntil(6, 5000);
      assertThat(TaskHarness.sqls(first))
          .containsExactly("c:a1", "r:row1", "r:row2", "r:row3", "r:row4", "r:row5");
      h.safeEnd = h.fake.nextScn();
      assertThat(TaskHarness.sqls(h.pollUntil(1, 5000))).containsExactly("c:b1");
      org.apache.kafka.connect.data.Struct source =
          ((org.apache.kafka.connect.data.Struct) first.get(1).value()).getStruct("source");
      assertThat(source.getString("snapshot")).isEqualTo("first");
      assertThat(source.getString("scn")).isEqualTo("1003");
      assertThat(
              ((org.apache.kafka.connect.data.Struct) first.get(5).value())
                  .getStruct("source")
                  .getString("snapshot"))
          .isEqualTo("last");
      // the last record of each chunk records the chunk as done
      sh.oso.connect.oracle.core.snapshot.SnapshotProgress afterChunk1 =
          sh.oso.connect.oracle.core.snapshot.SnapshotProgress.of(
              PositionCodec.read(first.get(2).sourceOffset()).snapshot());
      assertThat(afterChunk1.frontier(TaskHarness.T)).containsExactly("n:3");
      sh.oso.connect.oracle.core.snapshot.SnapshotProgress beforeChunk1End =
          sh.oso.connect.oracle.core.snapshot.SnapshotProgress.of(
              PositionCodec.read(first.get(1).sourceOffset()).snapshot());
      assertThat(beforeChunk1End.frontier(TaskHarness.T)).isNull();
      assertThat(beforeChunk1End.done(TaskHarness.T)).isFalse();
    }
  }

  @Test
  void aRestartResumesTheSnapshotAtTheFirstUnacknowledgedChunk() throws Exception {
    // PRD-02 SNAP-3 and the acceptance case: no acknowledged chunk is read again
    try (TaskHarness h = new TaskHarness()) {
      h.props.put(CoreConfig.SNAPSHOT_MODE, "initial");
      h.snapshots.rows(TaskHarness.T, 1, 5).scn = 1000;
      h.start();
      List<SourceRecord> rows = h.pollUntil(5, 5000);
      assertThat(TaskHarness.sqls(rows)).hasSize(5);
      h.acknowledge(rows.subList(0, 3)); // chunk [1,3) and the first row of chunk [3,5)
      h.snapshots.reads.clear();
      h.restart();
      List<SourceRecord> again = h.pollUntil(3, 5000);
      assertThat(TaskHarness.sqls(again)).containsExactly("r:row3", "r:row4", "r:row5");
      assertThat(h.snapshots.reads).allMatch(r -> !r.contains("[,"));
      h.acknowledge(again);
      h.acknowledge(h.pollUntil(10, 1000, true)); // the ops events and heartbeats after them
      h.restart();
      assertThat(h.pollUntil(1, 500)).as("the snapshot is complete").isEmpty();
    }
  }

  @Test
  void snapshotOnlyReadsTheTablesWithoutStreaming() throws Exception {
    try (TaskHarness h = new TaskHarness()) {
      h.props.put(CoreConfig.SNAPSHOT_MODE, "snapshot_only");
      h.snapshots.rows(TaskHarness.T, 1, 3);
      TxKey a = h.fake.tx(1, 1, 1);
      h.fake.start(a, "APP").insert(a, TaskHarness.T, "a1").commit(a);
      h.safeEnd = h.fake.nextScn();
      h.start();
      List<SourceRecord> all = h.pollUntil(10, 1500, true);
      assertThat(TaskHarness.sqls(all.stream().filter(r -> !TaskHarness.isInternal(r)).toList()))
          .containsExactly("r:row1", "r:row2", "r:row3");
      assertThat(all.stream().filter(TaskHarness::isOps).map(TaskHarness::opsType))
          .contains("snapshot-chunk-done", "snapshot-complete");
    }
  }

  @Test
  void aSnapshotSignalReadsTheNamedTablesAndIsAcknowledgedOnce() throws Exception {
    // SRC-SIG-1, SRC-SIG-3, SNAP-8: no automatic snapshot; one by signal, marked incremental
    try (TaskHarness h = new TaskHarness()) {
      h.props.put(OracleCdcSourceConnectorConfig.KAFKA_BOOTSTRAP_SERVERS, "kafka:9092");
      h.snapshots.rows(TaskHarness.T, 1, 3).scn = 1000;
      h.start();
      h.signal(
          "{\"id\": \"s1\", \"type\": \"snapshot\", \"data\": {\"tables\":"
              + " [\"FREEPDB1.APP.ORDERS\", \"FREEPDB1.APP.NOT_CAPTURED\"]}}");
      List<SourceRecord> all = h.pollUntil(12, 8000, true);
      List<SourceRecord> acks =
          all.stream()
              .filter(TaskHarness::isOps)
              .filter(r -> TaskHarness.opsType(r).equals("signal-ack"))
              .toList();
      assertThat(acks).hasSize(1);
      assertThat(TaskHarness.opsDetails(acks.get(0)))
          .containsEntry("id", "s1")
          .containsEntry("type", "snapshot")
          .containsEntry("outcome", "ok");
      assertThat(TaskHarness.opsDetails(acks.get(0)).get("message"))
          .contains("FREEPDB1.APP.NOT_CAPTURED");
      List<SourceRecord> rows = all.stream().filter(TaskHarness::isSnapshot).toList();
      assertThat(TaskHarness.sqls(rows)).containsExactly("r:row1", "r:row2", "r:row3");
      assertThat(
              ((org.apache.kafka.connect.data.Struct) rows.get(0).value())
                  .getStruct("source")
                  .getString("snapshot"))
          .isEqualTo("incremental");
      assertThat(PositionCodec.read(acks.get(0).sourceOffset()).extras())
          .containsEntry("signal_offset", 0L);
      h.acknowledge(all);

      // acknowledged: a restart neither repeats the signal nor the snapshot
      h.restart();
      List<SourceRecord> after = h.pollUntil(20, 2500, true);
      assertThat(after).noneMatch(TaskHarness::isSnapshot);
      assertThat(after.stream().filter(TaskHarness::isOps).map(TaskHarness::opsType))
          .doesNotContain("signal-ack");
    }
  }

  @Test
  void signalsForOtherConnectorsAreIgnoredAndTheRestAreAcknowledgedWithAnOutcome()
      throws Exception {
    try (TaskHarness h = new TaskHarness()) {
      h.props.put(OracleCdcSourceConnectorConfig.KAFKA_BOOTSTRAP_SERVERS, "kafka:9092");
      h.start();
      synchronized (h.signalTopic) {
        h.signalTopic.add(
            new sh.oso.connect.oracle.signals.SignalReader.RawSignal(
                0, "another-connector", "{\"id\": \"x\", \"type\": \"log-state\"}"));
      }
      h.signal("{\"id\": \"a\", \"type\": \"snapshot-pause\"}");
      h.signal("{\"id\": \"b\", \"type\": \"log-state\"}");
      h.signal("{\"id\": \"c\", \"type\": \"refresh-tables\"}");
      h.signal("{\"id\": \"d\", \"type\": \"frobnicate\"}");
      h.signal("not json");
      List<String> outcomes = new java.util.ArrayList<>();
      long deadline = System.currentTimeMillis() + 8000;
      Map<String, String> logState = null;
      while (outcomes.size() < 5 && System.currentTimeMillis() < deadline) {
        for (SourceRecord r : h.pollUntil(1, 500, true)) {
          if (TaskHarness.isOps(r) && TaskHarness.opsType(r).equals("signal-ack")) {
            Map<String, String> d = TaskHarness.opsDetails(r);
            outcomes.add(d.get("id") + ":" + d.get("outcome"));
            if ("b".equals(d.get("id"))) {
              logState = d;
            }
          }
        }
      }
      assertThat(outcomes)
          .containsExactly("a:rejected", "b:ok", "c:ok", "d:unknown", "null:invalid");
      assertThat(logState).containsKeys("mined_to_scn", "open_transactions", "largest");
    }
  }

  @Test
  void aTableThatJoinsTheCapturedSetIsAnnouncedAndSnapshottedWithoutARestart() throws Exception {
    // SRC-SEL-4: a CREATE TABLE AS SELECT arrives with rows; they are snapshotted, then streamed
    try (TaskHarness h = new TaskHarness()) {
      h.props.put(CoreConfig.SNAPSHOT_MODE, "initial");
      TableId fresh = new TableId("FREEPDB1", "APP", "NEW_T");
      h.dictionary.put(
          fresh,
          new sh.oso.connect.oracle.core.schema.TableSchema(
              fresh,
              TaskHarness.tableSchema().columns(),
              List.of("ID"),
              sh.oso.connect.oracle.core.schema.KeySource.PRIMARY_KEY,
              true,
              false));
      h.snapshots.rows(TaskHarness.T, 1, 1).rows(fresh, 1, 2).scn = 1000;
      h.onRefresh =
          () -> {
            h.captured = List.of(TaskHarness.T, fresh);
            h.tablesListener.accept(java.util.Set.of(fresh), java.util.Set.of());
          };
      TxKey d = h.fake.tx(1, 1, 1);
      h.fake.ddl(d, fresh, 4242, "CREATE TABLE new_t AS SELECT * FROM orders").commit(d);
      h.safeEnd = h.fake.nextScn();
      h.start();
      List<SourceRecord> all = h.pollUntil(30, 6000, true);
      assertThat(all.stream().filter(TaskHarness::isOps).map(TaskHarness::opsType))
          .contains("table-added");
      SourceRecord added =
          all.stream()
              .filter(r -> TaskHarness.isOps(r) && TaskHarness.opsType(r).equals("table-added"))
              .findFirst()
              .orElseThrow();
      assertThat(TaskHarness.opsDetails(added))
          .containsEntry("table", "FREEPDB1.APP.NEW_T")
          .containsEntry("snapshot", "true");
      assertThat(PositionCodec.read(added.sourceOffset()).extras())
          .as("pending until its snapshot starts, so a crash cannot lose it")
          .containsKey("snapshot_pending");
      List<String> rows =
          all.stream()
              .filter(TaskHarness::isSnapshot)
              .map(r -> r.topic().replaceAll(".*\\.", "") + ":" + TaskHarness.sql(r))
              .toList();
      assertThat(rows).containsExactly("ORDERS:r:row1", "NEW_T:r:row1", "NEW_T:r:row2");
    }
  }

  @Test
  void onlyACreateTableWithoutRowsSkipsTheNewTablesSnapshot() {
    TableId t = new TableId("FREEPDB1", "APP", "NEW_T");
    java.util.function.Function<String, sh.oso.connect.oracle.core.mining.event.MiningEvent.Ddl>
        ddl =
            sql ->
                new sh.oso.connect.oracle.core.mining.event.MiningEvent.Ddl(
                    new TxKey(3, new sh.oso.connect.oracle.core.model.Xid(1, 1, 1)),
                    new sh.oso.connect.oracle.core.model.RedoRecordId(1, "0x1", 0),
                    1,
                    "FREEPDB1",
                    "APP",
                    "NEW_T",
                    1,
                    1,
                    sql,
                    "APP",
                    0,
                    null,
                    java.time.Instant.EPOCH);
    assertThat(OracleCdcSourceTask.createdEmpty(ddl.apply("CREATE TABLE new_t (id NUMBER)"), t))
        .isTrue();
    assertThat(
            OracleCdcSourceTask.createdEmpty(
                ddl.apply("create table new_t (id number, v number generated always as (id * 2))"),
                t))
        .as("a virtual column is not a query")
        .isTrue();
    assertThat(
            OracleCdcSourceTask.createdEmpty(
                ddl.apply("CREATE TABLE new_t AS SELECT * FROM orders"), t))
        .isFalse();
    assertThat(
            OracleCdcSourceTask.createdEmpty(
                ddl.apply("create table new_t (id) as\n(select id from orders)"), t))
        .isFalse();
    assertThat(
            OracleCdcSourceTask.createdEmpty(
                ddl.apply(
                    "CREATE TABLE new_t AS WITH x AS (SELECT 1 id FROM dual) SELECT * FROM x"),
                t))
        .isFalse();
    assertThat(OracleCdcSourceTask.createdEmpty(ddl.apply("ALTER TABLE x RENAME TO new_t"), t))
        .as("a rename brings its rows")
        .isFalse();
    assertThat(OracleCdcSourceTask.createdEmpty(null, t)).as("a refresh by signal").isFalse();
  }

  @Test
  void aConfiguredStartScnTakesOverWhereAnotherConnectorStopped() throws Exception {
    // PRD-04 takeover: with no stored offset, streaming starts at cdc.start.scn, not now
    try (TaskHarness h = new TaskHarness()) {
      TxKey a = h.fake.tx(1, 1, 1);
      TxKey b = h.fake.tx(2, 2, 2);
      h.fake.start(a, "APP").insert(a, TaskHarness.T, "before").commit(a); // up to 1002
      long takeover = h.fake.nextScn();
      h.fake.start(b, "APP").insert(b, TaskHarness.T, "after").commit(b);
      h.safeEnd = h.fake.nextScn();
      h.currentScn = h.fake.nextScn();
      h.props.put(CoreConfig.START_SCN, Long.toString(takeover));
      h.start();
      List<SourceRecord> got = h.pollUntil(1, 5000);
      assertThat(TaskHarness.sqls(got)).containsExactly("c:after");
      assertThat(PositionCodec.read(got.get(0).sourceOffset()).resumeScn())
          .isGreaterThanOrEqualTo(takeover);
    }
    try (TaskHarness h = new TaskHarness()) {
      h.props.put(CoreConfig.START_SCN, "999999");
      assertThatThrownBy(h::start)
          .isInstanceOf(ConnectException.class)
          .hasMessageContaining("CDC-5001")
          .hasMessageContaining("ahead of the database");
    }
  }

  @Test
  void excludedColumnsAreLeftOutOfChangeAndSnapshotRecordsAndNeverSelected() throws Exception {
    // PRD-01 SRC-SEL-2
    try (TaskHarness h = new TaskHarness()) {
      h.props.put(OracleCdcSourceConnectorConfig.COLUMNS_EXCLUDE, "FREEPDB1\\.APP\\.ORDERS\\.SQL");
      h.props.put(CoreConfig.SNAPSHOT_MODE, "initial");
      h.snapshots.rows(TaskHarness.T, 1, 2).scn = 1000;
      TxKey a = h.fake.tx(1, 1, 1);
      h.fake.start(a, "APP").insert(a, TaskHarness.T, "a1").commit(a);
      h.safeEnd = h.fake.nextScn();
      h.start();
      List<SourceRecord> records = h.pollUntil(3, 5000);
      assertThat(records).hasSize(3);
      for (SourceRecord r : records) {
        org.apache.kafka.connect.data.Struct after =
            ((org.apache.kafka.connect.data.Struct) r.value()).getStruct("after");
        assertThat(after.schema().fields())
            .extracting(org.apache.kafka.connect.data.Field::name)
            .containsExactly("ID");
        assertThat(String.valueOf(after)).doesNotContain("row", "a1");
      }
      assertThat(h.snapshots.selected)
          .isNotEmpty()
          .allSatisfy(c -> assertThat(c).containsExactly("ID"));
    }
  }

  @Test
  void aTaskRefusesToStartWhenAKeyColumnIsExcluded() {
    try (TaskHarness h = new TaskHarness()) {
      h.props.put(OracleCdcSourceConnectorConfig.COLUMNS_EXCLUDE, ".*\\.ID");
      assertThatThrownBy(h::start)
          .isInstanceOf(ConnectException.class)
          .hasMessageContaining("CDC-3001")
          .hasMessageContaining("column ID of FREEPDB1.APP.ORDERS");
    }
  }

  /**
   * Regression for <a href="https://github.com/debezium/dbz/issues/2544">dbz#2544</a>: one change
   * can become several records (a delete and its tombstone; a key change as delete, tombstone and
   * create). The framework may commit the offset of the first while the others are unacknowledged,
   * so only the last record of a change may say that the change is done.
   */
  @Test
  @Tag("dbz-2544")
  void onlyTheLastRecordOfAChangeCountsItAsDelivered() throws Exception {
    try (TaskHarness h = new TaskHarness()) {
      TxKey a = h.fake.tx(1, 1, 1);
      h.fake
          .start(a, "APP")
          .insert(a, TaskHarness.T, "a1")
          .delete(a, TaskHarness.T, "d1")
          .commit(a);
      h.safeEnd = h.fake.nextScn();
      h.start();
      List<SourceRecord> records = h.pollUntil(3, 5000);
      assertThat(TaskHarness.sqls(records)).containsExactly("c:a1", "d:d1", "<tombstone>");
      assertThat(PositionCodec.read(records.get(1).sourceOffset()).eventIndex())
          .as("the delete record alone does not complete its change")
          .isEqualTo(1);
      assertThat(PositionCodec.read(records.get(2).sourceOffset()).eventIndex()).isEqualTo(2);
      h.acknowledge(records.subList(0, 2)); // crash before the tombstone was acknowledged
      h.restart();
      assertThat(TaskHarness.sqls(h.pollUntil(2, 5000)))
          .as("the change is delivered again, tombstone included")
          .containsExactly("d:d1", "<tombstone>");
    }
  }
}
