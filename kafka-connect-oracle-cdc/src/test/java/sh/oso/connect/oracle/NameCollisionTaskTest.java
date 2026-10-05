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

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.errors.NameCollisionException;
import sh.oso.connect.oracle.core.model.TableId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.schema.KeySource;
import sh.oso.connect.oracle.core.schema.TableSchema;

/**
 * CDC-6004 at the task: captured tables that would share a topic only because characters became
 * underscores stop the task before a record of the second table is built, whether both are captured
 * at start or the second joins later.
 */
class NameCollisionTaskTest {

  static final TableId HASH = new TableId("FREEPDB1", "APP", "ORDER#");
  static final TableId DOLLAR = new TableId("FREEPDB1", "APP", "ORDER$");

  static TableSchema like(TableId t) {
    return new TableSchema(
        t, TaskHarness.tableSchema().columns(), List.of("ID"), KeySource.PRIMARY_KEY, true, false);
  }

  @Test
  void collidingTablesCapturedAtStartStopTheTaskBeforeAnyRecord() {
    try (TaskHarness h = new TaskHarness()) {
      h.captured = List.of(TaskHarness.T, HASH, DOLLAR);
      h.dictionary.put(HASH, like(HASH));
      h.dictionary.put(DOLLAR, like(DOLLAR));
      assertThatThrownBy(h::start)
          .isInstanceOf(ConnectException.class)
          .hasCauseInstanceOf(NameCollisionException.class)
          .hasMessageContaining("CDC-6004")
          .hasMessageContaining("FREEPDB1.APP.ORDER# and FREEPDB1.APP.ORDER$")
          .hasMessageContaining("cdc.FREEPDB1.APP.ORDER_");
    }
  }

  @Test
  void aTemplateThatMergesTablesOnPurposeStartsNormally() throws Exception {
    try (TaskHarness h = new TaskHarness()) {
      h.props.put(OracleCdcSourceConnectorConfig.TOPIC_TEMPLATE, "${prefix}.${schema}");
      h.captured = List.of(TaskHarness.T, HASH, DOLLAR);
      TxKey a = h.fake.tx(1, 1, 1);
      h.fake.start(a, "APP").insert(a, TaskHarness.T, "a1").commit(a);
      h.safeEnd = h.fake.nextScn();
      h.start();
      List<SourceRecord> out = h.pollUntil(1, 5000);
      assertThat(out).hasSize(1);
      assertThat(out.get(0).topic()).isEqualTo("cdc.APP");
    }
  }

  @Test
  void aCollidingTableThatJoinsLaterStopsTheTaskBeforeItsFirstRecord() throws Exception {
    try (TaskHarness h = new TaskHarness()) {
      h.captured = List.of(TaskHarness.T, HASH);
      h.dictionary.put(HASH, like(HASH));
      h.dictionary.put(DOLLAR, like(DOLLAR));
      h.onRefresh =
          () -> {
            h.captured = List.of(TaskHarness.T, HASH, DOLLAR);
            h.tablesListener.accept(java.util.Set.of(DOLLAR), java.util.Set.of());
          };
      TxKey a = h.fake.tx(1, 1, 1);
      h.fake.start(a, "APP").insert(a, HASH, "h1").commit(a);
      TxKey d = h.fake.tx(1, 1, 2);
      h.fake.ddl(d, DOLLAR, 4243, "CREATE TABLE \"ORDER$\" (id NUMBER PRIMARY KEY)").commit(d);
      TxKey b = h.fake.tx(1, 1, 3);
      h.fake.start(b, "APP").insert(b, DOLLAR, "d1").commit(b);
      h.safeEnd = h.fake.nextScn();
      h.start();
      List<SourceRecord> seen = new ArrayList<>();
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
      assertThat(failure)
          .isNotNull()
          .hasMessageContaining("CDC-6004")
          .hasMessageContaining("FREEPDB1.APP.ORDER# and FREEPDB1.APP.ORDER$");
      assertThat(TaskHarness.sqls(seen.stream().filter(r -> !TaskHarness.isInternal(r)).toList()))
          .as("the first table's row went out, the second table's never did")
          .containsExactly("c:h1");
      assertThat(seen.stream().filter(r -> TaskHarness.sql(r).equals("<ops:stop>")))
          .as("the stop event names the code")
          .hasSize(1)
          .allSatisfy(r -> assertThat(TaskHarness.opsDetails(r)).containsEntry("code", "CDC-6004"));
    }
  }
}
