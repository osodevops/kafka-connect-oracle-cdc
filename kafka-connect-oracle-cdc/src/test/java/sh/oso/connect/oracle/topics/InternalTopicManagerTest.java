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
package sh.oso.connect.oracle.topics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfig;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfigTest;

class InternalTopicManagerTest {

  private static OracleCdcSourceConnectorConfig config(Map<String, String> extra) {
    Map<String, String> p = new HashMap<>(OracleCdcSourceConnectorConfigTest.minimal());
    p.putAll(extra);
    return new OracleCdcSourceConnectorConfig(p);
  }

  @Test
  void namesExpandThePrefixAndTheJournalDefaultsNextToThem() {
    Map<String, InternalTopics.Spec> specs = InternalTopics.of(config(Map.of()));
    assertThat(specs.keySet())
        .containsExactly("ops", "heartbeat", "signals", "schema", "txjournal");
    assertThat(specs.get("ops").name()).isEqualTo("cdc.cdc.ops");
    assertThat(specs.get("heartbeat").name()).isEqualTo("cdc.cdc.heartbeat");
    assertThat(specs.get("schema").compacted()).isTrue();
    assertThat(specs.get("txjournal").name()).isEqualTo("cdc.cdc.txjournal");
    assertThat(specs.get("txjournal").compacted()).isTrue();
    assertThat(specs.get("ops").compacted()).isFalse();
    Map<String, InternalTopics.Spec> withTx =
        InternalTopics.of(
            config(
                Map.of(
                    OracleCdcSourceConnectorConfig.TRANSACTIONS_TOPIC_ENABLED,
                    "true",
                    OracleCdcSourceConnectorConfig.OPS_TOPIC,
                    "audit.${prefix}")));
    assertThat(withTx.get("transactions").name()).isEqualTo("cdc.cdc.transactions");
    assertThat(withTx.get("ops").name()).isEqualTo("audit.cdc");
  }

  @Test
  void createsOnlyTheMissingTopicsWithTheRightCleanupPolicy() throws Exception {
    FakeTopicAdmin admin = new FakeTopicAdmin().existing("cdc.cdc.heartbeat", "unrelated");
    InternalTopicManager m = new InternalTopicManager(admin, (short) -1);
    assertThat(m.ensure(InternalTopics.of(config(Map.of()))))
        .containsExactly("cdc.cdc.ops", "cdc.cdc.signals", "cdc.cdc.schema", "cdc.cdc.txjournal");
    assertThat(admin.created.get("cdc.cdc.schema"))
        .containsEntry("cleanup.policy", "compact")
        .containsEntry("min.compaction.lag.ms", "0");
    assertThat(admin.created.get("cdc.cdc.txjournal")).containsEntry("cleanup.policy", "compact");
    assertThat(admin.created.get("cdc.cdc.ops")).containsEntry("cleanup.policy", "delete");
    assertThat(admin.created).doesNotContainKey("cdc.cdc.heartbeat");
    assertThat(admin.partitions).isEqualTo(1);
    assertThat(admin.replication).isEqualTo((short) -1);
    // second run: nothing to do
    assertThat(m.ensure(InternalTopics.of(config(Map.of())))).isEmpty();
    assertThat(m.missing(InternalTopics.of(config(Map.of())))).isEmpty();
  }

  @Test
  void heartbeatTopicGetsARetentionAndTheReplicationFactorIsPassedThrough() throws Exception {
    FakeTopicAdmin admin = new FakeTopicAdmin();
    new InternalTopicManager(admin, (short) 3).ensure(InternalTopics.of(config(Map.of())));
    assertThat(admin.created.get("cdc.cdc.heartbeat"))
        .containsEntry("cleanup.policy", "delete")
        .containsEntry("retention.ms", Long.toString(24L * 3600_000L));
    assertThat(admin.replication).isEqualTo((short) 3);
  }

  @Test
  void missingListsWhatValidationShouldReport() throws Exception {
    FakeTopicAdmin admin = new FakeTopicAdmin().existing("cdc.cdc.ops");
    assertThat(
            new InternalTopicManager(admin, (short) -1)
                .missing(InternalTopics.of(config(Map.of()))))
        .containsExactly(
            "cdc.cdc.heartbeat", "cdc.cdc.signals", "cdc.cdc.schema", "cdc.cdc.txjournal");
  }

  @Test
  void brokerErrorsPropagate() {
    FakeTopicAdmin admin = new FakeTopicAdmin();
    admin.failWith = new java.util.concurrent.TimeoutException("no brokers");
    assertThatThrownBy(
            () ->
                new InternalTopicManager(admin, (short) -1)
                    .ensure(InternalTopics.of(config(Map.of()))))
        .isInstanceOf(java.util.concurrent.TimeoutException.class);
  }
}
