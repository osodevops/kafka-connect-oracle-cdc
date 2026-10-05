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
package sh.oso.connect.oracle.core.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.util.List;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.buffer.BufferMetricsSnapshot;
import sh.oso.connect.oracle.core.buffer.TransactionBuffer;
import sh.oso.connect.oracle.core.engine.EngineMetrics;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;

class TaskMetricsTest {

  @Test
  void theMxBeanExposesEngineBufferAndSinkValuesUnderTheTaskName() throws Exception {
    EngineMetrics e = new EngineMetrics();
    e.steps.set(7);
    e.minedToScn.set(1000);
    e.safeEndScn.set(1250);
    e.buffer = new BufferMetricsSnapshot(2, 30, 4096, 900, 5, 1, 3, 0, 0, 1, 2048, 1);
    e.largest =
        List.of(
            new TransactionBuffer.OpenTransaction(
                new TxKey(3, new Xid(1, 2, 3)),
                "APP",
                "client",
                900,
                950,
                Instant.parse("2026-10-05T10:00:00Z"),
                10,
                20,
                25,
                3000,
                2048,
                true));
    TaskMetrics m =
        new TaskMetrics(
            e, SinkMetrics.NONE, 1 << 20, 1 << 30, () -> Instant.parse("2026-10-05T10:00:05Z"));
    ObjectName n = m.register("cdc-test");
    try {
      assertThat(n.toString()).isEqualTo("sh.oso.cdc:type=task,server=cdc-test");
      MBeanServer s = ManagementFactory.getPlatformMBeanServer();
      assertThat(s.getAttribute(n, "Steps")).isEqualTo(7L);
      assertThat(s.getAttribute(n, "ScnLag")).isEqualTo(250L);
      assertThat(s.getAttribute(n, "OpenTransactions")).isEqualTo(2);
      assertThat(s.getAttribute(n, "SpilledBytes")).isEqualTo(2048L);
      assertThat(s.getAttribute(n, "BufferMemoryMaxBytes")).isEqualTo((long) (1 << 20));
      assertThat(s.getAttribute(n, "MillisBehindSource")).isEqualTo(-1L);
      CompositeData[] top = (CompositeData[]) s.getAttribute(n, "LargestTransactions");
      assertThat(top).hasSize(1);
      assertThat(top[0].get("xid")).isEqualTo("1.2.3");
      assertThat(top[0].get("ageMillis")).isEqualTo(5000L);
      assertThat(top[0].get("journaled")).isEqualTo(true);
      // a second task with the same prefix replaces a leftover registration
      new TaskMetrics(new EngineMetrics(), SinkMetrics.NONE, 1, 1, Instant::now)
          .register("cdc-test");
      assertThat(s.getAttribute(n, "Steps")).isEqualTo(0L);
      assertThat(s.getAttribute(n, "OldestOpenScn")).isEqualTo(-1L);
    } finally {
      TaskMetrics.unregister(n);
    }
    assertThat(ManagementFactory.getPlatformMBeanServer().isRegistered(n)).isFalse();
    assertThat(TaskMetrics.name("odd,name").toString()).contains("\"odd,name\"");
  }
}
