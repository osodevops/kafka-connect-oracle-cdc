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
package sh.oso.connect.oracle.e2e.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.engine.LobAssembler;
import sh.oso.connect.oracle.core.jdbc.ConnectionRole;
import sh.oso.connect.oracle.core.jdbc.SessionInitializer;
import sh.oso.connect.oracle.core.metrics.SinkMetrics;
import sh.oso.connect.oracle.core.metrics.TaskMetrics;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * ADR-0013 and CORE-MINE-6 against Oracle: a step of more than 256 rows decoded on four threads,
 * and the task MXBean read through the platform MBean server, with an open transaction in the top
 * 20 (CORE-TX-8).
 */
@Tag("engine")
class MetricsEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void parallelDecodeAndTheTaskMxBeanAgainstRealRedo() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    AtomicReference<ObjectName> name = new AtomicReference<>();
    try (Connection meta = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection mining = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection reselect = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Connection open = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      SessionInitializer.apply(mining, ConnectionRole.MINING);
      SessionInitializer.apply(meta, ConnectionRole.METADATA);
      try (Statement s = w.createStatement()) {
        s.execute("CREATE TABLE m (id NUMBER PRIMARY KEY, v VARCHAR2(40), n NUMBER(12,2))");
        s.execute("ALTER TABLE m ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      }
      OracleSql.archiveLogCurrent(db);
      long start = LogMinerHelper.currentScn(meta);
      w.setAutoCommit(false);
      try (PreparedStatement ps = w.prepareStatement("INSERT INTO m VALUES (?, ?, ?)")) {
        for (int i = 0; i < 600; i++) {
          ps.setInt(1, i);
          ps.setString(2, "value " + i + " it's");
          ps.setBigDecimal(3, java.math.BigDecimal.valueOf(i * 25L, 2));
          ps.addBatch();
        }
        ps.executeBatch();
      }
      w.commit();
      open.setAutoCommit(false);
      try (Statement s = open.createStatement()) {
        s.execute("INSERT INTO m VALUES (1000, 'open', 1)");
        s.execute("INSERT INTO m VALUES (1001, 'open', 2)");
        s.execute("INSERT INTO m VALUES (1002, 'open', 3)");
      }
      OracleSql.archiveLogCurrent(db); // binds the open session's strand
      long end = LogMinerHelper.currentScn(meta);

      List<CommittedTransaction> committed =
          LobModesEngineIT.mine(
              meta,
              mining,
              reselect,
              "FREEPDB1\\." + schema + "\\.M",
              start,
              end,
              LobAssembler.Mode.SKIP,
              engine -> {
                engine.withDecodeThreads(4);
                try {
                  name.set(
                      new TaskMetrics(
                              engine.metrics(), SinkMetrics.NONE, 1 << 28, 1L << 33, Instant::now)
                          .register("metrics-it"));
                } catch (javax.management.JMException e) {
                  throw new IllegalStateException(e);
                }
              },
              engine -> {});
      open.rollback();

      List<RowChange> events = committed.get(0).events();
      assertThat(events).hasSize(600);
      for (int i = 0; i < 600; i++) {
        assertThat(events.get(i).after().get("V")).isEqualTo("value " + i + " it's");
        assertThat(
                ((java.math.BigDecimal) events.get(i).after().get("N"))
                    .compareTo(java.math.BigDecimal.valueOf(i * 25L, 2)))
            .isZero();
      }

      MBeanServer s = ManagementFactory.getPlatformMBeanServer();
      ObjectName n = name.get();
      assertThat(n.toString()).isEqualTo("sh.oso.cdc:type=task,server=metrics-it");
      assertThat((Long) s.getAttribute(n, "Steps")).isPositive();
      assertThat((Long) s.getAttribute(n, "TransactionsCommitted")).isEqualTo(1L);
      assertThat((Long) s.getAttribute(n, "RowsMined")).isGreaterThanOrEqualTo(603L);
      assertThat((Long) s.getAttribute(n, "ScnLag")).isNotNegative();
      assertThat(s.getAttribute(n, "OpenTransactions")).isEqualTo(1);
      CompositeData[] top = (CompositeData[]) s.getAttribute(n, "LargestTransactions");
      assertThat(top).hasSize(1);
      assertThat(top[0].get("events")).isEqualTo(3);
      assertThat(top[0].get("username")).isEqualTo(schema);
      assertThat((Long) top[0].get("heapBytes")).isPositive();
    } finally {
      TaskMetrics.unregister(name.get());
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }
}
