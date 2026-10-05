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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.config.CoreConfig.CaptureMode;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.errors.OracleCdcPurgedException;
import sh.oso.connect.oracle.core.logs.LogInventory;
import sh.oso.connect.oracle.core.logs.LogSet;
import sh.oso.connect.oracle.core.logs.LogSetProbe;
import sh.oso.connect.oracle.core.logs.RedoLog;
import sh.oso.connect.oracle.core.topology.JdbcCatalogSource;
import sh.oso.connect.oracle.core.topology.Topology;
import sh.oso.connect.oracle.core.topology.TopologyProbe;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;

/**
 * PRD-00 CORE-LOG against a real database: topology, contiguous inventory, archive-only safe end,
 * purge.
 */
@Tag("engine")
class LogInventoryEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void topologyAndInventoryOverForcedSwitches() throws Exception {
    try (Connection root = db.capture(OracleTestDatabase.CDB_SERVICE)) {
      JdbcCatalogSource catalog = new JdbcCatalogSource(root);
      Topology t = new TopologyProbe(catalog, null).probe();
      assertThat(t.database().cdb()).isTrue();
      assertThat(t.database().archivelog()).isTrue();
      assertThat(t.database().supplementalLogDataMin()).isTrue();
      assertThat(t.rac()).isFalse();
      assertThat(t.pdbs()).extracting(p -> p.name()).contains("FREEPDB1", "FREEPDB2");
      assertThat(t.archiveDestId()).isEqualTo(1);
      assertThat(t.fingerprint()).contains("threads=1");

      OracleSql.archiveLogCurrent(db);
      long start = catalog.currentScn();
      for (int i = 0; i < 3; i++) {
        OracleSql.archiveLogCurrent(db);
      }
      long end = catalog.currentScn();

      LogInventory inv = new LogInventory(catalog, CaptureMode.ONLINE, t.archiveDestId());
      LogSet set = inv.forRange(start, end);
      List<RedoLog> thread1 = set.byThread().get(1);
      assertThat(thread1).hasSizeGreaterThanOrEqualTo(4);
      assertThat(thread1.subList(0, thread1.size() - 1)).allMatch(RedoLog::archived);
      assertThat(thread1.get(thread1.size() - 1).archived())
          .as("the range end is in the current online log")
          .isFalse();
      for (int i = 1; i < thread1.size(); i++) {
        assertThat(thread1.get(i).sequence()).isEqualTo(thread1.get(i - 1).sequence() + 1);
      }
      new LogSetProbe(new OraErrorClassifier()).probeReadable(root, set);

      LogInventory archiveOnly =
          new LogInventory(catalog, CaptureMode.ARCHIVE_ONLY, t.archiveDestId());
      long safe = archiveOnly.archiveOnlySafeEnd(start);
      assertThat(safe).isGreaterThanOrEqualTo(start).isLessThanOrEqualTo(end);
      LogSet archived = archiveOnly.forRange(start, safe);
      assertThat(archived.logs()).allMatch(RedoLog::archived);
    }
  }

  @Test
  void aLogRemovedFromDiskIsDetectedByTheReadabilityProbeNotTheCatalog() throws Exception {
    try (Connection root = db.capture(OracleTestDatabase.CDB_SERVICE)) {
      JdbcCatalogSource catalog = new JdbcCatalogSource(root);
      OracleSql.archiveLogCurrent(db);
      long start = catalog.currentScn();
      OracleSql.archiveLogCurrent(db);
      OracleSql.archiveLogCurrent(db);
      long end = catalog.currentScn();
      // newer logs for the suites that mine the latest archived log
      OracleSql.archiveLogCurrent(db);
      OracleSql.archiveLogCurrent(db);

      LogInventory inv = new LogInventory(catalog, CaptureMode.ARCHIVE_ONLY, 1);
      LogSet set = inv.forRange(start, end - 1);
      RedoLog victim = set.logs().get(0);
      OracleSql.hideArchivedLog(db, victim.path());

      assertThat(inv.forRange(start, end - 1).logs())
          .extracting(RedoLog::sequence)
          .contains(victim.sequence());
      assertThatThrownBy(() -> new LogSetProbe(new OraErrorClassifier()).probeReadable(root, set))
          .isInstanceOf(OracleCdcPurgedException.class)
          .hasMessageContaining("sequence " + victim.sequence())
          .hasMessageContaining("ORA-01284")
          .hasMessageContaining("never skips");
    } finally {
      OracleSql.restoreHiddenLogs(db);
    }
  }
}
