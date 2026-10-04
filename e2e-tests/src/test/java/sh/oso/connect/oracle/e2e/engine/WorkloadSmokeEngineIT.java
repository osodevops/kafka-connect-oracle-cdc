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

import java.sql.Connection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.bench.workload.Ledger;
import sh.oso.connect.oracle.bench.workload.WorkloadGenerator;
import sh.oso.connect.oracle.bench.workload.WorkloadResult;
import sh.oso.connect.oracle.bench.workload.WorkloadSpec;
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * P0-13: the ledger agrees with LogMiner. Every ledger XID has a mined COMMIT and a ledger insert;
 * every mined transaction that touched the workload tables and is absent from the ledger ended in a
 * ROLLBACK; the generator's counters match the ledger row count.
 */
@Tag("engine")
class WorkloadSmokeEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void ledgerXidsEqualMinedCommittedXids() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection root = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      WorkloadSpec spec = WorkloadSpec.defaults();
      spec.seed = 7;
      spec.sessions = 2;
      spec.tables = 3;
      spec.transactionsPerSession = 30;
      spec.maxRowsPerTransaction = 8;
      spec.savepointRollbackProbability = 0.2;
      spec.fullRollbackProbability = 0.15;
      spec.truncateProbability = 0.05;
      spec.ddlProbability = 0.05;
      spec.lobMaxChars = 6000;
      WorkloadGenerator g =
          new WorkloadGenerator(spec, db.jdbcUrl(OracleTestDatabase.PDB1), schema, schema);
      g.reset();
      OracleSql.archiveLogCurrent(db);
      long startScn = LogMinerHelper.currentScn(root);

      WorkloadResult r = g.run();
      OracleSql.archiveLogCurrent(db);
      long endScn = LogMinerHelper.currentScn(root);

      Set<String> ledger = Ledger.committedXids(w, schema, spec.ledgerTable);
      assertThat(ledger).hasSize((int) r.committed.sum());
      assertThat(r.committed.sum() + r.rolledBack.sum()).isEqualTo(2L * 30);
      assertThat(r.rolledBack.sum()).isPositive();
      assertThat(r.savepointRollbacks.sum()).isPositive();

      LogMinerHelper.start(root, startScn, endScn);
      List<Map<String, String>> rows =
          LogMinerHelper.rows(
              root,
              "(SEG_OWNER = '"
                  + schema
                  + "' OR USERNAME = '"
                  + schema
                  + "') AND OPERATION IN ('INSERT','UPDATE','DELETE','COMMIT','ROLLBACK')");
      LogMinerHelper.end(root);

      Set<String> committed = new HashSet<>();
      Set<String> rolledBack = new HashSet<>();
      Set<String> ledgerInserts = new HashSet<>();
      Map<String, Integer> dmlOnData = new HashMap<>();
      for (Map<String, String> row : rows) {
        String xid =
            Ledger.xid(
                Long.parseLong(row.get("XIDUSN")),
                Long.parseLong(row.get("XIDSLT")),
                Long.parseLong(row.get("XIDSQN")));
        switch (row.get("OPERATION")) {
          case "COMMIT" -> committed.add(xid);
          case "ROLLBACK" -> rolledBack.add(xid);
          default -> {
            if (spec.ledgerTable.equals(row.get("TABLE_NAME"))) {
              ledgerInserts.add(xid);
            } else if (String.valueOf(row.get("TABLE_NAME")).startsWith(spec.tablePrefix)
                && !"1".equals(row.get("ROLLBACK"))) {
              dmlOnData.merge(xid, 1, Integer::sum);
            }
          }
        }
      }
      // every ledger row was committed and mined as such
      Set<String> minedLedgerCommits = new HashSet<>(ledgerInserts);
      minedLedgerCommits.retainAll(committed);
      assertThat(minedLedgerCommits).containsExactlyInAnyOrderElementsOf(ledger);
      // every transaction with data rows but no ledger row rolled back
      for (String xid : dmlOnData.keySet()) {
        if (!ledger.contains(xid)) {
          assertThat(rolledBack).as("xid %s has data rows but no ledger row", xid).contains(xid);
          assertThat(committed).as("xid %s rolled back", xid).doesNotContain(xid);
        }
      }
      System.out.println(
          "workload-smoke: "
              + r.toJson()
              + " minedCommits="
              + committed.size()
              + " minedRollbacks="
              + rolledBack.size());
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }
}
