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
package sh.oso.connect.oracle.e2e.connector;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.bench.check.CheckReport;
import sh.oso.connect.oracle.bench.check.CorrectnessCheck;
import sh.oso.connect.oracle.bench.workload.WorkloadGenerator;
import sh.oso.connect.oracle.bench.workload.WorkloadResult;
import sh.oso.connect.oracle.bench.workload.WorkloadSpec;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * P1-13: the correctness oracle's three assertions against a real worker: table state at the check
 * SCN equals the database AS OF that SCN, the committed set equals the ledger, and the event
 * invariants hold. The evidence JSON is written under target/ for inspection.
 */
@Tag("connector")
class CorrectnessOracleConnectorIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void topicsEqualTheDatabaseAtTheCheckScn() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (ConnectCluster cluster = new ConnectCluster().start();
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      WorkloadSpec spec = WorkloadSpec.defaults();
      spec.seed = 41;
      spec.sessions = 2;
      spec.tables = 3;
      spec.transactionsPerSession = 150;
      spec.maxRowsPerTransaction = 6;
      spec.savepointRollbackProbability = 0.15;
      spec.fullRollbackProbability = 0.1;
      spec.keyChangeWeight = 1;
      // CORE-DEC-6 and CORE-DEC-7: LOBs in and out of row; reselect fills what the redo cannot
      // carry, such as the LOB of a row whose primary key changed
      spec.lobWeight = 1;
      spec.lobMaxChars = 12000;
      WorkloadGenerator g =
          new WorkloadGenerator(spec, db.jdbcUrl(OracleTestDatabase.PDB1), schema, schema);
      g.reset();
      Thread.sleep(3500); // flashback queries need the SCN-to-time mapping past the CREATE TABLE

      Map<String, String> c = new HashMap<>(ConnectCluster.oracleDatabaseProps("FREEPDB1"));
      c.put("connector.class", FirstRecordConnectorIT.CONNECTOR_CLASS);
      c.put("tasks.max", "1");
      c.put("cdc.topic.prefix", "co");
      c.put("cdc.tables.include", "FREEPDB1\\." + schema + "\\.WL_.*");
      c.put("cdc.tables.exclude", "FREEPDB1\\." + schema + "\\.WL_LEDGER");
      c.put("cdc.lob.mode", "reselect");
      c.put("cdc.poll.linger.ms", "100");
      cluster.register("oracle-cdc", c);
      cluster.awaitRunning("oracle-cdc", Duration.ofMinutes(2));
      cluster.awaitOffsets("oracle-cdc", Duration.ofSeconds(60));

      WorkloadResult r = g.run();
      assertThat(r.committed.sum()).isPositive();

      List<String> tables = List.of("WL_T1", "WL_T2", "WL_T3");
      List<String> topics = tables.stream().map(t -> "co.FREEPDB1." + schema + "." + t).toList();
      CorrectnessCheck check =
          new CorrectnessCheck(
              cluster.bootstrapServers(),
              topics,
              w,
              schema,
              tables,
              spec.ledgerTable,
              Duration.ofMinutes(4),
              Duration.ofSeconds(15));
      CheckReport report = check.run();
      Path out = Path.of("target/correctness-evidence.json");
      Files.createDirectories(out.getParent());
      report.write(out);
      System.out.println(
          "correctness-oracle: "
              + r.toJson()
              + " verdict="
              + report.verdict()
              + " records="
              + report.recordsConsumed());
      assertThat(report.verdict()).as(report.toJson()).isEqualTo(CheckReport.Verdict.PASS);
      assertThat(report.transactions().get("seen")).isEqualTo(report.transactions().get("ledger"));
      assertThat(report.invariants().get("transactionsWithDuplicateEvents")).isEqualTo(0L);
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }
}
