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
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.ReferenceDoc;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * CORE-REF: is redo with an SCN at or below V$DATABASE.CURRENT_SCN visible to LogMiner at the
 * moment that SCN is read? Uncommitted redo may sit in the log buffer or in a session's private
 * redo strand until the transaction commits, the strand fills, LGWR's three-second cycle runs or a
 * log switch forces the strands out. This spike records what LogMiner returns for one SCN range at
 * four moments and which SCNs the late rows carry. It decides the online safe end rule (ADR-0014).
 */
@Tag("engine")
class RedoFlushLagRefEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void uncommittedRedoBelowTheCurrentScnMayNotBeMinedYet() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection root = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      w.setAutoCommit(false);
      try (Statement s = w.createStatement()) {
        s.execute("CREATE TABLE lag_t (id NUMBER PRIMARY KEY, v VARCHAR2(20))");
        s.execute("ALTER TABLE lag_t ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      }
      w.commit();
      Thread.sleep(3500);
      List<List<String>> rows = new ArrayList<>();
      int trials = 6;
      int immediatelyShort = 0;
      int laterComplete = 0;
      int afterSwitchComplete = 0;
      int afterCommitComplete = 0;
      boolean lateRowsCarryEarlierScns = false;
      boolean lateRowsBeyondBound = false;
      for (int t = 0; t < trials; t++) {
        long from = OracleSql.currentScn(root);
        try (Statement s = w.createStatement()) {
          for (int i = 0; i < 4; i++) {
            s.execute("INSERT INTO lag_t VALUES (" + (t * 10 + i) + ", 'uncommitted')");
          }
        }
        long to = OracleSql.currentScn(root); // the bound a naive step would use
        List<Long> immediately = insertScns(root, from, to, schema);
        Thread.sleep(4000); // past LGWR's three-second cycle
        List<Long> later = insertScns(root, from, to, schema);
        List<Long> afterSwitch;
        if (t % 2 == 1) {
          OracleSql.archiveLogCurrent(db); // a log switch flushes private redo strands
          Thread.sleep(500);
          afterSwitch = insertScns(root, from, to, schema);
        } else {
          afterSwitch = null;
        }
        w.commit();
        long commitScn = OracleSql.currentScn(root);
        Thread.sleep(500);
        List<Long> afterCommit = insertScns(root, from, to, schema);
        // the same rows mined with no upper bound: where do the late rows sit?
        List<Long> unbounded = insertScns(root, from, commitScn + 1000, schema);
        rows.add(
            List.of(
                "trial " + (t + 1),
                Long.toString(to),
                Integer.toString(immediately.size()),
                Integer.toString(later.size()),
                afterSwitch == null ? "(no switch)" : Integer.toString(afterSwitch.size()),
                Integer.toString(afterCommit.size()),
                range(unbounded),
                Long.toString(commitScn)));
        if (immediately.size() < 4) {
          immediatelyShort++;
        }
        if (later.size() == 4) {
          laterComplete++;
        }
        if (afterSwitch != null && afterSwitch.size() == 4) {
          afterSwitchComplete++;
        }
        if (afterCommit.size() == 4) {
          afterCommitComplete++;
        }
        for (Long scn : unbounded) {
          if (scn <= to) {
            lateRowsCarryEarlierScns = true;
          } else {
            lateRowsBeyondBound = true;
          }
        }
      }
      for (List<String> r : rows) {
        System.out.println("redo-flush-lag trial: " + r);
      }
      new ReferenceDoc(
              "redo-flush-lag",
              "Uncommitted redo and the current SCN",
              "Four uncommitted inserts in one transaction, then V$DATABASE.CURRENT_SCN read as the"
                  + " bound a mining step would use. The range [start, bound] is mined immediately,"
                  + " four seconds later, after a log switch on half the trials, and after the"
                  + " commit; a last pass mines from the same start with no upper bound to see"
                  + " which SCNs the late rows carry. Only facts that hold run after run are"
                  + " recorded here; the per-trial counts are printed by the spike.")
          .section("Facts")
          .bullet(
              "No late row ever carried an SCN above the bound read before it; late rows carry the"
                  + " SCNs of the original changes, at or below that bound (asserted by the"
                  + " spike).")
          .bullet(
              "After a log switch the range was complete in every switched trial while the"
                  + " transaction was still open: a switch binds private redo strands (asserted).")
          .bullet("After the commit the range was complete in every trial (asserted).")
          .bullet(
              "Immediately after the inserts, and again four seconds later, the range was"
                  + " incomplete in some trials: LGWR's cycle does not bind other sessions' private"
                  + " strands. How many trials varies from run to run; the spike prints the"
                  + " counts.")
          .paragraph(
              "Decision (ADR-0014): redo of an open transaction is not in the online redo log until"
                  + " the transaction commits, its private strand fills or a log switch binds it,"
                  + " and it then carries the SCNs of the original changes. A step bounded by an"
                  + " SCN can therefore miss rows that appear later below that bound. The mining"
                  + " cursor is the redo byte address of the last row applied, and LogMiner starts"
                  + " at the first SCN of the log holding it.")
          .assertUpToDate();
      assertThat(lateRowsBeyondBound).as("late rows above the bound").isFalse();
      assertThat(afterSwitchComplete).as("complete after a log switch").isEqualTo(trials / 2);
      assertThat(afterCommitComplete).as("complete after the commit").isEqualTo(trials);
      System.out.println(
          "redo-flush-lag: short immediately in "
              + immediatelyShort
              + "/"
              + trials
              + ", complete later in "
              + laterComplete
              + "/"
              + trials
              + ", complete after switch in "
              + afterSwitchComplete
              + "/"
              + (trials / 2)
              + ", earlier SCNs="
              + lateRowsCarryEarlierScns
              + ", beyond bound="
              + lateRowsBeyondBound);
      assertThat(rows).hasSize(trials);
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  private static String range(List<Long> scns) {
    if (scns.isEmpty()) {
      return "none";
    }
    long min = scns.stream().mapToLong(Long::longValue).min().getAsLong();
    long max = scns.stream().mapToLong(Long::longValue).max().getAsLong();
    return min == max ? Long.toString(min) : min + " to " + max;
  }

  private static List<Long> insertScns(Connection root, long from, long to, String schema)
      throws Exception {
    LogMinerHelper.startWithOnline(root, from, to);
    try {
      List<Map<String, String>> rows =
          LogMinerHelper.rows(
              root,
              "SEG_OWNER = '" + schema + "' AND TABLE_NAME = 'LAG_T' AND OPERATION = 'INSERT'");
      List<Long> out = new ArrayList<>();
      for (Map<String, String> r : rows) {
        out.add(Long.parseLong(r.get("SCN")));
      }
      return out;
    } finally {
      LogMinerHelper.end(root);
    }
  }
}
