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
import java.util.TreeMap;
import java.util.TreeSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.ReferenceDoc;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * CORE-REF-2 and ADR-0002 gate: mines interleaved transactions from FREEPDB1 and FREEPDB2 at
 * CDB$ROOT with the online catalog and records whether SRC_CON_ID, SRC_CON_NAME and CON_ID identify
 * the source PDB on DML, DDL and transaction control rows. Multi-PDB capture in 1.0 depends on a
 * container column being populated on DML rows for both PDBs.
 */
@Tag("engine")
class PdbRoutingRefEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void containerColumnsIdentifyTheSourcePdbUnderTheOnlineCatalog() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB2, schema);
    try (Connection root = db.capture(OracleTestDatabase.CDB_SERVICE);
        Connection p1 = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Connection p2 = db.connect(OracleTestDatabase.PDB2, schema, schema)) {
      for (Connection c : List.of(p1, p2)) {
        c.setAutoCommit(false);
        try (Statement s = c.createStatement()) {
          s.execute("CREATE TABLE r (id NUMBER PRIMARY KEY, v VARCHAR2(50))");
          s.execute("ALTER TABLE r ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
        }
        c.commit();
      }
      OracleSql.archiveLogCurrent(db);
      long start = LogMinerHelper.currentScn(root);
      try (Statement s1 = p1.createStatement();
          Statement s2 = p2.createStatement()) {
        s1.execute("INSERT INTO r VALUES (1, 'pdb1-a')");
        s2.execute("INSERT INTO r VALUES (1, 'pdb2-a')");
        s1.execute("UPDATE r SET v = 'pdb1-b' WHERE id = 1");
        p2.commit();
        s2.execute("DELETE FROM r WHERE id = 1");
        p1.commit();
        p2.commit();
        s1.execute("ALTER TABLE r ADD (x NUMBER)");
        s2.execute("ALTER TABLE r ADD (y NUMBER)");
      }
      OracleSql.archiveLogCurrent(db);
      long end = LogMinerHelper.currentScn(root);

      LogMinerHelper.start(root, start, end);
      List<Map<String, String>> rows =
          LogMinerHelper.rows(
              root,
              "(SEG_OWNER = '"
                  + schema
                  + "' OR USERNAME = '"
                  + schema
                  + "') AND OPERATION <> 'INTERNAL'");
      LogMinerHelper.end(root);
      assertThat(rows).isNotEmpty();

      TreeMap<String, TreeSet<String>> byOp = new TreeMap<>();
      TreeSet<String> dmlContainers = new TreeSet<>();
      TreeSet<String> xidsPdb1 = new TreeSet<>();
      TreeSet<String> xidsPdb2 = new TreeSet<>();
      for (Map<String, String> r : rows) {
        String containers =
            "SRC_CON_ID="
                + r.get("SRC_CON_ID")
                + " SRC_CON_NAME="
                + r.get("SRC_CON_NAME")
                + " CON_ID="
                + r.get("CON_ID");
        byOp.computeIfAbsent(r.get("OPERATION"), k -> new TreeSet<>()).add(containers);
        if (List.of("INSERT", "UPDATE", "DELETE").contains(r.get("OPERATION"))) {
          dmlContainers.add(r.get("SRC_CON_NAME") + "/" + r.get("SRC_CON_ID"));
          String xid = r.get("XIDUSN") + "." + r.get("XIDSLT") + "." + r.get("XIDSQN");
          if (OracleTestDatabase.PDB1.equalsIgnoreCase(r.get("SRC_CON_NAME"))) {
            xidsPdb1.add(xid);
          } else if (OracleTestDatabase.PDB2.equalsIgnoreCase(r.get("SRC_CON_NAME"))) {
            xidsPdb2.add(xid);
          }
        }
      }
      List<List<String>> table = new ArrayList<>();
      for (Map.Entry<String, TreeSet<String>> e : byOp.entrySet()) {
        for (String v : e.getValue()) {
          table.add(List.of(e.getKey(), v));
        }
      }
      boolean gatePasses =
          dmlContainers.stream().anyMatch(s -> s.toUpperCase().startsWith(OracleTestDatabase.PDB1))
              && dmlContainers.stream()
                  .anyMatch(s -> s.toUpperCase().startsWith(OracleTestDatabase.PDB2));
      TreeSet<String> collisions = new TreeSet<>(xidsPdb1);
      collisions.retainAll(xidsPdb2);
      System.out.println("pdb-routing: XIDs colliding across PDBs in this run: " + collisions);

      new ReferenceDoc(
              "pdb-routing",
              "PDB routing columns under the online catalog",
              "Which of `SRC_CON_ID`, `SRC_CON_NAME` and `CON_ID` identify the source pluggable"
                  + " database when mining at `CDB$ROOT` with `DICT_FROM_ONLINE_CATALOG` (PRD-00"
                  + " CORE-MINE-9, CORE-REF-2, ADR-0002). Oracle's documentation says the"
                  + " `SRC_CON_*` columns are populated \"only when mining with a LogMiner"
                  + " dictionary\"; this records what the online catalog actually does.")
          .section("Observed per operation")
          .table(List.of("OPERATION", "Container columns"), table)
          .section("Gate")
          .bullet("DML rows carry a source container for both PDBs: " + (gatePasses ? "yes" : "NO"))
          .bullet("Distinct DML containers seen: " + dmlContainers)
          .bullet(
              "XIDs (XIDUSN.XIDSLT.XIDSQN) are allocated per PDB because undo is local to each PDB,"
                  + " so the same XID can appear in two PDBs within one mining window (observed"
                  + " during testing). The buffer keys transactions by (SRC_CON_ID, XID), never by"
                  + " XID alone.")
          .blank()
          .paragraph(
              gatePasses
                  ? "Decision (ADR-0002): one connector captures several PDBs from a single"
                      + " CDB$ROOT session, routing by SRC_CON_NAME with SRC_CON_ID as the stable"
                      + " identity."
                  : "Decision (ADR-0002): the online catalog does not identify the source PDB; 1.0"
                      + " ships one PDB per connector and multi-PDB moves to Phase 2.")
          .assertUpToDate();
      assertThat(gatePasses)
          .as(
              "ADR-0002 gate: SRC_CON_NAME populated on DML rows for both PDBs (see"
                  + " pdb-routing.md)")
          .isTrue();
    } finally {
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
      SchemaFixtures.drop(db, OracleTestDatabase.PDB2, schema);
    }
  }
}
