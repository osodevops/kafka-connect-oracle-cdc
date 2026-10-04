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
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.e2e.support.LogMinerHelper;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.ReferenceDoc;

/**
 * CORE-REF-1 (first half): records every column of V$LOGMNR_CONTENTS on this Oracle version and
 * marks the ones the engine depends on. A depended-on column that is missing fails the build.
 */
@Tag("engine")
class LogMinerColumnsRefEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void everyDependedOnColumnExistsAndTheReferenceIsCurrent() throws Exception {
    Set<String> dependedOn = Set.copyOf(LogMinerHelper.COLUMNS);
    List<List<String>> rows = new ArrayList<>();
    List<String> present = new ArrayList<>();
    String version;
    try (Connection c = db.capture(OracleTestDatabase.CDB_SERVICE);
        Statement st = c.createStatement()) {
      try (ResultSet rs = st.executeQuery("SELECT version_full FROM v$instance")) {
        rs.next();
        version = rs.getString(1);
      }
      try (ResultSet rs =
          st.executeQuery(
              "SELECT column_name, data_type, data_length FROM dba_tab_columns WHERE owner = 'SYS'"
                  + " AND table_name = 'V_$LOGMNR_CONTENTS' ORDER BY column_id")) {
        while (rs.next()) {
          String name = rs.getString(1);
          present.add(name);
          rows.add(
              List.of(
                  name, rs.getString(2), rs.getString(3), dependedOn.contains(name) ? "yes" : ""));
        }
      }
    }
    assertThat(present).isNotEmpty();
    List<String> missing = new ArrayList<>(dependedOn);
    missing.removeAll(present);
    assertThat(missing)
        .as("V$LOGMNR_CONTENTS columns the engine depends on but this version lacks")
        .isEmpty();

    new ReferenceDoc(
            "logminer-columns",
            "V$LOGMNR_CONTENTS columns",
            "Every column of `V$LOGMNR_CONTENTS` as exposed to the capture user, with the engine's"
                + " dependencies marked (PRD-00 CORE-TEST-3, CORE-REF-1). Tested on Oracle Database"
                + " Free 23ai; the exact version checked is printed in the CI log, not recorded"
                + " here.")
        .section("Columns")
        .table(List.of("Column", "Type", "Length", "Depended on"), rows)
        .section("Notes")
        .bullet(
            "`SRC_CON_*` population under the online catalog is recorded separately in"
                + " `pdb-routing.md`.")
        .bullet("Operation codes observed per statement kind are in `operation-codes.md`.")
        .assertUpToDate();
    System.out.println("logminer-columns verified against Oracle " + version);
  }
}
