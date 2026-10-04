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
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;

/** Forced log switches must produce archived copies the capture user can see and read. */
@Tag("engine")
class ArchiveSwitchEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void threeForcedSwitchesProduceThreeContiguousArchivedLogs() throws SQLException {
    long before;
    try (Connection c = db.capture(OracleTestDatabase.CDB_SERVICE)) {
      before = OracleSql.maxArchivedSequence(c);
    }
    for (int i = 0; i < 3; i++) {
      OracleSql.archiveLogCurrent(db);
    }
    List<Long> sequences = new ArrayList<>();
    List<String> names = new ArrayList<>();
    try (Connection c = db.capture(OracleTestDatabase.CDB_SERVICE);
        Statement st = c.createStatement();
        ResultSet rs =
            st.executeQuery(
                "SELECT sequence#, name FROM v$archived_log WHERE thread# = 1 AND dest_id = 1 AND"
                    + " sequence# > "
                    + before
                    + " ORDER BY sequence#")) {
      while (rs.next()) {
        sequences.add(rs.getLong(1));
        names.add(rs.getString(2));
      }
    }
    assertThat(sequences).hasSizeGreaterThanOrEqualTo(3);
    for (int i = 1; i < sequences.size(); i++) {
      assertThat(sequences.get(i)).isEqualTo(sequences.get(i - 1) + 1);
    }
    assertThat(names).allMatch(n -> n.startsWith("/opt/oracle/archive/arch_1_"));
  }
}
