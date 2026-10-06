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
package sh.oso.connect.oracle.bench.soak;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Choosing a way to read the SCN that the workload user is allowed to run. */
class JdbcScnSourceTest {

  /** Answers by the first word after SELECT; a refused method throws its ORA code. */
  static final class FakeDb implements JdbcScnSource.Query {
    final Map<String, Integer> refused;
    final List<String> asked = new ArrayList<>();

    FakeDb(Map<String, Integer> refused) {
      this.refused = refused;
    }

    @Override
    public long scalar(String sql) throws SQLException {
      asked.add(sql);
      for (Map.Entry<String, Integer> e : refused.entrySet()) {
        if (sql.contains(e.getKey())) {
          throw new SQLException("ORA-" + e.getValue(), "42000", e.getValue());
        }
      }
      return 4242;
    }
  }

  @Test
  void vDatabaseIsUsedWhenGranted() throws Exception {
    FakeDb db = new FakeDb(Map.of());
    JdbcScnSource s = new JdbcScnSource(db, "WL_LEDGER");

    assertThat(s.currentScn()).isEqualTo(4242);
    assertThat(s.method()).isEqualTo("v$database");
    assertThat(db.asked).containsExactly("SELECT current_scn FROM v$database");
  }

  @Test
  void anOrdinaryUserFallsBackToTheLedgerAndKeepsThatChoice() throws Exception {
    FakeDb db = new FakeDb(Map.of("v$database", 942, "DBMS_FLASHBACK", 904));
    JdbcScnSource s = new JdbcScnSource(db, "WL_LEDGER");

    assertThat(s.currentScn()).isEqualTo(4242);
    assertThat(s.method()).isEqualTo("ledger-ora-rowscn");
    assertThat(db.asked.get(2))
        .contains("TIMESTAMP_TO_SCN(SYSTIMESTAMP)")
        .contains("MAX(ORA_ROWSCN)")
        .endsWith("FROM WL_LEDGER");
    s.currentScn();
    assertThat(db.asked).hasSize(4); // refused methods are not tried again
  }

  @Test
  void insufficientPrivilegesMoveOnToDbmsFlashback() throws Exception {
    JdbcScnSource s = new JdbcScnSource(new FakeDb(Map.of("v$database", 1031)), "WL_LEDGER");
    s.currentScn();
    assertThat(s.method()).isEqualTo("dbms_flashback");
  }

  @Test
  void otherDatabaseErrorsPropagate() {
    JdbcScnSource s = new JdbcScnSource(new FakeDb(Map.of("v$database", 3113)), "WL_LEDGER");
    assertThatThrownBy(s::currentScn)
        .isInstanceOf(SQLException.class)
        .extracting(e -> ((SQLException) e).getErrorCode())
        .isEqualTo(3113);
  }

  @Test
  void noPermittedMethodSaysWhatToGrant() {
    JdbcScnSource s =
        new JdbcScnSource(
            new FakeDb(Map.of("v$database", 942, "DBMS_FLASHBACK", 904, "ORA_ROWSCN", 942)),
            "WL_LEDGER");
    assertThatThrownBy(s::currentScn)
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("grant SELECT on V_$DATABASE");
  }
}
