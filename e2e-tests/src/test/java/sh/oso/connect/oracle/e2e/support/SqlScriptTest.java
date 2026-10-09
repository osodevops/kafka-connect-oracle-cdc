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
package sh.oso.connect.oracle.e2e.support;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.doctor.SetupSql;
import sh.oso.connect.oracle.core.topology.Platform;

class SqlScriptTest {

  @Test
  void theRdsSetupScriptSplitsIntoStatementsJdbcRuns() {
    String script =
        SetupSql.generate("cdc", "pw", false, SetupSql.Profile.PRODUCTION, Platform.RDS, List.of());
    List<String> s = SqlScript.statements(script);
    assertThat(s.get(0)).isEqualTo("CREATE USER cdc IDENTIFIED BY \"pw\"");
    assertThat(s).contains("GRANT LOGMINING TO cdc", "COMMIT");
    assertThat(s)
        .filteredOn(x -> x.startsWith("BEGIN"))
        .hasSize(3)
        .allMatch(x -> x.endsWith("END;"))
        .anyMatch(x -> x.contains("grant_sys_object('DBMS_LOGMNR_D', 'CDC', 'EXECUTE')"));
    assertThat(s).noneMatch(x -> x.startsWith("--") || x.startsWith("WHENEVER") || x.equals("/"));
    assertThat(s).filteredOn(x -> !x.startsWith("BEGIN")).noneMatch(x -> x.endsWith(";"));
  }

  @Test
  void execLinesBecomeAnonymousBlocks() {
    assertThat(SqlScript.statements("EXEC rdsadmin.rdsadmin_util.switch_logfile;\n"))
        .containsExactly("BEGIN rdsadmin.rdsadmin_util.switch_logfile; END;");
  }
}
