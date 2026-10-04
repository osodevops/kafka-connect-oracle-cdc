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
package sh.oso.connect.oracle.bench.workload;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.bench.BenchMain;

class WorkloadSpecTest {

  @Test
  void defaultsAreValidAndSpecFilesOverrideOnlyWhatTheyName() throws Exception {
    WorkloadSpec d = WorkloadSpec.defaults();
    d.validate();
    WorkloadSpec s = WorkloadSpec.parse("{\"seed\": 9, \"tables\": 5, \"unknown\": true}");
    assertThat(s.seed).isEqualTo(9);
    assertThat(s.tables).isEqualTo(5);
    assertThat(s.sessions).isEqualTo(d.sessions);
    assertThat(s.tableName(2)).isEqualTo("WL_T2");
  }

  @Test
  void shippedSpecsParse() throws Exception {
    for (String name : new String[] {"simple", "long-tx", "ddl"}) {
      Path p = Path.of("src/main/resources/workloads/" + name + ".json");
      assertThat(p).exists();
      WorkloadSpec s = WorkloadSpec.parse(Files.readString(p));
      s.validate();
    }
  }

  @Test
  void invalidSpecsAreRejected() {
    WorkloadSpec s = WorkloadSpec.defaults();
    s.transactionsPerSession = 0;
    assertThatThrownBy(s::validate).isInstanceOf(IllegalArgumentException.class);
    s.transactionsPerSession = 1;
    s.sessions = 0;
    assertThatThrownBy(s::validate).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new WorkloadGenerator(s, "u", "a", "b"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void ledgerXidFormatMatchesLogMinerColumns() {
    assertThat(Ledger.xid(7, 23, 1234)).isEqualTo("7.23.1234");
    assertThat(Ledger.ddl("WL_LEDGER")).startsWith("CREATE TABLE WL_LEDGER (xid VARCHAR2(40)");
  }

  @Test
  void cliRequiresConnectionOptions() {
    assertThat(BenchMain.run("workload")).isEqualTo(2);
    assertThat(BenchMain.run()).isEqualTo(2);
  }
}
