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
package sh.oso.connect.oracle.core.mining;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class LogMinerQueryTest {

  @Test
  void threeBranchesAreAlwaysPresent() {
    String sql = LogMinerQuery.sql(MiningFilter.of(3, Set.of(1001L, 1002L), Set.of("APP")));
    assertThat(sql)
        .startsWith("SELECT SCN, START_SCN, COMMIT_SCN")
        .contains(" FROM V$LOGMNR_CONTENTS WHERE SCN >= ? AND SCN < ? AND (")
        .contains("(SRC_CON_ID = 3 AND (DATA_OBJ# IN (1001, 1002)))")
        .contains(
            " OR (OPERATION_CODE = 5 AND (SEG_OWNER IS NULL OR SEG_OWNER NOT IN ('ANONYMOUS',")
        .contains(" OR (OPERATION_CODE IN (6, 7, 36))")
        .contains(" OR OPERATION_CODE = 34)")
        .doesNotContain("USERNAME NOT IN");
    assertThat(sql.chars().filter(ch -> ch == '?').count()).isEqualTo(2);
  }

  @Test
  void excludedUsersAreDroppedFromTransactionControlRows() {
    String sql =
        LogMinerQuery.sql(
            new MiningFilter(
                Map.of(3, Set.of(1L)), Set.of("APP"), Set.of("GGADMIN", "O'HARA"), 1000));
    assertThat(sql)
        .contains(
            "(OPERATION_CODE IN (6, 7, 36) AND (USERNAME IS NULL OR USERNAME NOT IN ('GGADMIN',"
                + " 'O''HARA')))");
  }

  @Test
  void emptyObjectSetsStillMineDdlAndTransactionControl() {
    String sql = LogMinerQuery.sql(MiningFilter.of(Map.of(), Set.of()));
    assertThat(sql)
        .contains("(OPERATION_CODE IN (1, 2, 3, 9, 10, 11, 28, 255) AND 1 = 0)")
        .contains("(OPERATION_CODE = 5 AND (SEG_OWNER IS NULL OR SEG_OWNER NOT IN (")
        .contains("(OPERATION_CODE IN (6, 7, 36))");
  }

  @Test
  void inListsAreChunkedBelowTheOracleLimitAndSorted() {
    Set<Long> ids = new HashSet<>();
    for (long i = 2500; i > 0; i--) {
      ids.add(i);
    }
    String sql = LogMinerQuery.sql(new MiningFilter(Map.of(0, ids), Set.of("A"), Set.of(), 1000));
    assertThat(sql.split("DATA_OBJ# IN \\(").length - 1).isEqualTo(3);
    assertThat(sql).contains("DATA_OBJ# IN (1, 2, 3,").contains(", 1000) OR DATA_OBJ# IN (1001,");
    assertThatThrownBy(() -> new MiningFilter(Map.of(0, ids), Set.of(), Set.of(), 1001))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void containersAreKeptApartAndNonCdbHasNoContainerPredicate() {
    String cdb =
        LogMinerQuery.sql(MiningFilter.of(Map.of(3, Set.of(10L), 4, Set.of(10L, 11L)), Set.of()));
    assertThat(cdb)
        .contains(
            "((SRC_CON_ID = 3 AND (DATA_OBJ# IN (10))) OR (SRC_CON_ID = 4 AND (DATA_OBJ# IN (10,"
                + " 11))))");
    String nonCdb = LogMinerQuery.sql(MiningFilter.of(0, Set.of(10L), Set.of()));
    assertThat(nonCdb).contains("((DATA_OBJ# IN (10)))").doesNotContain("SRC_CON_ID =");
    assertThat(MiningFilter.of(Map.of(3, Set.of()), Set.of()).isEmpty()).isTrue();
  }

  @Test
  void columnOrderIsTheMapperContract() {
    assertThat(LogMinerQuery.COLUMNS).hasSize(34).startsWith("SCN").endsWith("SQL_UNDO");
    assertThat(LogMinerQuery.COLUMNS.indexOf("CSF")).isEqualTo(24);
    assertThat(LogMinerQuery.COLUMNS.indexOf("SQL_REDO")).isEqualTo(32);
  }
}
