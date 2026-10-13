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

import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class JdbcLogMinerSessionSqlTextTest {
  @Test
  void requestedSqlRestoresOneDelimiterOnlyAfterTheFinalCsfPieceAndNeverAltersLobBlocks()
      throws Exception {
    for (int code : List.of(1, 2, 3, 5, 10, 11, 29)) {
      for (boolean continued : List.of(false, true)) {
        java.sql.ResultSet result = org.mockito.Mockito.mock(java.sql.ResultSet.class);
        org.mockito.Mockito.when(result.getInt(11)).thenReturn(code);
        org.mockito.Mockito.when(result.getInt(25)).thenReturn(continued ? 1 : 0);
        org.mockito.Mockito.when(result.getString(33)).thenReturn("redo");
        org.mockito.Mockito.when(result.getString(34)).thenReturn("undo");
        LogMinerRow row = JdbcLogMinerSession.map(result).withSqlDelimiters();
        boolean delimited = !continued && code < 10;
        assertThat(row.sqlRedo()).isEqualTo(delimited ? "redo;" : "redo");
        assertThat(row.sqlUndo()).isEqualTo(delimited ? "undo;" : "undo");
        assertThat(row.csf()).isEqualTo(continued);
        assertThat(row.operationCode()).isEqualTo(code);
      }
    }
  }

  @Test
  void onlyRequestedSqlTextChangesTheMiningFlagsInBothDictionaryModes() throws Exception {
    for (DictionaryMode mode : DictionaryMode.values()) {
      for (boolean enabled : List.of(false, true)) {
        List<String> calls = new ArrayList<>();
        CallableStatement statement =
            (CallableStatement)
                Proxy.newProxyInstance(
                    getClass().getClassLoader(),
                    new Class<?>[] {CallableStatement.class},
                    (p, m, a) -> m.getReturnType() == boolean.class ? false : null);
        Connection connection =
            (Connection)
                Proxy.newProxyInstance(
                    getClass().getClassLoader(),
                    new Class<?>[] {Connection.class},
                    (p, m, a) -> {
                      if (m.getName().equals("prepareCall")) {
                        calls.add((String) a[0]);
                        return statement;
                      }
                      if (m.getName().equals("createStatement")) {
                        return statement;
                      }
                      return m.getReturnType() == boolean.class ? false : null;
                    });
        try (JdbcLogMinerSession session =
            new JdbcLogMinerSession(connection, 100, Duration.ofMinutes(1))
                .withVerbatimSql(enabled)) {
          session.start(100, 200, mode);
          String start = calls.get(0);
          assertThat(start.contains("NO_ROWID_IN_STMT")).isEqualTo(!enabled);
          // Oracle otherwise splits the locator declaration from its LOB writes.
          assertThat(start).contains("NO_SQL_DELIMITER");
          assertThat(start).doesNotContain("COMMITTED_DATA_ONLY", "SKIP_CORRUPTION");
          assertThat(start)
              .contains(
                  mode == DictionaryMode.ONLINE_CATALOG
                      ? "DICT_FROM_ONLINE_CATALOG"
                      : "DICT_FROM_REDO_LOGS");
        }
      }
    }
  }
}
