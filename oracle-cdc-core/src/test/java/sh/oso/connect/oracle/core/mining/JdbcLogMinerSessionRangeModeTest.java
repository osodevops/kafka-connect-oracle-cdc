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

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.errors.DictionaryUnavailableException;
import sh.oso.connect.oracle.core.logs.RedoLog;

/**
 * ADR-0027: in range mode the session never adds a log (a PDB refuses ADD_LOGFILE with ORA-65040)
 * and a start that needs a dictionary from the redo stops with CDC-6001 instead of mining.
 */
class JdbcLogMinerSessionRangeModeTest {

  final AtomicInteger calls = new AtomicInteger();

  /** A connection that counts every prepared call and otherwise does nothing. */
  Connection connection() {
    return (Connection)
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {Connection.class},
            (p, m, a) -> {
              if (m.getName().equals("prepareCall")) {
                calls.incrementAndGet();
                throw new AssertionError("no LogMiner call expected: " + a[0]);
              }
              return m.getName().equals("isClosed") ? Boolean.FALSE : null;
            });
  }

  static RedoLog log(long seq) {
    return new RedoLog(
        1, seq, seq * 100, seq * 100 + 100, "/arch/s" + seq, true, "A", false, 1, false, false);
  }

  @Test
  void rangeModeNeverAddsALogFile() throws Exception {
    JdbcLogMinerSession s = new JdbcLogMinerSession(connection(), 100, Duration.ofMinutes(1), true);
    s.setLogs(List.of(log(1), log(2)));
    s.setLogs(List.of(log(2), log(3)));
    assertThat(calls).hasValue(0);
    assertThat(s.rangeOnly()).isTrue();
  }

  @Test
  void aRedoDictionaryStartStopsWithCdc6001InRangeMode() {
    JdbcLogMinerSession s = new JdbcLogMinerSession(connection(), 100, Duration.ofMinutes(1), true);
    assertThatThrownBy(() -> s.start(100, 200, DictionaryMode.REDO_LOGS_WITH_DDL_TRACKING))
        .isInstanceOf(DictionaryUnavailableException.class)
        .hasMessageContaining("CDC-6001")
        .hasMessageContaining("range mode");
    assertThat(calls).hasValue(0);
  }

  @Test
  void logsModeIsTheDefault() {
    assertThat(new JdbcLogMinerSession(connection(), 100, Duration.ofMinutes(1)).rangeOnly())
        .isFalse();
  }
}
