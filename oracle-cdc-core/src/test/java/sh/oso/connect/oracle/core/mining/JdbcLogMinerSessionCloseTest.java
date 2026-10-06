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
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.Statement;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/** Closing a mining session never fails on a connection that is already gone. */
class JdbcLogMinerSessionCloseTest {

  final AtomicBoolean closed = new AtomicBoolean();
  final AtomicInteger executes = new AtomicInteger();

  /** A connection whose statements throw {@code failure}; {@code isClosed} reports {@code gone}. */
  Connection connection(SQLException failure, boolean gone) {
    Statement st =
        (Statement)
            Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[] {Statement.class},
                (p, m, a) -> {
                  if (m.getName().equals("execute")) {
                    executes.incrementAndGet();
                    throw failure;
                  }
                  return null;
                });
    return (Connection)
        Proxy.newProxyInstance(
            getClass().getClassLoader(),
            new Class<?>[] {Connection.class},
            (p, m, a) ->
                switch (m.getName()) {
                  case "createStatement" -> st;
                  case "isClosed" -> gone || closed.get();
                  case "close" -> {
                    closed.set(true);
                    yield null;
                  }
                  default -> null;
                });
  }

  @Test
  void aBrokenConnectionClosesQuietly() throws Exception {
    SQLException dead = new SQLRecoverableException("ORA-17008: Closed connection", "08003", 17008);
    new JdbcLogMinerSession(connection(dead, false), 100, Duration.ofMinutes(1)).close();
    assertThat(closed).isTrue();
  }

  @Test
  void aClosedConnectionIsNotAskedToEndTheSession() throws Exception {
    SQLException any = new SQLException("unexpected", "99999", 1);
    new JdbcLogMinerSession(connection(any, true), 100, Duration.ofMinutes(1)).close();
    assertThat(executes).hasValue(0);
    assertThat(closed).isTrue();
  }

  @Test
  void anyOtherFailureToEndTheSessionIsStillReported() {
    SQLException denied = new SQLException("ORA-01031: insufficient privileges", "42000", 1031);
    assertThatThrownBy(
            () ->
                new JdbcLogMinerSession(connection(denied, false), 100, Duration.ofMinutes(1))
                    .close())
        .isSameAs(denied);
    assertThat(closed).isTrue();
  }
}
