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
package sh.oso.connect.oracle.core.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.errors.PrivilegeException;
import sh.oso.connect.oracle.core.errors.TransientDatabaseException;
import sh.oso.connect.oracle.core.testkit.FaultyJdbc;
import sh.oso.connect.oracle.core.testkit.FixedClock;

/** Drives the factory with FaultyJdbc over a stub connection: no database needed. */
class ConnectionFactoryTest {

  private static final OracleConnectionSpec SPEC =
      OracleConnectionSpec.from(
          new CoreConfig(
              Map.of(
                  CoreConfig.DATABASE_HOST, "h",
                  CoreConfig.DATABASE_SERVICE, "S",
                  CoreConfig.DATABASE_USER, "u",
                  CoreConfig.DATABASE_PASSWORD, "p")));

  private static Object defaultFor(Class<?> type) {
    if (type == boolean.class) {
      return false;
    }
    if (type == int.class) {
      return 0;
    }
    if (type == long.class) {
      return 0L;
    }
    return null;
  }

  /** A connection whose statements succeed and whose result sets never end. */
  static Connection stub(AtomicInteger executed) {
    ResultSet rs =
        (ResultSet)
            Proxy.newProxyInstance(
                ResultSet.class.getClassLoader(),
                new Class<?>[] {ResultSet.class},
                (p, m, a) ->
                    switch (m.getName()) {
                      case "next" -> true;
                      case "getString", "getObject" -> "x";
                      case "getLong" -> 1L;
                      case "getInt" -> 1;
                      default -> defaultFor(m.getReturnType());
                    });
    InvocationHandler statementHandler =
        (p, m, a) -> {
          if (m.getName().startsWith("execute")) {
            executed.incrementAndGet();
            return m.getName().equals("executeQuery") ? rs : defaultFor(m.getReturnType());
          }
          return defaultFor(m.getReturnType());
        };
    Statement st =
        (Statement)
            Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class<?>[] {Statement.class},
                statementHandler);
    CallableStatement call =
        (CallableStatement)
            Proxy.newProxyInstance(
                CallableStatement.class.getClassLoader(),
                new Class<?>[] {CallableStatement.class},
                statementHandler);
    return (Connection)
        Proxy.newProxyInstance(
            Connection.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (p, m, a) ->
                switch (m.getName()) {
                  case "createStatement", "prepareStatement" -> st;
                  case "prepareCall" -> call;
                  case "isValid" -> true;
                  default -> defaultFor(m.getReturnType());
                });
  }

  private static RetryPolicy fastRetry(FixedClock clock, Duration budget) {
    return new RetryPolicy(
        Duration.ofMillis(10), Duration.ofMillis(50), budget, clock, clock::sleep);
  }

  @Test
  void initialisesEverySessionAndRetriesTransientConnectFailures() throws Exception {
    AtomicInteger executed = new AtomicInteger();
    FaultyJdbc faults = new FaultyJdbc();
    faults.failConnect(3113, 1);
    faults.failConnect(12541, 2);
    FixedClock clock = new FixedClock(0);
    ConnectionFactory f =
        new ConnectionFactory(
            SPEC,
            faults.wrap(spec -> stub(executed)),
            fastRetry(clock, Duration.ofSeconds(10)),
            new OraErrorClassifier());
    Connection c = f.open(ConnectionRole.MINING);
    assertThat(c).isNotNull();
    // the NLS and time zone statements plus the DBMS_APPLICATION_INFO call
    assertThat(executed.get()).isEqualTo(SessionInitializer.STATEMENTS.size() + 1);
  }

  @Test
  void privilegeFailuresStopImmediately() {
    FaultyJdbc faults = new FaultyJdbc();
    faults.failAlways(FaultyJdbc.Point.CONNECT, 1017, 1);
    ConnectionFactory f =
        new ConnectionFactory(
            SPEC,
            faults.wrap(spec -> stub(new AtomicInteger())),
            fastRetry(new FixedClock(0), Duration.ofSeconds(10)),
            new OraErrorClassifier());
    assertThatThrownBy(() -> f.open(ConnectionRole.METADATA))
        .isInstanceOf(PrivilegeException.class)
        .hasMessageContaining("ORA-01017");
  }

  @Test
  void transientFailuresExhaustTheBudget() {
    FaultyJdbc faults = new FaultyJdbc();
    faults.failAlways(FaultyJdbc.Point.CONNECT, 3113, 1);
    FixedClock clock = new FixedClock(0);
    ConnectionFactory f =
        new ConnectionFactory(
            SPEC,
            faults.wrap(spec -> stub(new AtomicInteger())),
            fastRetry(clock, Duration.ofMillis(200)),
            new OraErrorClassifier());
    assertThatThrownBy(() -> f.open(ConnectionRole.SNAPSHOT))
        .isInstanceOf(TransientDatabaseException.class)
        .hasMessageContaining("retry budget");
    assertThat(clock.getAsLong()).isLessThanOrEqualTo(200L);
  }

  @Test
  void faultyJdbcInjectsAtTheNthNextAndCommit() throws Exception {
    FaultyJdbc faults = new FaultyJdbc();
    FaultyJdbc.Fault atThird = faults.failNext(310, 3);
    faults.failCommit(3114, 1);
    Connection c = faults.wrap(stub(new AtomicInteger()));
    try (Statement st = c.createStatement();
        ResultSet rs = st.executeQuery("SELECT 1")) {
      assertThat(rs.next()).isTrue();
      assertThat(rs.next()).isTrue();
      assertThatThrownBy(rs::next)
          .isInstanceOf(SQLException.class)
          .hasMessageContaining("ORA-00310");
      assertThat(rs.next()).as("faults fire once").isTrue();
    }
    assertThat(atThird.fired()).isEqualTo(1);
    assertThatThrownBy(c::commit)
        .isInstanceOf(SQLException.class)
        .hasMessageContaining("ORA-03114");
    c.commit();
  }
}
