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
package sh.oso.connect.oracle.core.testkit;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import sh.oso.connect.oracle.core.jdbc.ConnectionFactory;
import sh.oso.connect.oracle.core.jdbc.OracleConnectionSpec;

/**
 * JDBC fault injection (PRD-00 CORE-TEST-2): wraps a real or fake {@link Connection} in dynamic
 * proxies and throws a chosen ORA error at a chosen point: connect, the Nth execute, the Nth {@code
 * ResultSet.next()}, or commit. Faults fire once each unless marked sticky.
 */
public final class FaultyJdbc {

  public enum Point {
    CONNECT,
    EXECUTE,
    NEXT,
    COMMIT
  }

  /** One scheduled failure. */
  public static final class Fault {
    final Point point;
    final int oraCode;
    final int onCall;
    final boolean sticky;
    final AtomicInteger seen = new AtomicInteger();
    final AtomicInteger fired = new AtomicInteger();

    Fault(Point point, int oraCode, int onCall, boolean sticky) {
      this.point = point;
      this.oraCode = oraCode;
      this.onCall = onCall;
      this.sticky = sticky;
    }

    public int fired() {
      return fired.get();
    }

    boolean shouldFire() {
      int n = seen.incrementAndGet();
      if (sticky ? n >= onCall : n == onCall) {
        fired.incrementAndGet();
        return true;
      }
      return false;
    }

    SQLException exception() {
      String msg = "ORA-" + String.format("%05d", oraCode) + ": injected by FaultyJdbc";
      // network-style codes surface as recoverable, as the Oracle driver does
      return oraCode == 3113 || oraCode == 3114 || oraCode == 17002
          ? new SQLRecoverableException(msg, "08006", oraCode)
          : new SQLException(msg, "72000", oraCode);
    }
  }

  private final List<Fault> faults = new ArrayList<>();

  /** Fails the Nth connect with the given ORA code. */
  public Fault failConnect(int oraCode, int onCall) {
    return add(Point.CONNECT, oraCode, onCall, false);
  }

  /** Fails the Nth statement execution (execute, executeQuery, executeUpdate). */
  public Fault failExecute(int oraCode, int onCall) {
    return add(Point.EXECUTE, oraCode, onCall, false);
  }

  /** Fails the Nth {@code ResultSet.next()} across all result sets. */
  public Fault failNext(int oraCode, int onCall) {
    return add(Point.NEXT, oraCode, onCall, false);
  }

  public Fault failCommit(int oraCode, int onCall) {
    return add(Point.COMMIT, oraCode, onCall, false);
  }

  /** Fails every call at the point from the Nth on. */
  public Fault failAlways(Point point, int oraCode, int fromCall) {
    return add(point, oraCode, fromCall, true);
  }

  private Fault add(Point point, int oraCode, int onCall, boolean sticky) {
    Fault f = new Fault(point, oraCode, onCall, sticky);
    faults.add(f);
    return f;
  }

  private void check(Point point) throws SQLException {
    for (Fault f : faults) {
      if (f.point == point && f.shouldFire()) {
        throw f.exception();
      }
    }
  }

  /** Wraps an opener so connects can fail and every connection is proxied. */
  public ConnectionFactory.Opener wrap(ConnectionFactory.Opener real) {
    return spec -> {
      check(Point.CONNECT);
      return wrap(real.open(spec));
    };
  }

  /** Proxies a connection so statements and result sets report to this injector. */
  public Connection wrap(Connection real) {
    return (Connection) proxy(real, Connection.class);
  }

  private Object proxy(Object target, Class<?> iface) {
    InvocationHandler h =
        (p, m, args) -> {
          String name = m.getName();
          if (target instanceof Connection && "commit".equals(name)) {
            check(Point.COMMIT);
          }
          if (target instanceof Statement && name.startsWith("execute")) {
            check(Point.EXECUTE);
          }
          if (target instanceof ResultSet && "next".equals(name)) {
            check(Point.NEXT);
          }
          Object result;
          try {
            result = m.invoke(target, args);
          } catch (InvocationTargetException e) {
            throw e.getCause();
          }
          if (result instanceof CallableStatement cs) {
            return proxy(cs, CallableStatement.class);
          }
          if (result instanceof PreparedStatement ps) {
            return proxy(ps, PreparedStatement.class);
          }
          if (result instanceof Statement st) {
            return proxy(st, Statement.class);
          }
          if (result instanceof ResultSet rs) {
            return proxy(rs, ResultSet.class);
          }
          return result;
        };
    return Proxy.newProxyInstance(iface.getClassLoader(), new Class<?>[] {iface}, h);
  }

  /** Convenience for integration tests: a factory over a spec whose connects and calls can fail. */
  public ConnectionFactory.Opener thinOpener() {
    return wrap(
        spec -> java.sql.DriverManager.getConnection(spec.url(), spec.toDriverProperties()));
  }

  @SuppressWarnings("unused")
  private static OracleConnectionSpec unused() {
    return null;
  }
}
