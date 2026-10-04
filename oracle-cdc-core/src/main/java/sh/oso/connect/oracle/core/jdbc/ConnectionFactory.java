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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.oracle.core.errors.ErrorCode;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.errors.OracleCdcException;
import sh.oso.connect.oracle.core.errors.TransientDatabaseException;

/**
 * Opens initialised connections per role and retries transient failures within the budget. The
 * low-level opener is injectable so tests can wrap it with {@code FaultyJdbc}.
 */
public final class ConnectionFactory {

  /** Opens a raw connection; the default uses the Oracle thin driver. */
  public interface Opener {
    Connection open(OracleConnectionSpec spec) throws SQLException;
  }

  private static final Logger LOG = LoggerFactory.getLogger(ConnectionFactory.class);

  private final OracleConnectionSpec spec;
  private final Opener opener;
  private final RetryPolicy retry;
  private final OraErrorClassifier classifier;

  public ConnectionFactory(
      OracleConnectionSpec spec, RetryPolicy retry, OraErrorClassifier classifier) {
    this(spec, ConnectionFactory::openThin, retry, classifier);
  }

  public ConnectionFactory(
      OracleConnectionSpec spec, Opener opener, RetryPolicy retry, OraErrorClassifier classifier) {
    this.spec = Objects.requireNonNull(spec);
    this.opener = Objects.requireNonNull(opener);
    this.retry = Objects.requireNonNull(retry);
    this.classifier = Objects.requireNonNull(classifier);
  }

  static Connection openThin(OracleConnectionSpec spec) throws SQLException {
    return java.sql.DriverManager.getConnection(spec.url(), spec.toDriverProperties());
  }

  /**
   * Opens and initialises a connection, retrying transient errors; stop conditions throw at once.
   */
  public Connection open(ConnectionRole role) throws InterruptedException {
    RetryPolicy.Attempts attempts = retry.begin();
    while (true) {
      try {
        Connection c = opener.open(spec);
        try {
          SessionInitializer.apply(c, role);
        } catch (SQLException e) {
          try {
            c.close();
          } catch (SQLException ignore) {
            // closing a broken connection
          }
          throw e;
        }
        return c;
      } catch (SQLException e) {
        OracleCdcException typed =
            classifier.toException(
                e, "Opening the " + role.name().toLowerCase() + " connection to " + spec.url());
        if (typed == null || typed.code() != ErrorCode.TRANSIENT_DATABASE) {
          throw typed != null
              ? typed
              : new OracleCdcException(
                  ErrorCode.TRANSIENT_DATABASE,
                  "Opening the connection failed.",
                  "Check the database and network.",
                  e);
        }
        if (!attempts.backoff()) {
          throw new TransientDatabaseException(
              "Could not open the "
                  + role.name().toLowerCase()
                  + " connection within the retry budget of "
                  + retry.budget().toSeconds()
                  + " seconds.",
              "Check the database, listener and network, then restart the task.",
              e);
        }
        LOG.warn(
            "Connection attempt {} for role {} failed with ORA-{}; retrying",
            attempts.attempt(),
            role,
            OraErrorClassifier.oraCode(e));
      }
    }
  }

  public OracleConnectionSpec spec() {
    return spec;
  }

  /** Runs an action with retries for transient failures, used by probes and metadata queries. */
  public <T> T withRetry(Supplier<T> action, String context) throws InterruptedException {
    RetryPolicy.Attempts attempts = retry.begin();
    while (true) {
      try {
        return action.get();
      } catch (RuntimeException e) {
        OracleCdcException typed =
            e instanceof OracleCdcException oce ? oce : classifier.toException(e, context);
        if (typed == null || typed.code() != ErrorCode.TRANSIENT_DATABASE || !attempts.backoff()) {
          throw typed != null ? typed : e;
        }
      }
    }
  }
}
