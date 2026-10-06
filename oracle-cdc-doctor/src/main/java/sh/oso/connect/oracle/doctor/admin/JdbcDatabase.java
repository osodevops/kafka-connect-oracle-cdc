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
package sh.oso.connect.oracle.doctor.admin;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Set;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.doctor.DoctorCatalog;
import sh.oso.connect.oracle.core.doctor.JdbcDoctorCatalog;
import sh.oso.connect.oracle.core.doctor.JdbcRedoSampler;
import sh.oso.connect.oracle.core.doctor.RedoSampler;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.jdbc.ConnectionFactory;
import sh.oso.connect.oracle.core.jdbc.ConnectionRole;
import sh.oso.connect.oracle.core.jdbc.OracleConnectionSpec;
import sh.oso.connect.oracle.core.jdbc.RetryPolicy;
import sh.oso.connect.oracle.core.orphan.JdbcTransactionProbe;
import sh.oso.connect.oracle.core.orphan.TransactionProbe;

/** {@link Database} over one metadata connection opened from the connector's configuration. */
public final class JdbcDatabase implements Database {

  private final Connection c;

  private JdbcDatabase(Connection c) {
    this.c = c;
  }

  public static JdbcDatabase open(CoreConfig cfg) throws InterruptedException {
    ConnectionFactory factory =
        new ConnectionFactory(
            OracleConnectionSpec.from(cfg),
            new RetryPolicy(Duration.ofSeconds(30)),
            new OraErrorClassifier(Set.copyOf(cfg.extraRetryErrorCodes())));
    return new JdbcDatabase(factory.open(ConnectionRole.METADATA));
  }

  @Override
  public sh.oso.connect.oracle.core.doctor.RedoAvailability.LogProbe logProbe() {
    return logs -> {
      sh.oso.connect.oracle.core.mining.JdbcLogMinerSession session =
          new sh.oso.connect.oracle.core.mining.JdbcLogMinerSession(c, 1, Duration.ofMinutes(1));
      try {
        session.setLogs(logs); // names the first file LogMiner cannot open, with its ORA code
      } finally {
        session.end();
      }
    };
  }

  @Override
  public DoctorCatalog catalog() {
    return new JdbcDoctorCatalog(c);
  }

  @Override
  public TransactionProbe transactions() {
    return new JdbcTransactionProbe(() -> c);
  }

  @Override
  public RedoSampler sampler(Duration timeout) {
    return new JdbcRedoSampler(c, timeout);
  }

  @Override
  public Duration scnAge(long scn) throws SQLException {
    try (PreparedStatement ps =
        c.prepareStatement(
            "SELECT ROUND((SYSDATE - CAST(SCN_TO_TIMESTAMP(?) AS DATE)) * 86400) FROM dual")) {
      ps.setLong(1, scn);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? Duration.ofSeconds(rs.getLong(1)) : null;
      }
    } catch (SQLException e) {
      if (e.getErrorCode() == 8181) { // the SCN is older than the database's SCN to time map
        return null;
      }
      throw e;
    }
  }

  @Override
  public void close() {
    try {
      c.close();
    } catch (SQLException ignore) {
      // closing a broken connection
    }
  }
}
