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
package sh.oso.connect.oracle.validation;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfig;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.doctor.Doctor;
import sh.oso.connect.oracle.core.doctor.DoctorContext;
import sh.oso.connect.oracle.core.doctor.Finding;
import sh.oso.connect.oracle.core.doctor.JdbcDoctorCatalog;
import sh.oso.connect.oracle.core.doctor.Report;
import sh.oso.connect.oracle.core.doctor.Rules;
import sh.oso.connect.oracle.core.doctor.Severity;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.errors.OracleCdcException;
import sh.oso.connect.oracle.core.jdbc.ConnectionFactory;
import sh.oso.connect.oracle.core.jdbc.ConnectionRole;
import sh.oso.connect.oracle.core.jdbc.OracleConnectionSpec;
import sh.oso.connect.oracle.core.jdbc.RetryPolicy;

/**
 * SRC-LC-1: validate() runs the doctor's fast rules against the database and attaches blocking
 * findings to the configuration key an operator would change.
 */
public final class ConnectorValidator {

  private ConnectorValidator() {}

  /** Adds doctor findings to {@code config}; connection failures land on the host key. */
  public static Config validate(Map<String, String> props, Config config) {
    OracleCdcSourceConnectorConfig cfg;
    try {
      cfg = new OracleCdcSourceConnectorConfig(props);
    } catch (RuntimeException e) {
      return config; // field-level errors were already reported by the ConfigDef
    }
    Report report;
    try {
      report = run(cfg);
    } catch (OracleCdcException e) {
      report = new Report(List.of(Finding.blocking("CONNECT", e.getMessage(), null)));
    } catch (Exception e) {
      report =
          new Report(
              List.of(
                  Finding.blocking(
                      "CONNECT", "The database connection failed: " + e.getMessage(), null)));
    }
    Map<String, ConfigValue> byKey = new java.util.HashMap<>();
    for (ConfigValue v : config.configValues()) {
      byKey.put(v.name(), v);
    }
    for (Finding f : report.findings()) {
      if (f.severity() != Severity.BLOCKING) {
        continue;
      }
      ConfigValue target = byKey.get(keyFor(f.rule()));
      if (target == null) {
        target = byKey.get(CoreConfig.DATABASE_HOST);
      }
      if (target != null) {
        target.addErrorMessage(f.rule() + ": " + f.message());
      }
    }
    return config;
  }

  static String keyFor(String rule) {
    switch (rule) {
      case "DOC-3":
      case "DOC-5":
      case "DOC-6":
        return OracleCdcSourceConnectorConfig.TABLES_INCLUDE;
      case "DOC-7":
        return OracleCdcSourceConnectorConfig.KEY_MISSING;
      case "DOC-4":
        return CoreConfig.DATABASE_USER;
      case "DOC-12":
        return CoreConfig.ARCHIVE_DESTINATION;
      case "DOC-14":
        return CoreConfig.CAPTURE_MODE;
      default:
        return CoreConfig.DATABASE_HOST;
    }
  }

  static Report run(OracleCdcSourceConnectorConfig cfg) throws Exception {
    CoreConfig core = cfg.core();
    ConnectionFactory factory =
        new ConnectionFactory(
            OracleConnectionSpec.from(core),
            new RetryPolicy(Duration.ofSeconds(20)),
            new OraErrorClassifier(Set.copyOf(core.extraRetryErrorCodes())));
    try (Connection c = factory.open(ConnectionRole.METADATA)) {
      DoctorContext ctx =
          new DoctorContext(
              core,
              new JdbcDoctorCatalog(c),
              cfg.tablesInclude(),
              cfg.tablesExclude(),
              cfg.keyMissing().name());
      return new Doctor(Rules.fastMode()).run(ctx);
    } catch (SQLException e) {
      throw new sh.oso.connect.oracle.core.errors.TransientDatabaseException(
          "The database connection failed: " + e.getMessage(),
          "Check host, port, service and credentials.",
          e);
    }
  }
}
