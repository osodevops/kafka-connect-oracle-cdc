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
package sh.oso.connect.oracle.e2e.engine;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.errors.OraErrorClassifier;
import sh.oso.connect.oracle.core.errors.PrivilegeException;
import sh.oso.connect.oracle.core.jdbc.ConnectionFactory;
import sh.oso.connect.oracle.core.jdbc.ConnectionRole;
import sh.oso.connect.oracle.core.jdbc.OracleConnectionSpec;
import sh.oso.connect.oracle.core.jdbc.RetryPolicy;
import sh.oso.connect.oracle.core.jdbc.SessionInitializer;
import sh.oso.connect.oracle.core.testkit.FaultyJdbc;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;

/**
 * PRD-00 CORE-CONN against a real database: session settings, retry on connect, privilege errors.
 */
@Tag("engine")
class ConnectionEngineIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  CoreConfig config(String service, String user, String password) {
    Map<String, String> p = new HashMap<>();
    p.put(CoreConfig.DATABASE_HOST, db.container().getHost());
    p.put(CoreConfig.DATABASE_PORT, String.valueOf(db.container().getMappedPort(1521)));
    p.put(CoreConfig.DATABASE_SERVICE, service);
    p.put(CoreConfig.DATABASE_USER, user);
    p.put(CoreConfig.DATABASE_PASSWORD, password);
    return new CoreConfig(p);
  }

  @Test
  void everySessionCarriesTheFixedNlsSettingsTimeZoneAndModule() throws Exception {
    CoreConfig cfg =
        config(
            OracleTestDatabase.CDB_SERVICE,
            OracleTestDatabase.CAPTURE_USER,
            OracleTestDatabase.CAPTURE_PASSWORD);
    ConnectionFactory f =
        new ConnectionFactory(
            OracleConnectionSpec.from(cfg),
            new RetryPolicy(Duration.ofSeconds(30)),
            new OraErrorClassifier());
    try (Connection c = f.open(ConnectionRole.MINING);
        Statement st = c.createStatement()) {
      Map<String, String> nls = new HashMap<>();
      try (ResultSet rs = st.executeQuery("SELECT parameter, value FROM nls_session_parameters")) {
        while (rs.next()) {
          nls.put(rs.getString(1), rs.getString(2));
        }
      }
      assertThat(nls)
          .containsEntry("NLS_DATE_FORMAT", SessionInitializer.DATE_FORMAT)
          .containsEntry("NLS_TIMESTAMP_FORMAT", SessionInitializer.TIMESTAMP_FORMAT)
          .containsEntry("NLS_TIMESTAMP_TZ_FORMAT", SessionInitializer.TIMESTAMP_TZ_FORMAT)
          .containsEntry("NLS_NUMERIC_CHARACTERS", SessionInitializer.NUMERIC_CHARACTERS);
      try (ResultSet rs =
          st.executeQuery(
              "SELECT SESSIONTIMEZONE, SYS_CONTEXT('USERENV', 'MODULE'), SYS_CONTEXT('USERENV',"
                  + " 'ACTION') FROM dual")) {
        rs.next();
        assertThat(rs.getString(1)).isIn("UTC", "+00:00");
        assertThat(rs.getString(2)).isEqualTo("oso-cdc");
        assertThat(rs.getString(3)).isEqualTo("mining");
      }
      assertThat(c.getAutoCommit()).isFalse();
    }
  }

  @Test
  void transientFailureOnConnectIsRetried() throws Exception {
    CoreConfig cfg =
        config(
            OracleTestDatabase.CDB_SERVICE,
            OracleTestDatabase.CAPTURE_USER,
            OracleTestDatabase.CAPTURE_PASSWORD);
    FaultyJdbc faults = new FaultyJdbc();
    FaultyJdbc.Fault first = faults.failConnect(3113, 1);
    RetryPolicy quick =
        new RetryPolicy(
            Duration.ofMillis(50),
            Duration.ofMillis(200),
            Duration.ofSeconds(30),
            System::currentTimeMillis,
            d -> Thread.sleep(d.toMillis()));
    ConnectionFactory f =
        new ConnectionFactory(
            OracleConnectionSpec.from(cfg), faults.thinOpener(), quick, new OraErrorClassifier());
    try (Connection c = f.open(ConnectionRole.METADATA)) {
      assertThat(c.isValid(5)).isTrue();
    }
    assertThat(first.fired()).isEqualTo(1);
  }

  @Test
  void aUserWithoutLogMinerGrantsGetsAPrivilegeError() throws Exception {
    String user = "T_NOPRIV";
    try (Connection sys = db.sysdbaInPdb(OracleTestDatabase.PDB1);
        Statement st = sys.createStatement()) {
      st.execute(
          "BEGIN EXECUTE IMMEDIATE 'DROP USER "
              + user
              + " CASCADE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -1918 THEN RAISE; END IF;"
              + " END;");
      st.execute("CREATE USER " + user + " IDENTIFIED BY \"nopriv\"");
      st.execute("GRANT CREATE SESSION TO " + user);
    }
    CoreConfig cfg = config(OracleTestDatabase.PDB1, user, "nopriv");
    ConnectionFactory f =
        new ConnectionFactory(
            OracleConnectionSpec.from(cfg),
            new RetryPolicy(Duration.ofSeconds(10)),
            new OraErrorClassifier());
    OraErrorClassifier classifier = new OraErrorClassifier();
    try (Connection c = f.open(ConnectionRole.METADATA);
        Statement st = c.createStatement()) {
      SQLException denied =
          catchThrowableOfType(
              SQLException.class, () -> st.executeQuery("SELECT COUNT(*) FROM v$logmnr_contents"));
      assertThat((Throwable) denied).isNotNull();
      org.assertj.core.api.Assertions.assertThatObject(
              classifier.toException(denied, "Reading V$LOGMNR_CONTENTS"))
          .isInstanceOf(PrivilegeException.class);
    }
    assertThatThrownBy(
            () -> {
              CoreConfig wrong = config(OracleTestDatabase.PDB1, user, "wrong-password");
              new ConnectionFactory(
                      OracleConnectionSpec.from(wrong),
                      new RetryPolicy(Duration.ofSeconds(10)),
                      new OraErrorClassifier())
                  .open(ConnectionRole.METADATA);
            })
        .isInstanceOf(PrivilegeException.class)
        .hasMessageContaining("ORA-01017");
  }
}
