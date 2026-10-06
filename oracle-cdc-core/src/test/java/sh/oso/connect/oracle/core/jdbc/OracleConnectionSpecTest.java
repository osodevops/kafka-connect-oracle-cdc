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

import java.util.Map;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.config.CoreConfig;

class OracleConnectionSpecTest {

  private static CoreConfig config(Map<String, String> extra) {
    Map<String, String> p =
        new java.util.HashMap<>(
            Map.of(CoreConfig.DATABASE_USER, "C##CDC", CoreConfig.DATABASE_PASSWORD, "secret"));
    p.putAll(extra);
    return new CoreConfig(p);
  }

  @Test
  void serviceNameUrl() {
    OracleConnectionSpec s =
        OracleConnectionSpec.from(
            config(Map.of(CoreConfig.DATABASE_HOST, "db", CoreConfig.DATABASE_SERVICE, "FREE")));
    assertThat(s.url()).isEqualTo("jdbc:oracle:thin:@//db:1521/FREE");
    assertThat(s.toDriverProperties().getProperty("oracle.net.keepAlive")).isEqualTo("true");
    assertThat(s.toDriverProperties().getProperty("oracle.jdbc.ReadTimeout"))
        .as("the default mining query timeout plus a minute")
        .isEqualTo("660000");
    assertThat(s.toDriverProperties().getProperty("user")).isEqualTo("C##CDC");
    assertThat(s.toDriverProperties().getProperty("password")).isEqualTo("secret");
  }

  @Test
  void sidUrl() {
    OracleConnectionSpec s =
        OracleConnectionSpec.from(
            config(
                Map.of(
                    CoreConfig.DATABASE_HOST,
                    "db",
                    CoreConfig.DATABASE_PORT,
                    "1522",
                    CoreConfig.DATABASE_SID,
                    "ORCL")));
    assertThat(s.url()).isEqualTo("jdbc:oracle:thin:@db:1522:ORCL");
  }

  @Test
  void fullUrlWinsAndExtraPropertiesApply() {
    OracleConnectionSpec s =
        OracleConnectionSpec.from(
            config(
                Map.of(
                    CoreConfig.DATABASE_HOST, "ignored",
                    CoreConfig.DATABASE_SERVICE, "X",
                    CoreConfig.DATABASE_URL,
                        "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=tcps)(HOST=h)(PORT=2484))(CONNECT_DATA=(SERVICE_NAME=S)))",
                    CoreConfig.DATABASE_CONNECTION_PROPERTIES,
                        "oracle.net.ssl_server_dn_match=true; oracle.jdbc.fanEnabled=false",
                    CoreConfig.DATABASE_WALLET_LOCATION, "/wallet")));
    assertThat(s.url()).startsWith("jdbc:oracle:thin:@(DESCRIPTION");
    assertThat(s.properties().getProperty("oracle.net.ssl_server_dn_match")).isEqualTo("true");
    assertThat(s.properties().getProperty("oracle.jdbc.fanEnabled")).isEqualTo("false");
    assertThat(s.properties().getProperty("oracle.net.wallet_location")).contains("/wallet");
  }

  @Test
  void secretsNeverLeakThroughToStringOrProperties() {
    OracleConnectionSpec s =
        OracleConnectionSpec.from(
            config(
                Map.of(
                    CoreConfig.DATABASE_HOST,
                    "db",
                    CoreConfig.DATABASE_SERVICE,
                    "FREE",
                    CoreConfig.DATABASE_TLS_TRUSTSTORE_LOCATION,
                    "/ts",
                    CoreConfig.DATABASE_TLS_TRUSTSTORE_PASSWORD,
                    "tspw")));
    assertThat(s.toString()).doesNotContain("secret").doesNotContain("tspw");
    assertThat(s.properties())
        .doesNotContainKey("password")
        .doesNotContainKey("javax.net.ssl.trustStorePassword");
    assertThat(s.toDriverProperties().getProperty("javax.net.ssl.trustStorePassword"))
        .isEqualTo("tspw");
  }
}
