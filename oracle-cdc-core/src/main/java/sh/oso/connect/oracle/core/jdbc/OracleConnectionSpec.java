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

import java.util.Objects;
import java.util.Properties;
import sh.oso.connect.oracle.core.config.CoreConfig;

/**
 * How to reach the database: a JDBC URL and driver properties built from {@link CoreConfig} (PRD-00
 * CORE-CONN-1, CORE-CONN-3). Secrets never appear in {@link #toString()}.
 */
public final class OracleConnectionSpec {

  private final String url;
  private final String user;
  private final String password;
  private final Properties properties;

  OracleConnectionSpec(String url, String user, String password, Properties properties) {
    this.url = Objects.requireNonNull(url, "url");
    this.user = user;
    this.password = password;
    this.properties = properties;
  }

  public static OracleConnectionSpec from(CoreConfig config) {
    String url = config.getString(CoreConfig.DATABASE_URL);
    if (url == null || url.isBlank()) {
      String host = config.getString(CoreConfig.DATABASE_HOST);
      int port = config.getInt(CoreConfig.DATABASE_PORT);
      String service = config.getString(CoreConfig.DATABASE_SERVICE);
      String sid = config.getString(CoreConfig.DATABASE_SID);
      url =
          service != null && !service.isBlank()
              ? "jdbc:oracle:thin:@//" + host + ":" + port + "/" + service
              : "jdbc:oracle:thin:@" + host + ":" + port + ":" + sid;
    }
    Properties p = new Properties();
    // TCP keepalive so idle load balancers do not drop the mining connection (CORE-CONN-3)
    p.setProperty("oracle.net.keepAlive", "true");
    p.setProperty("oracle.jdbc.ReadTimeout", "0");
    p.setProperty("oracle.net.CONNECT_TIMEOUT", "30000");
    String wallet = config.getString(CoreConfig.DATABASE_WALLET_LOCATION);
    if (wallet != null && !wallet.isBlank()) {
      p.setProperty(
          "oracle.net.wallet_location",
          "(SOURCE=(METHOD=file)(METHOD_DATA=(DIRECTORY=" + wallet + ")))");
    }
    String ts = config.getString(CoreConfig.DATABASE_TLS_TRUSTSTORE_LOCATION);
    if (ts != null && !ts.isBlank()) {
      p.setProperty("javax.net.ssl.trustStore", ts);
      p.setProperty(
          "javax.net.ssl.trustStoreType",
          config.getString(CoreConfig.DATABASE_TLS_TRUSTSTORE_TYPE));
      if (config.getPassword(CoreConfig.DATABASE_TLS_TRUSTSTORE_PASSWORD) != null) {
        p.setProperty(
            "javax.net.ssl.trustStorePassword",
            config.getPassword(CoreConfig.DATABASE_TLS_TRUSTSTORE_PASSWORD).value());
      }
    }
    String extra = config.getString(CoreConfig.DATABASE_CONNECTION_PROPERTIES);
    if (extra != null && !extra.isBlank()) {
      for (String kv : extra.split(";")) {
        int eq = kv.indexOf('=');
        if (eq > 0) {
          p.setProperty(kv.substring(0, eq).trim(), kv.substring(eq + 1).trim());
        }
      }
    }
    String password = config.databasePassword() == null ? null : config.databasePassword().value();
    return new OracleConnectionSpec(url, config.getString(CoreConfig.DATABASE_USER), password, p);
  }

  public String url() {
    return url;
  }

  public String user() {
    return user;
  }

  /** Driver properties including credentials; never log the result. */
  public Properties toDriverProperties() {
    Properties all = new Properties();
    all.putAll(properties);
    if (user != null) {
      all.setProperty("user", user);
    }
    if (password != null) {
      all.setProperty("password", password);
    }
    return all;
  }

  /** Driver properties without credentials, for logging and diagnostics. */
  public Properties properties() {
    Properties copy = new Properties();
    copy.putAll(properties);
    copy.remove("password");
    copy.remove("javax.net.ssl.trustStorePassword");
    return copy;
  }

  @Override
  public String toString() {
    return "OracleConnectionSpec{url="
        + url
        + ", user="
        + user
        + ", properties="
        + properties().stringPropertyNames()
        + "}";
  }
}
