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
package sh.oso.connect.oracle.doctor.testing;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.doctor.MetricsSample;
import sh.oso.connect.oracle.doctor.admin.Database;
import sh.oso.connect.oracle.doctor.admin.Environment;
import sh.oso.connect.oracle.doctor.cli.AdminMain;
import sh.oso.connect.oracle.doctor.cli.DoctorMain;
import sh.oso.connect.oracle.doctor.connect.ConnectApi;
import sh.oso.connect.oracle.doctor.kafka.KafkaPort;
import sh.oso.connect.oracle.doctor.metrics.MetricsSource;

/**
 * Every outside dependency of the CLI in memory, sharing one action log so tests can assert the
 * order of stop, ops events, signals and the offset PATCH. Sleeping advances the clock.
 */
public final class FakeEnv implements Environment {

  public static final String PASSWORD = "s3cret-pw";

  public final List<String> log = new ArrayList<>();
  public final FakeConnect connect = new FakeConnect(log);
  public final FakeKafka kafka = new FakeKafka(log);
  public final FakeDatabase db = new FakeDatabase();
  public final Deque<MetricsSample> samples = new ArrayDeque<>();
  public Instant now = Instant.parse("2026-10-05T12:00:00Z");
  public Properties kafkaProps;
  public CoreConfig databaseConfig;
  public String connectUrl;
  public String metricsUrl;
  public Exception databaseFailure;

  public FakeEnv() {
    connect.config.putAll(connectorConfig());
  }

  /** A valid connector configuration with broker access, topic prefix {@code cdc}. */
  public static Map<String, String> connectorConfig() {
    Map<String, String> m = new LinkedHashMap<>();
    m.put("connector.class", "sh.oso.connect.oracle.OracleCdcSourceConnector");
    m.put(CoreConfig.DATABASE_HOST, "db");
    m.put(CoreConfig.DATABASE_SERVICE, "FREE");
    m.put(CoreConfig.DATABASE_USER, "C##CDC");
    m.put(CoreConfig.DATABASE_PASSWORD, PASSWORD);
    m.put(CoreConfig.DATABASE_PDBS, "FREEPDB1");
    m.put("cdc.topic.prefix", "cdc");
    m.put("cdc.tables.include", "FREEPDB1\\.APP\\..*");
    m.put("cdc.kafka.bootstrap.servers", "broker:9092");
    m.put("cdc.kafka.security.protocol", "PLAINTEXT");
    return m;
  }

  @Override
  public ConnectApi connect(String url) {
    connectUrl = url;
    return connect;
  }

  @Override
  public KafkaPort kafka(Properties clientProps) {
    kafkaProps = clientProps;
    return kafka;
  }

  @Override
  public Database database(CoreConfig config) throws Exception {
    if (databaseFailure != null) {
      throw databaseFailure;
    }
    databaseConfig = config;
    return db;
  }

  @Override
  public MetricsSource jmx(String url) {
    metricsUrl = url;
    return server -> samples.removeFirst();
  }

  @Override
  public MetricsSource prometheus(String url) {
    return jmx(url);
  }

  @Override
  public Clock clock() {
    FakeEnv env = this;
    return new Clock() {
      @Override
      public ZoneId getZone() {
        return ZoneOffset.UTC;
      }

      @Override
      public Clock withZone(ZoneId zone) {
        return this;
      }

      @Override
      public Instant instant() {
        return env.now;
      }
    };
  }

  @Override
  public void sleep(Duration d) {
    now = now.plus(d);
  }

  @Override
  public String operator() {
    return "tester";
  }

  /** Output of one CLI run. */
  public record Run(int exit, String out, String err) {}

  public Run doctor(String... args) {
    StringWriter out = new StringWriter();
    StringWriter err = new StringWriter();
    int exit = DoctorMain.run(this, new PrintWriter(out), new PrintWriter(err), args);
    return new Run(exit, out.toString(), err.toString());
  }

  public Run admin(String... args) {
    StringWriter out = new StringWriter();
    StringWriter err = new StringWriter();
    int exit = AdminMain.run(this, new PrintWriter(out), new PrintWriter(err), args);
    return new Run(exit, out.toString(), err.toString());
  }
}
