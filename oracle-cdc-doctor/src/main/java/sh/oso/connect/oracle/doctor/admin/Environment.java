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

import java.io.IOException;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;
import java.util.Properties;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.doctor.connect.ConnectApi;
import sh.oso.connect.oracle.doctor.connect.HttpConnectApi;
import sh.oso.connect.oracle.doctor.kafka.KafkaClientsPort;
import sh.oso.connect.oracle.doctor.kafka.KafkaPort;
import sh.oso.connect.oracle.doctor.metrics.JmxMetricsSource;
import sh.oso.connect.oracle.doctor.metrics.MetricsSource;
import sh.oso.connect.oracle.doctor.metrics.PrometheusMetricsSource;

/**
 * Everything the commands reach outside the process: the Connect REST API, Kafka, the database,
 * metrics endpoints, the clock and sleeping. {@link #standard()} is the real one; tests pass fakes.
 */
public interface Environment {

  ConnectApi connect(String url);

  KafkaPort kafka(Properties clientProps);

  Database database(CoreConfig config) throws Exception;

  MetricsSource jmx(String url) throws IOException;

  MetricsSource prometheus(String url);

  Clock clock();

  void sleep(Duration d) throws InterruptedException;

  /** Who runs the command, recorded on the ops topic. */
  default String operator() {
    return System.getProperty("user.name", "unknown");
  }

  static Environment standard() {
    Clock utc = Clock.system(ZoneOffset.UTC);
    return new Environment() {
      @Override
      public ConnectApi connect(String url) {
        return new HttpConnectApi(URI.create(url));
      }

      @Override
      public KafkaPort kafka(Properties clientProps) {
        return new KafkaClientsPort(clientProps);
      }

      @Override
      public Database database(CoreConfig config) throws Exception {
        return JdbcDatabase.open(config);
      }

      @Override
      public MetricsSource jmx(String url) throws IOException {
        return JmxMetricsSource.connect(url, utc);
      }

      @Override
      public MetricsSource prometheus(String url) {
        return new PrometheusMetricsSource(URI.create(url), utc);
      }

      @Override
      public Clock clock() {
        return utc;
      }

      @Override
      public void sleep(Duration d) throws InterruptedException {
        Thread.sleep(d.toMillis());
      }
    };
  }
}
