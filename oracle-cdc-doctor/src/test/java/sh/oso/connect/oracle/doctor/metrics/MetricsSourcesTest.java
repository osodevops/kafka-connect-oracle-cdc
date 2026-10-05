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
package sh.oso.connect.oracle.doctor.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.doctor.MetricsSample;
import sh.oso.connect.oracle.core.metrics.TaskMetrics;
import sh.oso.connect.oracle.core.metrics.TaskMetricsMXBean;
import sh.oso.connect.oracle.core.metrics.TransactionInfo;

class MetricsSourcesTest {

  static final Clock CLOCK = Clock.fixed(Instant.parse("2026-10-05T12:00:00Z"), ZoneOffset.UTC);

  static final String EXPORT =
      "# HELP oracle_cdc_queue_depth Records waiting for Kafka Connect to poll.\n"
          + "# TYPE oracle_cdc_queue_depth gauge\n"
          + "oracle_cdc_queue_depth{server=\"cdc\",} 7900.0\n"
          + "oracle_cdc_queue_depth{server=\"other\",} 1.0\n"
          + "oracle_cdc_steps_total{server=\"cdc\",} 1.2345E4\n"
          + "oracle_cdc_millis_behind_source{server=\"cdc\"} 45000\n"
          + "oracle_cdc_scn_lag{server=\"cdc\",} NaN\n"
          + "oracle_cdc_last_step_millis 9\n"
          + "jvm_threads_current 40\n";

  @Test
  void prometheusTextIsMappedToAttributes() {
    Map<String, Long> m = PrometheusMetricsSource.parse(EXPORT, "cdc");
    assertThat(m)
        .containsEntry("QueueDepth", 7900L)
        .containsEntry("Steps", 12345L)
        .containsEntry("MillisBehindSource", 45000L)
        .containsEntry("LastStepMillis", 9L)
        .doesNotContainKey("ScnLag");
    assertThat(PrometheusMetricsSource.snake("MillisBehindSource"))
        .isEqualTo("millis_behind_source");
  }

  @Test
  void prometheusEndpointIsReadOverHttp() throws Exception {
    HttpServer server =
        HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext(
        "/metrics",
        x -> {
          byte[] b = EXPORT.getBytes(StandardCharsets.UTF_8);
          x.sendResponseHeaders(200, b.length);
          try (OutputStream o = x.getResponseBody()) {
            o.write(b);
          }
        });
    server.start();
    try {
      String base = "http://127.0.0.1:" + server.getAddress().getPort();
      MetricsSample s =
          new PrometheusMetricsSource(URI.create(base + "/metrics"), CLOCK).read("cdc");
      assertThat(s.get("QueueDepth")).isEqualTo(7900);
      assertThat(s.largest()).isEmpty();
      assertThat(s.at()).isEqualTo(CLOCK.instant());
      assertThatThrownBy(
              () -> new PrometheusMetricsSource(URI.create(base + "/nothing"), CLOCK).read("cdc"))
          .isInstanceOf(IOException.class)
          .hasMessageContaining("HTTP 404");
    } finally {
      server.stop(0);
    }
  }

  @Test
  void jmxReadsTheTaskMxBeanWithItsLargestTransactions() throws Exception {
    TaskMetricsMXBean bean =
        (TaskMetricsMXBean)
            Proxy.newProxyInstance(
                TaskMetricsMXBean.class.getClassLoader(),
                new Class<?>[] {TaskMetricsMXBean.class},
                (proxy, method, args) -> {
                  switch (method.getName()) {
                    case "getLargestTransactions":
                      return List.of(
                          new TransactionInfo(
                              "5.12.900", "BATCH", "etl", 600_000, 4700, 2_000_000, 1024, 0, true));
                    case "getQueueDepth":
                    case "getWindowLogs":
                    case "getOpenTransactions":
                    case "getSpilledTransactions":
                    case "getJournaledTransactions":
                      return 7;
                    case "hashCode":
                      return 1;
                    case "equals":
                      return proxy == args[0];
                    case "toString":
                      return "fake";
                    default:
                      return 42L;
                  }
                });
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName name = TaskMetrics.name("jmx-test");
    mbs.registerMBean(new StandardMBean(bean, TaskMetricsMXBean.class, true), name);
    try (JmxMetricsSource source = new JmxMetricsSource(mbs, CLOCK)) {
      MetricsSample s = source.read("jmx-test");
      assertThat(s.get("QueueDepth")).isEqualTo(7);
      assertThat(s.get("MillisBehindSource")).isEqualTo(42);
      assertThat(s.values()).containsOnlyKeys(MetricsSample.ATTRIBUTES);
      assertThat(s.largest())
          .singleElement()
          .satisfies(
              t -> {
                assertThat(t.xid()).isEqualTo("5.12.900");
                assertThat(t.username()).isEqualTo("BATCH");
                assertThat(t.events()).isEqualTo(2_000_000);
                assertThat(t.journaled()).isTrue();
              });
      assertThatThrownBy(() -> source.read("missing"))
          .isInstanceOf(IOException.class)
          .hasMessageContaining("No task MXBean");
    } finally {
      mbs.unregisterMBean(name);
    }
  }
}
