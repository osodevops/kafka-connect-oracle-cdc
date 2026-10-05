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

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import sh.oso.connect.oracle.core.doctor.MetricsSample;
import sh.oso.connect.oracle.core.metrics.TaskMetrics;

/**
 * Reads the task MXBean ({@code sh.oso.cdc:type=task,server=...}) over a JMX connection, including
 * {@code LargestTransactions}, which the Prometheus exporter does not carry.
 */
public final class JmxMetricsSource implements MetricsSource {

  private final MBeanServerConnection connection;
  private final JMXConnector connector;
  private final Clock clock;

  public JmxMetricsSource(MBeanServerConnection connection, Clock clock) {
    this(connection, null, clock);
  }

  private JmxMetricsSource(MBeanServerConnection connection, JMXConnector connector, Clock clock) {
    this.connection = connection;
    this.connector = connector;
    this.clock = clock;
  }

  /**
   * Connects to a remote JMX URL such as {@code service:jmx:rmi:///jndi/rmi://host:9999/jmxrmi}.
   */
  public static JmxMetricsSource connect(String url, Clock clock) throws IOException {
    JMXConnector c = JMXConnectorFactory.connect(new JMXServiceURL(url));
    return new JmxMetricsSource(c.getMBeanServerConnection(), c, clock);
  }

  @Override
  public MetricsSample read(String server) throws Exception {
    ObjectName name = TaskMetrics.name(server);
    Map<String, Long> values = new LinkedHashMap<>();
    try {
      for (String attr : MetricsSample.ATTRIBUTES) {
        Object v = connection.getAttribute(name, attr);
        if (v instanceof Number n) {
          values.put(attr, n.longValue());
        }
      }
    } catch (InstanceNotFoundException e) {
      throw new IOException(
          "No task MXBean "
              + name
              + " on this JMX endpoint: the task is not running on this worker, or the server"
              + " name is not the connector's cdc.topic.prefix.",
          e);
    }
    List<MetricsSample.OpenTransaction> largest = new ArrayList<>();
    Object raw = connection.getAttribute(name, "LargestTransactions");
    if (raw instanceof CompositeData[] rows) {
      for (CompositeData d : rows) {
        largest.add(
            new MetricsSample.OpenTransaction(
                (String) d.get("xid"),
                (String) d.get("username"),
                ((Number) d.get("ageMillis")).longValue(),
                ((Number) d.get("firstScn")).longValue(),
                ((Number) d.get("events")).longValue(),
                ((Number) d.get("heapBytes")).longValue(),
                ((Number) d.get("spilledBytes")).longValue(),
                Boolean.TRUE.equals(d.get("journaled"))));
      }
    }
    return new MetricsSample(clock.instant(), values, largest);
  }

  @Override
  public void close() throws IOException {
    if (connector != null) {
      connector.close();
    }
  }
}
