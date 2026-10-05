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
package sh.oso.connect.oracle.e2e.support;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import eu.rekawek.toxiproxy.model.Toxic;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.testcontainers.containers.ToxiproxyContainer;

/**
 * Toxiproxy (Apache-2.0 container, MIT client) on the shared network between the Connect worker and
 * the Oracle listener: the connector connects to {@link #HOST}:{@link #PORT} instead of
 * oracle:1521, and the test adds and removes toxics. Workload sessions and the test's own
 * connections keep using the mapped Oracle port, so only the connector sees the faults.
 */
public final class OracleProxy implements AutoCloseable {

  public static final String IMAGE =
      System.getProperty("toxiproxy.image", "ghcr.io/shopify/toxiproxy:2.5.0");
  public static final String HOST = "toxiproxy";
  public static final int PORT = 8666;

  private final ToxiproxyContainer container;
  private final Proxy proxy;
  private final List<Toxic> active = new ArrayList<>();

  private OracleProxy(ToxiproxyContainer container, Proxy proxy) {
    this.container = container;
    this.proxy = proxy;
  }

  public static OracleProxy start() throws IOException {
    ToxiproxyContainer c =
        new ToxiproxyContainer(IMAGE)
            .withNetwork(OracleTestDatabase.NETWORK)
            .withNetworkAliases(HOST);
    c.start();
    ToxiproxyClient client = new ToxiproxyClient(c.getHost(), c.getControlPort());
    Proxy p =
        client.createProxy("oracle", "0.0.0.0:" + PORT, OracleTestDatabase.NETWORK_ALIAS + ":1521");
    return new OracleProxy(c, p);
  }

  /** The connector's database coordinates through the proxy. */
  public static Map<String, String> connectorDatabaseProps(String pdbs) {
    Map<String, String> m = new java.util.HashMap<>(ConnectCluster.oracleDatabaseProps(pdbs));
    m.put("cdc.database.host", HOST);
    m.put("cdc.database.port", Integer.toString(PORT));
    return m;
  }

  /** Latency on both directions, with jitter on the responses. */
  public void latency(long downMs, long jitterMs, long upMs) throws IOException {
    active.add(
        proxy.toxics().latency("lat-down", ToxicDirection.DOWNSTREAM, downMs).setJitter(jitterMs));
    active.add(proxy.toxics().latency("lat-up", ToxicDirection.UPSTREAM, upMs));
  }

  /** The next data in either direction resets the connection (TCP RST), new ones included. */
  public void resetPeer() throws IOException {
    active.add(proxy.toxics().resetPeer("reset-down", ToxicDirection.DOWNSTREAM, 0));
    active.add(proxy.toxics().resetPeer("reset-up", ToxicDirection.UPSTREAM, 0));
  }

  /**
   * Data stops flowing and every connection is closed after {@code ms} without traffic, the way a
   * load balancer forgets an idle flow and the next packet finds nothing behind it.
   */
  public void dropAfter(long ms) throws IOException {
    active.add(proxy.toxics().timeout("drop-down", ToxicDirection.DOWNSTREAM, ms));
    active.add(proxy.toxics().timeout("drop-up", ToxicDirection.UPSTREAM, ms));
  }

  /** Removes every toxic this helper added. */
  public void clear() throws IOException {
    IOException first = null;
    for (Toxic t : active) {
      try {
        t.remove();
      } catch (IOException e) {
        first = first == null ? e : first;
      }
    }
    active.clear();
    if (first != null) {
      throw first;
    }
  }

  @Override
  public void close() {
    container.stop();
  }
}
