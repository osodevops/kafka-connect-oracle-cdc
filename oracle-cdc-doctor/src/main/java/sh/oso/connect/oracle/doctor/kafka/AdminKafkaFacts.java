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
package sh.oso.connect.oracle.doctor.kafka;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeClusterOptions;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.config.ConfigResource;
import sh.oso.connect.oracle.core.doctor.KafkaFacts;

/** {@link KafkaFacts} over the Kafka admin client; exercised by the connector tier. */
public final class AdminKafkaFacts implements KafkaFacts {

  private static final long TIMEOUT_S = 30;

  private final Admin admin;

  public AdminKafkaFacts(Admin admin) {
    this.admin = admin;
  }

  @Override
  public Map<String, TopicFacts> topics(Collection<String> names) {
    try {
      Set<String> existing = admin.listTopics().names().get(TIMEOUT_S, TimeUnit.SECONDS);
      List<ConfigResource> wanted = new ArrayList<>();
      for (String n : names) {
        if (existing.contains(n)) {
          wanted.add(new ConfigResource(ConfigResource.Type.TOPIC, n));
        }
      }
      Map<String, TopicFacts> out = new LinkedHashMap<>();
      if (wanted.isEmpty()) {
        return out;
      }
      Map<ConfigResource, Config> configs =
          admin.describeConfigs(wanted).all().get(TIMEOUT_S, TimeUnit.SECONDS);
      for (Map.Entry<ConfigResource, Config> e : configs.entrySet()) {
        ConfigEntry policy = e.getValue().get("cleanup.policy");
        ConfigEntry retention = e.getValue().get("retention.ms");
        out.put(
            e.getKey().name(),
            new TopicFacts(
                policy == null ? null : policy.value(),
                retention == null || retention.value() == null
                    ? -1
                    : Long.parseLong(retention.value())));
      }
      return out;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("interrupted while describing topics", e);
    } catch (Exception e) {
      throw new IllegalStateException("describing the internal topics failed: " + e, e);
    }
  }

  @Override
  public CreateRights canCreateTopics() {
    try {
      DescribeClusterResult r =
          admin.describeCluster(new DescribeClusterOptions().includeAuthorizedOperations(true));
      Set<AclOperation> ops = r.authorizedOperations().get(TIMEOUT_S, TimeUnit.SECONDS);
      if (ops == null) {
        return CreateRights.UNKNOWN;
      }
      return ops.contains(AclOperation.CREATE) || ops.contains(AclOperation.ALL)
          ? CreateRights.ALLOWED
          : CreateRights.DENIED;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return CreateRights.UNKNOWN;
    } catch (Exception e) {
      return CreateRights.UNKNOWN;
    }
  }

  @Override
  public long transactionMaxTimeoutMs() {
    try {
      Collection<Node> nodes = admin.describeCluster().nodes().get(TIMEOUT_S, TimeUnit.SECONDS);
      if (nodes.isEmpty()) {
        return -1;
      }
      ConfigResource broker =
          new ConfigResource(ConfigResource.Type.BROKER, nodes.iterator().next().idString());
      Config c =
          admin.describeConfigs(List.of(broker)).all().get(TIMEOUT_S, TimeUnit.SECONDS).get(broker);
      ConfigEntry e = c == null ? null : c.get("transaction.max.timeout.ms");
      return e == null || e.value() == null ? -1 : Long.parseLong(e.value());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return -1;
    } catch (Exception e) {
      return -1;
    }
  }
}
