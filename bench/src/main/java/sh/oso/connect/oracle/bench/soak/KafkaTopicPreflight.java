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
package sh.oso.connect.oracle.bench.soak;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

/**
 * Refuses a soak whose table topics already hold records: the oracle reads them from the beginning,
 * and records of an earlier run (whose ledger the reset drops) would fail the first check. A topic
 * that does not exist yet is fine.
 */
public final class KafkaTopicPreflight implements Soak.Preflight {

  private final String bootstrapServers;
  private final List<String> topics;

  public KafkaTopicPreflight(String bootstrapServers, List<String> topics) {
    this.bootstrapServers = bootstrapServers;
    this.topics = List.copyOf(topics);
  }

  @Override
  public List<String> problems() throws Exception {
    Properties p = new Properties();
    p.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    p.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 60_000);
    List<String> out = new ArrayList<>();
    try (Admin admin = Admin.create(p)) {
      Set<String> existing = admin.listTopics().names().get(60, TimeUnit.SECONDS);
      List<String> present = topics.stream().filter(existing::contains).toList();
      if (present.isEmpty()) {
        return out;
      }
      Map<String, TopicDescription> d =
          admin.describeTopics(present).allTopicNames().get(60, TimeUnit.SECONDS);
      Map<TopicPartition, OffsetSpec> earliest = new HashMap<>();
      Map<TopicPartition, OffsetSpec> latest = new HashMap<>();
      for (TopicDescription t : d.values()) {
        for (TopicPartitionInfo pi : t.partitions()) {
          TopicPartition tp = new TopicPartition(t.name(), pi.partition());
          earliest.put(tp, OffsetSpec.earliest());
          latest.put(tp, OffsetSpec.latest());
        }
      }
      Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> lo =
          admin.listOffsets(earliest).all().get(60, TimeUnit.SECONDS);
      Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> hi =
          admin.listOffsets(latest).all().get(60, TimeUnit.SECONDS);
      Map<String, Long> perTopic = new HashMap<>();
      for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> e : hi.entrySet()) {
        long n = e.getValue().offset() - lo.get(e.getKey()).offset();
        perTopic.merge(e.getKey().topic(), n, Long::sum);
      }
      for (String t : present) {
        long n = perTopic.getOrDefault(t, 0L);
        if (n > 0) {
          out.add(
              "topic "
                  + t
                  + " already holds "
                  + n
                  + " offsets from an earlier run; start from a fresh stack (make down up"
                  + " register) or delete the connector and its topics first");
        }
      }
    }
    return out;
  }
}
