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
package sh.oso.connect.oracle.signals;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * {@link SignalReader} over a consumer with no group, assigned to partition 0 of the signal topic
 * (created with one partition) and positioned after the last handled signal.
 */
public final class KafkaSignalReader implements SignalReader {

  private final KafkaConsumer<String, String> consumer;
  private final String topic;
  private final long after;
  private boolean assigned;

  /** {@code after}: the offset of the last handled signal, or -1 for none. */
  public KafkaSignalReader(Properties clientProps, String topic, long after) {
    Properties p = new Properties();
    p.putAll(clientProps);
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    p.remove(ConsumerConfig.GROUP_ID_CONFIG);
    this.consumer = new KafkaConsumer<>(p);
    this.topic = topic;
    this.after = after;
  }

  @Override
  public List<RawSignal> poll() {
    List<RawSignal> out = new ArrayList<>();
    if (!assigned) {
      List<PartitionInfo> infos = consumer.partitionsFor(topic, Duration.ofSeconds(10));
      if (infos == null || infos.isEmpty()) {
        return out;
      }
      TopicPartition p = new TopicPartition(topic, 0);
      consumer.assign(List.of(p));
      if (after < 0) {
        consumer.seekToBeginning(List.of(p));
      } else {
        consumer.seek(p, after + 1);
      }
      assigned = true;
    }
    for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(100))) {
      out.add(new RawSignal(r.offset(), r.key(), r.value()));
    }
    return out;
  }

  @Override
  public void close() {
    consumer.close(Duration.ofSeconds(5));
  }
}
