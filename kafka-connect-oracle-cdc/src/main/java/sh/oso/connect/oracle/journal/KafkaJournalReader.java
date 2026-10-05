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
package sh.oso.connect.oracle.journal;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Reads the compacted journal topic from the beginning to the end offsets with a read_committed
 * consumer, so chunks from an aborted exactly-once batch are never restored.
 */
public final class KafkaJournalReader implements JournalReader {

  private final KafkaConsumer<byte[], byte[]> consumer;

  public KafkaJournalReader(Properties clientProps) {
    Properties p = new Properties();
    p.putAll(clientProps);
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    p.remove(ConsumerConfig.GROUP_ID_CONFIG);
    this.consumer = new KafkaConsumer<>(p);
  }

  private static final org.slf4j.Logger LOG =
      org.slf4j.LoggerFactory.getLogger(KafkaJournalReader.class);

  @Override
  public List<ConsumerRecord<byte[], byte[]>> readAll(String topic) {
    List<PartitionInfo> infos = consumer.partitionsFor(topic, Duration.ofSeconds(30));
    List<ConsumerRecord<byte[], byte[]>> out = new ArrayList<>();
    if (infos == null || infos.isEmpty()) {
      LOG.info("Journal topic {} does not exist yet: nothing to restore", topic);
      return out; // the topic does not exist yet: nothing journaled
    }
    List<TopicPartition> parts = new ArrayList<>();
    for (PartitionInfo i : infos) {
      parts.add(new TopicPartition(topic, i.partition()));
    }
    consumer.assign(parts);
    consumer.seekToBeginning(parts);
    // for a read_committed consumer the end offsets are the last stable offsets, so an open
    // producer transaction left by a killed worker does not block the read
    Map<TopicPartition, Long> end = consumer.endOffsets(parts, Duration.ofSeconds(30));
    LOG.info("Reading journal topic {} up to {}", topic, end);
    long deadline = System.currentTimeMillis() + Duration.ofSeconds(90).toMillis();
    long lastLog = System.currentTimeMillis();
    while (!reached(parts, end) && System.currentTimeMillis() < deadline) {
      for (ConsumerRecord<byte[], byte[]> r : consumer.poll(Duration.ofMillis(500))) {
        out.add(r);
      }
      if (System.currentTimeMillis() - lastLog > 10_000) {
        LOG.info(
            "Journal read in progress: {} records, positions {}", out.size(), positions(parts));
        lastLog = System.currentTimeMillis();
      }
    }
    if (!reached(parts, end)) {
      throw new IllegalStateException(
          "reading the journal topic "
              + topic
              + " stalled at "
              + positions(parts)
              + " before the end offsets "
              + end
              + "; check that the worker's exactly-once producer transactions are not left open");
    }
    LOG.info("Read {} journal records from {}", out.size(), topic);
    return out;
  }

  private Map<TopicPartition, Long> positions(List<TopicPartition> parts) {
    Map<TopicPartition, Long> m = new java.util.LinkedHashMap<>();
    for (TopicPartition tp : parts) {
      m.put(tp, consumer.position(tp));
    }
    return m;
  }

  private boolean reached(List<TopicPartition> parts, Map<TopicPartition, Long> end) {
    for (TopicPartition tp : parts) {
      if (consumer.position(tp) < end.get(tp)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void close() {
    consumer.close();
  }
}
