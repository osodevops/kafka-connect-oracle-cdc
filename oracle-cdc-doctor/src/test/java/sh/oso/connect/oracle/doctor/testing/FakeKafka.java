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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import sh.oso.connect.oracle.core.doctor.KafkaFacts;
import sh.oso.connect.oracle.doctor.kafka.KafkaPort;

/** Kafka in memory: records sent, topics to read back, broker facts. */
public final class FakeKafka implements KafkaPort {

  /** One record written. */
  public record Sent(String topic, byte[] key, byte[] value) {
    public String keyText() {
      return new String(key, StandardCharsets.UTF_8);
    }

    public String valueText() {
      return new String(value, StandardCharsets.UTF_8);
    }
  }

  private final List<String> log;
  public final List<Sent> sent = new ArrayList<>();
  public final Map<String, List<ConsumerRecord<byte[], byte[]>>> topics = new HashMap<>();
  public KafkaFacts facts;
  public Exception sendFailure;
  public boolean closed;

  public FakeKafka(List<String> log) {
    this.log = log;
  }

  @Override
  public void send(String topic, byte[] key, byte[] value) throws Exception {
    if (sendFailure != null) {
      throw sendFailure;
    }
    sent.add(new Sent(topic, key, value));
    log.add("send " + topic);
  }

  @Override
  public List<ConsumerRecord<byte[], byte[]>> readAll(String topic) {
    return topics.getOrDefault(topic, List.of());
  }

  @Override
  public KafkaFacts facts() {
    return facts;
  }

  @Override
  public void close() {
    closed = true;
  }

  /** The values sent to a topic, as text. */
  public List<String> values(String topic) {
    return sent.stream().filter(s -> s.topic().equals(topic)).map(Sent::valueText).toList();
  }
}
