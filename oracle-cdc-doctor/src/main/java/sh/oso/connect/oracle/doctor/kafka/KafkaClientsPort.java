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

import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import sh.oso.connect.oracle.core.doctor.KafkaFacts;
import sh.oso.connect.oracle.journal.KafkaJournalReader;

/**
 * {@link KafkaPort} over the Kafka clients, built from the connector's {@code cdc.kafka.*}
 * properties (or the command line). Clients are created on first use. Exercised by the connector
 * tier.
 */
public final class KafkaClientsPort implements KafkaPort {

  private final Properties props;
  private KafkaProducer<byte[], byte[]> producer;
  private Admin admin;

  public KafkaClientsPort(Properties clientProps) {
    this.props = new Properties();
    this.props.putAll(clientProps);
  }

  @Override
  public void send(String topic, byte[] key, byte[] value) throws Exception {
    if (producer == null) {
      Properties p = new Properties();
      p.putAll(props);
      p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
      p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
      p.put(ProducerConfig.ACKS_CONFIG, "all");
      p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
      p.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "30000");
      p.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "60000");
      producer = new KafkaProducer<>(p);
    }
    producer.send(new ProducerRecord<>(topic, key, value)).get(90, TimeUnit.SECONDS);
  }

  @Override
  public List<ConsumerRecord<byte[], byte[]>> readAll(String topic) throws Exception {
    try (KafkaJournalReader reader = new KafkaJournalReader(props)) {
      return reader.readAll(topic);
    }
  }

  @Override
  public KafkaFacts facts() {
    if (admin == null) {
      admin = Admin.create(props);
    }
    return new AdminKafkaFacts(admin);
  }

  @Override
  public void close() {
    if (producer != null) {
      producer.close();
    }
    if (admin != null) {
      admin.close();
    }
  }
}
