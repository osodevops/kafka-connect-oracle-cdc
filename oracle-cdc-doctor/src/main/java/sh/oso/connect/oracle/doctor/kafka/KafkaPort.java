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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import sh.oso.connect.oracle.core.doctor.KafkaFacts;

/**
 * The Kafka access the admin and doctor commands need: write one record and wait for the brokers to
 * acknowledge it, read a whole topic, and the broker facts of the doctor rules. {@link
 * KafkaClientsPort} is the real one; tests fake it.
 */
public interface KafkaPort extends AutoCloseable {

  /** Writes one record and returns once every in-sync replica has it (acks=all). */
  void send(String topic, byte[] key, byte[] value) throws Exception;

  /** Every record of the topic up to its end offsets, read_committed; empty when it is missing. */
  List<ConsumerRecord<byte[], byte[]>> readAll(String topic) throws Exception;

  /** Broker facts for DOC-17 and DOC-18. */
  KafkaFacts facts();

  @Override
  void close();
}
