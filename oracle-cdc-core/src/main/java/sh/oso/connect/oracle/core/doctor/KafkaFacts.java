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
package sh.oso.connect.oracle.core.doctor;

import java.util.Collection;
import java.util.Map;

/**
 * What the Kafka rules (PRD-05 DOC-17, DOC-18) read from the brokers. The doctor CLI implements it
 * over the Kafka admin client; unit tests fake it. Implementations wrap client errors in runtime
 * exceptions, which the doctor reports as a finding of the rule that asked.
 */
public interface KafkaFacts {

  /** Configuration of each named topic that exists; missing topics are absent from the map. */
  Map<String, TopicFacts> topics(Collection<String> names);

  /** Whether the client may create topics, as far as the cluster says. */
  CreateRights canCreateTopics();

  /** The cluster's answer on topic creation rights. */
  enum CreateRights {
    ALLOWED,
    DENIED,
    UNKNOWN
  }

  /** The brokers' {@code transaction.max.timeout.ms}, or -1 when it cannot be read. */
  long transactionMaxTimeoutMs();

  /** A topic's {@code cleanup.policy} and {@code retention.ms}. */
  record TopicFacts(String cleanupPolicy, long retentionMs) {

    public boolean compacted() {
      return cleanupPolicy != null && cleanupPolicy.contains("compact");
    }

    public boolean deletes() {
      return cleanupPolicy == null || cleanupPolicy.contains("delete");
    }
  }
}
