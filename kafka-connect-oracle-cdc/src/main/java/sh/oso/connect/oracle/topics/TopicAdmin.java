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
package sh.oso.connect.oracle.topics;

import java.util.Map;
import java.util.Set;

/** The slice of the Kafka admin API the topic manager needs; faked in unit tests. */
public interface TopicAdmin extends AutoCloseable {
  Set<String> existingTopics() throws Exception;

  /**
   * Creates the topics; {@code configs} per topic name, {@code replication} -1 for the broker
   * default.
   */
  void createTopics(Map<String, Map<String, String>> configs, int partitions, short replication)
      throws Exception;

  @Override
  void close();
}
