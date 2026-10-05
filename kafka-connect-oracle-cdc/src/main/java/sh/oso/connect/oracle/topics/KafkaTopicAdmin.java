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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

/**
 * {@link TopicAdmin} over the Kafka admin client; excluded from unit tests, exercised in the
 * connector tier.
 */
public final class KafkaTopicAdmin implements TopicAdmin {

  private final Admin admin;

  public KafkaTopicAdmin(Properties clientProps) {
    this.admin = Admin.create(clientProps);
  }

  @Override
  public Set<String> existingTopics() throws Exception {
    return admin.listTopics().names().get();
  }

  @Override
  public void createTopics(
      Map<String, Map<String, String>> configs, int partitions, short replication)
      throws Exception {
    List<NewTopic> topics = new ArrayList<>();
    for (Map.Entry<String, Map<String, String>> e : configs.entrySet()) {
      topics.add(
          new NewTopic(
                  e.getKey(),
                  Optional.of(partitions),
                  replication < 0 ? Optional.empty() : Optional.of(replication))
              .configs(e.getValue()));
    }
    try {
      admin.createTopics(topics).all().get();
    } catch (ExecutionException ex) {
      if (!(ex.getCause() instanceof TopicExistsException)) {
        throw ex;
      }
    }
  }

  @Override
  public void close() {
    admin.close();
  }
}
