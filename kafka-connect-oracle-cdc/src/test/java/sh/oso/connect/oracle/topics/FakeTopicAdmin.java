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

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/** In-memory {@link TopicAdmin}: records what was created and with which settings. */
public final class FakeTopicAdmin implements TopicAdmin {

  public final Set<String> topics = new LinkedHashSet<>();
  public final Map<String, Map<String, String>> created = new LinkedHashMap<>();
  public int partitions;
  public short replication;
  public boolean closed;
  public Exception failWith;

  public FakeTopicAdmin existing(String... names) {
    topics.addAll(Set.of(names));
    return this;
  }

  @Override
  public Set<String> existingTopics() throws Exception {
    if (failWith != null) {
      throw failWith;
    }
    return Set.copyOf(topics);
  }

  @Override
  public void createTopics(
      Map<String, Map<String, String>> configs, int partitions, short replication)
      throws Exception {
    if (failWith != null) {
      throw failWith;
    }
    this.partitions = partitions;
    this.replication = replication;
    created.putAll(configs);
    topics.addAll(configs.keySet());
  }

  @Override
  public void close() {
    closed = true;
  }
}
