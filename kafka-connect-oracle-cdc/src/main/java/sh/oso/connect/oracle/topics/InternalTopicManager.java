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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates the internal topics that do not exist yet with the settings they need (compaction for the
 * schema and journal topics), or reports which required topics are missing when creation is not
 * possible (SRC-TOP-6). Without broker access configured the connector relies on the worker's topic
 * creation or on pre-created topics, and validation says so.
 */
public final class InternalTopicManager {

  private static final Logger LOG = LoggerFactory.getLogger(InternalTopicManager.class);

  private final TopicAdmin admin;
  private final short replication;

  public InternalTopicManager(TopicAdmin admin, short replication) {
    this.admin = admin;
    this.replication = replication;
  }

  /** Creates what is missing; returns the names created. */
  public List<String> ensure(Map<String, InternalTopics.Spec> specs) throws Exception {
    Set<String> existing = admin.existingTopics();
    Map<String, Map<String, String>> toCreate = new LinkedHashMap<>();
    for (InternalTopics.Spec s : specs.values()) {
      if (existing.contains(s.name())) {
        continue;
      }
      Map<String, String> cfg = new LinkedHashMap<>();
      if (s.compacted()) {
        cfg.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        cfg.put(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "0");
      } else {
        cfg.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE);
        if (s.retentionMs() > 0) {
          cfg.put(TopicConfig.RETENTION_MS_CONFIG, Long.toString(s.retentionMs()));
        }
      }
      toCreate.put(s.name(), cfg);
    }
    if (toCreate.isEmpty()) {
      return List.of();
    }
    admin.createTopics(toCreate, 1, replication);
    LOG.info("Created internal topics {}", toCreate.keySet());
    return new ArrayList<>(toCreate.keySet());
  }

  /** The required topics that do not exist, for validation when creation is not allowed. */
  public List<String> missing(Map<String, InternalTopics.Spec> specs) throws Exception {
    Set<String> existing = admin.existingTopics();
    List<String> out = new ArrayList<>();
    for (InternalTopics.Spec s : specs.values()) {
      if (!existing.contains(s.name())) {
        out.add(s.name());
      }
    }
    return out;
  }
}
