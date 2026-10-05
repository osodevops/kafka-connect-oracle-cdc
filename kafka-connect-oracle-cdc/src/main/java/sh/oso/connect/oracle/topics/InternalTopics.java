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
import java.util.Map;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfig;

/** The connector's internal topics and the settings each needs (SRC-TOP-6). */
public final class InternalTopics {

  /** One topic to create or verify. */
  public record Spec(String name, boolean compacted, long retentionMs) {}

  private InternalTopics() {}

  public static Map<String, Spec> of(OracleCdcSourceConnectorConfig c) {
    Map<String, Spec> out = new LinkedHashMap<>();
    out.put("ops", new Spec(c.opsTopic(), false, -1));
    out.put("heartbeat", new Spec(c.heartbeatTopic(), false, 24L * 3600_000L));
    out.put("signals", new Spec(c.signalsTopic(), false, -1));
    out.put("schema", new Spec(c.schemaTopic(), true, -1));
    out.put("txjournal", new Spec(c.journalTopic(), true, -1));
    if (c.transactionsTopicEnabled()) {
      out.put("transactions", new Spec(c.transactionsTopic(), false, -1));
    }
    return out;
  }
}
