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
package sh.oso.connect.oracle.envelope;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import sh.oso.connect.oracle.core.errors.NameCollisionException;
import sh.oso.connect.oracle.core.model.TableId;

/**
 * SRC-TOP-1: topic name from the template; characters Kafka rejects become underscores. Two tables
 * whose expanded templates differ but sanitise to the same topic would share it, and on a compacted
 * topic equal keys of the two tables would overwrite each other, so the second table to be routed
 * there stops the task (CDC-6004). Tables whose expanded templates are identical share the topic by
 * design.
 */
public final class TopicRouter {

  private final String template;
  private final String prefix;
  private final String database;

  /** The table that first claimed a topic and the expanded template it routed from. */
  private record Claim(String expanded, TableId table) {}

  private final Map<TableId, String> routes = new ConcurrentHashMap<>();
  private final Map<String, Claim> claims = new java.util.HashMap<>();

  public TopicRouter(String template, String prefix, String database) {
    this.template = template;
    this.prefix = prefix;
    this.database = database;
  }

  /**
   * The topic of {@code t}. The first call for a table claims its topic; a table whose expanded
   * template differs from the claim's throws {@link NameCollisionException} before any record of it
   * is built.
   */
  public String topic(TableId t) {
    String routed = routes.get(t);
    return routed != null ? routed : route(t);
  }

  private synchronized String route(TableId t) {
    String routed = routes.get(t);
    if (routed != null) {
      return routed;
    }
    String expanded = expand(t);
    String topic = sanitise(expanded);
    Claim held = claims.putIfAbsent(topic, new Claim(expanded, t));
    if (held != null && !held.expanded().equals(expanded)) {
      throw new NameCollisionException(
          "Tables "
              + held.table().fqn()
              + " and "
              + t.fqn()
              + " both route to topic "
              + topic
              + ": the template names them "
              + held.expanded()
              + " and "
              + expanded
              + ", and characters Kafka does not allow in a topic name became underscores.",
          "Rename one of the tables, set cdc.topic.template so the two names stay apart, or"
              + " exclude one of the tables with cdc.tables.exclude; then restart the task.");
    }
    routes.put(t, topic);
    return topic;
  }

  /**
   * Forgets {@code t} after it left the captured set (DROP TABLE, a rename, a refresh), so a table
   * that takes its topic later is not held to its claim.
   */
  public synchronized void release(TableId t) {
    String topic = routes.remove(t);
    if (topic == null) {
      return;
    }
    claims.remove(topic);
    for (Map.Entry<TableId, String> e : routes.entrySet()) {
      if (e.getValue().equals(topic)) {
        claims.put(topic, new Claim(expand(e.getKey()), e.getKey()));
        return;
      }
    }
  }

  private String expand(TableId t) {
    String out = template;
    for (Map.Entry<String, String> e :
        Map.of(
                "${prefix}",
                prefix,
                "${pdb}",
                t.pdb() == null ? "" : t.pdb(),
                "${schema}",
                t.schema(),
                "${table}",
                t.table(),
                "${database}",
                database == null ? "" : database)
            .entrySet()) {
      out = out.replace(e.getKey(), e.getValue());
    }
    return out;
  }

  static String sanitise(String topic) {
    StringBuilder sb = new StringBuilder(topic.length());
    for (char c : topic.toCharArray()) {
      boolean ok =
          (c >= 'a' && c <= 'z')
              || (c >= 'A' && c <= 'Z')
              || (c >= '0' && c <= '9')
              || c == '.'
              || c == '_'
              || c == '-';
      sb.append(ok ? c : '_');
    }
    String s = sb.toString().replaceAll("\\.\\.+", ".");
    while (s.startsWith(".")) {
      s = s.substring(1);
    }
    return s.length() > 249 ? s.substring(0, 249) : s;
  }
}
