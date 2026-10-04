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
import sh.oso.connect.oracle.core.model.TableId;

/** SRC-TOP-1: topic name from the template; characters Kafka rejects become underscores. */
public final class TopicRouter {

  private final String template;
  private final String prefix;
  private final String database;

  public TopicRouter(String template, String prefix, String database) {
    this.template = template;
    this.prefix = prefix;
    this.database = database;
  }

  public String topic(TableId t) {
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
    return sanitise(out);
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
