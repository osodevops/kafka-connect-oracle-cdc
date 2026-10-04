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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.model.TableId;

class TopicRouterTest {

  @Test
  void expandsVariablesAndSanitises() {
    TopicRouter cdb = new TopicRouter("${prefix}.${pdb}.${schema}.${table}", "cdc", "FREE");
    assertThat(cdb.topic(new TableId("FREEPDB1", "APP", "ORDERS")))
        .isEqualTo("cdc.FREEPDB1.APP.ORDERS");
    assertThat(cdb.topic(new TableId("FREEPDB1", "APP", "T$WITH#CHARS")))
        .isEqualTo("cdc.FREEPDB1.APP.T_WITH_CHARS");
    TopicRouter non = new TopicRouter("${prefix}.${schema}.${table}", "cdc", "ORCL");
    assertThat(non.topic(new TableId(null, "APP", "ORDERS"))).isEqualTo("cdc.APP.ORDERS");
    TopicRouter db = new TopicRouter("${database}_${table}", "x", "ORCL");
    assertThat(db.topic(new TableId(null, "APP", "ORDERS"))).isEqualTo("ORCL_ORDERS");
    TopicRouter empty = new TopicRouter("${prefix}.${pdb}.${table}", "cdc", null);
    assertThat(empty.topic(new TableId(null, "APP", "ORDERS"))).isEqualTo("cdc.ORDERS");
    assertThat(TopicRouter.sanitise("..a..b")).isEqualTo("a.b");
    assertThat(TopicRouter.sanitise("x".repeat(300))).hasSize(249);
  }
}
