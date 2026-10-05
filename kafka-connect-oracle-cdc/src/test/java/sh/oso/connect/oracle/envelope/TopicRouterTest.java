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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.errors.ErrorCode;
import sh.oso.connect.oracle.core.errors.NameCollisionException;
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

  @Test
  void twoTablesThatShareATopicOnlyThroughSanitisingStopTheTask() {
    // CDC-6004: on a compacted topic equal keys of the two tables would overwrite each other
    TopicRouter r = new TopicRouter("${prefix}.${pdb}.${schema}.${table}", "cdc", "FREE");
    TableId hash = new TableId("FREEPDB1", "APP", "ORDER#");
    TableId dollar = new TableId("FREEPDB1", "APP", "ORDER$");
    assertThat(r.topic(hash)).isEqualTo("cdc.FREEPDB1.APP.ORDER_");
    assertThat(r.topic(hash)).as("the same table again").isEqualTo("cdc.FREEPDB1.APP.ORDER_");
    assertThatThrownBy(() -> r.topic(dollar))
        .isInstanceOfSatisfying(
            NameCollisionException.class,
            e -> assertThat(e.code()).isEqualTo(ErrorCode.NAME_COLLISION))
        .hasMessageStartingWith("[CDC-6004] Tables FREEPDB1.APP.ORDER# and FREEPDB1.APP.ORDER$")
        .hasMessageContaining("both route to topic cdc.FREEPDB1.APP.ORDER_")
        .hasMessageContaining("cdc.topic.template")
        .hasMessageEndingWith("/runbooks/name-collision");
    assertThatThrownBy(() -> r.topic(dollar))
        .as("still refused on the next attempt")
        .isInstanceOf(NameCollisionException.class);
    TableId underscore = new TableId("FREEPDB1", "APP", "ORDER_");
    assertThatThrownBy(() -> r.topic(underscore))
        .as("a table already named like the sanitised topic")
        .isInstanceOf(NameCollisionException.class);
  }

  @Test
  void aTemplateThatMergesTablesOnPurposeIsNotACollision() {
    TopicRouter bySchema = new TopicRouter("${prefix}.${schema}", "cdc", "FREE");
    assertThat(bySchema.topic(new TableId("FREEPDB1", "APP", "ORDER#"))).isEqualTo("cdc.APP");
    assertThat(bySchema.topic(new TableId("FREEPDB1", "APP", "ORDER$"))).isEqualTo("cdc.APP");
    TopicRouter noPdb = new TopicRouter("${prefix}.${schema}.${table}", "cdc", "FREE");
    assertThat(noPdb.topic(new TableId("FREEPDB1", "APP", "T")))
        .isEqualTo(noPdb.topic(new TableId("FREEPDB2", "APP", "T")));
  }

  @Test
  void aTableThatLeftTheCapturedSetReleasesItsTopic() {
    TopicRouter r = new TopicRouter("${prefix}.${schema}.${table}", "cdc", "FREE");
    TableId hash = new TableId("FREEPDB1", "APP", "ORDER#");
    TableId dollar = new TableId("FREEPDB1", "APP", "ORDER$");
    r.topic(hash);
    r.release(hash);
    assertThat(r.topic(dollar)).as("a rename of ORDER# to ORDER$").isEqualTo("cdc.APP.ORDER_");
    assertThatThrownBy(() -> r.topic(hash)).isInstanceOf(NameCollisionException.class);
    r.release(new TableId("FREEPDB1", "APP", "NEVER_ROUTED"));
    // a merged topic stays claimed while one of its tables remains
    TopicRouter held = new TopicRouter("${prefix}.${table}", "cdc", "FREE");
    held.topic(new TableId(null, "A", "X_"));
    held.topic(new TableId(null, "B", "X_"));
    held.release(new TableId(null, "A", "X_"));
    assertThatThrownBy(() -> held.topic(new TableId(null, "C", "X$")))
        .as("the remaining table keeps the claim")
        .isInstanceOf(NameCollisionException.class)
        .hasMessageContaining("Tables B.X_ and C.X$");
  }
}
