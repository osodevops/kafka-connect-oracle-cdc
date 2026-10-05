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
package sh.oso.connect.oracle.doctor.admin;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.doctor.CapturedTable;
import sh.oso.connect.oracle.core.position.PositionCodec;
import sh.oso.connect.oracle.core.topology.ThreadInfo;
import sh.oso.connect.oracle.doctor.testing.FakeEnv;

/** PRD-02 SNAP-7 through oracle-cdc-admin resnapshot. */
class ResnapshotAdminTest {

  static final ObjectMapper JSON = new ObjectMapper();

  FakeEnv env;

  @BeforeEach
  void setUp() {
    env = new FakeEnv();
    env.db.catalog.base.archivedRun(1, 10, 10, 1000, 100).onlineCurrent(1, 20, 2000);
    env.db.catalog.base.currentScn = 2500;
    CapturedTable.Column id = new CapturedTable.Column("ID", "NUMBER", false);
    for (String t : List.of("ORDERS", "ITEMS")) {
      env.db.catalog.tables.add(
          new CapturedTable(
              "FREEPDB1", "APP", t, List.of(id), true, false, true, false, false, false));
    }
    env.db.catalog.tables.add(
        new CapturedTable(
            "FREEPDB1", "OTHER", "T", List.of(id), true, false, true, false, false, false));
  }

  private FakeEnv.Run resnapshot(String... args) {
    String[] base = {"resnapshot", "--connect-url", "http://c:8083", "--name", "orders-cdc"};
    String[] all = new String[base.length + args.length];
    System.arraycopy(base, 0, all, 0, base.length);
    System.arraycopy(args, 0, all, base.length, args.length);
    return env.admin(all);
  }

  private JsonNode signal() throws Exception {
    List<String> values = env.kafka.values("cdc.cdc.signals");
    assertThat(values).hasSize(1);
    return JSON.readTree(values.get(0));
  }

  @Test
  void presentRedoOnlyWritesTheSignal() throws Exception {
    env.connect.offset = OffsetsAdminTest.offset(1500);
    FakeEnv.Run r = resnapshot("--tables", "freepdb1.app.orders", "--reason", "rebuild the topic");
    assertThat(r.err()).isEmpty();
    assertThat(r.exit()).isZero();
    assertThat(env.log).containsExactly("send cdc.cdc.signals");
    assertThat(env.kafka.sent.get(0).keyText()).isEqualTo("orders-cdc");
    JsonNode s = signal();
    assertThat(s.path("id").asText()).startsWith("oracle-cdc-admin-");
    assertThat(s.path("type").asText()).isEqualTo("snapshot");
    assertThat(s.path("data").path("tables").get(0).asText()).isEqualTo("FREEPDB1.APP.ORDERS");
    assertThat(s.path("data").path("tables")).hasSize(1);
    assertThat(env.connect.patched).isNull();
    assertThat(env.connect.state).isEqualTo("RUNNING");
    assertThat(r.out()).contains("the offset is unchanged");

    env.log.clear();
    env.connect.offset = null;
    assertThat(resnapshot("--tables", "FREEPDB1.APP.ITEMS", "--reason", "x").exit()).isZero();
    assertThat(env.log).containsExactly("send cdc.cdc.signals");
  }

  @Test
  void purgedRedoMovesTheOffsetPastTheGapAfterTheSignal() throws Exception {
    env.connect.offset = OffsetsAdminTest.offset(1450);
    env.db.catalog.base.markDeleted(1, 14).markDeleted(1, 15);
    FakeEnv.Run r =
        resnapshot(
            "--tables",
            "FREEPDB1.APP.ORDERS,FREEPDB1.APP.ITEMS",
            "--reason",
            "archive purged during the outage");
    assertThat(r.err()).isEmpty();
    assertThat(r.exit()).isZero();
    assertThat(env.log)
        .containsExactly(
            "stop", "send cdc.cdc.signals", "send cdc.cdc.ops", "patch", "send cdc.cdc.ops");
    assertThat(PositionCodec.read(env.connect.patched).resumeScn()).isEqualTo(1600);
    JsonNode applied = JSON.readTree(env.kafka.values("cdc.cdc.ops").get(1));
    JsonNode d = applied.path("details");
    assertThat(d.path("command").asText()).isEqualTo("resnapshot");
    assertThat(d.path("outcome").asText()).isEqualTo("applied");
    assertThat(d.path("previous_resume_scn").asText()).isEqualTo("1450");
    assertThat(d.path("new_resume_scn").asText()).isEqualTo("1600");
    assertThat(d.path("gap").asText()).startsWith("CDC-2002");
    assertThat(d.path("tables").asText()).isEqualTo("FREEPDB1.APP.ORDERS,FREEPDB1.APP.ITEMS");
    assertThat(d.path("signal_id").asText()).isEqualTo(signal().path("id").asText());
    assertThat(d.has("unlisted_tables")).isFalse();
    assertThat(applied.path("resume_scn").asLong()).isEqualTo(1600);
    assertThat(r.out()).contains("resumes at SCN 1600");
  }

  @Test
  void otherCapturedTablesBlockTheMoveUnlessTheGapIsAccepted() throws Exception {
    env.connect.offset = OffsetsAdminTest.offset(1450);
    env.db.catalog.base.markDeleted(1, 14);
    FakeEnv.Run refused = resnapshot("--tables", "FREEPDB1.APP.ORDERS", "--reason", "x");
    assertThat(refused.exit()).isEqualTo(1);
    assertThat(refused.err())
        .contains("would be lost: FREEPDB1.APP.ITEMS.")
        .contains("--skip-gap-for-unlisted-tables");
    assertThat(env.log).isEmpty();

    FakeEnv.Run ok =
        resnapshot(
            "--tables",
            "FREEPDB1.APP.ORDERS",
            "--reason",
            "x",
            "--skip-gap-for-unlisted-tables",
            "--resume");
    assertThat(ok.exit()).isZero();
    assertThat(PositionCodec.read(env.connect.patched).resumeScn()).isEqualTo(1500);
    assertThat(
            JSON.readTree(env.kafka.values("cdc.cdc.ops").get(0))
                .path("details")
                .path("unlisted_tables")
                .asText())
        .isEqualTo("FREEPDB1.APP.ITEMS");
    assertThat(env.log).last().isEqualTo("resume");
  }

  @Test
  void refusesUnknownTablesAndAGapWithNoWayPast() {
    env.connect.offset = OffsetsAdminTest.offset(1450);
    FakeEnv.Run unknown = resnapshot("--tables", "FREEPDB1.APP.NOPE", "--reason", "x");
    assertThat(unknown.exit()).isEqualTo(64);
    assertThat(unknown.err()).contains("is not one of the 2 tables the connector captures");
    // a table outside cdc.tables.include is not captured either
    assertThat(resnapshot("--tables", "FREEPDB1.OTHER.T", "--reason", "x").exit()).isEqualTo(64);

    env.db.catalog.base.markDeleted(1, 14);
    env.db.catalog.base.threads.add(new ThreadInfo(2, true, "OPEN", 4));
    FakeEnv.Run stuck =
        resnapshot("--tables", "FREEPDB1.APP.ORDERS,FREEPDB1.APP.ITEMS", "--reason", "x");
    assertThat(stuck.exit()).isEqualTo(1);
    assertThat(stuck.err()).contains("no later SCN has every log");
    assertThat(env.log).isEmpty();
    assertThat(resnapshot("--tables", "FREEPDB1.APP.ORDERS", "--reason", " ").exit()).isEqualTo(64);
  }

  @Test
  void aGapThatDisappearsWhileStoppingLeavesTheOffsetAlone() {
    env.connect.offset = OffsetsAdminTest.offset(1450);
    env.db.catalog.base.markDeleted(1, 14);
    env.connect.onStop = () -> env.connect.offset = OffsetsAdminTest.offset(1700);
    FakeEnv.Run r =
        resnapshot("--tables", "FREEPDB1.APP.ORDERS,FREEPDB1.APP.ITEMS", "--reason", "x");
    assertThat(r.exit()).isZero();
    assertThat(env.connect.patched).isNull();
    assertThat(env.log).containsExactly("stop", "send cdc.cdc.signals");
    assertThat(r.out()).contains("offset is unchanged");
  }
}
