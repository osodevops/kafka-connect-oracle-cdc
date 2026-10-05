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
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;
import sh.oso.connect.oracle.core.position.DatabaseIdentity;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.core.position.PositionCodec;
import sh.oso.connect.oracle.doctor.testing.FakeEnv;

class OffsetsAdminTest {

  static final ObjectMapper JSON = new ObjectMapper();
  static final DatabaseIdentity ID = new DatabaseIdentity(1234, 1);

  FakeEnv env;

  /** Thread 1: sequences 10 to 19 cover SCN 1000 to 2000, then the online log; now 2500. */
  @BeforeEach
  void setUp() {
    env = new FakeEnv();
    env.db.catalog.base.archivedRun(1, 10, 10, 1000, 100).onlineCurrent(1, 20, 2000);
    env.db.catalog.base.currentScn = 2500;
  }

  static Map<String, Object> offset(long resume, String... released) {
    Position p =
        Position.initial(resume, ID)
            .withCommit(
                new RedoRecordId(resume - 10, "0x000012.00000abc.0010", 3),
                1,
                new TxKey(3, new Xid(5, 12, 900)),
                0)
            .withResume(new RedoRecordId(resume, "0x000012.00000abb.0010", 0))
            .withReleased(List.of(released))
            .withSnapshot(
                Map.of(
                    "v",
                    1,
                    "complete",
                    false,
                    "tables",
                    Map.of(
                        "FREEPDB1.APP.ORDERS",
                        Map.of("done", false, "frontier", List.of("n:42")))));
    return new LinkedHashMap<>(PositionCodec.write(p));
  }

  private static final String[] TARGET = {
    "--connect-url", "http://connect:8083", "--name", "orders"
  };

  private FakeEnv.Run admin(String... args) {
    String[] all = new String[args.length + TARGET.length];
    System.arraycopy(args, 0, all, 0, args.length);
    System.arraycopy(TARGET, 0, all, args.length, TARGET.length);
    return env.admin(all);
  }

  private List<JsonNode> ops() throws IOException {
    List<JsonNode> out = new java.util.ArrayList<>();
    for (String v : env.kafka.values("cdc.cdc.ops")) {
      out.add(JSON.readTree(v));
    }
    return out;
  }

  @Test
  void showDecodesTheStoredOffset() throws Exception {
    env.connect.offset = offset(1500, "3:5.1.9");
    FakeEnv.Run r = admin("offsets", "show");
    assertThat(r.exit()).isZero();
    assertThat(env.connectUrl).isEqualTo("http://connect:8083");
    assertThat(r.out())
        .contains("Connector orders, partition {server=cdc}")
        .contains("resume_scn: 1500")
        .contains("resume_redo_address: 0x000012.00000abb.0010 ssn 0")
        .contains("last_commit_transaction: 3:5.12.900")
        .contains("released_transactions: [3:5.1.9]")
        .contains("snapshot: in progress: FREEPDB1.APP.ORDERS next chunk from [n:42]")
        .doesNotContain(FakeEnv.PASSWORD);
    assertThat(env.log).isEmpty();

    FakeEnv.Run json = admin("offsets", "show", "--format", "json", "--check-redo");
    JsonNode n = JSON.readTree(json.out());
    assertThat(n.path("position").path("resume_scn").asLong()).isEqualTo(1500);
    assertThat(n.path("offset").path("v").asInt()).isEqualTo(1);
    assertThat(n.path("redo_from_resume").asText()).isEqualTo("present");

    env.db.catalog.base.markDeleted(1, 15);
    assertThat(admin("offsets", "show", "--check-redo").out())
        .contains("redo_from_resume: missing:")
        .contains("CDC-2002");

    env.connect.offset = null;
    assertThat(admin("offsets", "show").out()).contains("No stored offset");
    assertThat(
            JSON.readTree(admin("offsets", "show", "--format", "json").out())
                .path("position")
                .isNull())
        .isTrue();
  }

  @Test
  void setMovesBackRecordsBeforeAndAfterAndLeavesTheConnectorStopped() throws Exception {
    env.connect.offset = offset(1500, "3:5.1.9", "3:6.2.1");
    FakeEnv.Run r =
        admin(
            "offsets",
            "set",
            "--scn",
            "1200",
            "--reason",
            "re-mine the orphan",
            "--forget-released",
            "3:5.1.9");
    assertThat(r.err()).isEmpty();
    assertThat(r.exit()).isZero();
    assertThat(env.log).containsExactly("stop", "send cdc.cdc.ops", "patch", "send cdc.cdc.ops");
    assertThat(env.connect.state).isEqualTo("STOPPED");
    Position patched = PositionCodec.read(env.connect.patched);
    assertThat(patched.resumeScn()).isEqualTo(1200);
    assertThat(patched.resumeRsId()).isNull();
    assertThat(patched.released()).containsExactly("3:6.2.1");
    assertThat(patched.lastCommitKey()).isEqualTo(new TxKey(3, new Xid(5, 12, 900)));
    List<JsonNode> ops = ops();
    assertThat(ops)
        .extracting(o -> o.path("details").path("outcome").asText())
        .containsExactly("applying", "applied");
    JsonNode e = ops.get(1);
    assertThat(e.path("type").asText()).isEqualTo("offsets-set");
    assertThat(e.path("server").asText()).isEqualTo("cdc");
    assertThat(e.path("resume_scn").asLong()).isEqualTo(1200);
    assertThat(e.path("v").asInt()).isEqualTo(1);
    assertThat(e.path("ts_ms").asLong()).isEqualTo(env.now.toEpochMilli());
    JsonNode d = e.path("details");
    assertThat(d.path("reason").asText()).isEqualTo("re-mine the orphan");
    assertThat(d.path("direction").asText()).isEqualTo("backward");
    assertThat(d.path("previous_resume_scn").asText()).isEqualTo("1500");
    assertThat(d.path("new_resume_scn").asText()).isEqualTo("1200");
    assertThat(d.path("operator").asText()).isEqualTo("tester");
    assertThat(d.path("forgotten_released").asText()).isEqualTo("3:5.1.9");
    assertThat(env.kafka.sent.get(0).keyText()).isEqualTo("{\"server\":\"cdc\"}");
    assertThat(env.kafkaProps)
        .containsEntry("bootstrap.servers", "broker:9092")
        .containsEntry("security.protocol", "PLAINTEXT");
    assertThat(r.out()).contains("resume it with PUT /connectors/orders/resume");
    assertThat(r.out() + r.err()).doesNotContain(FakeEnv.PASSWORD);
    assertThat(env.kafka.closed).isTrue();
    assertThat(env.db.closed).isTrue();
  }

  @Test
  void setResumesWhenAskedAndWaitsForStopped() {
    env.connect.offset = offset(1500);
    env.connect.stopWorks = false;
    env.connect.onStop = () -> env.connect.state = "RUNNING";
    FakeEnv.Run stuck = admin("offsets", "set", "--scn", "1400", "--reason", "x");
    assertThat(stuck.exit()).isEqualTo(1);
    assertThat(stuck.err()).contains("did not reach STOPPED");
    assertThat(env.connect.patched).isNull();
    assertThat(env.kafka.sent).isEmpty();

    env.log.clear();
    env.connect.stopWorks = true;
    env.connect.onStop = null;
    FakeEnv.Run r = admin("offsets", "set", "--scn", "1400", "--reason", "x", "--resume");
    assertThat(r.exit()).isZero();
    assertThat(env.log).last().isEqualTo("resume");
    assertThat(env.connect.state).isEqualTo("RUNNING");
  }

  @Test
  void setRefusesAnScnWhoseRedoIsPurged() {
    env.connect.offset = offset(1500);
    env.db.catalog.base.markDeleted(1, 12);
    FakeEnv.Run r = admin("offsets", "set", "--scn", "1250", "--reason", "x");
    assertThat(r.exit()).isEqualTo(1);
    assertThat(r.err()).contains("Refused").contains("CDC-2002").contains("resnapshot");
    assertThat(env.log).isEmpty();
  }

  @Test
  void setRefusesAForwardMoveUnlessSkippingIsAllowed() throws Exception {
    env.connect.offset = offset(1200);
    FakeEnv.Run r = admin("offsets", "set", "--scn", "1800", "--reason", "x");
    assertThat(r.exit()).isEqualTo(1);
    assertThat(r.err())
        .contains("skips every change committed between them")
        .contains("--allow-skip");
    assertThat(env.log).isEmpty();

    FakeEnv.Run ok = admin("offsets", "set", "--scn", "1800", "--reason", "x", "--allow-skip");
    assertThat(ok.exit()).isZero();
    assertThat(ops())
        .allSatisfy(
            o -> assertThat(o.path("details").path("direction").asText()).isEqualTo("forward"));
  }

  @Test
  void setReplansFromTheOffsetAsItStandsOnceStopped() throws Exception {
    env.connect.offset = offset(1500);
    // the task commits once more between the read and the stop
    env.connect.onStop = () -> env.connect.offset = offset(1600);
    FakeEnv.Run r = admin("offsets", "set", "--scn", "1400", "--reason", "x");
    assertThat(r.exit()).isZero();
    assertThat(ops().get(0).path("details").path("previous_resume_scn").asText()).isEqualTo("1600");
    assertThat(PositionCodec.read(env.connect.patched).lastCommitScn()).isEqualTo(1590);
  }

  @Test
  void setWithoutAStoredOffsetWritesAFreshPosition() {
    FakeEnv.Run r = admin("offsets", "set", "--scn", "1300", "--reason", "first start");
    assertThat(r.exit()).isZero();
    Position p = PositionCodec.read(env.connect.patched);
    assertThat(p.resumeScn()).isEqualTo(1300);
    assertThat(p.identity()).isEqualTo(ID);
    assertThat(p.hasCommit()).isFalse();
  }

  @Test
  void setValidatesItsArguments() {
    env.connect.offset = offset(1500);
    assertThat(admin("offsets", "set", "--scn", "1400").exit()).isEqualTo(64);
    assertThat(admin("offsets", "set", "--scn", "1400", "--reason", " ").err())
        .contains("--reason");
    assertThat(admin("offsets", "set", "--scn", "0", "--reason", "x").exit()).isEqualTo(64);
    assertThat(admin("offsets", "set", "--scn", "9999", "--reason", "x").err())
        .contains("ahead of the database's current SCN 2500");
    FakeEnv.Run unknown =
        admin("offsets", "set", "--scn", "1400", "--reason", "x", "--forget-released", "1:1.1.1");
    assertThat(unknown.exit()).isEqualTo(64);
    assertThat(unknown.err()).contains("is not in the offset's released transactions");
    env.db.catalog.base.database =
        new sh.oso.connect.oracle.core.topology.DatabaseInfo(
            999,
            "OTHER",
            true,
            "ARCHIVELOG",
            "READ WRITE",
            "PRIMARY",
            "23.0.0.0.0",
            1,
            true,
            "Linux");
    assertThat(admin("offsets", "set", "--scn", "1400", "--reason", "x").err())
        .contains("belongs to database 1234");
    assertThat(env.log).isEmpty();
  }

  @Test
  void setNeedsBrokerAccessForTheOpsTopic() {
    env.connect.offset = offset(1500);
    env.connect.config.remove("cdc.kafka.bootstrap.servers");
    FakeEnv.Run r = admin("offsets", "set", "--scn", "1400", "--reason", "x");
    assertThat(r.exit()).isEqualTo(64);
    assertThat(r.err()).contains("needs broker access");
    FakeEnv.Run viaFlag =
        admin(
            "offsets",
            "set",
            "--scn",
            "1400",
            "--reason",
            "x",
            "--bootstrap-servers",
            "other:9093");
    assertThat(viaFlag.exit()).isZero();
    assertThat(env.kafkaProps).containsEntry("bootstrap.servers", "other:9093");
  }

  @Test
  void aFailedPatchIsRecordedAsFailed() throws Exception {
    env.connect.offset = offset(1500);
    env.connect.patchFailure = new IOException("HTTP 500: boom");
    FakeEnv.Run r = admin("offsets", "set", "--scn", "1400", "--reason", "x");
    assertThat(r.exit()).isEqualTo(1);
    assertThat(r.err()).contains("The offset was not changed").contains("boom");
    assertThat(ops())
        .extracting(o -> o.path("details").path("outcome").asText())
        .containsExactly("applying", "failed");
    assertThat(ops().get(1).path("details").path("error").asText()).contains("boom");
  }

  @Test
  void opsRecordsFollowTheConnectorsJsonConverter() throws Exception {
    env.connect.offset = offset(1500);
    env.connect.config.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    assertThat(admin("offsets", "set", "--scn", "1400", "--reason", "x").exit()).isZero();
    JsonNode enveloped = ops().get(0);
    assertThat(enveloped.path("schema").path("name").asText()).isEqualTo("io.oso.cdc.ops.Event");
    assertThat(enveloped.path("payload").path("type").asText()).isEqualTo("offsets-set");

    env.connect.config.put("value.converter", "io.confluent.connect.avro.AvroConverter");
    FakeEnv.Run avro = admin("offsets", "set", "--scn", "1300", "--reason", "x");
    assertThat(avro.exit()).isEqualTo(64);
    assertThat(avro.err()).contains("--ops-format");
    assertThat(
            admin("offsets", "set", "--scn", "1300", "--reason", "x", "--ops-format", "json")
                .exit())
        .isZero();
  }

  @Test
  void aConfigFileReplacesTheWorkersCopy(@org.junit.jupiter.api.io.TempDir java.nio.file.Path dir)
      throws Exception {
    env.connect.config.put(
        sh.oso.connect.oracle.core.config.CoreConfig.DATABASE_PASSWORD, "${file:/x:p}");
    java.nio.file.Path file = dir.resolve("c.json");
    java.nio.file.Files.writeString(
        file, JSON.writeValueAsString(Map.of("config", FakeEnv.connectorConfig())));
    env.connect.offset = offset(1500);
    assertThat(admin("offsets", "show", "--config", file.toString()).exit()).isZero();
    assertThat(admin("offsets", "show", "--config", dir.resolve("missing.json").toString()).exit())
        .isEqualTo(1);
    env.connect.config.remove("cdc.topic.prefix");
    assertThat(admin("offsets", "show").exit()).isEqualTo(64);
  }
}
