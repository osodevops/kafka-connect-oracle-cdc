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
package sh.oso.connect.oracle.doctor.cli;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import sh.oso.connect.oracle.core.doctor.ArchiveStat;
import sh.oso.connect.oracle.core.doctor.CapturedTable;
import sh.oso.connect.oracle.core.doctor.KafkaFacts;
import sh.oso.connect.oracle.core.doctor.MetricsSample;
import sh.oso.connect.oracle.core.doctor.OnlineLogGroup;
import sh.oso.connect.oracle.core.doctor.RedoProfile;
import sh.oso.connect.oracle.doctor.testing.FakeEnv;

/** check, redo-profile, sizing and explain-lag against the in-memory environment. */
class DoctorCommandsTest {

  static final ObjectMapper JSON = new ObjectMapper();
  static final long MIB = 1024L * 1024;

  @TempDir Path dir;
  FakeEnv env;
  Path config;

  @BeforeEach
  void setUp() throws Exception {
    env = new FakeEnv();
    env.db.catalog.base.archivedRun(1, 10, 10, 1000, 100).onlineCurrent(1, 20, 2000);
    env.db.catalog.base.currentScn = 2500;
    env.db.catalog.tables.add(
        new CapturedTable(
            "FREEPDB1",
            "APP",
            "ORDERS",
            List.of(new CapturedTable.Column("ID", "NUMBER", false)),
            true,
            false,
            true,
            false,
            false,
            false));
    config = write(FakeEnv.connectorConfig());
  }

  private Path write(Map<String, String> props) throws Exception {
    Path p = Files.createTempFile(dir, "connector", ".json");
    Files.writeString(p, JSON.writeValueAsString(Map.of("name", "orders", "config", props)));
    return p;
  }

  static final class Facts implements KafkaFacts {
    final Map<String, TopicFacts> topics = new LinkedHashMap<>();

    @Override
    public Map<String, TopicFacts> topics(Collection<String> names) {
      Map<String, TopicFacts> out = new LinkedHashMap<>();
      names.forEach(
          n -> {
            if (topics.containsKey(n)) {
              out.put(n, topics.get(n));
            }
          });
      return out;
    }

    @Override
    public CreateRights canCreateTopics() {
      return CreateRights.ALLOWED;
    }

    @Override
    public long transactionMaxTimeoutMs() {
      return 900_000;
    }
  }

  @Test
  void checkRunsEveryRuleByDefaultAndTheFastSetOnRequest() throws Exception {
    env.kafka.facts = new Facts();
    FakeEnv.Run all = env.doctor("check", "--config", config.toString(), "--format", "junit");
    assertThat(all.exit()).isZero();
    assertThat(all.out())
        .contains("tests=\"23\" failures=\"0\"")
        .contains("<testcase name=\"DOC-20\" classname=\"oracle-cdc-doctor\">")
        .contains("<testcase name=\"DOC-1\" classname=\"oracle-cdc-doctor\"/>");
    assertThat(env.kafkaProps)
        .containsEntry("bootstrap.servers", "broker:9092")
        .containsEntry("security.protocol", "PLAINTEXT");
    assertThat(env.kafka.closed).isTrue();
    assertThat(env.db.closed).isTrue();
    assertThat(env.databaseConfig.databasePassword().value()).isEqualTo(FakeEnv.PASSWORD);
    assertThat(all.out() + all.err()).doesNotContain(FakeEnv.PASSWORD);

    FakeEnv.Run fast =
        env.doctor("check", "--config", config.toString(), "--rules", "fast", "--format", "json");
    assertThat(fast.out()).startsWith("{\"exitCode\":0,\"findings\":[]}");
  }

  @Test
  void checkReportsBrokerAndWorkerFindings() throws Exception {
    Facts facts = new Facts();
    facts.topics.put("cdc.cdc.txjournal", new KafkaFacts.TopicFacts("delete", 604_800_000));
    env.kafka.facts = facts;
    Map<String, String> props = new HashMap<>(FakeEnv.connectorConfig());
    props.put("exactly.once.support", "required");
    env.connect.validateErrors.put(
        "exactly.once.support",
        List.of("This worker does not have exactly-once source support enabled."));
    FakeEnv.Run r =
        env.doctor(
            "check",
            "--config",
            write(props).toString(),
            "--connect-url",
            "http://connect:8083",
            "--bootstrap-servers",
            "other:9093");
    assertThat(r.exit()).isEqualTo(1);
    assertThat(r.out())
        .contains("| DOC-17 | BLOCKING | cdc.cdc.txjournal (txjournal) must be compacted")
        .contains("| DOC-18 | BLOCKING | The Connect worker rejects exactly.once.support=required");
    assertThat(env.kafkaProps).containsEntry("bootstrap.servers", "other:9093");
    assertThat(env.connect.validated).containsEntry("exactly.once.support", "required");
  }

  @Test
  void checkWithoutBrokerAccessSaysSoAndAnUnreachableDatabaseIsBlocking() throws Exception {
    Map<String, String> props = new HashMap<>(FakeEnv.connectorConfig());
    props.remove("cdc.kafka.bootstrap.servers");
    FakeEnv.Run r = env.doctor("check", "--config", write(props).toString());
    assertThat(r.exit()).isZero();
    assertThat(r.out())
        .contains("| DOC-17 | INFO | Internal topics not checked: no broker access.");
    assertThat(env.kafkaProps).isNull();

    env.databaseFailure = new java.sql.SQLException("listener refused the connection");
    FakeEnv.Run down = env.doctor("check", "--config", config.toString(), "--format", "junit");
    assertThat(down.exit()).isEqualTo(1);
    assertThat(down.out()).contains("<failure").contains("CONNECT").contains("listener refused");

    assertThat(env.doctor("check", "--config", config.toString(), "--max-downtime", "soon").exit())
        .isEqualTo(64);
  }

  @Test
  void redoProfileSamplesTheNewestLogsAndRanksTables() throws Exception {
    Instant now = env.now;
    env.db.catalog.history.add(
        new ArchiveStat(
            1, 18, 1800, 1900, now.minusSeconds(1800), now.minusSeconds(1200), 300 * MIB, false));
    env.db.catalog.history.add(
        new ArchiveStat(
            1, 19, 1900, 2000, now.minusSeconds(1200), now.minusSeconds(600), 200 * MIB, false));
    env.db.sample =
        List.of(
            new RedoProfile.SampleRow("FREEPDB1", "STAGE", "PRICES", "DDL", 1, 1),
            new RedoProfile.SampleRow("FREEPDB1", "STAGE", "PRICES", "INSERT", 9000, 0),
            new RedoProfile.SampleRow("FREEPDB1", "APP", "ORDERS", "UPDATE", 1000, 0));
    FakeEnv.Run r =
        env.doctor(
            "redo-profile", "--config", config.toString(), "--sample-logs", "2", "--window", "1h");
    assertThat(r.err()).isEmpty();
    assertThat(r.exit()).isZero();
    assertThat(env.db.sampledRange).containsExactly(1800, 1999);
    assertThat(env.db.sampledLogs).extracting(l -> l.sequence()).containsExactly(18L, 19L);
    assertThat(r.out())
        .contains("| 2026-10-05T11:00:00Z | 1 | 2 | 500 MiB |")
        .contains("Mined SCN 1800 to 1999 in 2 logs (500 MiB)")
        .contains("| FREEPDB1.STAGE.PRICES | no | 9001 |")
        .contains("truncate and reload");

    env.db.catalog.base.markDeleted(1, 18);
    env.db.catalog.history.set(
        0,
        new ArchiveStat(
            1, 18, 1800, 1900, now.minusSeconds(1800), now.minusSeconds(1200), 1, true));
    env.db.catalog.history.add(
        new ArchiveStat(2, 3, 1850, 1950, now.minusSeconds(900), now.minusSeconds(300), 1, false));
    FakeEnv.Run gap =
        env.doctor("redo-profile", "--config", config.toString(), "--sample-logs", "2");
    assertThat(gap.exit()).isEqualTo(1);
    assertThat(gap.err()).contains("redo-profile");
  }

  @Test
  void sizingReportsRatesAndRetention() {
    Instant now = env.now;
    for (int i = 0; i < 8; i++) {
      env.db.catalog.history.add(
          new ArchiveStat(
              1,
              100 + i,
              i,
              i + 1,
              now.minusSeconds(7200),
              now.minusSeconds(3600 + 60 * (i + 1)),
              256 * MIB,
              false));
    }
    env.db.catalog.groups.add(new OnlineLogGroup(1, 1, 256 * MIB, "CURRENT"));
    FakeEnv.Run r = env.doctor("sizing", "--config", config.toString(), "--max-downtime", "2d");
    assertThat(r.exit()).isZero();
    assertThat(r.out())
        .contains("| 1 | 8 |")
        .contains("| 256 MiB | 512 MiB |")
        .contains("keep archived logs for at least 49 h (the downtime plus")
        .contains("cdc.txjournal.threshold.ms of 5 min")
        .contains("No archived log has been deleted yet");
  }

  private static MetricsSample sample(Instant at, long queue, long behind, long steps) {
    Map<String, Long> m = new HashMap<>();
    m.put("QueueDepth", queue);
    m.put("MillisBehindSource", behind);
    m.put("Steps", steps);
    m.put("LastStepMillis", 200L);
    return new MetricsSample(at, m, List.of());
  }

  @Test
  void explainLagReadsTwiceAndNamesTheCause() throws Exception {
    env.connect.state = "RUNNING";
    env.samples.add(sample(env.now, 7_900, 50_000, 10));
    env.samples.add(sample(env.now.plusSeconds(5), 8_000, 60_000, 11));
    FakeEnv.Run r =
        env.doctor(
            "explain-lag",
            "--connect-url",
            "http://connect:8083",
            "--name",
            "orders",
            "--metrics-url",
            "http://worker:9404/metrics",
            "--interval",
            "5s");
    assertThat(r.err()).isEmpty();
    assertThat(r.exit()).isZero();
    assertThat(env.now).isEqualTo(Instant.parse("2026-10-05T12:00:05Z"));
    assertThat(r.out())
        .contains("Connector orders is RUNNING.")
        .contains("The record queue is full (8000 of 8000 records)");

    assertThat(env.doctor("explain-lag", "--server", "cdc").exit()).isEqualTo(64);
    assertThat(env.doctor("explain-lag", "--jmx-url", "x").err()).contains("--server");
    env.samples.add(sample(env.now, 0, 0, 1));
    env.samples.add(sample(env.now, 0, 0, 2));
    FakeEnv.Run fromFile =
        env.doctor(
            "explain-lag",
            "--config",
            config.toString(),
            "--jmx-url",
            "service:jmx:x",
            "--interval",
            "PT1S");
    assertThat(fromFile.out()).contains("keeps up");
  }

  @Test
  void durations() {
    assertThat(Durations.parse("2h")).isEqualTo(Duration.ofHours(2));
    assertThat(Durations.parse("1h30m")).isEqualTo(Duration.ofMinutes(90));
    assertThat(Durations.parse("7d")).isEqualTo(Duration.ofDays(7));
    assertThat(Durations.parse("250ms")).isEqualTo(Duration.ofMillis(250));
    assertThat(Durations.parse("PT45S")).isEqualTo(Duration.ofSeconds(45));
    for (String bad : List.of("", "soon", "2x", "h2", "PTX")) {
      org.assertj.core.api.Assertions.assertThatThrownBy(() -> Durations.parse(bad))
          .isInstanceOf(picocli.CommandLine.TypeConversionException.class);
    }
  }

  @Test
  void adminIsAlsoASubcommandOfTheDoctor() {
    assertThat(env.doctor("admin").out()).contains("offsets").contains("resnapshot");
    assertThat(env.doctor("admin", "offsets").exit()).isEqualTo(64);
    assertThat(env.admin().out()).contains("Usage: oracle-cdc-admin");
    assertThat(env.admin("journal").exit()).isEqualTo(64);
    env.connect.offset = null;
    FakeEnv.Run show =
        env.doctor(
            "admin", "offsets", "show", "--connect-url", "http://c:8083", "--name", "orders");
    assertThat(show.exit()).isZero();
    assertThat(show.out()).contains("No stored offset");
  }
}
