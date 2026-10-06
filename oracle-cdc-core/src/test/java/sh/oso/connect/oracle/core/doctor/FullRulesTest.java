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
package sh.oso.connect.oracle.core.doctor;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.core.config.CoreConfig;
import sh.oso.connect.oracle.core.topology.ThreadInfo;

/** The full-mode rules (PRD-05 DOC-8 to DOC-11, DOC-13, DOC-16 to DOC-22) on their fixtures. */
class FullRulesTest {

  static final Instant NOW = Instant.parse("2026-10-05T12:00:00Z");
  static final long MIB = 1024L * 1024;

  private static CoreConfig config(Map<String, String> extra) {
    Map<String, String> p = new HashMap<>();
    p.put(CoreConfig.DATABASE_HOST, "h");
    p.put(CoreConfig.DATABASE_SERVICE, "FREE");
    p.put(CoreConfig.DATABASE_USER, "C##CDC");
    p.put(CoreConfig.DATABASE_PASSWORD, "x");
    p.put(CoreConfig.DATABASE_PDBS, "FREEPDB1");
    p.putAll(extra);
    return new CoreConfig(p);
  }

  private static DoctorContext ctx(FakeDoctorCatalog cat, Map<String, String> extra) {
    return new DoctorContext(config(extra), cat, List.of(".*"), List.of(), "fail")
        .withConnectorProperties(extra)
        .withClock(Clock.fixed(NOW, ZoneOffset.UTC));
  }

  private static List<Finding> run(Rule rule, DoctorContext ctx) {
    return new Doctor(List.of(rule)).run(ctx).findings();
  }

  private static CapturedTable table(String name, CapturedTable.Column... cols) {
    return new CapturedTable(
        "FREEPDB1", "APP", name, List.of(cols), true, false, true, false, false, false);
  }

  private static CapturedTable.Column col(String name, String type) {
    return new CapturedTable.Column(name, type, false);
  }

  /** {@code count} logs of {@code bytes} each on a thread, one every {@code every}, ending now. */
  private static void logs(
      FakeDoctorCatalog cat, int thread, int count, Duration every, long bytes, Instant end) {
    for (int i = 0; i < count; i++) {
      Instant next = end.minus(every.multipliedBy(count - 1 - i));
      cat.history.add(
          new ArchiveStat(
              thread,
              100 + i,
              1000 + i * 10L,
              1010 + i * 10L,
              next.minus(every),
              next,
              bytes,
              false));
    }
  }

  @Test
  void allRulesRunInRuleOrderAndACleanDatabaseOnlyGetsInformation() {
    FakeDoctorCatalog cat = new FakeDoctorCatalog();
    cat.tables.add(table("ORDERS", col("ID", "NUMBER")));
    Report r = new Doctor(Rules.all()).run(ctx(cat, Map.of()));
    assertThat(r.rules())
        .containsExactly(
            "DOC-1", "DOC-2", "DOC-3", "DOC-4", "DOC-5", "DOC-6", "DOC-7", "DOC-8", "DOC-9",
            "DOC-10", "DOC-11", "DOC-12", "DOC-13", "DOC-14", "DOC-15", "DOC-16", "DOC-17",
            "DOC-18", "DOC-19", "DOC-20", "DOC-21", "DOC-22");
    assertThat(r.findings()).extracting(Finding::severity).containsOnly(Severity.INFO);
    assertThat(r.exitCode()).isEqualTo(Report.EXIT_OK);
    assertThat(Rules.fastMode()).hasSize(11);
  }

  @Test
  void lobColumnsAreDescribedForTheChosenLobMode() {
    FakeDoctorCatalog cat = new FakeDoctorCatalog();
    cat.tables.add(
        table(
            "DOCS",
            col("ID", "NUMBER"),
            col("BODY", "CLOB"),
            col("PIC", "BLOB"),
            col("X", "XMLTYPE"),
            col("OLD", "LONG")));
    List<Finding> skip = run(Rules.lobs(), ctx(cat, Map.of()));
    assertThat(skip).extracting(Finding::severity).containsOnly(Severity.INFO);
    assertThat(skip)
        .extracting(Finding::message)
        .anyMatch(m -> m.contains("LOB columns BODY, PIC") && m.contains("leaves them out"))
        .anyMatch(m -> m.contains("XMLTYPE columns X") && m.contains("leaves them out"))
        .anyMatch(m -> m.contains("LONG or LONG RAW columns OLD"));

    List<Finding> inline = run(Rules.lobs(), ctx(cat, Map.of(CoreConfig.LOB_MODE, "inline")));
    assertThat(inline)
        .anyMatch(f -> f.severity() == Severity.INFO && f.message().contains("placeholder"))
        .anyMatch(
            f -> f.severity() == Severity.WARNING && f.message().contains("neither assembled"));
    List<Finding> reselect = run(Rules.lobs(), ctx(cat, Map.of(CoreConfig.LOB_MODE, "reselect")));
    assertThat(reselect).anyMatch(f -> f.message().contains("AS OF the commit SCN"));
  }

  @Test
  void switchRateAboveSixPerHourWarnsWithARecommendedSize() {
    FakeDoctorCatalog cat = new FakeDoctorCatalog();
    cat.groups.add(new OnlineLogGroup(1, 1, 200 * MIB, "CURRENT"));
    cat.groups.add(new OnlineLogGroup(1, 2, 200 * MIB, "INACTIVE"));
    logs(cat, 1, 10, Duration.ofMinutes(5), 200 * MIB, NOW.minus(Duration.ofMinutes(125)));
    List<Finding> f = run(Rules.switchRate(), ctx(cat, Map.of()));
    assertThat(f)
        .singleElement()
        .satisfies(
            w -> {
              assertThat(w.severity()).isEqualTo(Severity.WARNING);
              assertThat(w.message())
                  .contains("switched logs 10 times")
                  .contains("Online logs of 512 MiB (now 200 MiB)");
              assertThat(w.fixSql()).contains("ALTER DATABASE ADD LOGFILE THREAD 1 SIZE 512M;");
            });

    FakeDoctorCatalog calm = new FakeDoctorCatalog();
    logs(calm, 1, 5, Duration.ofMinutes(12), 200 * MIB, NOW.minus(Duration.ofHours(1)));
    assertThat(run(Rules.switchRate(), ctx(calm, Map.of()))).isEmpty();
  }

  @Test
  void retentionIsComparedWithJournalThresholdPlusDowntimeOnceAPurgeHasHappened() {
    FakeDoctorCatalog none = new FakeDoctorCatalog();
    assertThat(run(Rules.retention(), ctx(none, Map.of())))
        .singleElement()
        .satisfies(f -> assertThat(f.message()).contains("No archived log is present"));

    FakeDoctorCatalog young = new FakeDoctorCatalog();
    logs(young, 1, 6, Duration.ofHours(1), MIB, NOW);
    assertThat(run(Rules.retention(), ctx(young, Map.of())))
        .singleElement()
        .satisfies(
            f -> {
              assertThat(f.severity()).isEqualTo(Severity.INFO);
              assertThat(f.message()).contains("cannot be measured").contains("25 h");
            });

    FakeDoctorCatalog purged = new FakeDoctorCatalog();
    logs(purged, 1, 6, Duration.ofHours(1), MIB, NOW);
    purged.history.add(
        new ArchiveStat(
            1,
            99,
            900,
            1000,
            NOW.minus(Duration.ofHours(8)),
            NOW.minus(Duration.ofHours(6)),
            MIB,
            true));
    assertThat(run(Rules.retention(), ctx(purged, Map.of())))
        .singleElement()
        .satisfies(
            f -> {
              assertThat(f.severity()).isEqualTo(Severity.WARNING);
              assertThat(f.message())
                  .contains("reaches back 6 h")
                  .contains("needs 25 h")
                  .contains("CDC-2002");
              assertThat(f.fixSql()).contains("SYSDATE-25/24");
            });
    DoctorContext shortDowntime =
        ctx(purged, Map.of(CoreConfig.TXJOURNAL_THRESHOLD_MS, "60000"))
            .withMaxDowntime(Duration.ofHours(2));
    assertThat(run(Rules.retention(), shortDowntime)).isEmpty();
  }

  @Test
  void undoRetentionBelowAChunkReadWarns() {
    FakeDoctorCatalog cat = new FakeDoctorCatalog();
    assertThat(run(Rules.undoRetention(), ctx(cat, Map.of()))).isEmpty();
    cat.parameters.put("undo_retention", "60");
    assertThat(run(Rules.undoRetention(), ctx(cat, Map.of())))
        .singleElement()
        .satisfies(
            f -> {
              assertThat(f.severity()).isEqualTo(Severity.WARNING);
              assertThat(f.message()).contains("60 s").contains("ORA-01555");
              assertThat(f.fixSql()).contains("UNDO_RETENTION");
            });
    cat.parameters.remove("undo_retention");
    assertThat(run(Rules.undoRetention(), ctx(cat, Map.of())))
        .singleElement()
        .extracting(Finding::severity)
        .isEqualTo(Severity.INFO);
  }

  @Test
  void racThreadsAndFanAreReportedAsInformation() {
    FakeDoctorCatalog cat = new FakeDoctorCatalog();
    assertThat(run(Rules.rac(), ctx(cat, Map.of()))).isEmpty();
    cat.base.threads.add(new ThreadInfo(2, false, "CLOSED", 7));
    List<Finding> f = run(Rules.rac(), ctx(cat, Map.of(CoreConfig.DATABASE_FAN_ENABLED, "true")));
    assertThat(f).extracting(Finding::severity).containsOnly(Severity.INFO);
    assertThat(f)
        .extracting(Finding::message)
        .anyMatch(m -> m.contains("thread 1 OPEN, thread 2 disabled") && m.contains("Phase 2"))
        .anyMatch(m -> m.contains("does not subscribe to FAN"));
  }

  @Test
  void missingFixedObjectStatisticsWarn() {
    FakeDoctorCatalog cat = new FakeDoctorCatalog();
    assertThat(run(Rules.fixedObjectStatistics(), ctx(cat, Map.of()))).isEmpty();
    cat.fixedTablesWithStatistics = 0;
    assertThat(run(Rules.fixedObjectStatistics(), ctx(cat, Map.of())))
        .singleElement()
        .satisfies(f -> assertThat(f.fixSql()).contains("GATHER_FIXED_OBJECTS_STATS"));
    cat.fixedTablesWithStatistics = -1;
    assertThat(run(Rules.fixedObjectStatistics(), ctx(cat, Map.of())))
        .singleElement()
        .extracting(Finding::severity)
        .isEqualTo(Severity.INFO);
  }

  @Test
  void namesAvroRefusesNeedAdjustmentUnderAnAvroConverter() {
    FakeDoctorCatalog cat = new FakeDoctorCatalog();
    cat.tables.add(table("ORDERS", col("ID", "NUMBER")));
    cat.tables.add(table("ORDER#ITEMS", col("ID", "NUMBER"), col("AMT$", "NUMBER")));
    Map<String, String> avro =
        Map.of(
            "value.converter", "io.apicurio.registry.utils.converter.AvroConverter",
            "cdc.topic.prefix", "oso-cdc");
    List<Finding> f = run(Rules.avroNames(), ctx(cat, avro));
    assertThat(f).hasSize(2);
    assertThat(f.get(0).severity()).isEqualTo(Severity.BLOCKING);
    assertThat(f.get(0).message())
        .contains("FREEPDB1.APP.ORDER#ITEMS.AMT$")
        .contains("cdc.field.name.adjustment.mode=avro");
    assertThat(f.get(1).severity()).isEqualTo(Severity.WARNING);
    assertThat(f.get(1).message())
        .contains("cdc.topic.prefix part oso-cdc")
        .contains("FREEPDB1.APP.ORDER#ITEMS")
        .doesNotContain("APP.ORDERS,");
    Map<String, String> adjusted = new HashMap<>(avro);
    adjusted.put("cdc.schema.name.adjustment.mode", "avro");
    adjusted.put("cdc.field.name.adjustment.mode", "avro_unicode");
    assertThat(run(Rules.avroNames(), ctx(cat, adjusted))).isEmpty();
    // a column cdc.columns.exclude drops never reaches the converter
    Map<String, String> excludedColumn = new HashMap<>(avro);
    excludedColumn.put("cdc.columns.exclude", "FREEPDB1\\.APP\\.ORDER#ITEMS\\.AMT\\$");
    assertThat(run(Rules.avroNames(), ctx(cat, excludedColumn)))
        .extracting(Finding::severity)
        .containsExactly(Severity.WARNING);
    // a JSON converter, or none on the connector (the worker's is not visible): nothing to say
    assertThat(run(Rules.avroNames(), ctx(cat, Map.of("cdc.topic.prefix", "oso-cdc")))).isEmpty();
    assertThat(
            run(
                Rules.avroNames(),
                ctx(cat, Map.of("value.converter", "org.apache.kafka.connect.json.JsonConverter"))))
        .isEmpty();
  }

  @Test
  void closedPluggableDatabasesWarnAndUnsavedStatesInform() {
    FakeDoctorCatalog cat = new FakeDoctorCatalog();
    assertThat(run(Rules.pdbsOpen(), ctx(cat, Map.of()))).isEmpty();
    cat.pdbStates =
        List.of(
            new PdbState("FREEPDB1", "READ WRITE", true),
            new PdbState("FREEPDB2", "MOUNTED", false),
            new PdbState("FREEPDB3", "READ WRITE", false));
    List<Finding> f = run(Rules.pdbsOpen(), ctx(cat, Map.of()));
    assertThat(f).hasSize(2);
    assertThat(f.get(0).severity()).isEqualTo(Severity.WARNING);
    assertThat(f.get(0).message()).contains("FREEPDB2 is MOUNTED").contains("ORA-16331");
    assertThat(f.get(0).fixSql()).contains("FREEPDB2 OPEN").contains("SAVE STATE");
    assertThat(f.get(1).severity()).isEqualTo(Severity.INFO);
    assertThat(f.get(1).message()).contains("FREEPDB3 has no saved state");
    cat.pdbStates = null;
    assertThat(run(Rules.pdbsOpen(), ctx(cat, Map.of())))
        .singleElement()
        .extracting(Finding::severity)
        .isEqualTo(Severity.INFO);
    cat.pdbStates = List.of();
    assertThat(run(Rules.pdbsOpen(), ctx(cat, Map.of()))).as("a non-CDB").isEmpty();
  }

  /** Kafka facts from maps. */
  static final class FakeKafka implements KafkaFacts {
    final Map<String, TopicFacts> topics = new LinkedHashMap<>();
    CreateRights canCreate = CreateRights.ALLOWED;
    long txMax = 900_000;

    @Override
    public Map<String, TopicFacts> topics(Collection<String> names) {
      Map<String, TopicFacts> out = new LinkedHashMap<>();
      for (String n : names) {
        if (topics.containsKey(n)) {
          out.put(n, topics.get(n));
        }
      }
      return out;
    }

    @Override
    public CreateRights canCreateTopics() {
      return canCreate;
    }

    @Override
    public long transactionMaxTimeoutMs() {
      return txMax;
    }
  }

  static final List<InternalTopic> TOPICS =
      List.of(
          new InternalTopic("ops", "cdc.cdc.ops", false, -1),
          new InternalTopic("heartbeat", "cdc.cdc.heartbeat", false, 86_400_000),
          new InternalTopic("signals", "cdc.cdc.signals", false, -1),
          new InternalTopic("schema", "cdc.cdc.schema", true, -1),
          new InternalTopic("txjournal", "cdc.cdc.txjournal", true, -1));

  @Test
  void internalTopicsNeedTheirCleanupPolicyOrCreationRights() {
    FakeDoctorCatalog cat = new FakeDoctorCatalog();
    assertThat(run(Rules.kafkaTopics(), ctx(cat, Map.of())))
        .singleElement()
        .satisfies(f -> assertThat(f.message()).contains("no broker access"));

    FakeKafka kafka = new FakeKafka();
    kafka.topics.put("cdc.cdc.ops", new KafkaFacts.TopicFacts("delete", 604_800_000));
    kafka.topics.put("cdc.cdc.signals", new KafkaFacts.TopicFacts("compact", -1));
    kafka.topics.put("cdc.cdc.txjournal", new KafkaFacts.TopicFacts("compact,delete", 86_400_000));
    kafka.canCreate = KafkaFacts.CreateRights.DENIED;
    Map<String, String> own = Map.of(Rules.BOOTSTRAP, "broker:9092");
    List<Finding> f = run(Rules.kafkaTopics(), ctx(cat, own).withKafka(kafka, TOPICS));
    assertThat(f)
        .extracting(Finding::severity, Finding::message)
        .anySatisfy(
            t -> {
              assertThat(t.toList().get(0)).isEqualTo(Severity.BLOCKING);
              assertThat((String) t.toList().get(1))
                  .contains("cdc.cdc.txjournal")
                  .contains("CDC-4002");
            })
        .anySatisfy(
            t -> {
              assertThat(t.toList().get(0)).isEqualTo(Severity.BLOCKING);
              assertThat((String) t.toList().get(1))
                  .contains("cdc.cdc.schema")
                  .contains("may not create");
            })
        .anySatisfy(
            t -> {
              assertThat(t.toList().get(0)).isEqualTo(Severity.WARNING);
              assertThat((String) t.toList().get(1)).contains("cdc.cdc.signals");
            });
    assertThat(f).noneMatch(x -> x.message().contains("cdc.cdc.ops"));

    kafka.canCreate = KafkaFacts.CreateRights.ALLOWED;
    kafka.topics.put("cdc.cdc.txjournal", new KafkaFacts.TopicFacts("compact", -1));
    kafka.topics.put("cdc.cdc.signals", new KafkaFacts.TopicFacts("delete", -1));
    assertThat(run(Rules.kafkaTopics(), ctx(cat, own).withKafka(kafka, TOPICS)))
        .extracting(Finding::severity)
        .containsOnly(Severity.INFO);
    kafka.canCreate = KafkaFacts.CreateRights.UNKNOWN;
    assertThat(run(Rules.kafkaTopics(), ctx(cat, own).withKafka(kafka, TOPICS)))
        .extracting(Finding::severity)
        .contains(Severity.WARNING);

    // without broker access of its own the connector writes no journal or schema records
    List<Finding> worker = run(Rules.kafkaTopics(), ctx(cat, Map.of()).withKafka(kafka, TOPICS));
    assertThat(worker).noneMatch(x -> x.message().contains("txjournal"));
    assertThat(worker)
        .singleElement()
        .satisfies(x -> assertThat(x.message()).contains("cdc.cdc.heartbeat").contains("auto"));
  }

  @Test
  void exactlyOnceIsCheckedAgainstTheBrokerAndTheWorker() {
    FakeDoctorCatalog cat = new FakeDoctorCatalog();
    assertThat(run(Rules.exactlyOnce(), ctx(cat, Map.of()))).isEmpty();

    Map<String, String> eos =
        Map.of(
            "exactly.once.support",
            "required",
            "transaction.boundary",
            "connector",
            "producer.override.transaction.timeout.ms",
            "900001");
    FakeKafka kafka = new FakeKafka();
    List<Finding> f =
        run(
            Rules.exactlyOnce(),
            ctx(cat, eos)
                .withKafka(kafka, TOPICS)
                .withWorker(new WorkerFacts("exactly-once source support is not enabled.")));
    assertThat(f).extracting(Finding::severity).containsOnly(Severity.BLOCKING);
    assertThat(f)
        .extracting(Finding::message)
        .anyMatch(m -> m.contains("900001 ms") && m.contains("transaction.max.timeout.ms"))
        .anyMatch(m -> m.contains("rejects exactly.once.support=required"));

    Map<String, String> slowBatch =
        Map.of(
            "transaction.boundary",
            "connector",
            "producer.override.transaction.timeout.ms",
            "1000",
            "cdc.eos.batch.max.ms",
            "1000");
    assertThat(run(Rules.exactlyOnce(), ctx(cat, slowBatch)))
        .extracting(Finding::severity, Finding::message)
        .anySatisfy(t -> assertThat(t.toList().get(0)).isEqualTo(Severity.BLOCKING))
        .anySatisfy(t -> assertThat((String) t.toList().get(1)).contains("no broker access"));

    kafka.txMax = -1;
    assertThat(
            run(
                Rules.exactlyOnce(),
                ctx(cat, Map.of("exactly.once.support", "required"))
                    .withKafka(kafka, TOPICS)
                    .withWorker(new WorkerFacts(null))))
        .singleElement()
        .extracting(Finding::severity)
        .isEqualTo(Severity.WARNING);
    kafka.txMax = 900_000;
    assertThat(
            run(
                Rules.exactlyOnce(),
                ctx(cat, Map.of("exactly.once.support", "required")).withKafka(kafka, TOPICS)))
        .singleElement()
        .satisfies(x -> assertThat(x.message()).contains("--connect-url"));
  }

  @Test
  void idleTimeoutGuardIsNotedUnlessKeepaliveIsBelowLoadBalancerLimits() {
    FakeDoctorCatalog cat = new FakeDoctorCatalog();
    assertThat(run(Rules.idleTimeout(), ctx(cat, Map.of())))
        .singleElement()
        .satisfies(
            f -> {
              assertThat(f.severity()).isEqualTo(Severity.INFO);
              assertThat(f.message()).contains("operating system").contains("350 s");
            });
    String key = CoreConfig.DATABASE_CONNECTION_PROPERTIES;
    assertThat(run(Rules.idleTimeout(), ctx(cat, Map.of(key, "a=b;oracle.net.TCP_KEEPIDLE=120"))))
        .isEmpty();
    assertThat(run(Rules.idleTimeout(), ctx(cat, Map.of(key, "oracle.net.TCP_KEEPIDLE=600"))))
        .singleElement()
        .satisfies(f -> assertThat(f.message()).contains("is 600 s"));
  }

  private static FakeDoctorCatalog withBuild() {
    FakeDoctorCatalog cat = new FakeDoctorCatalog();
    cat.base.archivedRun(1, 10, 5, 1000, 100).onlineCurrent(1, 15, 1500);
    cat.base.currentScn = 2000;
    return cat;
  }

  @Test
  void lagRecoveryNeedsTheBuildPrivilegeOrAUsableBuild() {
    FakeDoctorCatalog ok = withBuild();
    ok.base.dictionaryBuild(1, 11, 12);
    assertThat(run(Rules.lagRecovery(), ctx(ok, Map.of())))
        .singleElement()
        .satisfies(
            f -> {
              assertThat(f.severity()).isEqualTo(Severity.INFO);
              assertThat(f.message()).contains("Lag recovery is possible").contains("SCN 1100");
              assertThat(f.fixSql()).isNull();
            });

    ok.dictionaryPackage = false;
    assertThat(run(Rules.lagRecovery(), ctx(ok, Map.of())))
        .singleElement()
        .satisfies(
            f -> {
              assertThat(f.severity()).isEqualTo(Severity.INFO);
              assertThat(f.message()).contains("for now");
              assertThat(f.fixSql())
                  .isEqualTo("GRANT EXECUTE ON DBMS_LOGMNR_D TO C##CDC CONTAINER=ALL;");
            });

    FakeDoctorCatalog none = withBuild();
    assertThat(run(Rules.lagRecovery(), ctx(none, Map.of())))
        .singleElement()
        .satisfies(
            f -> {
              assertThat(f.severity()).isEqualTo(Severity.INFO);
              assertThat(f.message())
                  .contains("no complete dictionary build")
                  .contains("next start");
            });
    none.dictionaryPackage = false;
    assertThat(run(Rules.lagRecovery(), ctx(none, Map.of())))
        .singleElement()
        .satisfies(
            f -> {
              assertThat(f.severity()).isEqualTo(Severity.WARNING);
              assertThat(f.message()).contains("CDC-6001").contains("DBMS_LOGMNR_D.BUILD");
              assertThat(f.fixSql()).contains("GRANT EXECUTE ON DBMS_LOGMNR_D");
            });

    FakeDoctorCatalog broken = withBuild();
    broken.base.dictionaryBuild(1, 11, 12).markDeleted(1, 13);
    assertThat(run(Rules.lagRecovery(), ctx(broken, Map.of())))
        .singleElement()
        .satisfies(
            f -> assertThat(f.message()).contains("cannot be replayed").contains("CDC-2002"));
    assertThat(
            run(
                Rules.lagRecovery(),
                ctx(broken, Map.of(CoreConfig.DICTIONARY_BUILD_INTERVAL_MS, "0"))))
        .singleElement()
        .satisfies(
            f -> {
              assertThat(f.severity()).isEqualTo(Severity.WARNING);
              assertThat(f.message()).contains("switched off");
            });

    FakeDoctorCatalog off = withBuild();
    off.base.dictionaryBuild(1, 11, 12);
    assertThat(
            run(
                Rules.lagRecovery(),
                ctx(off, Map.of(CoreConfig.DICTIONARY_BUILD_INTERVAL_MS, "0"))))
        .singleElement()
        .satisfies(f -> assertThat(f.message()).contains("serves until its logs are purged"));
  }

  @Test
  void noValidDestinationLeavesTheArchiveRulesToDoc12() throws Exception {
    FakeDoctorCatalog cat = new FakeDoctorCatalog();
    cat.base.destinations.clear();
    DoctorContext c = ctx(cat, Map.of());
    assertThat(c.archiveDestId()).isEqualTo(-1);
    assertThat(run(Rules.switchRate(), c)).isEmpty();
    assertThat(run(Rules.retention(), c)).isEmpty();
    assertThat(c.maxDowntime()).isEqualTo(DoctorContext.DEFAULT_MAX_DOWNTIME);
    assertThat(c.property("missing", "d")).isEqualTo("d");
  }
}
