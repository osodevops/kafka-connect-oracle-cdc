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
package sh.oso.connect.oracle.e2e.qual;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import sh.oso.connect.oracle.e2e.support.Cli;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.Evidence;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;
import sh.oso.connect.oracle.e2e.support.TestDatabase;

/**
 * PRD-04: a cutover from Confluent's Oracle CDC Source connector to this connector, with the
 * operator's tools, under a live workload. Confluent's connector is proprietary and used under its
 * 30-day evaluation licence for development and testing only, so it is never downloaded here: the
 * suite runs only when {@code -De2e.confluent.plugin} names a local, unzipped copy of it, and skips
 * otherwise (CI never has one).
 *
 * <p>Confluent captures a keyed table (snapshot, then streaming); a transaction is opened and left
 * uncommitted; Confluent is stopped with {@code PUT /connectors/{name}/stop}; {@code
 * migrate_from_confluent.py} translates its configuration, {@code takeover_scn.py} reads its offset
 * and writes {@code cdc.start.scn}; this connector starts under its own name and topics; the open
 * transaction then commits. Every committed row must be in Confluent's topic or in the new one, and
 * the transaction that spanned the cutover must be in the new one. The evidence records what
 * Confluent actually stored and emitted, for the migration guide.
 */
@Tag("qual")
class ConfluentCutoverQualIT {

  static final String CONFLUENT_CLASS = "io.confluent.connect.oracle.cdc.OracleCdcSourceConnector";
  static final String OSO_CLASS = "sh.oso.connect.oracle.OracleCdcSourceConnector";

  /** Confluent's connectors run only on Confluent Platform workers (licensing classes). */
  static final String WORKER_IMAGE = "confluentinc/cp-server-connect:7.9.2";

  static final String OLD = "confluent-orders";
  static final String NEW = "oso-orders";
  static final String PREFIX = "osocut";
  static final int SEED = 20;
  static final int SPANNING_ID = 900;
  static final Duration TOOL_TIMEOUT = Duration.ofMinutes(10);
  static final ObjectMapper JSON = new ObjectMapper();

  private final TestDatabase db = TestDatabase.get();
  @TempDir Path dir;

  @Test
  void aTakeoverFromConfluentLosesNoCommittedRow() throws Exception {
    String plugin = System.getProperty("e2e.confluent.plugin", "");
    assumeTrue(
        !plugin.isBlank() && Files.isDirectory(Path.of(plugin)),
        "-De2e.confluent.plugin names no local copy of Confluent's connector");
    assumeTrue(Cli.uvAvailable(), "uv is not on the PATH; the migration tools cannot run");
    assumeFalse(
        db.platform() == sh.oso.connect.oracle.core.topology.Platform.RDS && !db.pdbs().isEmpty(),
        "Confluent documents that its connector does not support RDS with the CDB architecture");
    Evidence ev =
        Evidence.of(getClass(), "Confluent Oracle CDC to OSO cutover")
            .param("target", db.describe());
    String schema = SchemaFixtures.nameFor(getClass());
    Workload workload = null;
    String workerImage = System.getProperty("e2e.confluent.worker.image", WORKER_IMAGE);
    ev.param("confluentWorkerImage", workerImage);
    try (ConnectCluster cluster =
        ConnectCluster.confluentPlatform(workerImage)
            .withPlugin(Path.of(plugin), "confluent-oracle-cdc")
            .start()) {
      assumeTrue(
          cluster.workerReaches(db.workerHost(), db.workerPort()),
          "the Connect worker has no route to " + db.workerHost() + ":" + db.workerPort());
      db.recreateSchema(schema);
      try (Connection w = db.workload(schema)) {
        exec(
            w,
            "CREATE TABLE orders (id NUMBER(9) PRIMARY KEY, name VARCHAR2(40), qty NUMBER(9))",
            "ALTER TABLE orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
            "INSERT INTO orders SELECT LEVEL, 'seed ' || LEVEL, LEVEL FROM dual CONNECT BY LEVEL <="
                + " "
                + SEED);
      }
      db.archiveLogCurrent();
      Thread.sleep(3500); // a snapshot AS OF an SCN right after DDL meets ORA-01466

      // 1. Confluent captures the table: snapshot, then streaming
      Map<String, String> confluent = confluentConfig(schema);
      cluster.register(OLD, confluent);
      cluster.awaitRunning(OLD, Duration.ofMinutes(5));
      String oldTopic = awaitTopic(cluster, schema, Duration.ofMinutes(5), false);
      ev.param("confluentTopic", oldTopic);
      awaitIds(cluster, oldTopic, idsBetween(1, SEED), Duration.ofMinutes(5), false);

      workload = new Workload(db, schema);
      workload.start();
      awaitIds(cluster, oldTopic, Set.of(Workload.FIRST_ID), Duration.ofMinutes(5), false);

      // 2. a transaction that spans the cutover: written before Confluent stops, committed after
      Connection spanning = db.workload(schema);
      spanning.setAutoCommit(false);
      exec(spanning, "INSERT INTO orders VALUES (" + SPANNING_ID + ", 'spans the cutover', 1)");
      Thread.sleep(5000); // Confluent mines past the open transaction's first change

      // 3. stop Confluent: its offset is final; record what it stored
      cluster.lifecycle(OLD, "stop");
      cluster.awaitConnectorState(OLD, "STOPPED", Duration.ofMinutes(2));
      JsonNode offsets = cluster.offsets(OLD);
      ev.param("confluentOffsets", offsets.toString());
      Path source = dir.resolve("confluent-connector.json");
      Files.writeString(source, cluster.connectorInfo(OLD).toPrettyString());

      // 4. translate its configuration
      Path translated = dir.resolve("oso-connector.json");
      Cli.Result migrate =
          Cli.migration(
              "migrate_from_confluent.py",
              Map.of(),
              TOOL_TIMEOUT,
              "--input",
              source.toString(),
              "--output",
              translated.toString(),
              "--report",
              dir.resolve("migration-report.md").toString(),
              "--name",
              NEW,
              "--topic-prefix",
              PREFIX,
              "--format",
              "debezium",
              "--connect-url",
              cluster.restUrl());
      assertThat(migrate.exit()).as(migrate.toString()).isIn(0, 2);
      JsonNode cfg = JSON.readTree(translated.toFile()).path("config");
      ev.param("translatedTopicTemplate", cfg.path("cdc.topic.template").asText());
      ev.param("translatedInclude", cfg.path("cdc.tables.include").asText());
      assertThat(cfg.path("connector.class").asText()).isEqualTo(OSO_CLASS);
      assertThat(cfg.path("cdc.topic.template").asText())
          .as("new topics, never Confluent's: its records are flat rows, ours are envelopes")
          .startsWith("${prefix}.");

      // 5. the takeover SCN from Confluent's offset, against the live database
      Path takenOver = dir.resolve("oso-connector-takeover.json");
      Cli.Result take =
          Cli.migration(
              "takeover_scn.py",
              Map.of("ORACLE_PASSWORD", db.capturePassword()),
              TOOL_TIMEOUT,
              "--connect-url",
              cluster.restUrl(),
              "--connector",
              OLD,
              "--target-config",
              translated.toString(),
              "--db-dsn",
              db.hostDsn(),
              "--db-user",
              db.captureUser(),
              "--db-password-env",
              "ORACLE_PASSWORD",
              "--output",
              takenOver.toString(),
              "--report",
              dir.resolve("takeover-report.md").toString(),
              "--json");
      ev.param("takeoverExit", take.exit());
      assertThat(take.exit()).as(take.toString()).isIn(0, 2);
      JsonNode takeover = JSON.readTree(take.out());
      ev.param("takeover", takeover.toString());
      long start = takeover.path("start_scn").asLong();
      assertThat(start).isPositive();
      assertThat(takeover.path("old_commit_scn").asLong())
          .as("the last commit Confluent delivered bounds the overlap; the start is at or below it")
          .isGreaterThanOrEqualTo(start);

      // 6. the new connector under its own name and topics
      Map<String, String> config = new LinkedHashMap<>();
      JSON.readTree(takenOver.toFile())
          .path("config")
          .fields()
          .forEachRemaining(e -> config.put(e.getKey(), e.getValue().asText()));
      config.putAll(db.connectorDatabaseProps()); // the operator's follow-up: user and password
      config.put("cdc.kafka.bootstrap.servers", "kafka:19092");
      cluster.register(NEW, config);
      cluster.awaitRunning(NEW, Duration.ofMinutes(5));

      // 7. the spanning transaction commits; the workload runs on across the overlap, then stops
      spanning.commit();
      spanning.close();
      Thread.sleep(20_000);
      workload.stop();
      Set<Integer> committed = new TreeSet<>(idsBetween(1, SEED));
      committed.addAll(workload.committed());
      committed.add(SPANNING_ID);

      // 8. every committed row is in one of the two topics; the spanning one in the new topic
      String newTopic = awaitTopic(cluster, schema, Duration.ofMinutes(5), true);
      ev.param("osoTopic", newTopic);
      Set<Integer> inNew =
          awaitIds(cluster, newTopic, Set.of(SPANNING_ID), Duration.ofMinutes(5), true);
      Set<Integer> inOld = readIds(cluster, oldTopic, false, Duration.ofSeconds(20));
      inNew = readIds(cluster, newTopic, true, Duration.ofSeconds(20));
      Set<Integer> missing = new TreeSet<>(committed);
      missing.removeAll(inOld);
      missing.removeAll(inNew);
      Set<Integer> both = new TreeSet<>(inOld);
      both.retainAll(inNew);
      ev.count("committedRows", committed.size())
          .count("inConfluentTopic", inOld.size())
          .count("inOsoTopic", inNew.size())
          .count("inBothTopics", both.size())
          .count("missing", missing.size());
      ev.param("sampleConfluentRecord", sample(cluster, oldTopic));
      assertThat(missing).as("committed rows in neither topic").isEmpty();
      assertThat(inNew).as("the transaction that spanned the cutover").contains(SPANNING_ID);

      // 9. the operator's verification: Confluent's topic by key, the new topic value by value
      long checkScn;
      try (Connection meta = db.capture()) {
        checkScn = sh.oso.connect.oracle.e2e.support.LogMinerHelper.currentScn(meta);
      }
      Path registered = dir.resolve("oso-connector-registered.json");
      Files.writeString(
          registered,
          JSON.writerWithDefaultPrettyPrinter()
              .writeValueAsString(Map.of("name", NEW, "config", config)));
      Path evidence = Cli.repoRoot().resolve("e2e-tests/target/confluent-migration-evidence.json");
      Cli.Result verify =
          Cli.migration(
              "verify_cutover.py",
              Map.of("ORACLE_PASSWORD", db.capturePassword()),
              TOOL_TIMEOUT,
              "--input",
              registered.toString(),
              "--table",
              schema + ".ORDERS",
              "--confluent-topic",
              oldTopic,
              "--db-dsn",
              db.tablesDsn(),
              "--db-user",
              db.captureUser(),
              "--db-password-env",
              "ORACLE_PASSWORD",
              "--bootstrap-servers",
              cluster.bootstrapServers(),
              "--connect-url",
              cluster.restUrl(),
              "--connector",
              NEW,
              "--check-scn",
              Long.toString(checkScn),
              "--wait-seconds",
              "300",
              "--output",
              evidence.toString(),
              "--report",
              dir.resolve("verification.md").toString(),
              "--json");
      ev.param("verifyExit", verify.exit());
      JsonNode doc = JSON.readTree(evidence.toFile());
      JsonNode table = doc.path("tables").get(0);
      ev.param("verification", table.toString());
      assertThat(verify.exit()).as(verify.toString()).isZero();
      assertThat(table.path("status").asText()).as(table.toPrettyString()).isEqualTo("PASS");
      assertThat(table.path("records").path("confluent_key_only").asLong())
          .as("rows only Confluent delivered, verified by key")
          .isPositive();
    } catch (Throwable t) {
      ev.failed(t);
      throw t;
    } finally {
      if (workload != null) {
        workload.stop();
      }
      ev.write();
      db.dropSchema(schema);
    }
  }

  private Map<String, String> confluentConfig(String schema) {
    Map<String, String> p = db.connectorDatabaseProps();
    String pdb = p.get("cdc.database.pdbs");
    String service = p.get("cdc.database.service");
    String first = pdb == null || pdb.isBlank() ? service : pdb;
    Map<String, String> c = new LinkedHashMap<>();
    c.put("connector.class", CONFLUENT_CLASS);
    c.put("tasks.max", "1");
    c.put("oracle.server", p.get("cdc.database.host"));
    c.put("oracle.port", p.get("cdc.database.port"));
    c.put("oracle.sid", service);
    if (pdb != null && !pdb.isBlank()) {
      c.put("oracle.pdb.name", pdb);
    }
    c.put("oracle.username", p.get("cdc.database.user"));
    c.put("oracle.password", p.get("cdc.database.password"));
    c.put("table.inclusion.regex", first + "[.]" + schema + "[.]ORDERS");
    c.put("start.from", "snapshot");
    c.put("redo.log.consumer.bootstrap.servers", "kafka:19092");
    c.put("confluent.topic.bootstrap.servers", "kafka:19092");
    c.put("confluent.topic.replication.factor", "1");
    c.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
    c.put("key.converter.schemas.enable", "false");
    c.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
    c.put("value.converter.schemas.enable", "false");
    // numeric.mapping=none (Confluent's default) makes NUMBER a Connect Decimal, which the JSON
    // converter writes as base64 bytes unless told otherwise
    c.put("key.converter.decimal.format", "NUMERIC");
    c.put("value.converter.decimal.format", "NUMERIC");
    c.put("topic.creation.default.replication.factor", "1");
    c.put("topic.creation.default.partitions", "1");
    return c;
  }

  /**
   * The row id of a record from either connector: Confluent's flat row or this connector's after.
   */
  static int idOf(ConsumerRecord<String, String> r, boolean oso) throws Exception {
    if (r.value() == null) {
      return -1;
    }
    JsonNode v = ConnectCluster.json(r.value());
    JsonNode row = oso ? v.path("after") : v;
    JsonNode id = row.path("ID");
    return id.isNumber() ? id.asInt() : id.isTextual() ? parseOr(id.asText(), -1) : -1;
  }

  static int parseOr(String s, int dflt) {
    try {
      return new java.math.BigDecimal(s).intValueExact();
    } catch (RuntimeException e) {
      return dflt;
    }
  }

  static Set<Integer> readIds(ConnectCluster cluster, String topic, boolean oso, Duration idle)
      throws Exception {
    Set<Integer> ids = new TreeSet<>();
    try (KafkaConsumer<String, String> c =
        cluster.consumer("read-" + topic + "-" + System.nanoTime(), topic)) {
      for (ConsumerRecord<String, String> r :
          ConnectCluster.consume(c, 1, Duration.ofMinutes(3), idle)) {
        int id = idOf(r, oso);
        if (id > 0) {
          ids.add(id);
        }
      }
    }
    return ids;
  }

  static Set<Integer> awaitIds(
      ConnectCluster cluster, String topic, Set<Integer> wanted, Duration timeout, boolean oso)
      throws Exception {
    Set<Integer> seen = new TreeSet<>();
    try (KafkaConsumer<String, String> c =
        cluster.consumer("await-" + topic + "-" + System.nanoTime(), topic)) {
      long deadline = System.currentTimeMillis() + timeout.toMillis();
      while (!seen.containsAll(wanted) && System.currentTimeMillis() < deadline) {
        for (ConsumerRecord<String, String> r : c.poll(Duration.ofSeconds(1))) {
          int id = idOf(r, oso);
          if (id > 0) {
            seen.add(id);
          }
        }
      }
    }
    assertThat(seen).as("rows in " + topic).containsAll(wanted);
    return seen;
  }

  static String sample(ConnectCluster cluster, String topic) throws Exception {
    try (KafkaConsumer<String, String> c = cluster.consumer("sample-" + System.nanoTime(), topic)) {
      for (ConsumerRecord<String, String> r :
          ConnectCluster.consume(c, 1, Duration.ofMinutes(1), Duration.ofSeconds(2))) {
        if (r.value() != null) {
          return "key=" + r.key() + " value=" + r.value();
        }
      }
    }
    return "";
  }

  static String awaitTopic(ConnectCluster cluster, String schema, Duration timeout, boolean oso)
      throws InterruptedException {
    String suffix = ("." + schema + ".ORDERS").toUpperCase(java.util.Locale.ROOT);
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    while (System.currentTimeMillis() < deadline) {
      for (String t : cluster.topics()) {
        String u = t.toUpperCase(java.util.Locale.ROOT);
        boolean ours = t.startsWith(PREFIX + ".");
        if (u.endsWith(suffix) && ours == oso) {
          return t;
        }
      }
      Thread.sleep(2000);
    }
    throw new AssertionError("no " + (oso ? "OSO" : "Confluent") + " topic for " + schema);
  }

  static Set<Integer> idsBetween(int from, int to) {
    Set<Integer> s = new HashSet<>();
    for (int i = from; i <= to; i++) {
      s.add(i);
    }
    return s;
  }

  static void exec(Connection c, String... sql) throws SQLException {
    try (Statement s = c.createStatement()) {
      for (String q : sql) {
        s.execute(q);
      }
    }
  }

  /** Single-row committed inserts from id 1000 up, one every 200 ms, until stopped. */
  static final class Workload {
    static final int FIRST_ID = 1000;
    private final TestDatabase db;
    private final String schema;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Set<Integer> committed = java.util.Collections.synchronizedSet(new TreeSet<>());
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final Thread thread = new Thread(this::run, "confluent-cutover-workload");

    Workload(TestDatabase db, String schema) {
      this.db = db;
      this.schema = schema;
    }

    void start() {
      thread.start();
    }

    private void run() {
      try (Connection c = db.workload(schema);
          Statement s = c.createStatement()) {
        int id = FIRST_ID;
        while (running.get()) {
          s.execute("INSERT INTO orders VALUES (" + id + ", 'live " + id + "', " + id + ")");
          committed.add(id);
          id++;
          Thread.sleep(200);
        }
      } catch (Throwable t) {
        failure.set(t);
      }
    }

    void stop() throws InterruptedException {
      running.set(false);
      thread.join(30_000);
      if (failure.get() != null) {
        throw new IllegalStateException("workload failed", failure.get());
      }
    }

    Set<Integer> committed() {
      synchronized (committed) {
        return new TreeSet<>(committed);
      }
    }
  }
}
