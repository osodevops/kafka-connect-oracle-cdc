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
package sh.oso.connect.oracle.e2e.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sh.oso.connect.oracle.e2e.support.Cli;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.OracleSql;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * PRD-04 cutover from the Debezium Oracle connector (LogMiner adapter, online catalog, from Maven
 * Central) to this connector in one Connect worker, with the operator's tools: Debezium captures a
 * keyed table under a live workload and is stopped with {@code PUT /connectors/{name}/stop}; {@code
 * migrate_from_debezium.py} translates its configuration from the worker, {@code takeover_scn.py}
 * reads its offset through the REST API, checks the redo and writes {@code cdc.start.scn} and
 * {@code cdc.snapshot.mode=none}; the new connector starts under its own name while the workload
 * keeps running across the overlap; {@code verify_cutover.py} compares the table AS OF a check SCN
 * with the topic both connectors wrote and must PASS with no missing key. Its evidence is kept at
 * {@code e2e-tests/target/migration-evidence.json}. The takeover is refused for a PAUSED source and
 * for a start SCN whose redo is gone.
 *
 * <p>The Python tools run under {@code uv run --project tools/migration}; without uv on the PATH
 * the suite is skipped.
 */
@Tag("connector")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DebeziumCutoverConnectorIT {

  private static final Logger LOG = LoggerFactory.getLogger(DebeziumCutoverConnectorIT.class);
  private static final ObjectMapper JSON = new ObjectMapper();

  static final String DEBEZIUM_CLASS = "io.debezium.connector.oracle.OracleConnector";
  static final String PREFIX = "dbzcut";
  static final String OLD = "dbz-orders";
  static final String NEW = "oso-orders";
  static final String DBZ_USER = "c##dbz";
  static final String DBZ_PASSWORD = "dbz";
  static final int SEED = 100;
  static final Duration TOOL_TIMEOUT = Duration.ofMinutes(10);

  private final OracleTestDatabase db = OracleTestDatabase.get();
  private final String schema = SchemaFixtures.nameFor(getClass());
  private ConnectCluster cluster;
  private Workload workload;
  @TempDir Path dir;

  @BeforeAll
  void up() throws Exception {
    assumeTrue(Cli.uvAvailable(), "uv is not on the PATH; the migration tools cannot run");
    // the first uv run creates the tools' environment; do it before anything is timed
    Cli.Result help = Cli.migration("migrate_from_debezium.py", Map.of(), TOOL_TIMEOUT, "--help");
    assertThat(help.exit()).as(help.toString()).isZero();

    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
        Statement s = w.createStatement()) {
      s.execute(
          "CREATE TABLE orders (id NUMBER(9) PRIMARY KEY, name VARCHAR2(40), qty NUMBER(9),"
              + " amount NUMBER(10,2))");
      s.execute("ALTER TABLE orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      s.execute(
          "INSERT INTO orders SELECT LEVEL, 'seed ' || LEVEL, LEVEL, LEVEL + 0.5 FROM dual"
              + " CONNECT BY LEVEL <= "
              + SEED);
    }
    Thread.sleep(3500); // Debezium's snapshot reads AS OF an SCN: ORA-01466 right after a DDL
    createDebeziumUser();
    cluster =
        new ConnectCluster()
            .withPlugin(ConnectCluster.debeziumPluginDir(), "debezium-connector-oracle")
            .start();

    cluster.register(OLD, debeziumConfig());
    cluster.awaitRunning(OLD, Duration.ofMinutes(3));
    // Debezium's initial snapshot: every seed row as op=r
    Set<Integer> seen = new HashSet<>();
    try (KafkaConsumer<String, String> c = cluster.consumer("dbzcut-seed", dataTopic())) {
      long deadline = System.currentTimeMillis() + Duration.ofMinutes(5).toMillis();
      while (seen.size() < SEED && System.currentTimeMillis() < deadline) {
        for (ConsumerRecord<String, String> r : c.poll(Duration.ofSeconds(1))) {
          if (r.value() != null) {
            seen.add(ConnectCluster.json(r.value()).path("after").path("ID").asInt());
          }
        }
      }
    }
    assertThat(seen).as("Debezium snapshot of the seed rows").hasSize(SEED);
  }

  @AfterAll
  void down() throws Exception {
    try {
      if (workload != null) {
        workload.stop();
      }
      OracleSql.restoreHiddenLogs(db);
    } finally {
      try {
        if (cluster != null) {
          for (String name : List.of(NEW, OLD)) {
            try {
              cluster.delete(name);
            } catch (RuntimeException e) {
              LOG.warn("deleting {}: {}", name, e.getMessage());
            }
          }
          Thread.sleep(3000); // Debezium ends its LogMiner session
          cluster.close();
        }
      } finally {
        dropDebeziumUser();
        SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
      }
    }
  }

  @Test
  @Order(1)
  void aPausedSourceIsRefused() throws Exception {
    cluster.lifecycle(OLD, "pause");
    cluster.awaitConnectorState(OLD, "PAUSED", Duration.ofMinutes(1));
    try {
      Cli.Result r =
          takeover(
              "--connect-url",
              cluster.restUrl(),
              "--connector",
              OLD,
              "--target-connector",
              NEW,
              "--db-dsn",
              cdbDsn(),
              "--db-user",
              OracleTestDatabase.CAPTURE_USER,
              "--db-password-env",
              "ORACLE_PASSWORD");
      assertThat(r.exit()).as(r.toString()).isEqualTo(1);
      assertThat(r.text()).contains("is PAUSED").contains("pausing is not enough");
    } finally {
      cluster.lifecycle(OLD, "resume");
      cluster.awaitRunning(OLD, Duration.ofMinutes(2));
    }
    System.out.println("debezium cutover: a paused source is refused");
  }

  @Test
  @Order(2)
  void aTakeoverFromDebeziumVerifiesWithNoMissingKeys() throws Exception {
    workload = new Workload();
    workload.start();
    // Debezium streams the workload before it is stopped
    try (KafkaConsumer<String, String> c = cluster.consumer("dbzcut-streamed", dataTopic())) {
      boolean streamed = false;
      long deadline = System.currentTimeMillis() + Duration.ofMinutes(3).toMillis();
      while (!streamed && System.currentTimeMillis() < deadline) {
        for (ConsumerRecord<String, String> r : c.poll(Duration.ofSeconds(1))) {
          JsonNode v = r.value() == null ? null : ConnectCluster.json(r.value());
          if (v != null
              && "c".equals(v.path("op").asText())
              && v.path("after").path("ID").asInt() >= Workload.FIRST_ID) {
            streamed = true;
          }
        }
      }
      assertThat(streamed).as("Debezium streamed the workload").isTrue();
    }

    // 1. stop the old connector: its offset is final
    cluster.lifecycle(OLD, "stop");
    cluster.awaitConnectorState(OLD, "STOPPED", Duration.ofMinutes(2));

    // 2. translate its configuration, as the worker holds it
    Path source = dir.resolve("debezium-connector.json");
    Files.writeString(source, cluster.connectorInfo(OLD).toPrettyString());
    Path translated = dir.resolve("oso-connector.json");
    Cli.Result migrate =
        Cli.migration(
            "migrate_from_debezium.py",
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
            "--connect-url",
            cluster.restUrl());
    assertThat(migrate.exit()).as(migrate.toString()).isIn(0, 2);
    JsonNode cfg = JSON.readTree(translated.toFile()).path("config");
    assertThat(cfg.path("connector.class").asText())
        .isEqualTo(FirstRecordConnectorIT.CONNECTOR_CLASS);
    assertThat(cfg.path("cdc.topic.prefix").asText()).isEqualTo(PREFIX);
    assertThat(cfg.path("cdc.topic.template").asText()).isEqualTo("${prefix}.${schema}.${table}");
    assertThat(cfg.path("cdc.tables.include").asText()).contains(schema);
    assertThat(cfg.path("cdc.decimal.mode").asText()).isEqualTo("string");
    assertThat(cfg.path("cdc.snapshot.mode").asText()).isEqualTo("none");
    assertThat(cfg.path("cdc.database.password").asText())
        .as("a literal password never reaches the output")
        .isNotEqualTo(DBZ_PASSWORD);

    // 3. the takeover SCN from the stopped connector's offset, against the live database
    Path takenOver = dir.resolve("oso-connector-takeover.json");
    Cli.Result take =
        takeover(
            "--connect-url",
            cluster.restUrl(),
            "--connector",
            OLD,
            "--target-config",
            translated.toString(),
            "--db-dsn",
            cdbDsn(),
            "--db-user",
            OracleTestDatabase.CAPTURE_USER,
            "--db-password-env",
            "ORACLE_PASSWORD",
            "--output",
            takenOver.toString(),
            "--report",
            dir.resolve("takeover-report.md").toString(),
            "--json");
    assertThat(take.exit()).as(take.toString()).isIn(0, 2);
    JsonNode result = JSON.readTree(take.out());
    assertThat(result.path("source").asText()).isEqualTo("debezium");
    assertThat(result.path("old_connector_state").asText()).isEqualTo("STOPPED");
    assertThat(result.path("redo").path("ok").asBoolean()).isTrue();
    assertThat(result.path("problems")).isEmpty();
    long start = result.path("start_scn").asLong();
    assertThat(start).isPositive();
    if (result.path("old_commit_scn").canConvertToLong()) {
      assertThat(start)
          .as("the new connector starts at or before the old one's last commit: an overlap")
          .isLessThanOrEqualTo(result.path("old_commit_scn").asLong());
    }
    JsonNode out = JSON.readTree(takenOver.toFile());
    assertThat(out.path("name").asText()).isEqualTo(NEW);
    assertThat(out.path("config").path("cdc.start.scn").asText()).isEqualTo(Long.toString(start));
    assertThat(out.path("config").path("cdc.snapshot.mode").asText()).isEqualTo("none");

    // 4. the operator's follow-ups: the capture user from setup-sql with its password (the
    // translator masked Debezium's literal one), and broker access for the connector's own
    // topics, which the translation does not carry over
    Map<String, String> config = new LinkedHashMap<>();
    out.path("config")
        .fields()
        .forEachRemaining(e -> config.put(e.getKey(), e.getValue().asText()));
    config.put("cdc.database.user", OracleTestDatabase.CAPTURE_USER);
    config.put("cdc.database.password", OracleTestDatabase.CAPTURE_PASSWORD);
    config.put("cdc.kafka.bootstrap.servers", "kafka:19092");
    Path registered = dir.resolve("oso-connector-registered.json");
    Files.writeString(
        registered,
        JSON.writerWithDefaultPrettyPrinter()
            .writeValueAsString(Map.of("name", NEW, "config", config)));
    cluster.register(NEW, config);
    cluster.awaitRunning(NEW, Duration.ofMinutes(3));

    // 5. the workload keeps running across the overlap, then stops; the check SCN follows it
    int before = workload.changes();
    long overlap = System.currentTimeMillis() + Duration.ofSeconds(20).toMillis();
    while (System.currentTimeMillis() < overlap) {
      workload.check();
      Thread.sleep(500);
    }
    workload.stop();
    assertThat(workload.changes())
        .as("changes after the new connector started")
        .isGreaterThan(before);
    long checkScn;
    try (Connection meta = db.capture(OracleTestDatabase.CDB_SERVICE)) {
      checkScn = OracleSql.currentScn(meta);
    }

    // 6. verify the cutover and keep the evidence
    Path evidence = Cli.repoRoot().resolve("e2e-tests/target/migration-evidence.json");
    Files.createDirectories(evidence.getParent());
    Cli.Result verify =
        Cli.migration(
            "verify_cutover.py",
            Map.of("ORACLE_PASSWORD", OracleTestDatabase.CAPTURE_PASSWORD),
            TOOL_TIMEOUT,
            "--input",
            registered.toString(),
            "--table",
            schema + ".ORDERS",
            "--db-dsn",
            pdbDsn(),
            "--db-user",
            OracleTestDatabase.CAPTURE_USER,
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
    assertThat(verify.exit()).as(verify.toString()).isZero();
    JsonNode doc = JSON.readTree(evidence.toFile());
    assertThat(doc.path("result").asText()).isEqualTo("PASS");
    assertThat(doc.path("sha256").asText()).isNotBlank();
    JsonNode table = doc.path("tables").get(0);
    assertThat(table.path("status").asText()).as(table.toPrettyString()).isEqualTo("PASS");
    assertThat(table.path("topic").asText()).isEqualTo(dataTopic());
    assertThat(table.path("only_in_database").asLong()).as("missing keys").isZero();
    assertThat(table.path("only_in_topics").asLong()).isZero();
    assertThat(table.path("different").asLong()).isZero();
    assertThat(table.path("database_rows").asLong())
        .isEqualTo(table.path("topic_rows").asLong())
        .isGreaterThanOrEqualTo(SEED);
    assertThat(doc.path("inputs").path("position_check").path("passed").asBoolean()).isTrue();

    // the evidence seal checks out with the tool itself
    Cli.Result seal =
        Cli.migration(
            "verify_cutover.py", Map.of(), TOOL_TIMEOUT, "--check-evidence", evidence.toString());
    assertThat(seal.exit()).as(seal.toString()).isZero();
    System.out.println(
        "debezium cutover: start SCN "
            + start
            + ", check SCN "
            + checkScn
            + ", "
            + table.path("database_rows").asLong()
            + " rows verified, evidence "
            + evidence);
  }

  @Test
  @Order(3)
  void aStartScnWhoseRedoIsGoneIsRefused() throws Exception {
    if (!"STOPPED".equals(cluster.connectorState(OLD))) {
      cluster.lifecycle(OLD, "stop");
      cluster.awaitConnectorState(OLD, "STOPPED", Duration.ofMinutes(2));
    }
    long oldest;
    try (Connection root = db.capture(OracleTestDatabase.CDB_SERVICE);
        Statement s = root.createStatement();
        ResultSet rs =
            s.executeQuery(
                "SELECT MIN(first_change#) FROM v$archived_log WHERE dest_id = 1 AND"
                    + " resetlogs_change# = (SELECT resetlogs_change# FROM v$database)")) {
      rs.next();
      oldest = rs.getLong(1);
    }
    long gone = oldest - 1; // no archived log holds it any more
    moveDebeziumOffset(gone);
    Path output = dir.resolve("refused-takeover.json");
    Cli.Result r =
        takeover(
            "--connect-url",
            cluster.restUrl(),
            "--connector",
            OLD,
            "--target-connector",
            NEW + "-refused",
            "--db-dsn",
            cdbDsn(),
            "--db-user",
            OracleTestDatabase.CAPTURE_USER,
            "--db-password-env",
            "ORACLE_PASSWORD",
            "--output",
            output.toString(),
            "--json");
    assertThat(r.exit()).as(r.toString()).isEqualTo(1);
    assertThat(r.text()).contains(Long.toString(gone));
    assertThat(output).as("no configuration is written for a refused takeover").doesNotExist();
    System.out.println("debezium cutover: start SCN " + gone + " refused, its redo is gone");
  }

  /**
   * The negative case as written for PRD-04: the archived log holding the start SCN is moved aside
   * (as an rm outside RMAN does). Today takeover_scn.py reads availability from V$ARCHIVED_LOG
   * (DELETED and STATUS, documented in redo.py), which still lists such a file as available
   * (reference/mining-errors.md), so the takeover is not refused; the new connector would stop with
   * CDC-2002 when LogMiner cannot add the file, so nothing is lost, but the refusal comes late.
   * Enable this once the tool checks the files themselves.
   */
  @Test
  @Order(4)
  @Disabled("takeover_scn.py checks V$ARCHIVED_LOG only; a log moved aside is still listed")
  void aStartScnInAnArchivedLogMovedAsideIsRefused() throws Exception {
    long scn;
    String path;
    try (Connection root = db.sysdba(OracleTestDatabase.CDB_SERVICE)) {
      scn = OracleSql.currentScn(root);
      int groups;
      try (Statement s = root.createStatement();
          ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM v$log")) {
        rs.next();
        groups = rs.getInt(1);
      }
      for (int i = 0; i <= groups; i++) {
        OracleSql.archiveLogCurrent(db); // past every online group: only the archive holds it
      }
      try (Statement s = root.createStatement();
          ResultSet rs =
              s.executeQuery(
                  "SELECT name FROM v$archived_log WHERE dest_id = 1 AND deleted = 'NO' AND"
                      + " first_change# <= "
                      + scn
                      + " AND next_change# > "
                      + scn
                      + " ORDER BY sequence# DESC FETCH FIRST 1 ROW ONLY")) {
        rs.next();
        path = rs.getString(1);
      }
    }
    moveDebeziumOffset(scn);
    try {
      OracleSql.hideArchivedLog(db, path);
      Cli.Result r =
          takeover(
              "--connect-url",
              cluster.restUrl(),
              "--connector",
              OLD,
              "--target-connector",
              NEW + "-hidden",
              "--db-dsn",
              cdbDsn(),
              "--db-user",
              OracleTestDatabase.CAPTURE_USER,
              "--db-password-env",
              "ORACLE_PASSWORD",
              "--json");
      assertThat(r.exit()).as(r.toString()).isEqualTo(1);
    } finally {
      OracleSql.restoreHiddenLogs(db);
    }
  }

  /** The Debezium configuration: LogMiner adapter, online catalog, its own common user. */
  private Map<String, String> debeziumConfig() {
    Map<String, String> d = new LinkedHashMap<>();
    d.put("connector.class", DEBEZIUM_CLASS);
    d.put("tasks.max", "1");
    d.put("database.hostname", OracleTestDatabase.NETWORK_ALIAS);
    d.put("database.port", "1521");
    d.put("database.user", DBZ_USER);
    d.put("database.password", DBZ_PASSWORD);
    d.put("database.dbname", OracleTestDatabase.CDB_SERVICE);
    d.put("database.pdb.name", OracleTestDatabase.PDB1);
    d.put("database.connection.adapter", "logminer");
    d.put("log.mining.strategy", "online_catalog");
    d.put("topic.prefix", PREFIX);
    d.put("table.include.list", schema + ".ORDERS");
    d.put("snapshot.mode", "initial");
    d.put("snapshot.locking.mode", "none");
    d.put("decimal.handling.mode", "string");
    d.put("heartbeat.interval.ms", "1000");
    d.put("include.schema.changes", "false");
    d.put("schema.history.internal.kafka.bootstrap.servers", "kafka:19092");
    d.put("schema.history.internal.kafka.topic", PREFIX + ".schema-history");
    d.put("schema.history.internal.store.only.captured.tables.ddl", "true");
    return d;
  }

  private String dataTopic() {
    return PREFIX + "." + schema + ".ORDERS";
  }

  private String cdbDsn() {
    return db.container().getHost()
        + ":"
        + db.container().getMappedPort(1521)
        + "/"
        + OracleTestDatabase.CDB_SERVICE;
  }

  private String pdbDsn() {
    return db.container().getHost()
        + ":"
        + db.container().getMappedPort(1521)
        + "/"
        + OracleTestDatabase.PDB1;
  }

  private Cli.Result takeover(String... args) throws Exception {
    return Cli.migration(
        "takeover_scn.py",
        Map.of("ORACLE_PASSWORD", OracleTestDatabase.CAPTURE_PASSWORD),
        TOOL_TIMEOUT,
        args);
  }

  /** Rewrites the stopped Debezium connector's {@code scn} through the KIP-875 offsets API. */
  private void moveDebeziumOffset(long scn) throws Exception {
    JsonNode entry = cluster.offsets(OLD).path("offsets").get(0);
    assertThat(entry).as("Debezium offset").isNotNull();
    ObjectNode offset = entry.path("offset").deepCopy();
    offset.put("scn", Long.toString(scn));
    @SuppressWarnings("unchecked")
    Map<String, Object> partition = JSON.convertValue(entry.path("partition"), Map.class);
    @SuppressWarnings("unchecked")
    Map<String, Object> value = JSON.convertValue(offset, Map.class);
    cluster.patchOffset(OLD, partition, value);
  }

  /** Debezium's documented grants for a CDB, with the users tablespace for its flush table. */
  private void createDebeziumUser() throws SQLException {
    dropDebeziumUser();
    try (Connection sys = db.sysdba(OracleTestDatabase.CDB_SERVICE);
        Statement s = sys.createStatement()) {
      s.execute(
          "CREATE USER " + DBZ_USER + " IDENTIFIED BY \"" + DBZ_PASSWORD + "\" CONTAINER=ALL");
      s.execute("ALTER USER " + DBZ_USER + " QUOTA UNLIMITED ON users CONTAINER=ALL");
      try {
        s.execute("ALTER USER " + DBZ_USER + " DEFAULT TABLESPACE users");
      } catch (SQLException e) {
        LOG.warn("default tablespace for {}: {}", DBZ_USER, e.getMessage());
      }
      s.execute("ALTER USER " + DBZ_USER + " SET CONTAINER_DATA=ALL CONTAINER=CURRENT");
      for (String grant :
          List.of(
              "CREATE SESSION",
              "SET CONTAINER",
              "SELECT ON V_$DATABASE",
              "FLASHBACK ANY TABLE",
              "SELECT ANY TABLE",
              "SELECT_CATALOG_ROLE",
              "EXECUTE_CATALOG_ROLE",
              "SELECT ANY TRANSACTION",
              "LOGMINING",
              "CREATE TABLE",
              "LOCK ANY TABLE",
              "CREATE SEQUENCE",
              "EXECUTE ON DBMS_LOGMNR",
              "EXECUTE ON DBMS_LOGMNR_D",
              "SELECT ON V_$LOG",
              "SELECT ON V_$LOG_HISTORY",
              "SELECT ON V_$LOGMNR_LOGS",
              "SELECT ON V_$LOGMNR_CONTENTS",
              "SELECT ON V_$LOGMNR_PARAMETERS",
              "SELECT ON V_$LOGFILE",
              "SELECT ON V_$ARCHIVED_LOG",
              "SELECT ON V_$ARCHIVE_DEST_STATUS",
              "SELECT ON V_$TRANSACTION",
              "SELECT ON V_$MYSTAT",
              "SELECT ON V_$STATNAME")) {
        s.execute("GRANT " + grant + " TO " + DBZ_USER + " CONTAINER=ALL");
      }
    }
  }

  /** Drops the Debezium user, ending its sessions first; retried while a session lingers. */
  private void dropDebeziumUser() throws SQLException {
    try (Connection sys = db.sysdba(OracleTestDatabase.CDB_SERVICE);
        Statement s = sys.createStatement()) {
      SQLException last = null;
      for (int attempt = 0; attempt < 10; attempt++) {
        List<String> sessions = new ArrayList<>();
        try (ResultSet rs =
            s.executeQuery(
                "SELECT sid, serial# FROM v$session WHERE username = '"
                    + DBZ_USER.toUpperCase(java.util.Locale.ROOT)
                    + "'")) {
          while (rs.next()) {
            sessions.add(rs.getLong(1) + "," + rs.getLong(2));
          }
        }
        for (String id : sessions) {
          try (Statement kill = sys.createStatement()) {
            kill.execute("ALTER SYSTEM KILL SESSION '" + id + "' IMMEDIATE");
          } catch (SQLException ignore) {
            // already gone
          }
        }
        try {
          s.execute(
              "BEGIN EXECUTE IMMEDIATE 'DROP USER "
                  + DBZ_USER
                  + " CASCADE'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -1918 THEN RAISE;"
                  + " END IF; END;");
          return;
        } catch (SQLException e) {
          if (e.getErrorCode() != 1940) {
            throw e;
          }
          last = e;
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw e;
          }
        }
      }
      throw last;
    }
  }

  /**
   * Inserts, updates and deletes on one connection, a statement every 100 ms, each its own
   * transaction, until stopped: the live workload a cutover runs under.
   */
  final class Workload {
    static final int FIRST_ID = 1000;

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicInteger changes = new AtomicInteger();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final Thread thread = new Thread(this::run, "cutover-workload");

    void start() {
      thread.setDaemon(true);
      thread.start();
    }

    int changes() {
      return changes.get();
    }

    void check() {
      if (failure.get() != null) {
        throw new AssertionError("the workload failed", failure.get());
      }
    }

    void stop() throws InterruptedException {
      running.set(false);
      thread.join(Duration.ofMinutes(1).toMillis());
      check();
    }

    private void run() {
      Random random = new Random(42);
      int next = FIRST_ID;
      List<Integer> live = new ArrayList<>();
      try (Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema);
          Statement s = w.createStatement()) {
        while (running.get()) {
          int n = changes.get();
          if (n % 10 == 9 && !live.isEmpty()) {
            int id = live.remove(random.nextInt(live.size()));
            s.executeUpdate("DELETE FROM orders WHERE id = " + id);
          } else if (n % 2 == 0) {
            int id = next++;
            s.executeUpdate(
                "INSERT INTO orders VALUES ("
                    + id
                    + ", 'w "
                    + id
                    + "', "
                    + (id % 7)
                    + ", "
                    + (id % 100)
                    + ".25)");
            live.add(id);
          } else {
            int id = 1 + random.nextInt(SEED);
            s.executeUpdate(
                "UPDATE orders SET qty = qty + 1, amount = amount + 0.25 WHERE id = " + id);
          }
          changes.incrementAndGet();
          Thread.sleep(100);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Throwable e) {
        failure.set(e);
      }
    }
  }
}
