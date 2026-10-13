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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.e2e.support.Cli;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.Evidence;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;
import sh.oso.connect.oracle.e2e.support.TestDatabase;

/**
 * ADR-0028 reference spike: runs Confluent's Oracle CDC Source connector, once per configuration
 * profile, against the same tables and the same script of changes, and records every record it
 * writes, so the Confluent-compatible format can be specified where Confluent's documentation is
 * silent. Confluent's connector is used under its evaluation licence in a disposable lab container
 * and only its external output is observed; it is never downloaded here (the suite runs only when
 * {@code -De2e.confluent.plugin} names a local copy) and never in CI.
 *
 * <p>With {@code -De2e.confluent.reference.update=true} the normalised records are written to
 * {@code kafka-connect-oracle-cdc/src/test/resources/confluent-reference/<profile>/}, one JSON line
 * per record, and the Avro schemas Schema Registry received to {@code subjects/}. Volatile values
 * (SCNs, timestamps, ROWIDs, the generated schema name) become placeholders that keep their
 * relations, so the fixtures hold only this suite's deterministic test data. {@code
 * -De2e.confluent.reference.profiles=a,b} runs a subset.
 */
@Tag("qual")
class ConfluentFormatRefQualIT {

  static final String CONFLUENT_CLASS = "io.confluent.connect.oracle.cdc.OracleCdcSourceConnector";
  static final String WORKER_IMAGE = "confluentinc/cp-server-connect:7.9.2";
  static final ObjectMapper JSON = new ObjectMapper();
  static final HttpClient HTTP = HttpClient.newHttpClient();
  static final Duration TOOL_IDLE = Duration.ofSeconds(25);
  static final List<String> TABLES =
      List.of("TYPES", "EXOTIC", "COMPOSITE", "NOPK", "LOBS", "NUMS", "LOBS2");

  /** One run of Confluent's connector: a name, its extra properties and the converter. */
  record Profile(String name, Map<String, String> props, boolean avro) {}

  static List<Profile> profiles() {
    List<Profile> p = new ArrayList<>();
    p.add(new Profile("default", Map.of(), false));
    p.add(new Profile("default-avro", Map.of(), true));
    p.add(new Profile("before-state-avro", Map.of("output.before.state.field", "before"), true));
    p.add(
        new Profile(
            "all-fields",
            Map.of(
                "output.commit.scn.field", "cscn",
                "output.redo.field", "redo",
                "output.undo.field", "undo",
                "output.before.state.field", "before"),
            false));
    Map<String, String> renamed = new LinkedHashMap<>();
    renamed.put("output.table.name.field", "t_name");
    renamed.put("output.scn.field", "header:x_scn");
    renamed.put("output.commit.scn.field", "x_cscn");
    renamed.put("output.op.type.field", "x_op");
    renamed.put("output.op.ts.field", "x_meta.op_ts");
    renamed.put("output.current.ts.field", "x_meta.current_ts");
    renamed.put("output.row.id.field", "header:x_rowid");
    renamed.put("output.username.field", "x_user");
    renamed.put("output.redo.field", "x_redo");
    renamed.put("output.undo.field", "x_undo");
    renamed.put("output.before.state.field", "x_before");
    renamed.put("output.op.type.read.value", "SNAP");
    renamed.put("output.op.type.insert.value", "INS");
    renamed.put("output.op.type.update.value", "UPD");
    renamed.put("output.op.type.delete.value", "DEL");
    renamed.put("output.op.type.truncate.value", "TRN");
    p.add(new Profile("renamed", renamed, false));
    p.add(new Profile("key-struct", Map.of("key.template", "${primaryKeyStruct}"), false));
    p.add(new Profile("key-concat", Map.of("key.template", "${primaryKey}"), false));
    for (String m :
        List.of(
            "best_fit_or_decimal", "best_fit_or_double", "best_fit_or_string", "precision_only")) {
      p.add(new Profile("numeric-" + m.replace('_', '-'), Map.of("numeric.mapping", m), false));
    }
    p.add(
        new Profile(
            "numeric-none-scale2",
            Map.of("numeric.default.scale", "2", "log.sensitive.data", "true"),
            false));
    p.add(new Profile("date-timestamp", Map.of("oracle.date.mapping", "timestamp"), false));
    p.add(new Profile("tombstones", Map.of("emit.tombstone.on.delete", "true"), false));
    p.add(
        new Profile(
            "lob-topics",
            Map.of(
                "lob.topic.name.template",
                "${connectorName}.lob.${fullyQualifiedTableName}.${columnName}",
                "enable.large.lob.object.support",
                "true"),
            false));
    return p;
  }

  private final TestDatabase db = TestDatabase.get();

  @Test
  void recordsWhatConfluentsConnectorWritesForEveryProfile() throws Exception {
    assumeTrue(
        !"true".equalsIgnoreCase(System.getenv("CI"))
            && !"true".equalsIgnoreCase(System.getenv("GITHUB_ACTIONS")),
        "Confluent's connector must never run in CI");
    String plugin = System.getProperty("e2e.confluent.plugin", "");
    assumeTrue(
        !plugin.isBlank() && Files.isDirectory(Path.of(plugin)),
        "-De2e.confluent.plugin names no local copy of Confluent's connector");
    boolean update = Boolean.getBoolean("e2e.confluent.reference.update");
    Set<String> only =
        Set.of(System.getProperty("e2e.confluent.reference.profiles", "").split(","));
    Evidence ev =
        Evidence.of(getClass(), "Confluent record format reference").param("target", db.describe());
    try (ConnectCluster cluster =
        ConnectCluster.confluentPlatform(WORKER_IMAGE)
            .withPlugin(Path.of(plugin), "confluent-oracle-cdc")
            .withConfluentSchemaRegistry()
            .start()) {
      assumeTrue(
          cluster.workerReaches(db.workerHost(), db.workerPort()),
          "the Connect worker has no route to the database");
      for (Profile p : profiles()) {
        if (only.size() > 1 || !only.contains("")) {
          if (!only.contains(p.name())) {
            continue;
          }
        }
        Map<String, Integer> counts = runProfile(cluster, p, update);
        ev.param("profile." + p.name(), counts.toString());
        assertThat(counts.values().stream().mapToInt(Integer::intValue).sum())
            .as("records written by Confluent's connector for profile " + p.name())
            .isPositive();
      }
    } catch (Throwable t) {
      ev.failed(t);
      throw t;
    } finally {
      ev.write();
    }
  }

  private Map<String, Integer> runProfile(ConnectCluster cluster, Profile p, boolean update)
      throws Exception {
    String schema = SchemaFixtures.nameFor(getClass()) + "_" + profiles().indexOf(p);
    String connector = "ref-" + p.name();
    db.recreateSchema(schema);
    try (Connection w = db.workload(schema)) {
      createAndSeed(w);
    }
    // Confluent's default dictionary mode reads the dictionary from the redo once it meets DDL
    // (the script alters a table): without a build in the archived logs it stops with ORA-01371
    try (Connection c = db.capture();
        Statement st = c.createStatement()) {
      st.execute("BEGIN DBMS_LOGMNR_D.BUILD(OPTIONS => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS); END;");
    }
    db.archiveLogCurrent();
    Thread.sleep(3500); // a snapshot AS OF an SCN right after DDL meets ORA-01466
    cluster.register(connector, config(cluster, p, schema, connector));
    cluster.awaitRunning(connector, Duration.ofMinutes(5));
    String prefix = connector + ".";
    awaitTopic(cluster, prefix, "TYPES", Duration.ofMinutes(5));
    Thread.sleep(10_000); // the snapshot finishes before the script starts
    cluster.awaitRunning(connector, Duration.ofSeconds(10));
    boolean scale2 = p.name().equals("numeric-none-scale2");
    try (Connection w = db.workload(schema);
        Connection other = db.workload(schema)) {
      if (scale2) {
        // First prove that a streaming NUMBER fitting the configured scale works. Only then
        // introduce the value needing rounding; its failure is part of this reference.
        exec(w, numsRow(10, "0.12"));
        awaitJsonRow(cluster, prefix, "NUMS", "ID", 10, Duration.ofMinutes(3));
        exec(w, numsRow(11, "0.125"));
        JsonNode status = cluster.awaitTaskState(connector, "FAILED", Duration.ofMinutes(3));
        assertThat(status.path("tasks").get(0).path("trace").asText())
            .contains("Exception converting redo to change event");
        // Diagnostic output can contain test row values. Keep it local and out of the fixtures.
        Files.writeString(
            Path.of(System.getProperty("java.io.tmpdir"), "oso-cdc-confluent-scale2-status.json"),
            JSON.writerWithDefaultPrettyPrinter().writeValueAsString(status));
      } else {
        script(w, other);
        Thread.sleep(40_000); // Confluent mines the row changes before the DDL
        scriptAfterDdl(w);
      }
    }
    db.archiveLogCurrent();
    Map<String, List<ConsumerRecord<byte[], byte[]>>> byTopic =
        readUntilIdle(cluster, prefix, Duration.ofMinutes(6));
    if (!scale2) {
      cluster.awaitRunning(connector, Duration.ofSeconds(10));
      assertThat(
              byTopic.entrySet().stream()
                  .filter(e -> e.getKey().endsWith(".TYPES"))
                  .mapToInt(e -> e.getValue().size())
                  .sum())
          .as("snapshot and streaming records, including the DDL script")
          .isGreaterThan(3);
    }
    cluster.delete(connector);
    Map<String, Integer> counts = new TreeMap<>();
    Normaliser n = new Normaliser(schema, p, cluster.confluentSchemaRegistryUrl(), prefix);
    Path dir =
        Cli.repoRoot()
            .resolve("kafka-connect-oracle-cdc/src/test/resources/confluent-reference")
            .resolve(p.name());
    if (update) {
      Files.createDirectories(dir);
    }
    for (Map.Entry<String, List<ConsumerRecord<byte[], byte[]>>> e : byTopic.entrySet()) {
      String file = n.text(e.getKey().substring(prefix.length())) + ".jsonl";
      StringBuilder lines = new StringBuilder();
      for (ConsumerRecord<byte[], byte[]> r : e.getValue()) {
        lines.append(JSON.writeValueAsString(n.record(r, p.avro()))).append('\n');
      }
      counts.put(file, e.getValue().size());
      if (update) {
        Files.writeString(dir.resolve(file), lines.toString());
      }
    }
    if (p.avro() && update) {
      writeSubjects(cluster, prefix, dir.resolve("subjects"), n);
    }
    db.dropSchema(schema);
    return counts;
  }

  Map<String, String> config(ConnectCluster cluster, Profile p, String schema, String name) {
    Map<String, String> d = db.connectorDatabaseProps();
    String pdb = d.get("cdc.database.pdbs");
    String first = pdb == null || pdb.isBlank() ? d.get("cdc.database.service") : pdb;
    Map<String, String> c = new LinkedHashMap<>();
    c.put("connector.class", CONFLUENT_CLASS);
    c.put("tasks.max", "1");
    c.put("oracle.server", d.get("cdc.database.host"));
    c.put("oracle.port", d.get("cdc.database.port"));
    c.put("oracle.sid", d.get("cdc.database.service"));
    if (pdb != null && !pdb.isBlank()) {
      c.put("oracle.pdb.name", pdb);
    }
    c.put("oracle.username", d.get("cdc.database.user"));
    c.put("oracle.password", d.get("cdc.database.password"));
    c.put(
        "table.inclusion.regex", first + "[.]" + schema + "[.](" + String.join("|", TABLES) + ")");
    c.put("start.from", "snapshot");
    c.put("snapshot.threads.per.task", "1"); // one table at a time: deterministic order
    c.put(
        "log.sensitive.data",
        Boolean.toString(Boolean.getBoolean("e2e.confluent.reference.sensitive")));
    // the default (auto) switched to a dictionary from the redo at the script's DDL and stopped
    // with ORA-01371 even after a dictionary build; the record format is the same either way
    c.put("oracle.dictionary.mode", "online");
    c.put("table.topic.name.template", "${connectorName}.${fullyQualifiedTableName}");
    c.put("redo.log.consumer.bootstrap.servers", "kafka:19092");
    c.put("confluent.topic.bootstrap.servers", "kafka:19092");
    c.put("confluent.topic.replication.factor", "1");
    c.put("topic.creation.default.replication.factor", "1");
    c.put("topic.creation.default.partitions", "1");
    if (p.avro()) {
      for (String side : List.of("key", "value")) {
        c.put(side + ".converter", "io.confluent.connect.avro.AvroConverter");
        c.put(
            side + ".converter.schema.registry.url", ConnectCluster.CONFLUENT_SCHEMA_REGISTRY_URL);
      }
    } else {
      // the converter's own defaults otherwise (decimal.format BASE64): the fixtures are what a
      // consumer of a default JSON pipeline reads
      for (String side : List.of("key", "value")) {
        c.put(side + ".converter", "org.apache.kafka.connect.json.JsonConverter");
        c.put(side + ".converter.schemas.enable", "true");
      }
    }
    c.putAll(p.props());
    return c;
  }

  /** Tables and seed rows: the snapshot reads these as op R. Data is deterministic. */
  static void createAndSeed(Connection w) throws SQLException {
    exec(
        w,
        "CREATE TABLE types (id NUMBER(10) PRIMARY KEY, n_plain NUMBER, n_20_0 NUMBER(20,0),"
            + " n_10_2 NUMBER(10,2), n_5 NUMBER(5), f_float FLOAT(10), c_char CHAR(5),"
            + " c_vc VARCHAR2(40), c_nchar NCHAR(5), c_nvc NVARCHAR2(40), d_date DATE,"
            + " t_ts0 TIMESTAMP(0), t_ts3 TIMESTAMP(3), t_ts6 TIMESTAMP(6),"
            + " t_tz TIMESTAMP(6) WITH TIME ZONE, t_ltz TIMESTAMP(6) WITH LOCAL TIME ZONE,"
            + " r_raw RAW(16))",
        "CREATE TABLE exotic (id NUMBER(10) PRIMARY KEY, f_binf BINARY_FLOAT,"
            + " f_bind BINARY_DOUBLE, i_ym INTERVAL YEAR(2) TO MONTH,"
            + " i_ds INTERVAL DAY(2) TO SECOND(6), t_ts9 TIMESTAMP(9), n_neg NUMBER(5,-2))",
        "CREATE TABLE composite (a NUMBER(5), b VARCHAR2(10), v VARCHAR2(20), PRIMARY KEY (a, b))",
        "CREATE TABLE nopk (x NUMBER(5), y VARCHAR2(10))",
        "CREATE TABLE lobs (id NUMBER(10) PRIMARY KEY, v VARCHAR2(10), c CLOB, n NCLOB, b BLOB)",
        "CREATE TABLE lobs2 (a NUMBER(5), b VARCHAR2(5), c CLOB, PRIMARY KEY (a, b))",
        "CREATE TABLE nums (id NUMBER(10) PRIMARY KEY, n2 NUMBER(2), n3 NUMBER(3),"
            + " n4 NUMBER(4), n9 NUMBER(9), n18 NUMBER(18), n19 NUMBER(19),"
            + " n3m2 NUMBER(3,-2), n17m2 NUMBER(17,-2), n15_2 NUMBER(15,2),"
            + " n16_2 NUMBER(16,2), n38_10 NUMBER(38,10), n_round NUMBER,"
            + " nn VARCHAR2(10) NOT NULL)",
        "CREATE TABLE scratch (id NUMBER)");
    for (String t : TABLES) {
      exec(w, "ALTER TABLE " + t + " ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
    }
    exec(
        w,
        typesRow(1, "'one'"),
        typesRow(2, "NULL"),
        "INSERT INTO types (id) VALUES (3)",
        exoticRow(1),
        "INSERT INTO exotic (id) VALUES (2)",
        "INSERT INTO composite VALUES (0, 's', 'seed')",
        "INSERT INTO nopk VALUES (0, 'seed')",
        "INSERT INTO lobs VALUES (0, 'seed', 'small clob', N'small nclob', HEXTORAW('DEADBEEF'))",
        "INSERT INTO lobs2 VALUES (0, 's', 'composite seed')",
        numsRow(1, "0.125"),
        numsRow(2, "0.135"),
        numsRow(3, "-0.125"),
        // Confluent's snapshot reads table row counts from the statistics; without them its
        // 2.15.2 build failed the snapshot with a NullPointerException (observed 13 October 2026)
        "BEGIN DBMS_STATS.GATHER_SCHEMA_STATS(USER); END;");
  }

  static String numsRow(int id, String rounding) {
    return "INSERT INTO nums VALUES ("
        + id
        + ", 99, 999, 9999, 999999999, 999999999999999999, 9999999999999999999,"
        // Negative scale does not widen Confluent's integer mapping: 99900 in NUMBER(3,-2)
        // failed its snapshot with JDBC Numeric Overflow (getShort). Keep the reference input
        // within the mapped integer too, so the remaining schemas and streaming can be observed.
        + " 12300, 1234567890123456700, 9999999999999.99, 99999999999999.99,"
        + " 1234567890123456789012345678.1234567890, "
        + rounding
        + ", 'required')";
  }

  static String typesRow(int id, String vc) {
    return "INSERT INTO types VALUES ("
        + id
        + ", 12345.678901234567, 12345678901234567890, -1234.56, 42, 3.14159,"
        + " 'ab', "
        + vc
        + ", N'xy', N'nvc-"
        + id
        + "', TO_DATE('2024-03-01 10:11:12', 'YYYY-MM-DD HH24:MI:SS'),"
        + " TIMESTAMP '2024-03-01 10:11:12', TIMESTAMP '2024-03-01 10:11:12.123',"
        + " TIMESTAMP '2024-03-01 10:11:12.123456',"
        + " TIMESTAMP '2024-03-01 10:11:12.123456 +02:00',"
        + " TIMESTAMP '2024-03-01 10:11:12.123456 UTC', HEXTORAW('00FF10'))";
  }

  static String exoticRow(int id) {
    return "INSERT INTO exotic VALUES ("
        + id
        + ", 1.5, 2.25, INTERVAL '1-2' YEAR TO MONTH,"
        + " INTERVAL '3 04:05:06.789' DAY TO SECOND, TIMESTAMP '1969-12-31 23:59:59.123456789',"
        + " 12300)";
  }

  /** The changes every profile observes, each statement its own transaction unless stated. */
  static void script(Connection w, Connection other) throws SQLException {
    exec(
        w,
        typesRow(10, "'ten'"),
        "INSERT INTO composite VALUES (1, 'a', 'x')",
        "INSERT INTO nopk VALUES (1, 'a')",
        "INSERT INTO lobs VALUES (1, 'v1', 'clob one', N'nclob one', HEXTORAW('0102'))",
        exoticRow(10),
        numsRow(10, "0.12"),
        "INSERT INTO lobs2 VALUES (1, 'a', 'composite clob')",
        "UPDATE types SET c_vc = 'upd' WHERE id = 10",
        "UPDATE types SET n_10_2 = NULL WHERE id = 10");
    w.setAutoCommit(false);
    exec(w, typesRow(11, "'eleven'"));
    // Committing another connection advances the database SCN without committing this one.
    // The two captured statements now distinguish row SCNs from the transaction start SCN.
    exec(other, "INSERT INTO scratch VALUES (1)");
    exec(w, typesRow(12, "'twelve'"));
    exec(other, "INSERT INTO scratch VALUES (2)");
    exec(w, "UPDATE composite SET v = 'y' WHERE a = 1");
    w.commit();
    exec(w, typesRow(13, "'rolled back'"));
    w.rollback();
    w.setAutoCommit(true);
    exec(
        w,
        "DELETE FROM types WHERE id = 11",
        "UPDATE types SET id = 20 WHERE id = 12",
        "UPDATE composite SET b = 'b' WHERE a = 1",
        "UPDATE nopk SET y = 'b' WHERE x = 1",
        "DELETE FROM nopk WHERE x = 1",
        "UPDATE lobs SET c = RPAD('large clob ', 3000, 'x') WHERE id = 1",
        "UPDATE lobs SET v = 'v2' WHERE id = 1",
        "DELETE FROM lobs WHERE id = 1",
        "UPDATE lobs2 SET c = 'composite updated' WHERE a = 1",
        "UPDATE lobs2 SET b = 'b' WHERE a = 1",
        "DELETE FROM lobs2 WHERE a = 1");
  }

  static void awaitJsonRow(
      ConnectCluster cluster,
      String prefix,
      String table,
      String field,
      int value,
      Duration timeout)
      throws Exception {
    awaitTopic(cluster, prefix, table, timeout);
    String topic =
        cluster.topics().stream()
            .filter(t -> t.startsWith(prefix) && t.endsWith("." + table))
            .findFirst()
            .orElseThrow();
    try (KafkaConsumer<byte[], byte[]> c =
        cluster.byteConsumer("ref-await-" + System.nanoTime(), topic)) {
      long deadline = System.currentTimeMillis() + timeout.toMillis();
      while (System.currentTimeMillis() < deadline) {
        for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofSeconds(1))) {
          if (r.value() == null) {
            continue;
          }
          // The ID is Decimal(10,0), represented as base64 by JsonConverter.
          JsonNode payload = JSON.readTree(r.value()).path("payload");
          JsonNode id = payload.path(field);
          if (id.isTextual()
              && new java.math.BigInteger(java.util.Base64.getDecoder().decode(id.asText()))
                      .intValueExact()
                  == value) {
            return;
          }
        }
      }
    }
    throw new AssertionError("no streaming row " + table + "." + field + "=" + value);
  }

  /**
   * DDL and truncate, run once Confluent has mined everything before: rows written before a DDL but
   * mined after it decode against the new dictionary in online mode, which Confluent stops on
   * (observed 13 October 2026), so the two parts are kept apart.
   */
  static void scriptAfterDdl(Connection w) throws SQLException {
    exec(
        w,
        "ALTER TABLE types ADD (extra VARCHAR2(10))",
        "INSERT INTO types (id, c_vc, extra) VALUES (30, 'after ddl', 'x')",
        "TRUNCATE TABLE nopk",
        "INSERT INTO nopk VALUES (2, 'post-trunc')",
        "INSERT INTO types (id, c_vc) VALUES (99, 'last')");
  }

  static void awaitTopic(ConnectCluster cluster, String prefix, String table, Duration timeout)
      throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    while (System.currentTimeMillis() < deadline) {
      for (String t : cluster.topics()) {
        if (t.startsWith(prefix) && t.endsWith("." + table)) {
          return;
        }
      }
      Thread.sleep(2000);
    }
    throw new AssertionError("no topic " + prefix + "*." + table);
  }

  /** Every record on the profile's topics, read until nothing new arrives for a while. */
  static Map<String, List<ConsumerRecord<byte[], byte[]>>> readUntilIdle(
      ConnectCluster cluster, String prefix, Duration timeout) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeout.toMillis();
    Map<String, List<ConsumerRecord<byte[], byte[]>>> out = new TreeMap<>();
    // LOB topics appear only with the first LOB change: let the script's changes arrive before
    // listing the topics, then read until quiet
    Thread.sleep(45_000);
    List<String> topics =
        cluster.topics().stream()
            .filter(t -> t.startsWith(prefix) && !t.contains("redo-log") && !t.contains(".cdc."))
            .sorted()
            .toList();
    try (KafkaConsumer<byte[], byte[]> c =
        cluster.byteConsumer("ref-" + System.nanoTime(), topics.toArray(String[]::new))) {
      long lastNew = System.currentTimeMillis();
      while (System.currentTimeMillis() < deadline
          && System.currentTimeMillis() - lastNew < TOOL_IDLE.toMillis()) {
        for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofSeconds(1))) {
          out.computeIfAbsent(r.topic(), k -> new ArrayList<>()).add(r);
          lastNew = System.currentTimeMillis();
        }
      }
    }
    return out;
  }

  private void writeSubjects(ConnectCluster cluster, String prefix, Path dir, Normaliser n)
      throws Exception {
    Files.createDirectories(dir);
    String sr = cluster.confluentSchemaRegistryUrl();
    JsonNode subjects = get(sr + "/subjects");
    for (JsonNode s : subjects) {
      String subject = s.asText();
      if (!subject.startsWith(prefix)) {
        continue;
      }
      for (JsonNode v : get(sr + "/subjects/" + subject + "/versions")) {
        JsonNode version = get(sr + "/subjects/" + subject + "/versions/" + v.asText());
        JsonNode schema = JSON.readTree(version.path("schema").asText());
        Files.writeString(
            dir.resolve(n.text(subject.substring(prefix.length())) + ".v" + v.asText() + ".avsc"),
            JSON.writerWithDefaultPrettyPrinter().writeValueAsString(n.json(schema)) + "\n");
      }
    }
  }

  static JsonNode get(String url) throws Exception {
    HttpResponse<String> r =
        HTTP.send(
            HttpRequest.newBuilder(URI.create(url)).GET().build(),
            HttpResponse.BodyHandlers.ofString());
    if (r.statusCode() != 200) {
      throw new IllegalStateException("GET " + url + ": HTTP " + r.statusCode() + " " + r.body());
    }
    return JSON.readTree(r.body());
  }

  static void exec(Connection c, String... sql) throws SQLException {
    try (Statement s = c.createStatement()) {
      for (String q : sql) {
        s.execute(q);
      }
    }
  }

  /**
   * Replaces values that differ between runs with placeholders that keep their relations: the
   * generated schema name with {@code SCHEMA}; SCNs with {@code <scn:n>}; epoch-millisecond times
   * with {@code <ts:n>}; ROWIDs with {@code <rowid:n>}; each numbered in order of first appearance,
   * so equal values stay equal and order is kept. Applied to keys, values, headers and the record
   * timestamp alike.
   */
  static final class Normaliser {
    private static final Pattern ROWID =
        Pattern.compile("(?<![A-Za-z0-9+/])[A-Za-z0-9+/]{18}(?![A-Za-z0-9+/])");
    private final String schema;
    private final Set<String> scnFields;
    private final Set<String> tsFields;
    private final Set<String> rowIdFields;
    private final Map<String, String> scns = new LinkedHashMap<>();
    private final Map<String, String> times = new LinkedHashMap<>();
    private final Map<String, String> rowIds = new LinkedHashMap<>();
    private final String registry;
    private final String prefix;
    private final Map<Integer, JsonNode> schemaVersions = new LinkedHashMap<>();

    Normaliser(String schema, Profile p, String registry, String prefix) {
      this.schema = schema;
      this.registry = registry;
      this.prefix = prefix;
      Map<String, String> props = p.props();
      this.scnFields =
          Set.of(
              field(props, "output.scn.field", "scn"),
              field(props, "output.commit.scn.field", "cscn"));
      this.tsFields =
          Set.of(
              field(props, "output.op.ts.field", "op_ts"),
              field(props, "output.current.ts.field", "current_ts"));
      this.rowIdFields = Set.of(field(props, "output.row.id.field", "row_id"));
    }

    private static String field(Map<String, String> props, String key, String dflt) {
      String v = props.getOrDefault(key, dflt);
      v = v.startsWith("header:") ? v.substring("header:".length()) : v;
      int dot = v.lastIndexOf('.');
      return dot >= 0 ? v.substring(dot + 1) : v;
    }

    String text(String s) {
      return s == null ? null : s.replace(schema, "SCHEMA");
    }

    private String placeholder(Map<String, String> map, String kind, String value) {
      return map.computeIfAbsent(value, v -> "<" + kind + ":" + (map.size() + 1) + ">");
    }

    JsonNode json(JsonNode node) {
      return json(node, null);
    }

    private JsonNode json(JsonNode node, String fieldName) {
      if (node == null) {
        return null;
      }
      if (node.isObject()) {
        ObjectNode o = JSON.createObjectNode();
        node.fields().forEachRemaining(e -> o.set(e.getKey(), json(e.getValue(), e.getKey())));
        return o;
      }
      if (node.isArray()) {
        ArrayNode a = JSON.createArrayNode();
        node.forEach(x -> a.add(json(x, fieldName)));
        return a;
      }
      if (node.isNull() || fieldName == null) {
        return node.isTextual() ? TextNode.valueOf(rowIdsIn(text(node.asText()))) : node;
      }
      String v = node.asText();
      if (scnFields.contains(fieldName)) {
        return TextNode.valueOf(placeholder(scns, "scn", v));
      }
      if (tsFields.contains(fieldName)) {
        return TextNode.valueOf(placeholder(times, "ts", v));
      }
      if (rowIdFields.contains(fieldName)) {
        return TextNode.valueOf(placeholder(rowIds, "rowid", v));
      }
      return node.isTextual() ? TextNode.valueOf(rowIdsIn(text(v))) : node;
    }

    private String rowIdsIn(String s) {
      Matcher m = ROWID.matcher(s);
      StringBuilder sb = new StringBuilder();
      while (m.find()) {
        m.appendReplacement(sb, Matcher.quoteReplacement(placeholder(rowIds, "rowid", m.group())));
      }
      m.appendTail(sb);
      return sb.toString();
    }

    ObjectNode record(ConsumerRecord<byte[], byte[]> r, boolean avro) throws Exception {
      ObjectNode o = JSON.createObjectNode();
      o.put("timestampType", r.timestampType().name);
      o.put("timestamp", placeholder(times, "ts", Long.toString(r.timestamp())));
      o.set("key", side(r.key(), avro, r.topic() + "-key"));
      o.set("value", side(r.value(), avro, r.topic() + "-value"));
      ArrayNode headers = JSON.createArrayNode();
      for (Header h : r.headers()) {
        ObjectNode hn = JSON.createObjectNode();
        hn.put("key", h.key());
        String value = h.value() == null ? null : new String(h.value(), StandardCharsets.UTF_8);
        hn.set("value", value == null ? null : json(TextNode.valueOf(value), h.key()));
        headers.add(hn);
      }
      o.set("headers", headers);
      return o;
    }

    private JsonNode side(byte[] bytes, boolean avro, String subject) throws Exception {
      if (bytes == null) {
        return null;
      }
      if (avro) {
        assertThat(bytes.length).isGreaterThanOrEqualTo(5);
        assertThat(bytes[0]).isZero();
        int id = ByteBuffer.wrap(bytes, 1, 4).getInt();
        JsonNode versions = schemaVersions.get(id);
        if (versions == null) {
          versions = get(registry + "/schemas/ids/" + id + "/versions");
          schemaVersions.put(id, versions);
        }
        JsonNode version = null;
        for (JsonNode entry : versions) {
          if (subject.equals(entry.path("subject").asText())) {
            version = entry.path("version");
            break;
          }
        }
        assertThat(version).as("registered subject version for " + subject).isNotNull();
        ObjectNode a = JSON.createObjectNode();
        a.put("avro", true);
        a.put("magic", bytes[0]);
        a.put("subject", text(subject.substring(prefix.length())));
        a.set("version", version);
        return a;
      }
      return json(JSON.readTree(bytes));
    }
  }
}
