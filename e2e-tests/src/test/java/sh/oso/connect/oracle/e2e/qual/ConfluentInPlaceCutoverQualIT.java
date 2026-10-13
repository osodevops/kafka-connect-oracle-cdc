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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import sh.oso.connect.oracle.e2e.support.Cli;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.Evidence;
import sh.oso.connect.oracle.e2e.support.TestDatabase;

/** A stopped Confluent connector is replaced in its own Avro topic, with its consumer unchanged. */
@Tag("qual")
class ConfluentInPlaceCutoverQualIT {
  private final TestDatabase db = TestDatabase.get();
  @TempDir Path temporary;

  @Test
  void unchangedAvroConsumerReadsEveryCommittedRowAcrossTheSameTopicHandover() throws Exception {
    Path plugin = ConfluentFormatLab.plugin();
    assumeTrue(Cli.uvAvailable(), "uv is needed for the authorised migration tools");
    String schema = "CF_IN_PLACE";
    String old = "confluent-in-place";
    String next = "oso-in-place";
    Evidence evidence = Evidence.of(getClass(), "Confluent same-topic Avro cutover");
    try (ConnectCluster cluster =
        ConnectCluster.confluentPlatform(ConfluentFormatRefQualIT.WORKER_IMAGE)
            .withPlugin(plugin, "confluent-oracle-cdc")
            .withConfluentSchemaRegistry()
            .start()) {
      db.recreateSchema(schema);
      try (Connection work = db.workload(schema)) {
        ConfluentFormatRefQualIT.exec(
            work,
            "CREATE TABLE orders (id NUMBER(9) PRIMARY KEY, name VARCHAR2(40), qty NUMBER(9))",
            "ALTER TABLE orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS",
            "INSERT INTO orders SELECT LEVEL, 'seed ' || LEVEL, LEVEL FROM dual CONNECT BY LEVEL <="
                + " 20",
            "BEGIN DBMS_STATS.GATHER_SCHEMA_STATS(USER); END;");
      }
      db.archiveLogCurrent();
      Thread.sleep(3500);
      var profile =
          new ConfluentFormatRefQualIT.Profile(
              "in-place", Map.of("numeric.mapping", "best_fit_or_decimal"), true);
      Map<String, String> source =
          new ConfluentFormatRefQualIT().config(cluster, profile, schema, old);
      String first = source.getOrDefault("oracle.pdb.name", source.get("oracle.sid"));
      String topic = first + "." + schema + ".ORDERS";
      source.put("table.inclusion.regex", first + "[.]" + schema + "[.]ORDERS");
      source.put("table.topic.name.template", "${fullyQualifiedTableName}");
      cluster.register(old, source);
      cluster.awaitRunning(old, Duration.ofMinutes(3));
      ConfluentFormatRefQualIT.awaitTopic(cluster, first + ".", "ORDERS", Duration.ofMinutes(3));
      ConfluentFormatLab.read(cluster, List.of(topic), 20, Duration.ofMinutes(3));
      try (Connection work = db.workload(schema)) {
        ConfluentFormatRefQualIT.exec(work, "INSERT INTO orders VALUES (100, 'before stop', 1)");
      }
      db.archiveLogCurrent();
      ConfluentFormatLab.read(cluster, List.of(topic), 21, Duration.ofMinutes(3));
      String registry = cluster.confluentSchemaRegistryUrl();
      String subject = topic + "-value";
      JsonNode before =
          ConfluentFormatRefQualIT.get(registry + "/subjects/" + subject + "/versions/latest");
      Schema readerSchema = new Schema.Parser().parse(before.path("schema").asText());
      JsonNode beforeKey =
          ConfluentFormatRefQualIT.get(registry + "/subjects/" + topic + "-key/versions/latest");
      Schema keyReaderSchema = new Schema.Parser().parse(beforeKey.path("schema").asText());
      // Written before stop, committed only once OSO is running. Its earlier redo must survive.
      try (Connection spanning = db.workload(schema)) {
        spanning.setAutoCommit(false);
        ConfluentFormatRefQualIT.exec(spanning, "INSERT INTO orders VALUES (900, 'spanning', 9)");
        Thread.sleep(5000);
        cluster.lifecycle(old, "stop");
        cluster.awaitConnectorState(old, "STOPPED", Duration.ofMinutes(2));
        Path input = temporary.resolve("source.json");
        Files.writeString(input, cluster.connectorInfo(old).toPrettyString());
        Path translated = temporary.resolve("translated.json");
        Cli.Result migrate =
            Cli.migration(
                "migrate_from_confluent.py",
                Map.of(),
                Duration.ofMinutes(3),
                "--input",
                input.toString(),
                "--output",
                translated.toString(),
                "--name",
                next,
                "--connect-url",
                cluster.restUrl(),
                "--report",
                temporary.resolve("migration.md").toString());
        assertThat(migrate.exit()).as(migrate.toString()).isIn(0, 2);
        Path takeover = temporary.resolve("takeover.json");
        Cli.Result position =
            Cli.migration(
                "takeover_scn.py",
                Map.of("ORACLE_PASSWORD", db.capturePassword()),
                Duration.ofMinutes(5),
                "--connect-url",
                cluster.restUrl(),
                "--connector",
                old,
                "--target-config",
                translated.toString(),
                "--db-dsn",
                db.hostDsn(),
                "--db-user",
                db.captureUser(),
                "--db-password-env",
                "ORACLE_PASSWORD",
                "--output",
                takeover.toString(),
                "--report",
                temporary.resolve("takeover.md").toString(),
                "--json");
        assertThat(position.exit()).as(position.toString()).isIn(0, 2);
        Map<String, String> config = new LinkedHashMap<>();
        ConfluentFormatRefQualIT.JSON
            .readTree(takeover.toFile())
            .path("config")
            .fields()
            .forEachRemaining(e -> config.put(e.getKey(), e.getValue().asText()));
        config.putAll(db.connectorDatabaseProps());
        assertThat(config.get("cdc.output.format")).isEqualTo("confluent");
        assertThat(config.get("cdc.topic.template"))
            .isEqualTo(
                source.containsKey("oracle.pdb.name")
                    ? "${pdb}.${schema}.${table}"
                    : "${database}.${schema}.${table}");
        config.put("cdc.kafka.bootstrap.servers", "kafka:19092");
        cluster.register(next, config);
        cluster.awaitRunning(next, Duration.ofMinutes(3));
        spanning.commit();
      }
      try (Connection work = db.workload(schema)) {
        ConfluentFormatRefQualIT.exec(
            work,
            "INSERT INTO orders VALUES (901, 'after takeover', 10)",
            "UPDATE orders SET qty = 11 WHERE id = 100");
        work.setAutoCommit(false);
        ConfluentFormatRefQualIT.exec(work, "INSERT INTO orders VALUES (999, 'rolled back', 0)");
        work.rollback();
      }
      db.archiveLogCurrent();
      ConfluentFormatLab.read(cluster, List.of(topic), 24, Duration.ofMinutes(5));
      cluster.lifecycle(next, "stop");
      cluster.awaitConnectorState(next, "STOPPED", Duration.ofMinutes(2));
      cluster.lifecycle(next, "resume");
      cluster.awaitRunning(next, Duration.ofMinutes(3));
      try (Connection work = db.workload(schema)) {
        ConfluentFormatRefQualIT.exec(work, "INSERT INTO orders VALUES (902, 'after restart', 12)");
      }
      db.archiveLogCurrent();
      var records = ConfluentFormatLab.read(cluster, List.of(topic), 25, Duration.ofMinutes(5));
      Map<Integer, Map<String, Object>> rows = new TreeMap<>();
      int osoRecords = 0;
      for (var record : records) {
        if (record.value() == null) continue;
        byte[] wire = record.value();
        assertThat(wire[0]).isZero();
        int id = ByteBuffer.wrap(wire, 1, 4).getInt();
        Schema writer =
            new Schema.Parser()
                .parse(
                    ConfluentFormatRefQualIT.get(registry + "/schemas/ids/" + id)
                        .path("schema")
                        .asText());
        // This reader schema was cached before stopping Confluent and is never updated.
        GenericRecord row =
            new GenericDatumReader<GenericRecord>(writer, readerSchema)
                .read(null, DecoderFactory.get().binaryDecoder(wire, 5, wire.length - 5, null));
        byte[] keyWire = record.key();
        Schema keyWriter =
            new Schema.Parser()
                .parse(
                    ConfluentFormatRefQualIT.get(
                            registry + "/schemas/ids/" + ByteBuffer.wrap(keyWire, 1, 4).getInt())
                        .path("schema")
                        .asText());
        Object key =
            new GenericDatumReader<>(keyWriter, keyReaderSchema)
                .read(
                    null, DecoderFactory.get().binaryDecoder(keyWire, 5, keyWire.length - 5, null));
        int rowId = ((Number) row.get("ID")).intValue();
        assertThat(((Number) key).intValue()).isEqualTo(rowId);
        rows.put(
            rowId,
            Map.of(
                "NAME", row.get("NAME").toString(), "QTY", ((Number) row.get("QTY")).intValue()));
        if (record.headers().lastHeader("cdc.commit_scn") != null) osoRecords++;
      }
      Set<Integer> expected = new java.util.TreeSet<>();
      for (int id = 1; id <= 20; id++) expected.add(id);
      expected.addAll(List.of(100, 900, 901, 902));
      assertThat(rows.keySet()).containsExactlyInAnyOrderElementsOf(expected);
      assertThat(osoRecords).isGreaterThanOrEqualTo(4);
      Map<Integer, Map<String, Object>> databaseRows = new TreeMap<>();
      try (Connection work = db.workload(schema);
          java.sql.Statement statement = work.createStatement();
          java.sql.ResultSet result = statement.executeQuery("SELECT id, name, qty FROM orders")) {
        while (result.next())
          databaseRows.put(
              result.getInt(1), Map.of("NAME", result.getString(2), "QTY", result.getInt(3)));
      }
      assertThat(rows).isEqualTo(databaseRows);
      assertThat(rows.get(100).get("QTY")).isEqualTo(11);
      JsonNode after =
          ConfluentFormatRefQualIT.get(registry + "/subjects/" + subject + "/versions/latest");
      assertThat(after.path("id")).isEqualTo(before.path("id"));
      assertThat(after.path("version")).isEqualTo(before.path("version"));
      JsonNode afterKey =
          ConfluentFormatRefQualIT.get(registry + "/subjects/" + topic + "-key/versions/latest");
      assertThat(afterKey.path("id")).isEqualTo(beforeKey.path("id"));
      assertThat(afterKey.path("version")).isEqualTo(beforeKey.path("version"));
      cluster.awaitRunning(next, Duration.ofSeconds(10));
      cluster.delete(next);
      cluster.delete(old);
      evidence
          .count("committedRows", expected.size())
          .count("missing", 0)
          .param("sameTopic", true)
          .param("unchangedReaderSchema", true)
          .param("schemaSubjectVersionReused", true);
    } catch (Throwable failure) {
      evidence.failed(failure);
      throw failure;
    } finally {
      evidence.write();
      db.dropSchema(schema);
    }
  }
}
