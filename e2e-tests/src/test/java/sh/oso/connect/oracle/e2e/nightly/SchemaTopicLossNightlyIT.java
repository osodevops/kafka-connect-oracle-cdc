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
package sh.oso.connect.oracle.e2e.nightly;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.bench.check.CheckReport;
import sh.oso.connect.oracle.bench.check.CorrectnessCheck;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.Evidence;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * T2 schema topic loss (testing strategy section 4, PRD-03 SCH-1 and SCH-6): the compacted schema
 * topic is deleted while the connector is stopped.
 *
 * <p>Rebuild: with no DDL between the resume point and the restart, the task starts without stored
 * versions, reads them from the dictionary, writes the topic again, and every record before and
 * after the loss (including after a later DDL) carries the right values; the oracle over the table
 * passes.
 *
 * <p>Loss with a DDL while stopped: rows written before a DROP COLUMN made while the connector was
 * down can only be decoded with the version the lost topic held. ADR-0016 says the task then stops
 * with CDC-6001. The invariant asserted is the one that matters whichever way the product goes:
 * every published record carries the values the row had, and if anything is not published the task
 * is stopped with a typed CDC code; the evidence records which way it went.
 */
@Tag("nightly")
class SchemaTopicLossNightlyIT {

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void aLostSchemaTopicIsRebuiltFromTheDictionaryAndRecordsStayCorrect() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    String name = "schema-loss";
    String prefix = "sl";
    Evidence ev =
        Evidence.of(
            getClass(),
            "rebuild",
            "schema topic deleted while stopped, no DDL in between: versions rebuilt from the"
                + " dictionary, records correct before and after a later DDL");
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (ConnectCluster c = new ConnectCluster().start();
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      sql(
          w,
          "CREATE TABLE sl (id NUMBER(9) PRIMARY KEY, name VARCHAR2(50))",
          "ALTER TABLE sl ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      Thread.sleep(3500);
      c.register(name, config(prefix, schema, "SL"));
      c.awaitRunning(name, Duration.ofMinutes(3));
      c.awaitOffsets(name, Duration.ofSeconds(90));
      String data = NightlyRun.topic(prefix, schema, "SL");
      String schemaTopic = prefix + ".cdc.schema";

      for (int i = 1; i <= 5; i++) {
        sql(w, "INSERT INTO sl VALUES (" + i + ", 'name-" + i + "')");
      }
      sql(w, "ALTER TABLE sl ADD (note VARCHAR2(20))");
      for (int i = 6; i <= 10; i++) {
        sql(w, "INSERT INTO sl VALUES (" + i + ", 'name-" + i + "', 'note-" + i + "')");
      }
      assertThat(rows(c, data, 10, Duration.ofMinutes(3))).hasSize(10);
      JsonNode before = latestVersions(c, schemaTopic);
      ev.count("versionsBeforeLoss", before == null ? 0 : before.path("versions").size());
      try (Connection root = db.capture(OracleTestDatabase.CDB_SERVICE)) {
        assertThat(NightlyRun.awaitResumePast(c, name, NightlyRun.scn(root), Duration.ofMinutes(3)))
            .as("the DDL is behind the committed position")
            .isTrue();
      }
      c.lifecycle(name, "stop");
      c.awaitTaskState(name, "STOPPED", Duration.ofMinutes(2));
      deleteTopic(c, schemaTopic);
      ev.fault("schema-topic-deleted", "topic", schemaTopic);

      for (int i = 11; i <= 15; i++) {
        sql(w, "INSERT INTO sl VALUES (" + i + ", 'name-" + i + "', 'note-" + i + "')");
      }
      c.lifecycle(name, "resume");
      c.awaitRunning(name, Duration.ofMinutes(3));
      sql(w, "ALTER TABLE sl ADD (extra NUMBER(5))");
      for (int i = 16; i <= 20; i++) {
        sql(
            w,
            "INSERT INTO sl VALUES ("
                + i
                + ", 'name-"
                + i
                + "', 'note-"
                + i
                + "', "
                + i * 10
                + ")");
      }

      Map<Integer, JsonNode> got = rows(c, data, 20, Duration.ofMinutes(4));
      List<String> wrong = new ArrayList<>();
      for (int i = 1; i <= 20; i++) {
        JsonNode a = got.get(i);
        if (a == null) {
          wrong.add(i + ": missing");
          continue;
        }
        expect(wrong, i, a, "NAME", "name-" + i);
        if (i >= 6) {
          expect(wrong, i, a, "NOTE", "note-" + i);
        }
        if (i >= 16) {
          BigDecimal extra = NightlyRun.number(a.path("EXTRA"));
          if (extra == null || extra.intValue() != i * 10) {
            wrong.add(i + ".EXTRA: " + a.path("EXTRA"));
          }
        }
      }
      JsonNode rebuilt = latestVersions(c, schemaTopic);
      JsonNode last =
          rebuilt == null
              ? null
              : rebuilt.path("versions").get(rebuilt.path("versions").size() - 1);
      CheckReport report =
          new CorrectnessCheck(
                  c.bootstrapServers(),
                  List.of(data),
                  w,
                  schema,
                  List.of("SL"),
                  null,
                  Duration.ofMinutes(3),
                  Duration.ofSeconds(15))
              .run();
      ev.check("oracle", report)
          .count("rowsChecked", got.size())
          .count("wrongRecords", wrong.size())
          .count("versionsAfterRebuild", rebuilt == null ? 0 : rebuilt.path("versions").size())
          .count("columnsInNewestVersion", last == null ? 0 : last.path("columns").size());
      wrong.stream().limit(10).forEach(x -> ev.note("wrong: " + x));
      System.out.println("schema-loss rebuild: wrong=" + wrong + " verdict=" + report.verdict());
      assertThat(wrong).as("every record carries the row's values").isEmpty();
      assertThat(rebuilt).as("the schema topic is written again").isNotNull();
      assertThat(last.path("columns").size())
          .as("the newest version holds ID, NAME, NOTE and EXTRA")
          .isEqualTo(4);
      assertThat(report.verdict()).as(report.toJson()).isEqualTo(CheckReport.Verdict.PASS);
    } catch (Throwable t) {
      ev.failed(t);
      throw t;
    } finally {
      ev.write();
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  @Test
  void aLostSchemaTopicWithADdlWhileStoppedNeverPublishesWrongValues() throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    String name = "schema-loss-ddl";
    String prefix = "sm";
    Evidence ev =
        Evidence.of(
            getClass(),
            "ddl-while-stopped",
            "schema topic deleted while stopped, then rows and a DROP COLUMN: every published"
                + " record correct, anything unpublished only behind a typed stop");
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (ConnectCluster c = new ConnectCluster().start();
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      sql(
          w,
          "CREATE TABLE sm (id NUMBER(9) PRIMARY KEY, name VARCHAR2(50), note VARCHAR2(20))",
          "ALTER TABLE sm ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      Thread.sleep(3500);
      c.register(name, config(prefix, schema, "SM"));
      c.awaitRunning(name, Duration.ofMinutes(3));
      c.awaitOffsets(name, Duration.ofSeconds(90));
      String data = NightlyRun.topic(prefix, schema, "SM");
      String schemaTopic = prefix + ".cdc.schema";
      for (int i = 1; i <= 5; i++) {
        sql(w, "INSERT INTO sm VALUES (" + i + ", 'name-" + i + "', 'note-" + i + "')");
      }
      assertThat(rows(c, data, 5, Duration.ofMinutes(3))).hasSize(5);
      try (Connection root = db.capture(OracleTestDatabase.CDB_SERVICE)) {
        assertThat(NightlyRun.awaitResumePast(c, name, NightlyRun.scn(root), Duration.ofMinutes(3)))
            .isTrue();
      }
      c.lifecycle(name, "stop");
      c.awaitTaskState(name, "STOPPED", Duration.ofMinutes(2));
      deleteTopic(c, schemaTopic);
      ev.fault("schema-topic-deleted", "topic", schemaTopic);
      for (int i = 6; i <= 10; i++) {
        sql(w, "INSERT INTO sm VALUES (" + i + ", 'name-" + i + "', 'note-" + i + "')");
      }
      sql(w, "ALTER TABLE sm DROP COLUMN note");
      for (int i = 11; i <= 15; i++) {
        sql(w, "INSERT INTO sm VALUES (" + i + ", 'name-" + i + "')");
      }
      ev.fault("ddl-while-stopped", "ddl", "ALTER TABLE sm DROP COLUMN note");
      c.lifecycle(name, "resume");

      // either every row arrives, or the task stops typed: wait for one or the other
      long deadline = System.currentTimeMillis() + Duration.ofMinutes(5).toMillis();
      String trace = null;
      Map<Integer, JsonNode> got = new TreeMap<>();
      while (System.currentTimeMillis() < deadline) {
        trace = NightlyRun.failedTrace(c, name);
        got = rows(c, data, 15, Duration.ofSeconds(30));
        if (trace != null || got.size() >= 15) {
          break;
        }
      }
      if (trace != null) {
        got = rows(c, data, 15, Duration.ofSeconds(30)); // whatever was published before the stop
      }
      List<String> wrong = new ArrayList<>();
      for (Map.Entry<Integer, JsonNode> e : got.entrySet()) {
        int i = e.getKey();
        JsonNode a = e.getValue();
        expect(wrong, i, a, "NAME", "name-" + i);
        if (i <= 10) {
          expect(wrong, i, a, "NOTE", "note-" + i);
        } else if (a.has("NOTE") && !a.path("NOTE").isNull()) {
          wrong.add(i + ".NOTE: " + a.path("NOTE") + " after the column was dropped");
        }
      }
      String path = trace != null ? "typed-stop " + NightlyRun.cdcCode(trace) : "decoded";
      ev.count("path", path).count("published", got.size()).count("wrongRecords", wrong.size());
      if (trace != null) {
        ev.note("stop: " + NightlyRun.firstLine(trace));
      }
      wrong.stream().limit(10).forEach(x -> ev.note("wrong: " + x));
      System.out.println("schema-loss ddl: path=" + path + " published=" + got.size());
      assertThat(wrong).as("no published record carries wrong or missing values").isEmpty();
      if (got.size() < 15) {
        assertThat(trace).as("rows are unpublished only behind a stopped task").isNotNull();
        assertThat(NightlyRun.cdcCode(trace)).as("the stop is typed: " + trace).isNotNull();
      }
    } catch (Throwable t) {
      ev.failed(t);
      throw t;
    } finally {
      ev.write();
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  private static Map<String, String> config(String prefix, String schema, String table) {
    Map<String, String> c =
        NightlyRun.connector(prefix, schema, table, ConnectCluster.oracleDatabaseProps("FREEPDB1"));
    c.put("cdc.kafka.bootstrap.servers", "kafka:19092");
    c.put("cdc.decimal.mode", "string");
    return c;
  }

  private static void expect(List<String> wrong, int id, JsonNode after, String col, String v) {
    if (!v.equals(after.path(col).asText(null))) {
      wrong.add(id + "." + col + ": " + after.path(col) + " expected " + v);
    }
  }

  private static void sql(Connection w, String... statements) throws Exception {
    try (Statement s = w.createStatement()) {
      for (String q : statements) {
        s.execute(q);
      }
    }
  }

  /** The newest after image per ID on the topic (deletes are not used here). */
  private static Map<Integer, JsonNode> rows(
      ConnectCluster c, String topic, int n, Duration timeout) throws Exception {
    Map<Integer, JsonNode> out = new TreeMap<>();
    try (KafkaConsumer<String, String> consumer =
        c.consumer("schema-loss-" + System.nanoTime(), topic)) {
      for (ConsumerRecord<String, String> r :
          ConnectCluster.consume(consumer, n, timeout, Duration.ofSeconds(3))) {
        if (r.value() == null) {
          continue;
        }
        JsonNode after = ConnectCluster.json(r.value()).path("after");
        BigDecimal id = NightlyRun.number(after.path("ID"));
        if (id != null) {
          out.put(id.intValue(), after);
        }
      }
    }
    return out;
  }

  /** The newest record on the schema topic, or null when there is none. */
  private static JsonNode latestVersions(ConnectCluster c, String topic) throws Exception {
    try (KafkaConsumer<String, String> consumer =
        c.consumer("schema-versions-" + System.nanoTime(), topic)) {
      List<ConsumerRecord<String, String>> got =
          ConnectCluster.consume(consumer, 1, Duration.ofSeconds(45), Duration.ofSeconds(3));
      for (int i = got.size() - 1; i >= 0; i--) {
        if (got.get(i).value() != null) {
          return ConnectCluster.json(got.get(i).value());
        }
      }
      return null;
    }
  }

  private static void deleteTopic(ConnectCluster c, String topic) throws Exception {
    Properties p = new Properties();
    p.put("bootstrap.servers", c.bootstrapServers());
    try (Admin admin = Admin.create(p)) {
      admin.deleteTopics(List.of(topic)).all().get(1, TimeUnit.MINUTES);
      long deadline = System.currentTimeMillis() + 60_000;
      while (admin.listTopics().names().get(30, TimeUnit.SECONDS).contains(topic)) {
        if (System.currentTimeMillis() > deadline) {
          throw new IllegalStateException("topic " + topic + " still listed after deletion");
        }
        Thread.sleep(500);
      }
    }
  }
}
