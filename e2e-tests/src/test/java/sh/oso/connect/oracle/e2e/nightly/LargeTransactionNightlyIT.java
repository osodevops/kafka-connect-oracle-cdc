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
import java.util.BitSet;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import sh.oso.connect.oracle.e2e.support.ConnectCluster;
import sh.oso.connect.oracle.e2e.support.Evidence;
import sh.oso.connect.oracle.e2e.support.OracleTestDatabase;
import sh.oso.connect.oracle.e2e.support.SchemaFixtures;

/**
 * PRD-00 acceptance: a five million row single transaction completes with the heap capped at the
 * configured budget. The buffer may hold 16 MiB on the heap, so almost the whole transaction is
 * spilled to disk while it is open, and every row must reach Kafka once, followed by a commit
 * behind it. Values carry no embedded schema, to keep the volume through Kafka modest.
 *
 * <p>{@code -Dnightly.large.rows} sets the size (default 5,000,000).
 */
@Tag("nightly")
class LargeTransactionNightlyIT {

  static final String NAME = "large-tx";
  static final String PREFIX = "lt";
  static final long HEAP_BUDGET = 16L * 1024 * 1024;

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void aFiveMillionRowTransactionCompletesWithTheHeapBudgetCapped() throws Exception {
    int rows = Integer.getInteger("nightly.large.rows", 5_000_000);
    int afterId = rows + 1;
    String schema = SchemaFixtures.nameFor(getClass());
    Evidence ev =
        Evidence.of(
            getClass(),
            "one transaction of "
                + rows
                + " rows against a 16 MiB heap budget: every row once, and the commit behind it");
    ev.param("rows", rows).param("heapBudgetBytes", HEAP_BUDGET);
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    try (ConnectCluster c = new ConnectCluster().start();
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      try (Statement s = w.createStatement()) {
        s.execute("CREATE TABLE bt (id NUMBER(9) PRIMARY KEY, v VARCHAR2(20) NOT NULL)");
        s.execute("ALTER TABLE bt ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      }
      Map<String, String> config =
          NightlyRun.connector(
              PREFIX, schema, "BT", ConnectCluster.oracleDatabaseProps("FREEPDB1"));
      config.put("cdc.decimal.mode", "string");
      config.put("cdc.buffer.memory.max.bytes", Long.toString(HEAP_BUDGET));
      config.put("cdc.buffer.spill.max.bytes", Long.toString(16L << 30));
      config.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
      config.put("key.converter.schemas.enable", "false");
      config.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
      config.put("value.converter.schemas.enable", "false");
      c.register(NAME, config);
      c.awaitRunning(NAME, Duration.ofMinutes(3));
      c.awaitOffsets(NAME, Duration.ofSeconds(90));

      // one transaction: a cross join keeps CONNECT BY small (a five million level CONNECT BY
      // runs out of memory on Oracle Database Free)
      long started = System.currentTimeMillis();
      w.setAutoCommit(false);
      int outer = (rows + 999) / 1000;
      try (Statement s = w.createStatement()) {
        s.executeUpdate(
            "INSERT INTO bt SELECT id, 'r' || id FROM (SELECT (a.l - 1) * 1000 + b.l AS id FROM"
                + " (SELECT LEVEL l FROM dual CONNECT BY LEVEL <= "
                + outer
                + ") a, (SELECT LEVEL l FROM dual CONNECT BY LEVEL <= 1000) b) WHERE id <= "
                + rows);
      }
      w.commit();
      try (Statement s = w.createStatement()) {
        s.executeUpdate("INSERT INTO bt VALUES (" + afterId + ", 'after')");
      }
      w.commit();
      ev.fault("large-transaction-committed", "rows", rows, "seconds", seconds(started));

      BitSet seen = new BitSet(afterId + 1);
      long duplicates = 0;
      long records = 0;
      String topic = NightlyRun.topic(PREFIX, schema, "BT");
      long readStarted = System.currentTimeMillis();
      try (KafkaConsumer<String, String> consumer = c.consumer("large-tx-reader", topic)) {
        long deadline = System.currentTimeMillis() + Duration.ofMinutes(120).toMillis();
        long lastNew = System.currentTimeMillis();
        while (System.currentTimeMillis() < deadline && seen.cardinality() < afterId) {
          ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(500));
          if (batch.isEmpty()) {
            if (System.currentTimeMillis() - lastNew > Duration.ofMinutes(5).toMillis()) {
              break; // nothing for five minutes: the task has stopped or stalled
            }
            continue;
          }
          lastNew = System.currentTimeMillis();
          for (ConsumerRecord<String, String> r : batch) {
            if (r.value() == null) {
              continue;
            }
            records++;
            JsonNode v = ConnectCluster.json(r.value());
            BigDecimal id = NightlyRun.number(v.path("after").path("ID"));
            if (id == null) {
              continue;
            }
            int i = id.intValue();
            if (seen.get(i)) {
              duplicates++;
            }
            seen.set(i);
          }
        }
      }
      int distinct = seen.cardinality();
      ev.count("records", records)
          .count("distinctRows", distinct)
          .count("duplicates", duplicates)
          .count("readSeconds", seconds(readStarted))
          .count("taskState", c.status(NAME).path("tasks").get(0).path("state").asText());
      System.out.println(
          "large-tx: rows="
              + rows
              + " distinct="
              + distinct
              + " duplicates="
              + duplicates
              + " task="
              + c.status(NAME).path("tasks").get(0).path("state").asText());
      assertThat(c.status(NAME).path("tasks").get(0).path("state").asText())
          .as("the task survives the transaction with its heap budget")
          .isEqualTo("RUNNING");
      assertThat(distinct)
          .as("every row of the transaction, and the one after it")
          .isEqualTo(afterId);
      assertThat(seen.get(afterId)).isTrue();
      assertThat(duplicates).as("no row delivered twice").isZero();
    } catch (Throwable t) {
      ev.failed(t);
      throw t;
    } finally {
      ev.write();
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  private static long seconds(long since) {
    return (System.currentTimeMillis() - since) / 1000;
  }
}
