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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;
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
 * T2 disk-full spill (testing strategy section 4, PRD-00 CORE-TX-3): with the smallest heap budget
 * the configuration allows (16 MiB), one large committed transaction spills to disk. Two ways the
 * spill can run out: the configured cap (64 MiB, the smallest allowed) and a real full volume (an 8
 * MiB tmpfs in the worker container, a genuine ENOSPC). Each must stop the task with
 * BufferExhaustedException (CDC-4001) before anything of the transaction is published, and after
 * the operator's remedy (a larger cap, a larger volume) the restarted task must deliver the whole
 * transaction, every event exactly once: a typed stop, never data loss.
 */
@Tag("nightly")
class SpillDiskFullNightlyIT {

  static final long HEAP_BUDGET = 16L * 1024 * 1024;
  static final long SMALLEST_CAP = 64L * 1024 * 1024;
  static final int SMALL = 20;
  static final int PAD = 3000;
  static final int AFTER_ID = 3_000_000;

  private final OracleTestDatabase db = OracleTestDatabase.get();

  @Test
  void exceedingTheSpillCapIsATypedStopAndARaisedCapDeliversTheWholeTransaction() throws Exception {
    // above the 16 MiB heap budget plus the 64 MiB cap at about 3 KB a row on disk
    int rows = Integer.getInteger("nightly.spill.cap.rows", 30_000);
    Map<String, String> extra = new HashMap<>();
    extra.put("cdc.buffer.spill.max.bytes", Long.toString(SMALLEST_CAP));
    Map<String, String> remedy = Map.of("cdc.buffer.spill.max.bytes", Long.toString(1L << 32));
    scenario("spill-cap", "SC", "sc", rows, extra, remedy, null, "cdc.buffer.spill.max.bytes");
  }

  @Test
  void aFullSpillVolumeIsATypedStopAndALargerVolumeDeliversTheWholeTransaction() throws Exception {
    // above the 16 MiB heap budget plus the 8 MiB volume
    int rows = Integer.getInteger("nightly.spill.volume.rows", 12_000);
    Map<String, String> extra = new HashMap<>();
    extra.put("cdc.buffer.spill.dir", "/spill/spill-volume");
    Map<String, String> remedy = Map.of("cdc.buffer.spill.dir", "/tmp/oracle-cdc-spill-large");
    scenario("spill-volume", "SV", "sv", rows, extra, remedy, "8m", "/spill/spill-volume");
  }

  private void scenario(
      String name,
      String table,
      String prefix,
      int rows,
      Map<String, String> extra,
      Map<String, String> remedy,
      String tmpfsSize,
      String messageMustName)
      throws Exception {
    String schema = SchemaFixtures.nameFor(getClass());
    Evidence ev =
        Evidence.of(
            getClass(),
            name,
            "a large transaction against a 16 MiB heap budget and "
                + (tmpfsSize == null ? "the 64 MiB spill cap" : "an " + tmpfsSize + " spill volume")
                + ": expect CDC-4001 with nothing of it published, then the whole transaction"
                + " after the remedy");
    ev.param("heapBudgetBytes", HEAP_BUDGET)
        .param("rowsInLargeTransaction", rows)
        .param("padChars", PAD)
        .param("config", extra)
        .param("remedy", remedy);
    if (tmpfsSize != null) {
      ev.param("workerTmpfs", "/spill size=" + tmpfsSize);
    }
    SchemaFixtures.recreate(db, OracleTestDatabase.PDB1, schema);
    ConnectCluster cluster = new ConnectCluster();
    if (tmpfsSize != null) {
      cluster.withWorkerTmpFs("/spill", tmpfsSize);
    }
    try (ConnectCluster c = cluster.start();
        Connection w = db.connect(OracleTestDatabase.PDB1, schema, schema)) {
      try (Statement s = w.createStatement()) {
        s.execute(
            "CREATE TABLE " + table + " (id NUMBER(9) PRIMARY KEY, pad VARCHAR2(4000) NOT NULL)");
        s.execute("ALTER TABLE " + table + " ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS");
      }
      Map<String, String> config =
          NightlyRun.connector(
              prefix, schema, table, ConnectCluster.oracleDatabaseProps("FREEPDB1"));
      config.put("cdc.decimal.mode", "string");
      config.put("cdc.buffer.memory.max.bytes", Long.toString(HEAP_BUDGET));
      config.putAll(extra);
      c.register(name, config);
      c.awaitRunning(name, Duration.ofMinutes(3));
      c.awaitOffsets(name, Duration.ofSeconds(90));
      String topic = NightlyRun.topic(prefix, schema, table);

      // small transactions first, delivered and acknowledged
      w.setAutoCommit(false);
      try (PreparedStatement ps = w.prepareStatement("INSERT INTO " + table + " VALUES (?, ?)")) {
        for (int i = 1; i <= SMALL; i++) {
          ps.setInt(1, i);
          ps.setString(2, "small-" + i);
          ps.executeUpdate();
          w.commit();
        }
      }
      long afterSmall;
      try (Connection root = db.capture(OracleTestDatabase.CDB_SERVICE)) {
        afterSmall = NightlyRun.scn(root);
      }
      assertThat(NightlyRun.awaitResumePast(c, name, afterSmall, Duration.ofMinutes(3)))
          .as("the small transactions are acknowledged before the large one")
          .isTrue();

      // one large committed transaction, then one more small one behind it
      String bigXid;
      try (Statement s = w.createStatement()) {
        s.executeUpdate(
            "INSERT INTO "
                + table
                + " SELECT 1000000 + LEVEL, RPAD('big-' || LEVEL || '-', "
                + PAD
                + ", 'x') FROM dual CONNECT BY LEVEL <= "
                + rows);
        try (ResultSet rs =
            s.executeQuery("SELECT DBMS_TRANSACTION.LOCAL_TRANSACTION_ID FROM dual")) {
          rs.next();
          bigXid = rs.getString(1);
        }
      }
      w.commit();
      try (Statement s = w.createStatement()) {
        s.executeUpdate("INSERT INTO " + table + " VALUES (" + AFTER_ID + ", 'after')");
      }
      w.commit();
      ev.fault("large-transaction-committed", "xid", bigXid, "rows", rows);

      JsonNode failed = c.awaitTaskState(name, "FAILED", Duration.ofMinutes(12));
      String trace = failed.path("tasks").get(0).path("trace").asText();
      ev.count("stopCode", NightlyRun.cdcCode(trace)).note("stop: " + NightlyRun.firstLine(trace));
      Delivery before = read(c, topic, bigXid, SMALL, Duration.ofSeconds(45));
      ev.count("publishedBeforeRemedy", before.ids.size())
          .count("largeTransactionEventsBeforeRemedy", before.bigEvents);
      assertThat(trace)
          .as("BufferExhaustedException is a typed stop")
          .contains("CDC-4001")
          .contains(messageMustName);
      assertThat(before.bigEvents)
          .as("nothing of the large transaction is published before the stop")
          .isZero();
      assertThat(before.ids).as("only the small transactions before it").hasSize(SMALL);

      // the operator's remedy, then a restart: the whole transaction, each event once
      Map<String, String> fixed = new HashMap<>(config);
      fixed.putAll(remedy);
      c.updateConfig(name, fixed);
      ev.fault("remedy-applied", "config", remedy);
      Thread.sleep(5_000);
      NightlyRun.restartUntilRunning(c, name, Duration.ofMinutes(4));
      Delivery after = read(c, topic, bigXid, SMALL + rows + 1, Duration.ofMinutes(15));
      ev.count("publishedAfterRemedy", after.ids.size())
          .count("largeTransactionEvents", after.bigEvents)
          .count("largeTransactionDistinctIndices", after.bigIndices.size())
          .count("largeTransactionDuplicateIndices", after.bigDuplicates);
      System.out.println(
          name
              + ": stop="
              + NightlyRun.cdcCode(trace)
              + " published after remedy="
              + after.ids.size()
              + " large events="
              + after.bigEvents);
      assertThat(after.bigIndices)
          .as("every event of the large transaction")
          .hasSize(rows)
          .first()
          .isEqualTo(0);
      assertThat(after.bigIndices.last()).isEqualTo(rows - 1);
      assertThat(after.bigDuplicates).as("no event of it delivered twice").isZero();
      assertThat(after.ids).as("every row, the one behind it included").hasSize(SMALL + rows + 1);
      assertThat(after.ids).contains(AFTER_ID);
    } catch (Throwable t) {
      ev.failed(t);
      throw t;
    } finally {
      ev.write();
      SchemaFixtures.drop(db, OracleTestDatabase.PDB1, schema);
    }
  }

  /** What a read_committed consumer finds on the topic: row ids, and the large transaction. */
  static final class Delivery {
    final TreeSet<Integer> ids = new TreeSet<>();
    final TreeSet<Integer> bigIndices = new TreeSet<>();
    long bigEvents;
    long bigDuplicates;
  }

  /**
   * Reads the topic from the start until {@code expected} distinct rows are seen and nothing new
   * arrived for 20 seconds, or the timeout passes. Records are parsed and dropped as they come.
   */
  private static Delivery read(
      ConnectCluster c, String topic, String bigXid, int expected, Duration timeout)
      throws Exception {
    Delivery d = new Delivery();
    try (KafkaConsumer<String, String> consumer =
        c.consumer("spill-read-" + System.nanoTime(), topic)) {
      long deadline = System.currentTimeMillis() + timeout.toMillis();
      long lastNew = System.currentTimeMillis();
      while (System.currentTimeMillis() < deadline) {
        ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(500));
        if (batch.isEmpty()) {
          if (System.currentTimeMillis() - lastNew > 20_000
              && (d.ids.size() >= expected || System.currentTimeMillis() - lastNew > 60_000)) {
            break;
          }
          continue;
        }
        lastNew = System.currentTimeMillis();
        for (ConsumerRecord<String, String> r : batch) {
          if (r.value() == null) {
            continue;
          }
          JsonNode v = ConnectCluster.json(r.value());
          BigDecimal id = NightlyRun.number(v.path("after").path("ID"));
          if (id != null) {
            d.ids.add(id.intValue());
          }
          if (bigXid.equals(v.path("source").path("txId").asText())) {
            d.bigEvents++;
            String idx = NightlyRun.header(r, "cdc.event_index");
            if (idx != null && !d.bigIndices.add(Integer.parseInt(idx))) {
              d.bigDuplicates++;
            }
          }
        }
      }
    }
    return d;
  }
}
