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
package sh.oso.connect.oracle.bench.check;

import com.fasterxml.jackson.databind.JsonNode;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * The correctness oracle (testing strategy section 3, ADR-0012). Consumes the table topics with
 * read_committed until nothing new arrives, materialises them, then asserts: (1) each table's
 * materialised state equals the database AS OF the highest commit SCN seen; (2) the committed
 * transactions in the ledger (ops > 0) are exactly the transactions seen in Kafka; (3) per
 * transaction the event indices are contiguous from zero to event_count minus one with no
 * duplicates, and commit SCNs never go backwards within a partition.
 */
public final class CorrectnessCheck {

  private final String bootstrapServers;
  private final List<String> topics;
  private final Connection db;
  private final String owner;
  private final List<String> tables;
  private final String ledgerTable;
  private final Duration timeout;
  private final Duration idle;
  private long stateScn;

  public CorrectnessCheck(
      String bootstrapServers,
      List<String> topics,
      Connection db,
      String owner,
      List<String> tables,
      String ledgerTable,
      Duration timeout,
      Duration idle) {
    this.bootstrapServers = bootstrapServers;
    this.topics = topics;
    this.db = db;
    this.owner = owner;
    this.tables = tables;
    this.ledgerTable = ledgerTable;
    this.timeout = timeout;
    this.idle = idle;
  }

  /**
   * Compares the tables AS OF {@code scn} instead of the newest commit SCN consumed. Valid only
   * when nothing was committed to the tables between that commit and {@code scn} (a quiesced
   * workload), and useful when the newest commit is too close to an instance restart or a DDL for a
   * flashback read (ORA-01466). An SCN below the newest commit consumed makes the check fail.
   */
  public CorrectnessCheck stateAt(long scn) {
    this.stateScn = scn;
    return this;
  }

  public CheckReport run() throws Exception {
    CheckReport report = new CheckReport();
    Materialiser m = consume(report);
    report.recordsConsumed(m.records());
    report.tombstones(m.tombstones());
    report.checkScn(m.maxCommitScn());

    // 2. committed transaction set versus the ledger
    if (ledgerTable != null) {
      Set<String> ledger = ledgerXids();
      Set<String> seen = m.xids();
      Set<String> missing = new TreeSet<>(ledger);
      missing.removeAll(seen);
      Set<String> extra = new TreeSet<>(seen);
      extra.removeAll(ledger);
      report.transactions().put("ledger", ledger.size());
      report.transactions().put("seen", seen.size());
      report.transactions().put("missingFromKafka", missing.size());
      report.transactions().put("notInLedger", extra.size());
      if (!missing.isEmpty()) {
        report.transactions().put("missingExamples", missing.stream().limit(5).toList());
        report.fail(missing.size() + " committed transactions in the ledger are not in Kafka");
      }
      if (!extra.isEmpty()) {
        report.transactions().put("notInLedgerExamples", extra.stream().limit(5).toList());
        report.fail(
            extra.size() + " transactions in Kafka are not in the ledger (uncommitted or foreign)");
      }
    }

    // 3. event invariants
    long gaps = 0;
    long duplicates = 0;
    long countMismatch = 0;
    for (Map.Entry<String, Materialiser.TxFacts> e : m.transactions().entrySet()) {
      Materialiser.TxFacts f = e.getValue();
      int expected = f.eventCount;
      boolean dup = false;
      for (Map.Entry<String, Integer> ic : f.opCopies.entrySet()) {
        dup |= ic.getValue() > 1;
      }
      if (dup) {
        duplicates++;
      }
      if (expected >= 0) {
        for (int i = 0; i < expected; i++) {
          if (!f.indices.contains(i)) {
            gaps++;
          }
        }
        if (f.indices.size() > expected) {
          countMismatch++;
        }
      }
    }
    report.invariants().put("transactionsWithDuplicateEvents", duplicates);
    report.invariants().put("eventGaps", gaps);
    report.invariants().put("eventCountMismatches", countMismatch);
    report.invariants().put("orderViolations", m.orderViolations().size());
    if (gaps > 0) {
      report.fail(gaps + " event indices are missing inside delivered transactions");
    }
    if (countMismatch > 0) {
      report.fail(countMismatch + " transactions delivered more events than their event_count");
    }
    if (!m.orderViolations().isEmpty()) {
      report
          .invariants()
          .put(
              "orderViolationExamples",
              m.orderViolations().subList(0, Math.min(5, m.orderViolations().size())));
      report.fail("commit SCN went backwards inside a partition");
    }

    // 1. state at the check SCN
    if (m.maxCommitScn() <= 0) {
      report.inconclusive("no change records were consumed, so there is no check SCN");
      return report;
    }
    long at = m.maxCommitScn();
    if (stateScn > 0) {
      if (stateScn < at) {
        report.fail("the state SCN " + stateScn + " is below the newest commit consumed, " + at);
        return report;
      }
      at = stateScn;
    }
    StateChecker checker = new StateChecker(db, owner);
    Map<String, Object> tablesOut = new LinkedHashMap<>();
    for (String table : tables) {
      Map<String, JsonNode> rows = m.tables().getOrDefault(owner + "." + table, Map.of());
      try {
        StateChecker.TableDiff d = checker.compare(table, rows, at);
        Map<String, Object> t = new LinkedHashMap<>();
        t.put("databaseRows", d.databaseRows);
        t.put("kafkaRows", d.kafkaRows);
        t.put("missingInKafka", d.missingCount);
        t.put("extraInKafka", d.extraCount);
        t.put("mismatchedValues", d.mismatchCount);
        if (!d.clean()) {
          t.put("missingExamples", d.missingInKafka);
          t.put("extraExamples", d.extraInKafka);
          t.put("mismatchExamples", d.mismatched);
          report.fail(
              table
                  + ": "
                  + d.missingCount
                  + " missing, "
                  + d.extraCount
                  + " extra, "
                  + d.mismatchCount
                  + " mismatched values");
        }
        tablesOut.put(table, t);
      } catch (SQLException e) {
        if (StateChecker.isSnapshotTooOld(e)) {
          report.inconclusive(
              "AS OF SCN "
                  + at
                  + " is no longer available (ORA-"
                  + e.getErrorCode()
                  + (e.getErrorCode() == 1466
                      ? "); the table's definition changed near that SCN"
                      : "); increase undo retention"));
          tablesOut.put(table, Map.of("inconclusive", e.getMessage().split("\n")[0]));
        } else {
          throw e;
        }
      }
    }
    report.state().put("checkScn", at);
    report.state().put("tables", tablesOut);
    return report;
  }

  private Materialiser consume(CheckReport report) throws Exception {
    Properties p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "oracle-cdc-check-" + System.nanoTime());
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5000);
    Materialiser m = new Materialiser();
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p)) {
      consumer.subscribe(topics);
      long deadline = System.currentTimeMillis() + timeout.toMillis();
      long lastNew = System.currentTimeMillis();
      boolean any = false;
      while (System.currentTimeMillis() < deadline) {
        ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(500));
        if (batch.isEmpty()) {
          if (any && System.currentTimeMillis() - lastNew > idle.toMillis()) {
            break;
          }
          continue;
        }
        any = true;
        lastNew = System.currentTimeMillis();
        for (ConsumerRecord<String, String> r : batch) {
          m.apply(new RecordJson(r));
        }
      }
      if (!any) {
        report.inconclusive("no records arrived on " + topics + " within " + timeout);
      }
    }
    return m;
  }

  private Set<String> ledgerXids() throws SQLException {
    Set<String> out = new TreeSet<>();
    try (PreparedStatement ps =
            db.prepareStatement("SELECT xid FROM " + owner + "." + ledgerTable + " WHERE ops > 0");
        ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        out.add(rs.getString(1));
      }
    }
    return out;
  }
}
