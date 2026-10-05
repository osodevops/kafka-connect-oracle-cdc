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
package sh.oso.connect.oracle.doctor.admin;

import java.io.PrintWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import sh.oso.connect.oracle.core.doctor.MetricsSample;
import sh.oso.connect.oracle.core.doctor.Sizing;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.doctor.metrics.MetricsSource;

/**
 * PRD-05 {@code transactions}: the buffered transactions the task reports over JMX (its {@code
 * LargestTransactions}, the 20 largest by bytes) and the journaled ones in the journal topic, each
 * with age, size, user and whether GV$TRANSACTION still lists it.
 */
public final class TransactionsAdmin {

  private TransactionsAdmin() {}

  /** One listed transaction; {@code container} is -1 when the source does not say. */
  public record Row(
      String xid,
      int container,
      String source,
      Duration age,
      long events,
      long bytes,
      String username,
      String clientId,
      long firstScn,
      Boolean open) {}

  public static int run(AdminSession s, PrintWriter out, MetricsSource metrics) throws Exception {
    if (metrics == null && !s.hasKafka()) {
      throw AdminException.usage(
          "transactions needs --jmx-url for buffered transactions, or broker access for the"
              + " journaled ones, or both.");
    }
    List<Row> rows = new ArrayList<>();
    if (metrics != null) {
      MetricsSample sample = metrics.read(s.config().topicPrefix());
      for (MetricsSample.OpenTransaction t : sample.largest()) {
        rows.add(
            new Row(
                t.xid(),
                -1,
                t.journaled() ? "buffered, journaled" : "buffered",
                Duration.ofMillis(t.ageMillis()),
                t.events(),
                t.bytes(),
                t.username(),
                null,
                t.firstScn(),
                null));
      }
    }
    if (s.hasKafka()) {
      String topic = s.config().journalTopic();
      JournalInspector.Result journal =
          JournalInspector.inspect(
              topic,
              s.kafka("transactions").readAll(topic),
              JournalInspector.converter(s.config(), true),
              JournalInspector.converter(s.config(), false),
              s.config().topicPrefix(),
              s.storedPosition());
      for (JournalInspector.Transaction t : journal.transactions()) {
        String xid = t.key().xid().toString();
        boolean known = rows.stream().anyMatch(r -> r.xid().equals(xid));
        if (known) {
          continue; // the buffered row already says it is journaled
        }
        rows.add(
            new Row(
                xid,
                t.key().srcConId(),
                "journaled",
                s.db().scnAge(t.firstScn()),
                t.events(),
                t.payloadBytes(),
                t.username(),
                t.clientId(),
                t.firstScn(),
                null));
      }
    }
    Set<TxKey> active = s.db().transactions().activeTransactions();
    List<Row> checked = new ArrayList<>();
    for (Row r : rows) {
      boolean open =
          active.stream()
              .anyMatch(
                  k ->
                      k.xid().toString().equals(r.xid())
                          && (r.container() < 0 || k.srcConId() == r.container()));
      checked.add(
          new Row(
              r.xid(),
              r.container(),
              r.source(),
              r.age(),
              r.events(),
              r.bytes(),
              r.username(),
              r.clientId(),
              r.firstScn(),
              open));
    }
    print(checked, metrics != null, s.hasKafka(), out);
    return 0;
  }

  static void print(List<Row> rows, boolean buffered, boolean journaled, PrintWriter out) {
    out.println(
        "Sources: "
            + (buffered ? "buffered (the 20 largest by bytes, over JMX)" : "no JMX, so no buffered")
            + "; "
            + (journaled ? "journaled (the journal topic)" : "no broker access, so no journaled")
            + ".");
    if (rows.isEmpty()) {
      out.println("No open transactions.");
      return;
    }
    out.println(
        "| Transaction | Source | Age | Changes | Size | User | Client | First SCN |"
            + " In GV$TRANSACTION |");
    out.println("|---|---|---|---|---|---|---|---|---|");
    for (Row r : rows) {
      out.println(
          "| "
              + (r.container() < 0 ? "" : r.container() + ":")
              + r.xid()
              + " | "
              + r.source()
              + " | "
              + (r.age() == null ? "unknown" : Sizing.duration(r.age()))
              + " | "
              + r.events()
              + " | "
              + Sizing.bytes(r.bytes())
              + " | "
              + (r.username() == null ? "" : r.username())
              + " | "
              + (r.clientId() == null ? "" : r.clientId())
              + " | "
              + r.firstScn()
              + " | "
              + (Boolean.TRUE.equals(r.open()) ? "yes" : "no")
              + " |");
    }
    if (rows.stream().anyMatch(r -> !Boolean.TRUE.equals(r.open()))) {
      out.println();
      out.println(
          "A transaction missing from GV$TRANSACTION has ended without its COMMIT or ROLLBACK"
              + " being mined yet, or is an orphan: "
              + sh.oso.connect.oracle.core.errors.ErrorCode.ORPHAN_TRANSACTION.runbookUrl());
    }
  }
}
