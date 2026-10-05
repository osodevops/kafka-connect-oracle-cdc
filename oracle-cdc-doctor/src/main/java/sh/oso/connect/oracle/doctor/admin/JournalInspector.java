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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.storage.Converter;
import sh.oso.connect.oracle.OracleCdcSourceConnectorConfig;
import sh.oso.connect.oracle.core.buffer.JournalChunk;
import sh.oso.connect.oracle.core.errors.JournalCorruptionException;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.position.Position;
import sh.oso.connect.oracle.journal.JournalRecords;
import sh.oso.connect.oracle.journal.JournalTopicLoader;

/**
 * PRD-05 {@code journal inspect}: reads the transaction journal topic the way the task does at
 * start (same converter, latest record per key, tombstones remove), summarises each journaled
 * transaction, and runs the task's own loader ({@link JournalTopicLoader}) against the stored
 * offset to say what the next start would restore and whether the journal is consistent (CDC-4002
 * otherwise).
 */
public final class JournalInspector {

  private JournalInspector() {}

  /** One journaled transaction as the topic holds it now. */
  public record Transaction(
      TxKey key,
      List<Integer> chunks,
      List<Long> generations,
      int events,
      int undos,
      long firstScn,
      long lastScn,
      String username,
      String clientId,
      long payloadBytes,
      String nextStart) {}

  /** The whole topic. {@code problem} is null when the journal is consistent. */
  public record Result(
      String topic,
      int records,
      int foreign,
      int tombstones,
      List<Transaction> transactions,
      int staleChunks,
      String problem) {}

  public static Result inspect(
      String topic,
      List<ConsumerRecord<byte[], byte[]>> records,
      Converter keys,
      Converter values,
      String server,
      Position position) {
    int foreign = 0;
    int tombstones = 0;
    String problem = null;
    Map<JournalRecords.ChunkKey, JournalChunk> latest = new LinkedHashMap<>();
    for (ConsumerRecord<byte[], byte[]> r : records) {
      JournalRecords.ChunkKey k;
      try {
        k = JournalRecords.parseKey(keys.toConnectData(topic, r.key()).value(), server);
        if (k == null) {
          foreign++;
          continue;
        }
        if (r.value() == null) {
          tombstones++;
          latest.remove(k);
          continue;
        }
        latest.put(k, JournalRecords.parseValue(k, values.toConnectData(topic, r.value()).value()));
      } catch (RuntimeException e) {
        problem =
            "The record at offset "
                + r.offset()
                + " cannot be read with cdc.journal.converter: "
                + e.getMessage();
        break;
      }
    }
    Map<TxKey, List<JournalChunk>> restore = Map.of();
    int stale = 0;
    if (problem == null && position != null) {
      try {
        JournalTopicLoader.Loaded loaded =
            new JournalTopicLoader(keys, values, server).load(records, position);
        restore = loaded.restore();
        stale = loaded.stale().size();
      } catch (JournalCorruptionException e) {
        problem = e.getMessage();
      }
    }
    Map<TxKey, List<JournalChunk>> byTx = new TreeMap<>();
    for (JournalChunk c : latest.values()) {
      byTx.computeIfAbsent(c.key(), x -> new ArrayList<>()).add(c);
    }
    List<Transaction> txs = new ArrayList<>();
    for (Map.Entry<TxKey, List<JournalChunk>> e : byTx.entrySet()) {
      TreeSet<Integer> numbers = new TreeSet<>();
      TreeSet<Long> generations = new TreeSet<>();
      int events = 0;
      int undos = 0;
      long first = Long.MAX_VALUE;
      long last = 0;
      long bytes = 0;
      JournalChunk any = e.getValue().get(0);
      for (JournalChunk c : e.getValue()) {
        numbers.add(c.chunk());
        generations.add(c.generation());
        events += c.events();
        undos += c.undos();
        first = Math.min(first, c.firstCaptured().scn());
        last = Math.max(last, c.last().scn());
        bytes += c.payload().length;
      }
      String next;
      if (position == null) {
        next = "no stored offset";
      } else if (restore.containsKey(e.getKey())) {
        next = "restored (" + restore.get(e.getKey()).size() + " chunks)";
      } else {
        next = "not restored: mined again or stale";
      }
      txs.add(
          new Transaction(
              e.getKey(),
              List.copyOf(numbers),
              List.copyOf(generations),
              events,
              undos,
              first,
              last,
              any.username(),
              any.clientId(),
              bytes,
              next));
    }
    return new Result(topic, records.size(), foreign, tombstones, txs, stale, problem);
  }

  /** Reads the topic through the session and prints the summary; exit 1 when inconsistent. */
  public static int run(AdminSession s, PrintWriter out) throws Exception {
    OracleCdcSourceConnectorConfig cfg = s.config();
    String topic = cfg.journalTopic();
    List<ConsumerRecord<byte[], byte[]>> records = s.kafka("journal inspect").readAll(topic);
    Position position = s.storedPosition();
    Result r =
        inspect(
            topic,
            records,
            converter(cfg, true),
            converter(cfg, false),
            cfg.topicPrefix(),
            position);
    print(r, position, out);
    return r.problem() == null ? 0 : AdminException.REFUSED;
  }

  static void print(Result r, Position position, PrintWriter out) {
    int live = r.transactions().stream().mapToInt(t -> t.chunks().size()).sum();
    out.println(
        "Journal topic "
            + r.topic()
            + ": "
            + r.records()
            + " records ("
            + r.foreign()
            + " of other connectors, "
            + r.tombstones()
            + " tombstones), "
            + live
            + " live chunks for "
            + r.transactions().size()
            + " transactions.");
    out.println(
        position == null
            ? "No stored offset: the journal is not read until the connector has one."
            : "Stored offset: resume SCN "
                + position.resumeScn()
                + ", journal generation "
                + position.journalGeneration()
                + "; "
                + r.staleChunks()
                + " chunks would be tombstoned at the next start.");
    out.println(r.problem() == null ? "Consistency: OK." : "Consistency: " + r.problem());
    if (r.transactions().isEmpty()) {
      return;
    }
    out.println();
    out.println(
        "| Transaction | Chunks | Generations | Changes | Undo | First SCN | Last SCN | User |"
            + " Client | Next start |");
    out.println("|---|---|---|---|---|---|---|---|---|---|");
    for (Transaction t : r.transactions()) {
      out.println(
          "| "
              + t.key()
              + " | "
              + t.chunks()
              + " | "
              + t.generations()
              + " | "
              + t.events()
              + " | "
              + t.undos()
              + " | "
              + t.firstScn()
              + " | "
              + t.lastScn()
              + " | "
              + (t.username() == null ? "" : t.username())
              + " | "
              + (t.clientId() == null ? "" : t.clientId())
              + " | "
              + t.nextStart()
              + " |");
    }
  }

  /** The converter the task reads the journal with ({@code cdc.journal.converter}). */
  static Converter converter(OracleCdcSourceConnectorConfig cfg, boolean isKey) {
    try {
      Converter c =
          (Converter) Class.forName(cfg.journalConverter()).getDeclaredConstructor().newInstance();
      c.configure(cfg.journalConverterProperties(), isKey);
      return c;
    } catch (ReflectiveOperationException | ClassCastException e) {
      throw AdminException.usage(
          "cdc.journal.converter " + cfg.journalConverter() + " cannot be instantiated: " + e);
    }
  }
}
