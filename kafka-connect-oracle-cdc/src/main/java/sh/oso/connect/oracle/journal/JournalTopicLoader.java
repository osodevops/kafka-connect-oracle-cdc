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
package sh.oso.connect.oracle.journal;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;
import sh.oso.connect.oracle.core.buffer.JournalChunk;
import sh.oso.connect.oracle.core.buffer.JournalFrames;
import sh.oso.connect.oracle.core.errors.JournalCorruptionException;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.position.Position;

/**
 * Turns the journal topic's content into buffer entries for the position being resumed (CORE-TX-5,
 * ADR-0003 amendment): chunks of a newer generation than the position's are stale writes of a task
 * that never committed an offset; frames at or after {@code resume_scn} are dropped because mining
 * produces them again; what remains must be contiguous chunks from zero.
 */
public final class JournalTopicLoader {

  /** What to restore and which stale records to tombstone. */
  public record Loaded(
      Map<TxKey, List<JournalChunk>> restore, List<JournalRecords.ChunkKey> stale) {
    public int chunks() {
      return restore.values().stream().mapToInt(List::size).sum();
    }
  }

  private final Converter keys;
  private final Converter values;
  private final String server;

  public JournalTopicLoader(Converter keys, Converter values, String server) {
    this.keys = keys;
    this.values = values;
    this.server = server;
  }

  public Loaded load(List<ConsumerRecord<byte[], byte[]>> records, Position position) {
    // last record per key wins, like compaction; a tombstone removes the key
    Map<JournalRecords.ChunkKey, JournalChunk> latest = new LinkedHashMap<>();
    for (ConsumerRecord<byte[], byte[]> r : records) {
      JournalRecords.ChunkKey k;
      try {
        SchemaAndValue key = keys.toConnectData(r.topic(), r.key());
        k = JournalRecords.parseKey(key.value(), server);
      } catch (RuntimeException e) {
        throw new JournalCorruptionException(
            "Journal record at offset "
                + r.offset()
                + " of "
                + r.topic()
                + " has an unreadable key: "
                + e.getMessage(),
            "Set cdc.journal.converter to the converter the worker uses for this connector"
                + " (key.converter), or reset the journal topic.",
            e);
      }
      if (k == null) {
        continue; // another connector's records on a shared topic
      }
      if (r.value() == null) {
        latest.remove(k);
        continue;
      }
      try {
        SchemaAndValue value = values.toConnectData(r.topic(), r.value());
        latest.put(k, JournalRecords.parseValue(k, value.value()));
      } catch (RuntimeException e) {
        throw new JournalCorruptionException(
            "Journal chunk "
                + k.chunk()
                + " of "
                + k.key()
                + " at offset "
                + r.offset()
                + " cannot be read: "
                + e.getMessage(),
            "Set cdc.journal.converter to the converter the worker uses for this connector"
                + " (value.converter); if it already matches, the topic holds a damaged record.",
            e);
      }
    }
    Map<TxKey, TreeMap<Integer, JournalChunk>> byTx = new LinkedHashMap<>();
    List<JournalRecords.ChunkKey> stale = new ArrayList<>();
    for (Map.Entry<JournalRecords.ChunkKey, JournalChunk> e : latest.entrySet()) {
      JournalRecords.ChunkKey k = e.getKey();
      if (k.generation() > position.journalGeneration()
          || !before(e.getValue().first(), position.resumePoint())) {
        stale.add(k); // never acknowledged before the position, or re-mined from the position
        continue;
      }
      TreeMap<Integer, JournalChunk> chunks = byTx.computeIfAbsent(k.key(), x -> new TreeMap<>());
      JournalChunk other = chunks.get(k.chunk());
      if (other != null) {
        // the same chunk number under two generations: keep the newest acknowledged one
        if (other.generation() > k.generation()) {
          stale.add(k);
          continue;
        }
        stale.add(new JournalRecords.ChunkKey(other.key(), other.chunk(), other.generation()));
      }
      chunks.put(k.chunk(), e.getValue());
    }
    Map<TxKey, List<JournalChunk>> restore = new LinkedHashMap<>();
    for (Map.Entry<TxKey, TreeMap<Integer, JournalChunk>> e : byTx.entrySet()) {
      List<JournalChunk> ordered = new ArrayList<>();
      int expected = 0;
      for (JournalChunk c : e.getValue().values()) {
        if (c.chunk() != expected) {
          throw new JournalCorruptionException(
              "Journal for transaction "
                  + e.getKey()
                  + " is missing chunk "
                  + expected
                  + " (next found is "
                  + c.chunk()
                  + ").",
              "The journal topic lost a chunk, probably to retention or compaction settings."
                  + " Restore cleanup.policy=compact with unlimited retention on the journal"
                  + " topic, then reset the offsets to a position before the transaction started.");
        }
        expected++;
        JournalChunk trimmed = trim(c, position.resumePoint());
        if (trimmed != null) {
          ordered.add(trimmed);
        }
      }
      if (!ordered.isEmpty()) {
        restore.put(e.getKey(), ordered);
      }
    }
    return new Loaded(restore, stale);
  }

  /**
   * True when {@code id} was written before the resume point (ADR-0014: redo order when both carry
   * a redo byte address, SCN order otherwise), so mining from the position will not produce it
   * again.
   */
  static boolean before(RedoRecordId id, RedoRecordId resume) {
    if (id.hasRba() && resume.hasRba()) {
      return id.compareTo(resume) < 0;
    }
    return id.scn() < resume.scn();
  }

  /** Drops the frames mining will produce again; null when nothing of the chunk remains. */
  static JournalChunk trim(JournalChunk c, RedoRecordId resume) {
    if (before(c.last(), resume)) {
      return c;
    }
    List<JournalFrames.Frame> kept = new ArrayList<>();
    int undos = 0;
    for (JournalFrames.Frame f :
        JournalFrames.decode(c.payload(), "Journal chunk " + c.chunk() + " of " + c.key())) {
      if (f.id() != null && before(f.id(), resume)) {
        kept.add(f);
        if (f.isUndo()) {
          undos++;
        }
      }
    }
    if (kept.isEmpty()) {
      return null;
    }
    return new JournalChunk(
        c.key(),
        c.chunk(),
        c.generation(),
        kept.get(0).id(),
        kept.get(kept.size() - 1).id(),
        kept.size() - undos,
        undos,
        c.firstCaptured(),
        c.startId(),
        c.thread(),
        c.username(),
        c.clientId(),
        JournalFrames.encode(kept));
  }
}
