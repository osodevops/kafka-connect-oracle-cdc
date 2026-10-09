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
package sh.oso.connect.oracle.core.position;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import sh.oso.connect.oracle.core.errors.OracleCdcCorruptionException;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.TxKey;
import sh.oso.connect.oracle.core.model.Xid;

/**
 * Kafka Connect offset map form of a {@link Position}: flat keys with JSON-friendly values. One
 * reader per format version; an unknown higher version is refused rather than guessed, unknown keys
 * of a known version are preserved.
 */
public final class PositionCodec {

  static final String V = "v";
  static final String RESUME_SCN = "resume_scn";
  static final String LAST_COMMIT_SCN = "last_commit_scn";
  static final String LAST_COMMIT_XID = "last_commit_xid";
  static final String LAST_COMMIT_THREAD = "last_commit_thread";
  static final String EVENT_INDEX = "event_index";
  static final String JOURNAL_GENERATION = "journal_generation";
  static final String SCHEMA_EPOCH = "schema_epoch";
  static final String DBID = "dbid";
  static final String RESETLOGS_SCN = "resetlogs_scn";
  static final String RELEASED_XIDS = "released_xids";
  static final String SNAPSHOT = "snapshot";
  static final String RESUME_RS_ID = "resume_rs_id";
  static final String RESUME_SSN = "resume_ssn";
  static final String LAST_COMMIT_RS_ID = "last_commit_rs_id";
  static final String LAST_COMMIT_SSN = "last_commit_ssn";

  /** ADR-0026: one mark per redo thread, JSON; written only for a position over several threads. */
  static final String THREADS = "threads";

  private static final Set<String> KNOWN_V1 =
      Set.of(
          V,
          RESUME_SCN,
          LAST_COMMIT_SCN,
          LAST_COMMIT_XID,
          LAST_COMMIT_THREAD,
          EVENT_INDEX,
          JOURNAL_GENERATION,
          SCHEMA_EPOCH,
          DBID,
          RESETLOGS_SCN,
          RELEASED_XIDS,
          SNAPSHOT,
          RESUME_RS_ID,
          RESUME_SSN,
          LAST_COMMIT_RS_ID,
          LAST_COMMIT_SSN,
          THREADS);

  private PositionCodec() {}

  public static Map<String, Object> write(Position p) {
    Map<String, Object> m = new LinkedHashMap<>();
    m.put(V, p.version());
    m.put(RESUME_SCN, p.resumeScn());
    m.put(LAST_COMMIT_SCN, p.lastCommitScn());
    m.put(LAST_COMMIT_XID, p.lastCommitKey() == null ? null : p.lastCommitKey().toString());
    m.put(LAST_COMMIT_THREAD, p.lastCommitThread());
    m.put(EVENT_INDEX, p.eventIndex());
    m.put(JOURNAL_GENERATION, p.journalGeneration());
    m.put(SCHEMA_EPOCH, p.schemaEpoch());
    m.put(DBID, p.identity().dbid());
    m.put(RESETLOGS_SCN, p.identity().resetlogsScn());
    m.put(RELEASED_XIDS, p.released().isEmpty() ? null : String.join(",", p.released()));
    m.put(SNAPSHOT, p.snapshot() == null ? null : snapshotJson(p.snapshot()));
    m.put(RESUME_RS_ID, p.resumeRsId());
    m.put(RESUME_SSN, p.resumeRsId() == null ? null : p.resumeSsn());
    m.put(LAST_COMMIT_RS_ID, p.lastCommitRsId());
    m.put(LAST_COMMIT_SSN, p.lastCommitRsId() == null ? null : p.lastCommitSsn());
    if (p.perThread()) {
      m.put(THREADS, threadsJson(p.threads())); // absent for one thread: the offset is unchanged
    }
    for (Map.Entry<String, Object> e : p.extras().entrySet()) {
      m.putIfAbsent(e.getKey(), e.getValue());
    }
    return m;
  }

  public static Position read(Map<String, ?> m) {
    if (m == null || m.isEmpty()) {
      throw new OracleCdcCorruptionException(
          "The stored offset is empty",
          "Delete the connector's offsets or set them with oracle-cdc-admin.");
    }
    int version = integer(m, V, -1);
    switch (version) {
      case 1:
        return readV1(m);
      case -1:
        throw new OracleCdcCorruptionException(
            "The stored offset has no format version: " + m.keySet(),
            "The offset was not written by this connector; reset it with oracle-cdc-admin.");
      default:
        throw new OracleCdcCorruptionException(
            "The stored offset is format version "
                + version
                + "; this connector reads up to "
                + Position.CURRENT_VERSION,
            "Upgrade the connector, or downgrade by at most one minor version.");
    }
  }

  @SuppressWarnings("unchecked")
  private static Position readV1(Map<String, ?> m) {
    String xid = (String) m.get(LAST_COMMIT_XID);
    List<String> released = new ArrayList<>();
    Object rel = m.get(RELEASED_XIDS);
    if (rel instanceof List<?> l) { // pre-release form, read for completeness
      for (Object o : l) {
        released.add(String.valueOf(o));
      }
    } else if (rel instanceof String str && !str.isBlank()) {
      for (String x : str.split(",")) {
        if (!x.isBlank()) {
          released.add(x.trim());
        }
      }
    }
    Map<String, Object> extras = new LinkedHashMap<>();
    for (Map.Entry<String, ?> e : m.entrySet()) {
      if (!KNOWN_V1.contains(e.getKey())) {
        extras.put(e.getKey(), e.getValue());
      }
    }
    Object snap = m.get(SNAPSHOT);
    Position legacy =
        new Position(
            1,
            longValue(m, RESUME_SCN),
            longValue(m, LAST_COMMIT_SCN),
            xid == null ? null : parseKey(xid),
            integer(m, LAST_COMMIT_THREAD, 0),
            integer(m, EVENT_INDEX, 0),
            longValue(m, JOURNAL_GENERATION),
            longValue(m, SCHEMA_EPOCH),
            new DatabaseIdentity(longValue(m, DBID), longValue(m, RESETLOGS_SCN)),
            released,
            snapshot(snap),
            extras,
            text(m, RESUME_RS_ID),
            text(m, RESUME_RS_ID) == null ? 0 : longValue(m, RESUME_SSN),
            text(m, LAST_COMMIT_RS_ID),
            text(m, LAST_COMMIT_RS_ID) == null ? 0 : longValue(m, LAST_COMMIT_SSN));
    java.util.SortedMap<Integer, ThreadMark> threads = threads(m.get(THREADS));
    return agrees(threads, legacy) ? legacy.withThreads(threads) : legacy;
  }

  /**
   * ADR-0026: the per-thread block is trusted only while it agrees with the legacy keys, which
   * every version writes and keeps authoritative: the lowest resume SCN of the threads is the
   * resume SCN, and the last acknowledged commit is the commit of its thread. A block that an older
   * version carried along unchanged in the extras while it advanced the legacy keys fails this, and
   * the position is then read as a single-thread one from the legacy keys.
   */
  static boolean agrees(java.util.SortedMap<Integer, ThreadMark> threads, Position legacy) {
    if (threads.isEmpty()) {
      return false;
    }
    long lowest = Long.MAX_VALUE;
    for (ThreadMark t : threads.values()) {
      lowest = Math.min(lowest, t.resume().scn());
    }
    if (lowest != legacy.resumeScn()) {
      return false;
    }
    if (!legacy.hasCommit()) {
      return threads.values().stream().noneMatch(ThreadMark::hasCommit);
    }
    ThreadMark last = threads.get(legacy.lastCommitThread());
    return last != null
        && last.hasCommit()
        && last.commitKey().equals(legacy.lastCommitKey())
        && last.commit().scn() == legacy.lastCommitScn();
  }

  static String threadsJson(java.util.SortedMap<Integer, ThreadMark> threads) {
    Map<String, Object> out = new LinkedHashMap<>();
    for (Map.Entry<Integer, ThreadMark> e : threads.entrySet()) {
      ThreadMark t = e.getValue();
      Map<String, Object> mark = new LinkedHashMap<>();
      mark.put("resume_scn", t.resume().scn());
      if (t.resume().hasRba()) {
        mark.put("resume_rs_id", t.resume().rsId());
        mark.put("resume_ssn", t.resume().ssn());
      }
      if (t.hasCommit()) {
        mark.put("commit_scn", t.commit().scn());
        if (t.commit().hasRba()) {
          mark.put("commit_rs_id", t.commit().rsId());
          mark.put("commit_ssn", t.commit().ssn());
        }
        mark.put("commit_xid", t.commitKey().toString());
      }
      out.put(String.valueOf(e.getKey()), mark);
    }
    try {
      return JSON.writeValueAsString(out);
    } catch (com.fasterxml.jackson.core.JsonProcessingException ex) {
      throw new OracleCdcCorruptionException(
          "The per-thread block cannot be serialised: " + ex.getMessage(),
          "Report the connector logs; the position is not written.",
          ex);
    }
  }

  @SuppressWarnings("unchecked")
  static java.util.SortedMap<Integer, ThreadMark> threads(Object raw) {
    java.util.SortedMap<Integer, ThreadMark> out = new java.util.TreeMap<>();
    if (raw == null || raw.toString().isBlank()) {
      return out;
    }
    try {
      Map<String, Object> block =
          raw instanceof Map<?, ?> rm
              ? (Map<String, Object>) rm
              : JSON.readValue(raw.toString(), Map.class);
      for (Map.Entry<String, Object> e : block.entrySet()) {
        Map<String, ?> mark = (Map<String, ?>) e.getValue();
        RedoRecordId resume =
            new RedoRecordId(
                longValue(mark, "resume_scn"),
                text(mark, "resume_rs_id"),
                longValue(mark, "resume_ssn"));
        String xid = text(mark, "commit_xid");
        RedoRecordId commit =
            xid == null
                ? null
                : new RedoRecordId(
                    longValue(mark, "commit_scn"),
                    text(mark, "commit_rs_id"),
                    longValue(mark, "commit_ssn"));
        out.put(
            Integer.parseInt(e.getKey()),
            new ThreadMark(resume, commit, xid == null ? null : parseKey(xid)));
      }
      return out;
    } catch (java.io.IOException | RuntimeException ex) {
      throw new OracleCdcCorruptionException(
          "The stored offset's per-thread block cannot be read: " + raw,
          "Reset the connector's offsets with oracle-cdc-admin.",
          ex);
    }
  }

  private static String text(Map<String, ?> m, String key) {
    Object v = m.get(key);
    return v == null || v.toString().isBlank() ? null : v.toString();
  }

  private static final com.fasterxml.jackson.databind.ObjectMapper JSON =
      new com.fasterxml.jackson.databind.ObjectMapper();

  static String snapshotJson(Map<String, Object> snapshot) {
    try {
      return JSON.writeValueAsString(snapshot);
    } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
      throw new OracleCdcCorruptionException(
          "The snapshot block cannot be serialised: " + e.getMessage(),
          "Report the connector logs; the position is not written.",
          e);
    }
  }

  @SuppressWarnings("unchecked")
  static Map<String, Object> snapshot(Object raw) {
    if (raw == null) {
      return null;
    }
    if (raw instanceof Map<?, ?> sm) {
      return (Map<String, Object>) sm;
    }
    try {
      return JSON.readValue(raw.toString(), Map.class);
    } catch (java.io.IOException e) {
      throw new OracleCdcCorruptionException(
          "The stored offset's snapshot block is not JSON: " + raw,
          "Reset the connector's offsets with oracle-cdc-admin.",
          e);
    }
  }

  static TxKey parseKey(String s) {
    int colon = s.indexOf(':');
    if (colon < 0) {
      return new TxKey(0, Xid.parse(s));
    }
    return new TxKey(Integer.parseInt(s.substring(0, colon)), Xid.parse(s.substring(colon + 1)));
  }

  private static long longValue(Map<String, ?> m, String key) {
    Object v = m.get(key);
    if (v == null) {
      return 0;
    }
    if (v instanceof Number n) {
      return n.longValue();
    }
    return Long.parseLong(v.toString());
  }

  private static int integer(Map<String, ?> m, String key, int dflt) {
    Object v = m.get(key);
    if (v == null) {
      return dflt;
    }
    if (v instanceof Number n) {
      return n.intValue();
    }
    return Integer.parseInt(v.toString());
  }
}
