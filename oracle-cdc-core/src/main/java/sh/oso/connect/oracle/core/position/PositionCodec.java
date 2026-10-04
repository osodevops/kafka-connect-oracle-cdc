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
          SNAPSHOT);

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
    m.put(RELEASED_XIDS, new ArrayList<>(p.released()));
    m.put(SNAPSHOT, p.snapshot());
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
    if (rel instanceof List<?> l) {
      for (Object o : l) {
        released.add(String.valueOf(o));
      }
    }
    Map<String, Object> extras = new LinkedHashMap<>();
    for (Map.Entry<String, ?> e : m.entrySet()) {
      if (!KNOWN_V1.contains(e.getKey())) {
        extras.put(e.getKey(), e.getValue());
      }
    }
    Object snap = m.get(SNAPSHOT);
    return new Position(
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
        snap instanceof Map<?, ?> sm ? (Map<String, Object>) sm : null,
        extras);
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
