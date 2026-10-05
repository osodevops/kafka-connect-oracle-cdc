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
package sh.oso.connect.oracle.ops;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An operational event the connector publishes about itself on the ops topic (PRD-01 SRC-OPS): what
 * happened, when, at which position, with string details. Wire names are the PRD's and are stable;
 * the record schema carries a version so consumers can tell shapes apart.
 */
public record OpsEvent(Type type, long tsMs, Long resumeScn, Map<String, String> details) {

  public static final int SCHEMA_VERSION = 1;

  /** Event types; {@link #wire()} is the string written to the record. */
  public enum Type {
    STARTUP("startup"),
    STOP("stop"),
    POSITION_COMMITTED("position-committed"),
    LOG_SWITCH_DETECTED("log-switch-detected"),
    THREAD_STATE_CHANGED("thread-state-changed"),
    DDL_SEEN("ddl-seen"),
    DDL_APPLIED("ddl-applied"),
    TABLE_ADDED("table-added"),
    TABLE_REMOVED("table-removed"),
    IDS_REFRESHED("ids-refreshed"),
    RECONNECTED("reconnected"),
    UNSUPPORTED_ROW("unsupported-row"),
    DECODE_ERROR_DLQ("decode-error-dlq"),
    TRANSACTION_JOURNALED("transaction-journaled"),
    TRANSACTION_DISCARDED("transaction-discarded"),
    TRANSACTION_ORPHAN_RELEASED("transaction-orphan-released"),
    TRANSACTION_SPLIT("transaction-split"),
    SNAPSHOT_CHUNK_DONE("snapshot-chunk-done"),
    SNAPSHOT_COMPLETE("snapshot-complete"),
    SIGNAL_ACK("signal-ack"),
    OFFSETS_SET("offsets-set");

    private final String wire;

    Type(String wire) {
      this.wire = wire;
    }

    public String wire() {
      return wire;
    }
  }

  public OpsEvent {
    details = Map.copyOf(new LinkedHashMap<>(details));
  }

  /** Builds an event from alternating key and value arguments; null values are dropped. */
  public static OpsEvent of(Type type, long tsMs, Long resumeScn, String... kv) {
    Map<String, String> m = new LinkedHashMap<>();
    for (int i = 0; i + 1 < kv.length; i += 2) {
      if (kv[i + 1] != null) {
        m.put(kv[i], kv[i + 1]);
      }
    }
    return new OpsEvent(type, tsMs, resumeScn, m);
  }
}
