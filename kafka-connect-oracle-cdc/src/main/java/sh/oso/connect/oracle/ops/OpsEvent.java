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
import java.util.List;
import java.util.Map;

/**
 * An operational event the connector publishes about itself on the ops topic (PRD-01 SRC-OPS): what
 * happened, when, at which position, with string details. Wire names are the PRD's and are stable;
 * the record schema carries a version so consumers can tell shapes apart.
 */
public record OpsEvent(Type type, long tsMs, Long resumeScn, Map<String, String> details) {

  public static final int SCHEMA_VERSION = 1;

  /**
   * Event types; {@link #wire()} is the string written to the record. A type built with a
   * description is emitted by the connector today; a type built with its wire name alone is
   * reserved for a feature that does not exist yet and never appears on the topic. The event table
   * of website/docs/reference/ops-topic.md is generated from these (OpsTopicReferenceTest), and the
   * same test fails when a reserved type is emitted or a described type is not.
   */
  public enum Type {
    STARTUP(
        "startup",
        "The task started and its start position is durable.",
        "resume_scn",
        "last_commit (absent on a fresh start)",
        "database",
        "version"),
    STOP(
        "stop",
        "The capture engine or the snapshot publisher stopped with an error; the task fails right"
            + " after this record.",
        "exception",
        "message",
        "code (for example CDC-2002; absent for an error without a code)",
        "runbook",
        "operator_action"),
    POSITION_COMMITTED("position-committed"),
    LOG_SWITCH_DETECTED("log-switch-detected"),
    THREAD_STATE_CHANGED("thread-state-changed"),
    DDL_SEEN(
        "ddl-seen",
        "A DDL statement by an owner that Oracle does not maintain was mined.",
        "pdb",
        "owner",
        "object",
        "scn",
        "sql (truncated to 2,000 characters)"),
    DDL_APPLIED(
        "ddl-applied",
        "A DDL gave a captured table a new schema version, or dropped or renamed it away.",
        "pdb",
        "owner",
        "object",
        "scn",
        "version (the new version number, or removed)",
        "columns (absent when removed)"),
    DICTIONARY_REPLAY(
        "dictionary-replay",
        "A step was mined again with a data dictionary from the redo, because rows of these tables"
            + " were written before a later DDL on them.",
        "from_scn",
        "to_scn",
        "tables"),
    DICTIONARY_BUILD(
        "dictionary-build",
        "A dictionary build into the redo ran, failed, or was switched off because the connector"
            + " user cannot execute DBMS_LOGMNR_D.",
        "status (built, failed or disabled)",
        "millis (when built)",
        "message (when failed or disabled)"),
    TABLE_ADDED(
        "table-added",
        "A refresh of the object ids (after a CREATE TABLE, a rename or a refresh-tables signal)"
            + " added a table matching the include patterns; it is captured from the next step.",
        "table",
        "snapshot (true when the table is snapshotted because it may already hold rows)"),
    TABLE_REMOVED(
        "table-removed",
        "A refresh of the object ids removed a table from the captured set (dropped, or renamed"
            + " away).",
        "table"),
    IDS_REFRESHED(
        "ids-refreshed",
        "The captured object ids were resolved again, after a CREATE TABLE, DROP TABLE or"
            + " partition DDL, or a refresh-tables signal.",
        "owners"),
    RECONNECTED(
        "reconnected", "The database sessions were reopened after a transient error.", "cause"),
    UNSUPPORTED_ROW(
        "unsupported-row",
        "LogMiner marked a row of a captured table unsupported and cdc.on.decode.error is dlq.",
        "table",
        "xid",
        "scn",
        "status",
        "info"),
    DECODE_ERROR_DLQ(
        "decode-error-dlq",
        "A row of a captured table could not be decoded and cdc.on.decode.error is dlq.",
        "table",
        "xid",
        "scn",
        "operation",
        "error"),
    TRANSACTION_JOURNALED("transaction-journaled"),
    TRANSACTION_DISCARDED(
        "transaction-discarded",
        "A transaction open longer than cdc.transaction.max.age.ms was dropped because"
            + " cdc.transaction.max.age.action is discard.",
        "xid",
        "con_id",
        "user",
        "first_scn",
        "last_scn",
        "events",
        "age_ms"),
    TRANSACTION_ORPHAN_RELEASED(
        "transaction-orphan-released",
        "Orphan detection released a transaction that the database no longer knows, as a"
            + " rollback.",
        "xid",
        "con_id",
        "user",
        "client_id",
        "first_scn",
        "last_scn",
        "events",
        "absent_at_scn",
        "reason"),
    TRANSACTION_SPLIT(
        "transaction-split",
        "In exactly-once mode, an Oracle transaction above cdc.eos.split.max.records or"
            + " cdc.eos.split.max.bytes was delivered in several Kafka transactions.",
        "xid",
        "commit_scn",
        "events",
        "kafka_transactions"),
    SNAPSHOT_CHUNK_DONE(
        "snapshot-chunk-done", "A snapshot chunk was published.", "table", "scn", "rows"),
    SNAPSHOT_COMPLETE(
        "snapshot-complete",
        "A table's snapshot is complete, or the whole snapshot is complete or was stopped by a"
            + " signal.",
        "table (the table, or an asterisk for the whole snapshot)",
        "stopped (true when a snapshot-stop signal ended the snapshot)",
        "skipped (why the table was not read: it no longer exists, or a column type has no"
            + " record mapping under cdc.on.decode.error=dlq)"),
    SIGNAL_ACK(
        "signal-ack",
        "A signal addressed to this connector was handled.",
        "id",
        "type",
        "outcome (ok, rejected, unknown, invalid or failed)",
        "message (when there is one)",
        "mined_to_scn (log-state only)",
        "open_transactions (log-state only)",
        "buffered_events (log-state only)",
        "oldest_open_scn (log-state only)",
        "largest (log-state only)",
        "snapshot_running (log-state only)"),
    OFFSETS_SET(
        "offsets-set",
        "oracle-cdc-admin offsets set changed the stored offset while the connector was stopped:"
            + " one event before the change, one after it or on failure.",
        "connector",
        "command",
        "reason",
        "operator",
        "direction (forward or backward)",
        "previous_resume_scn (none when there was no offset)",
        "new_resume_scn",
        "forgotten_released (when released transactions were forgotten)",
        "outcome (applying, applied or failed)",
        "error (when failed)");

    private final String wire;
    private final String description;
    private final List<String> details;

    /** A reserved type: named for a future feature, never emitted. */
    Type(String wire) {
      this(wire, null);
    }

    /**
     * An emitted type. Each detail is a key of the record's {@code details} map, optionally
     * followed by a note in parentheses.
     */
    Type(String wire, String description, String... details) {
      this.wire = wire;
      this.description = description;
      this.details = List.of(details);
    }

    public String wire() {
      return wire;
    }

    /** True when no code path emits this type yet. */
    public boolean reserved() {
      return description == null;
    }

    /** When the event is written, in one sentence; null for a reserved type. */
    public String description() {
      return description;
    }

    /** The keys of the details map, each with an optional note in parentheses. */
    public List<String> details() {
      return details;
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
