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
package sh.oso.connect.oracle.core.errors;

/**
 * Stable error codes for every {@link OracleCdcException}. The runbook slug is the page name under
 * {@code website/docs/operations/runbooks/} (served at {@code /runbooks/<slug>}), so an operator
 * can go from a Connect status message straight to the recovery procedure. Codes are never reused
 * or renumbered; retired codes are kept as deprecated constants.
 *
 * <p>Classification follows PRD-00 section 4.7 (CORE-ERR).
 */
public enum ErrorCode {
  /** ORA-03113, ORA-03114, ORA-12170, ORA-12541, ORA-12514, ORA-01033, ORA-01089 and I/O errors. */
  TRANSIENT_DATABASE(
      "CDC-1001",
      "transient-database",
      true,
      "The connection or the instance became unavailable. The engine reconnects with backoff and"
          + " stops only when cdc.retry.max.time.ms runs out, or at once for an ORA code it does"
          + " not classify."),
  /** ORA-00310, ORA-00334, ORA-01289 or our own cancel: re-mine the same range. */
  MINING_STEP_RETRY(
      "CDC-1002",
      "mining-step-retry",
      true,
      "A LogMiner step failed with an error that mining the same range again fixes. The step is"
          + " mined again; the task stops after more than 20 such failures in a row."),
  /** A redo log sequence is missing for a required thread. */
  LOG_GAP(
      "CDC-2001",
      "log-gap",
      false,
      "The redo logs found for a thread do not cover the range to mine: a sequence is missing, or"
          + " the first or last log falls inside the range."),
  /** A redo log the connector still needs has been deleted. */
  LOG_PURGED(
      "CDC-2002",
      "log-purged",
      false,
      "A redo log the connector still needs is marked deleted in the catalog, or cannot be read"
          + " although the catalog lists it."),
  /** SQL_REDO could not be decoded, or STATUS 2 or 3 for a captured object. */
  DECODE(
      "CDC-3001",
      "decode",
      false,
      "A row of a captured table could not be decoded with cdc.on.decode.error=fail, or a captured"
          + " table cannot be keyed or read."),
  /** CORRUPTED_BLOCKS or MISSING_SCN for a captured object. */
  CORRUPTION(
      "CDC-3002",
      "corruption",
      false,
      "LogMiner reported MISSING_SCN, a spill file failed its integrity check, or the stored offset"
          + " cannot be read."),
  /** cdc.lob.oversize.action=fail found a LOB value above cdc.lob.max.bytes. */
  LOB_TOO_LARGE(
      "CDC-3003",
      "lob-too-large",
      false,
      "A committed LOB value was larger than cdc.lob.max.bytes with cdc.lob.oversize.action=fail."),
  /** The spill cap was exceeded. */
  BUFFER_EXHAUSTED(
      "CDC-4001",
      "buffer-exhausted",
      false,
      "Spilled changes of open transactions exceeded cdc.buffer.spill.max.bytes, or writing to the"
          + " spill directory failed."),
  /** A journal chunk is missing or inconsistent on reload. */
  JOURNAL_CORRUPTION(
      "CDC-4002",
      "journal-corruption",
      false,
      "At start, a journaled transaction is missing a chunk, or a journal record cannot be read"
          + " with cdc.journal.converter."),
  /** Mining queries keep timing out even at a single log window. */
  MINING_STALLED(
      "CDC-4003",
      "mining-stalled",
      false,
      "A mining step over a single redo log timed out at cdc.mining.query.timeout.ms three times in"
          + " a row."),
  /** cdc.transaction.max.age.action=fail found a transaction open longer than the limit. */
  TRANSACTION_TOO_OLD(
      "CDC-4004",
      "transaction-too-old",
      false,
      "A transaction stayed open longer than cdc.transaction.max.age.ms with"
          + " cdc.transaction.max.age.action=fail."),
  /** An unsupported topology change was detected. */
  TOPOLOGY(
      "CDC-5001",
      "topology",
      false,
      "The stored offset belongs to another database (DBID or RESETLOGS changed), no valid"
          + " archive destination can be mined, or the database has more than one enabled redo"
          + " thread (RAC), which this release does not capture."),
  /** ORA-01031 or ORA-00942 on a required view or package. */
  PRIVILEGE(
      "CDC-5002",
      "privilege",
      false,
      "The database refused the connector user: a grant or a view is missing, or the login itself"
          + " failed."),
  /** The redo dictionary needed for a DDL replay window is not available. */
  DICTIONARY_UNAVAILABLE(
      "CDC-6001",
      "dictionary-unavailable",
      false,
      "Rows were written before a later DDL on their table, and no usable dictionary build in the"
          + " redo or no exact schema version covers them."),
  /** A DDL on a captured table could not be classified. */
  UNSUPPORTED_DDL(
      "CDC-6002",
      "unsupported-ddl",
      false,
      "A DDL statement on a captured table could not be classified."),
  /** A stored schema version differs from the dictionary and no later DDL explains it (SCH-6). */
  SCHEMA_MISMATCH(
      "CDC-6003",
      "schema-mismatch",
      false,
      "At start, a schema version on the schema topic differs from the dictionary and no DDL ahead"
          + " of the resume point explains it."),
  /**
   * Two source names map to one target name only because characters were replaced: two columns to
   * one field, or two tables to one topic.
   */
  NAME_COLLISION(
      "CDC-6004",
      "name-collision",
      false,
      "Two columns of a table adjust to the same field name under"
          + " cdc.field.name.adjustment.mode, or two tables route to the same topic only because"
          + " characters Kafka does not allow became underscores."),
  /** A COMMIT arrived for a transaction that orphan detection had released. */
  ORPHAN_RELEASE_VIOLATION(
      "CDC-7001",
      "orphan-release-violation",
      false,
      "A COMMIT arrived for a transaction that orphan detection released or the age policy"
          + " discarded."),
  /** cdc.transaction.orphan.action=fail found an orphaned transaction. */
  ORPHAN_TRANSACTION(
      "CDC-7002",
      "orphan-transaction",
      false,
      "A buffered transaction is no longer known to the database and"
          + " cdc.transaction.orphan.action=fail."),
  /** A snapshot chunk at the smallest size still met ORA-01555 or ORA-08181 (PRD-02 SNAP-4). */
  SNAPSHOT_TOO_OLD(
      "CDC-8001",
      "snapshot-too-old",
      false,
      "A snapshot chunk still met ORA-01555 or ORA-08181 at the smallest chunk size.");

  private final String code;
  private final String runbookSlug;
  private final boolean retriable;
  private final String description;

  ErrorCode(String code, String runbookSlug, boolean retriable, String description) {
    this.code = code;
    this.runbookSlug = runbookSlug;
    this.retriable = retriable;
    this.description = description;
  }

  /** Stable machine-readable code, for example {@code CDC-2002}. */
  public String code() {
    return code;
  }

  /** Runbook page name under {@code website/docs/operations/runbooks/}. */
  public String runbookSlug() {
    return runbookSlug;
  }

  /** Whether the engine retries with backoff instead of stopping. */
  public boolean retriable() {
    return retriable;
  }

  /**
   * One sentence for operators: what the connector saw when it raised this code. The error classes
   * reference page is generated from it (ErrorClassesReferenceTest).
   */
  public String description() {
    return description;
  }

  /** Public runbook URL for this code. */
  public String runbookUrl() {
    return "https://kafkacdcconnector.com/runbooks/" + runbookSlug;
  }
}
