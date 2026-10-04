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
 * {@code website/docs/runbooks/}, so an operator can go from a Connect status message straight to
 * the recovery procedure. Codes are never reused or renumbered; retired codes are kept as
 * deprecated constants.
 *
 * <p>Classification follows PRD-00 section 4.7 (CORE-ERR).
 */
public enum ErrorCode {
  /** ORA-03113, ORA-03114, ORA-12170, ORA-12541, ORA-12514, ORA-01033, ORA-01089 and I/O errors. */
  TRANSIENT_DATABASE("CDC-1001", "transient-database", true),
  /** ORA-00310, ORA-00334, ORA-01289 or our own cancel: re-mine the same range. */
  MINING_STEP_RETRY("CDC-1002", "mining-step-retry", true),
  /** A redo log sequence is missing for a required thread. */
  LOG_GAP("CDC-2001", "log-gap", false),
  /** A redo log the connector still needs has been deleted. */
  LOG_PURGED("CDC-2002", "log-purged", false),
  /** SQL_REDO could not be decoded, or STATUS 2 or 3 for a captured object. */
  DECODE("CDC-3001", "decode", false),
  /** CORRUPTED_BLOCKS or MISSING_SCN for a captured object. */
  CORRUPTION("CDC-3002", "corruption", false),
  /** The spill cap was exceeded. */
  BUFFER_EXHAUSTED("CDC-4001", "buffer-exhausted", false),
  /** A journal chunk is missing or inconsistent on reload. */
  JOURNAL_CORRUPTION("CDC-4002", "journal-corruption", false),
  /** Mining queries keep timing out even at a single log window. */
  MINING_STALLED("CDC-4003", "mining-stalled", false),
  /** An unsupported topology change was detected. */
  TOPOLOGY("CDC-5001", "topology", false),
  /** ORA-01031 or ORA-00942 on a required view or package. */
  PRIVILEGE("CDC-5002", "privilege", false),
  /** The redo dictionary needed for a DDL replay window is not available. */
  DICTIONARY_UNAVAILABLE("CDC-6001", "dictionary-unavailable", false),
  /** A DDL on a captured table could not be classified. */
  UNSUPPORTED_DDL("CDC-6002", "unsupported-ddl", false),
  /** A COMMIT arrived for a transaction that orphan detection had released. */
  ORPHAN_RELEASE_VIOLATION("CDC-7001", "orphan-release-violation", false);

  private final String code;
  private final String runbookSlug;
  private final boolean retriable;

  ErrorCode(String code, String runbookSlug, boolean retriable) {
    this.code = code;
    this.runbookSlug = runbookSlug;
    this.retriable = retriable;
  }

  /** Stable machine-readable code, for example {@code CDC-2002}. */
  public String code() {
    return code;
  }

  /** Runbook page name under {@code website/docs/runbooks/}. */
  public String runbookSlug() {
    return runbookSlug;
  }

  /** Whether the engine retries with backoff instead of stopping. */
  public boolean retriable() {
    return retriable;
  }

  /** Public runbook URL for this code. */
  public String runbookUrl() {
    return "https://kafkacdcconnector.com/runbooks/" + runbookSlug;
  }
}
