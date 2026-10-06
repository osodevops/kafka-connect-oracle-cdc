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

import java.io.IOException;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientConnectionException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Maps a JDBC failure to the engine's typed errors (PRD-00 CORE-ERR). The code lists come from
 * Oracle's documentation and the observed behaviour recorded in {@code reference/mining-errors.md}.
 * {@code cdc.retry.extra.error.codes} widens the transient set; nothing can turn a stop into a
 * continue.
 */
public final class OraErrorClassifier {

  private static final Pattern ORA = Pattern.compile("ORA-(\\d{5})");

  /** Connection and instance availability failures: retry with backoff. */
  static final Set<Integer> TRANSIENT =
      Set.of(
          28, 31, 3113, 3114, 3135, 12170, 12541, 12514, 12516, 12528, 12537, 12543, 1033, 1034,
          1089, 1090, 1092, 1109, 17002, 17008, 17410, 17800, 25408, 4068);

  /**
   * Online log reuse or a log not yet visible: discard the step and mine the same range again.
   * ORA-01368 is a registered log whose header no longer matches: the group was reused under a long
   * step.
   */
  static final Set<Integer> STEP_RETRY = Set.of(310, 334, 1289, 1291, 1368, 1013);

  /** A log the catalog lists cannot be read any more. */
  static final Set<Integer> PURGED = Set.of(1284, 308, 1285, 16226);

  /** Missing grants or views. */
  static final Set<Integer> PRIVILEGE = Set.of(1031, 942, 1017, 28000, 1045);

  private final Set<Integer> extraTransient;

  public OraErrorClassifier() {
    this(Collections.emptySet());
  }

  public OraErrorClassifier(Set<Integer> extraTransient) {
    this.extraTransient = new HashSet<>(extraTransient);
  }

  /** The ORA code of an exception chain, or -1 when there is none. */
  public static int oraCode(Throwable t) {
    for (Throwable c = t; c != null; c = c.getCause()) {
      if (c instanceof SQLException sql && sql.getErrorCode() > 0) {
        return sql.getErrorCode();
      }
      if (c.getMessage() != null) {
        Matcher m = ORA.matcher(c.getMessage());
        if (m.find()) {
          return Integer.parseInt(m.group(1));
        }
      }
    }
    return -1;
  }

  public ErrorCode classify(Throwable t) {
    for (Throwable c = t; c != null; c = c.getCause()) {
      if (c instanceof java.sql.SQLRecoverableException
          || c instanceof java.sql.SQLTransientException) {
        return ErrorCode.TRANSIENT_DATABASE; // the driver itself says the connection is gone
      }
    }
    int code = oraCode(t);
    if (code > 0) {
      if (PRIVILEGE.contains(code)) {
        return ErrorCode.PRIVILEGE;
      }
      if (STEP_RETRY.contains(code)) {
        return ErrorCode.MINING_STEP_RETRY;
      }
      if (PURGED.contains(code)) {
        return ErrorCode.LOG_PURGED;
      }
      if (TRANSIENT.contains(code) || extraTransient.contains(code)) {
        return ErrorCode.TRANSIENT_DATABASE;
      }
      if (code == 604 && hasNetworkCause(t)) {
        return ErrorCode.TRANSIENT_DATABASE;
      }
    }
    for (Throwable c = t; c != null; c = c.getCause()) {
      if (c instanceof IOException
          || c instanceof SQLRecoverableException
          || c instanceof SQLTransientConnectionException) {
        return ErrorCode.TRANSIENT_DATABASE;
      }
    }
    return null;
  }

  /**
   * Wraps a failure in the typed exception for its class, or returns null when it is not ours to
   * classify.
   */
  /**
   * The typed exception for a database error, never null when {@code t} has an SQLException in its
   * cause chain (an unknown ORA code stops the task typed); null only for non-database throwables,
   * which callers rethrow unchanged.
   */
  public OracleCdcException toException(Throwable t, String context) {
    ErrorCode code = classify(t);
    int ora = oraCode(t);
    String what =
        context + (ora > 0 ? " failed with ORA-" + String.format("%05d", ora) : " failed") + ".";
    if (code == null) {
      boolean sql = false;
      for (Throwable c = t; c != null; c = c.getCause()) {
        sql |= c instanceof java.sql.SQLException;
      }
      if (!sql) {
        return null; // not a database error: the caller rethrows the original as it is
      }
      // an ORA code the connector does not know: stop with a typed error rather than guess
      return new OracleCdcException(
          ErrorCode.TRANSIENT_DATABASE,
          what + " The connector does not classify this error.",
          "The task stopped and restarts under the Connect restart policy. Report ORA-"
              + (ora > 0 ? String.format("%05d", ora) : "?")
              + " so it can be classified, or add it to cdc.retry.extra.error.codes if it is"
              + " transient in your environment.",
          t);
    }
    return switch (code) {
      case TRANSIENT_DATABASE ->
          new TransientDatabaseException(
              what, "No action needed; the engine reconnects with backoff.", t);
      case MINING_STEP_RETRY ->
          new MiningStepRetryException(
              what, "No action needed; the engine mines the same range again.", t);
      case LOG_PURGED ->
          new OracleCdcPurgedException(
              what,
              "A needed redo log is gone. Run oracle-cdc-admin resnapshot for the affected tables.",
              t);
      case PRIVILEGE ->
          new PrivilegeException(
              what,
              "Grant the missing privilege with the script from oracle-cdc-doctor setup-sql.",
              t);
      default -> new OracleCdcException(code, what, "See the runbook.", t);
    };
  }

  private static boolean hasNetworkCause(Throwable t) {
    for (Throwable c = t; c != null; c = c.getCause()) {
      int code = c instanceof SQLException sql ? sql.getErrorCode() : -1;
      if (TRANSIENT.contains(code) || c instanceof IOException) {
        return true;
      }
    }
    return false;
  }
}
