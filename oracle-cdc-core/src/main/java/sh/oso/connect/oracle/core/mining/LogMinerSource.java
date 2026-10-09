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
package sh.oso.connect.oracle.core.mining;

import java.sql.SQLException;
import java.util.List;
import sh.oso.connect.oracle.core.logs.RedoLog;

/**
 * One LogMiner session on one mining connection (CORE-MINE-1, CORE-MINE-7). The log set is re-added
 * only when it changes; the SCN range is restarted per step; END_LOGMNR runs on close.
 */
public interface LogMinerSource extends AutoCloseable {

  /** Makes the session's log list equal to {@code logs}; a no-op when unchanged. */
  void setLogs(List<RedoLog> logs) throws SQLException;

  void start(long startScn, long endScn, DictionaryMode mode) throws SQLException;

  /**
   * Rows after the cursor (redo byte address when it has one, else SCN) and below {@code endScn}.
   */
  RowCursor query(
      MiningFilter filter, sh.oso.connect.oracle.core.mining.step.StepCursor from, long endScn)
      throws SQLException;

  void end() throws SQLException;

  /** END_LOGMNR and forget the added logs, so the next {@link #setLogs} adds them again. */
  void reset() throws SQLException;

  /**
   * True when Oracle chooses the logs from the SCN range and nothing is added (range mode,
   * ADR-0027); a dictionary from the redo is then unavailable.
   */
  default boolean rangeOnly() {
    return false;
  }

  /** PGA bytes of the mining session's server process, or -1 when not readable. */
  long pgaUsedBytes() throws SQLException;

  @Override
  void close() throws SQLException;
}
