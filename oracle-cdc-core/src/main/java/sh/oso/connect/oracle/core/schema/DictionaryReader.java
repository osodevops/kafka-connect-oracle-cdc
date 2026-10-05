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
package sh.oso.connect.oracle.core.schema;

import java.sql.SQLException;
import java.util.Optional;
import sh.oso.connect.oracle.core.model.TableId;

/** Reads a table's current structure and key candidates from the Oracle dictionary. */
public interface DictionaryReader {
  Optional<TableSchema> read(TableId table) throws SQLException;

  KeySelector.Candidates keyCandidates(TableId table) throws SQLException;

  /** DBA_OBJECTS.LAST_DDL_TIME of the table, when known (SCH-6). */
  default Optional<java.time.Instant> lastDdlTime(TableId table) throws SQLException {
    return Optional.empty();
  }

  /**
   * TIMESTAMP_TO_SCN of a time, or the current SCN for a time not yet reached; empty when the time
   * is older than the database's SCN-to-time mapping (P1-17).
   */
  default Optional<Long> scnAt(java.time.Instant time) throws SQLException {
    return Optional.empty();
  }

  /** SCN_TO_TIMESTAMP of an SCN, when the database still maps it (SCH-6). */
  default Optional<java.time.Instant> timeOfScn(long scn) throws SQLException {
    return Optional.empty();
  }
}
