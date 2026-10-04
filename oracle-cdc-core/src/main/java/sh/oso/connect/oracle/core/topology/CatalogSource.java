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
package sh.oso.connect.oracle.core.topology;

import java.sql.SQLException;
import java.util.List;
import sh.oso.connect.oracle.core.logs.RedoLog;

/**
 * Everything the engine reads from the data dictionary and the fixed views, behind one interface so
 * unit tests drive it with {@code FakeCatalog} (PRD-00 CORE-CONN-5, CORE-LOG-1, CORE-LOG-7).
 */
public interface CatalogSource {

  DatabaseInfo database() throws SQLException;

  long currentScn() throws SQLException;

  List<ThreadInfo> threads() throws SQLException;

  List<PdbInfo> pdbs() throws SQLException;

  List<ArchiveDestination> archiveDestinations() throws SQLException;

  /** Online redo logs (one member per group) with their current status. */
  List<RedoLog> onlineLogs() throws SQLException;

  /**
   * Archived logs overlapping the SCN range at the given destination, including deleted entries.
   */
  List<RedoLog> archivedLogs(long startScn, long endScn, int destId) throws SQLException;

  /** Archived-log completions per thread in the trailing window, for switch-rate metrics. */
  List<RedoLog> archivedSince(java.time.Instant since, int destId) throws SQLException;
}
