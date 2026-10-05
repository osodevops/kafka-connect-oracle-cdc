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
package sh.oso.connect.oracle.core.doctor;

import java.sql.SQLException;
import java.util.List;
import sh.oso.connect.oracle.core.logs.RedoLog;

/** Mines a set of logs with no table filter and counts the rows (PRD-05 {@code redo-profile}). */
public interface RedoSampler {

  /**
   * Rows of {@code [startScn, endScn]} in the given logs, counted by container, owner, table and
   * operation; {@code truncates} counts the DDL rows that are TRUNCATE statements.
   */
  List<RedoProfile.SampleRow> sample(List<RedoLog> logs, long startScn, long endScn)
      throws SQLException;
}
