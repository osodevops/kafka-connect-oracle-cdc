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
package sh.oso.connect.oracle.core.engine;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.schema.TableSchema;

/** Fetches LOB values the redo did not carry, as of the commit SCN (PRD-00 CORE-DEC-7). */
public interface LobReselector {

  /**
   * The values of {@code columns} for the row of {@code change} AS OF {@code scn}. A column missing
   * from the result stays unavailable: the row no longer exists at that SCN, it has no usable key,
   * or the undo needed for the query is gone (ORA-01555, ORA-08181).
   */
  Map<String, Object> reselect(TableSchema schema, RowChange change, long scn, List<String> columns)
      throws SQLException;
}
