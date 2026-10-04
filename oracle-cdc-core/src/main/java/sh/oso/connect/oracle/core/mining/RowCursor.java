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

/** A forward-only cursor over mined rows; {@link #cancel()} aborts the server-side query. */
public interface RowCursor extends AutoCloseable {
  boolean next() throws SQLException;

  LogMinerRow row();

  void cancel() throws SQLException;

  @Override
  void close() throws SQLException;
}
