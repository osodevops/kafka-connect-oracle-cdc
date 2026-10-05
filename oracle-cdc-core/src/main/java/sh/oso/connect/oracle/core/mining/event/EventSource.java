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
package sh.oso.connect.oracle.core.mining.event;

import java.sql.SQLException;

/** Opens the events of one SCN range; the real one mines Oracle, the fake replays a script. */
public interface EventSource extends AutoCloseable {
  /**
   * Opens the rows after {@code from} with SCN below {@code endScn}. The cursor's redo byte address
   * selects the rows (ADR-0014): rows whose SCN is below the cursor's SCN but which reached the log
   * later are included, because the log is append-only in that order.
   */
  EventCursor open(sh.oso.connect.oracle.core.mining.step.StepCursor from, long endScn)
      throws SQLException;

  /**
   * Releases server-side resources so the next {@link #open} starts a fresh session (CORE-MINE-7).
   */
  default void recycle() throws SQLException {}

  @Override
  void close() throws SQLException;
}
