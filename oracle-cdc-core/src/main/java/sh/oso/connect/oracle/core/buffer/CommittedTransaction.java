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
package sh.oso.connect.oracle.core.buffer;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.RowChange;
import sh.oso.connect.oracle.core.model.TxKey;

/**
 * A transaction the buffer released on COMMIT: its surviving changes in redo order. The position of
 * a change in {@code events} is its {@code event_index} (CORE-POS-1); commit order across the
 * stream is {@code (commitScn, thread, key)} (CORE-POS-4).
 */
public record CommittedTransaction(
    TxKey key,
    RedoRecordId firstCaptured,
    RedoRecordId startId,
    RedoRecordId commitId,
    Instant commitTimestamp,
    int thread,
    String username,
    String clientId,
    List<RowChange> events) {

  public CommittedTransaction {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(firstCaptured, "firstCaptured");
    Objects.requireNonNull(commitId, "commitId");
    events = List.copyOf(events);
  }

  public long commitScn() {
    return commitId.scn();
  }

  public int size() {
    return events.size();
  }
}
