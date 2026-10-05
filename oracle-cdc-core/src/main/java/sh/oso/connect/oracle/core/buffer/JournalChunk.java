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

import java.util.Objects;
import sh.oso.connect.oracle.core.model.RedoRecordId;
import sh.oso.connect.oracle.core.model.TxKey;

/**
 * One journal record (CORE-TX-4, ADR-0003): a slice of a long transaction's changes, in redo order,
 * with the transaction's identity so a restart can rebuild the buffer entry without the redo.
 *
 * @param key container and XID
 * @param chunk zero-based chunk number within the transaction
 * @param generation the task generation that wrote it (ADR-0003)
 * @param first the first redo record in this chunk
 * @param last the last redo record in this chunk; the resume position may not pass it
 * @param events change frames in this chunk
 * @param undos undo frames in this chunk
 * @param firstCaptured the transaction's first captured record (chunk 0 of every generation)
 * @param startId the START row, or null when it was not mined
 * @param thread redo thread of the START row, 0 when unknown
 * @param username session user, may be null
 * @param clientId client identifier, may be null
 * @param payload the frames, encoded by {@link JournalFrames}
 */
public record JournalChunk(
    TxKey key,
    int chunk,
    long generation,
    RedoRecordId first,
    RedoRecordId last,
    int events,
    int undos,
    RedoRecordId firstCaptured,
    RedoRecordId startId,
    int thread,
    String username,
    String clientId,
    byte[] payload) {

  /** Identity of a chunk record in the topic: its number and the generation that wrote it. */
  public record Ref(int chunk, long generation) {}

  public Ref ref() {
    return new Ref(chunk, generation);
  }

  public JournalChunk {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(first, "first");
    Objects.requireNonNull(last, "last");
    Objects.requireNonNull(firstCaptured, "firstCaptured");
    Objects.requireNonNull(payload, "payload");
  }
}
