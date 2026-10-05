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

import sh.oso.connect.oracle.core.model.TxKey;

/**
 * Where journal chunks go (ADR-0003): in the connector, records emitted from poll() to the
 * compacted journal topic, so they share acknowledgement order with the data records.
 */
public interface JournalSink {

  /** A sink that journals nothing; the buffer then never marks a transaction as journaled. */
  JournalSink NONE = new JournalSink() {};

  default void chunk(JournalChunk chunk) {}

  /**
   * The transaction ended; one tombstone per chunk record, each under the generation that wrote it
   * (chunks restored from an earlier generation keep that generation in their key).
   */
  default void tombstones(TxKey key, java.util.List<JournalChunk.Ref> chunks) {}
}
