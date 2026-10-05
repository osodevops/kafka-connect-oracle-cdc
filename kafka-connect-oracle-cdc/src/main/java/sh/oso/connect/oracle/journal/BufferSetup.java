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
package sh.oso.connect.oracle.journal;

import java.util.List;
import java.util.Map;
import sh.oso.connect.oracle.core.buffer.JournalChunk;
import sh.oso.connect.oracle.core.buffer.JournalPolicy;
import sh.oso.connect.oracle.core.buffer.JournalSink;
import sh.oso.connect.oracle.core.model.TxKey;

/**
 * What the engine factory needs to build the transaction buffer for one task start: the journal
 * policy and sink, this start's generation and the transactions reloaded from the journal topic.
 */
public record BufferSetup(
    JournalPolicy policy,
    JournalSink journal,
    long generation,
    Map<TxKey, List<JournalChunk>> restored) {

  public static BufferSetup none() {
    return new BufferSetup(JournalPolicy.NEVER, JournalSink.NONE, 0, Map.of());
  }
}
