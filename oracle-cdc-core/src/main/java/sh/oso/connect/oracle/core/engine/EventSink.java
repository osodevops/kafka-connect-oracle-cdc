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

import sh.oso.connect.oracle.core.buffer.CommittedTransaction;
import sh.oso.connect.oracle.core.errors.DecodeException;
import sh.oso.connect.oracle.core.mining.event.MiningEvent;

/**
 * What the engine hands to its owner, in commit order. The owner (the Connect task) turns committed
 * transactions into records and decides, from acknowledgements, what the position becomes.
 */
public interface EventSink {

  /**
   * A transaction committed; {@code skipped} leading events were already acknowledged before this
   * run started (CORE-POS-3) and must not be emitted again.
   */
  void committed(CommittedTransaction tx, int skipped);

  /**
   * A step was applied. {@code minedToScn} is where mining continues; {@code resumeCandidate} is
   * the resume SCN the position may advance to once every emitted record is acknowledged
   * (CORE-POS-2, CORE-POS-5).
   */
  void stepApplied(long minedToScn, long resumeCandidate);

  /** A DDL on a captured owner was mined; informational until PRD-03 lands. */
  default void ddl(MiningEvent.Ddl ddl) {}

  /**
   * A row could not be decoded and {@code cdc.on.decode.error=dlq}; the raw event is handed over.
   */
  default void decodeFailed(MiningEvent.Dml dml, DecodeException cause) {}

  /** An UNSUPPORTED row for a captured table under the DLQ policy. */
  default void unsupported(MiningEvent.Unsupported event) {}
}
