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
import sh.oso.connect.oracle.core.model.RedoRecordId;

/**
 * What the engine hands to its owner, in commit order. The owner (the Connect task) turns committed
 * transactions into records and decides, from acknowledgements, what the position becomes.
 */
public interface EventSink {

  /**
   * A transaction committed; {@code skipped} leading events were already acknowledged before this
   * run started (CORE-POS-3) and must not be emitted again. {@code resumeCandidate} is the SCN a
   * restart may mine from once this transaction is fully acknowledged: the lower of its commit SCN
   * and the first capture of every transaction still open at this moment (CORE-POS-2), so an
   * interleaved transaction that commits later is never skipped (dbz#2544).
   */
  void committed(CommittedTransaction tx, int skipped, RedoRecordId resumeCandidate);

  /**
   * A step was applied. {@code minedToScn} is where mining continues; {@code resumeCandidate} is
   * the resume SCN the position may advance to once every emitted record is acknowledged
   * (CORE-POS-2, CORE-POS-5).
   */
  void stepApplied(long minedToScn, RedoRecordId resumeCandidate);

  /**
   * Nothing to mine: the cursor is at the safe end. Reported so the owner can heartbeat the
   * position on a quiet database (CORE-POS-5); {@code resumeCandidate} as for {@link #stepApplied}.
   */
  default void idle(long minedToScn, RedoRecordId resumeCandidate) {}

  /** A DDL on a captured owner was mined; informational until PRD-03 lands. */
  default void ddl(MiningEvent.Ddl ddl) {}

  /**
   * PRD-03: a DDL gave a captured table a new schema version ({@code schema}), or removed it under
   * this name ({@code schema} null: dropped or renamed away).
   */
  default void schemaChanged(
      sh.oso.connect.oracle.core.schema.TableSchema schema, MiningEvent.Ddl ddl) {}

  /**
   * A row could not be decoded and {@code cdc.on.decode.error=dlq}; the raw event is handed over.
   */
  default void decodeFailed(MiningEvent.Dml dml, DecodeException cause) {}

  /** An UNSUPPORTED row for a captured table under the DLQ policy. */
  default void unsupported(MiningEvent.Unsupported event) {}

  /**
   * P1-17: the step from {@code fromScn} to {@code toScn} was mined again with a dictionary from
   * the redo because rows of {@code tables} predate a later DDL.
   */
  default void dictionaryReplayed(
      long fromScn, long toScn, java.util.Set<sh.oso.connect.oracle.core.model.TableId> tables) {}

  /** The pushed-down object ids were re-resolved after a DDL step cut. */
  default void idsRefreshed(java.util.Set<String> owners) {}

  /** The database sessions were reopened after a transient error (CORE-CONN-6). */
  default void reconnected(String cause) {}

  /**
   * An orphaned transaction was released (CORE-TX-7, ADR-0006); {@code released} is the whole
   * ledger the position must carry from now on.
   */
  /**
   * A transaction open longer than cdc.transaction.max.age.ms was discarded (CORE-TX-6); like an
   * orphan release it joins the released ledger, so a later COMMIT for it stops the task rather
   * than publishing a partial transaction.
   */
  default void transactionDiscarded(
      sh.oso.connect.oracle.core.buffer.TransactionBuffer.OpenTransaction tx,
      java.time.Duration age,
      java.util.List<String> released) {}

  default void orphanReleased(
      sh.oso.connect.oracle.core.orphan.OrphanDetector.Release release,
      java.util.List<String> released) {}
}
