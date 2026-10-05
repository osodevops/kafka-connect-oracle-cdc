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
package sh.oso.connect.oracle.core.metrics;

import java.util.List;

/**
 * One capture task's metrics as a JMX MXBean, registered as {@code
 * sh.oso.cdc:type=task,server=<cdc.topic.prefix>} (ADR-0013). The reference page
 * website/docs/reference/metrics.md and the shipped exporter rules follow these attributes.
 */
public interface TaskMetricsMXBean {

  @Description(kind = Description.Kind.COUNTER, value = "Mining steps applied.")
  long getSteps();

  @Description(
      kind = Description.Kind.COUNTER,
      value =
          "Rows returned by LogMiner, including rows for other tables in the same logs and"
              + " transaction control rows.")
  long getRowsMined();

  @Description(
      kind = Description.Kind.COUNTER,
      value = "Mining events applied to the transaction buffer.")
  long getEventsApplied();

  @Description(
      kind = Description.Kind.COUNTER,
      value = "Steps discarded and mined again after a retriable LogMiner error (CDC-1002).")
  long getStepRetries();

  @Description(
      kind = Description.Kind.COUNTER,
      value = "Steps cancelled at cdc.mining.query.timeout.ms; the next step covers fewer logs.")
  long getStepTimeouts();

  @Description(
      kind = Description.Kind.COUNTER,
      value = "Steps cut after a DDL that changed the captured object ids.")
  long getStepCuts();

  @Description(
      kind = Description.Kind.COUNTER,
      value = "Reconnections after a transient database error (CDC-1001).")
  long getReconnects();

  @Description(kind = Description.Kind.COUNTER, value = "Polls that found no new redo.")
  long getIdlePolls();

  @Description(
      kind = Description.Kind.COUNTER,
      value = "LogMiner sessions restarted at cdc.mining.session.max.age.ms.")
  long getSessionRecycles();

  @Description(
      kind = Description.Kind.COUNTER,
      value = "Committed transactions handed to the sink.")
  long getTransactionsCommitted();

  @Description(
      kind = Description.Kind.COUNTER,
      value = "Committed transactions already delivered before a restart, skipped whole.")
  long getTransactionsSkipped();

  @Description(
      kind = Description.Kind.COUNTER,
      value = "Rows that could not be decoded and went to the DLQ (cdc.on.decode.error=dlq).")
  long getDecodeFailures();

  @Description(
      kind = Description.Kind.COUNTER,
      value = "LOB locator updates folded into their INSERT.")
  long getLobInsertsMerged();

  @Description(
      kind = Description.Kind.COUNTER,
      value = "LOB_WRITE, LOB_TRIM and LOB_ERASE rows applied.")
  long getLobRowsApplied();

  @Description(
      kind = Description.Kind.COUNTER,
      value = "Open transactions released by orphan detection (CORE-TX-7).")
  long getOrphansReleased();

  @Description(
      kind = Description.Kind.COUNTER,
      value =
          "Steps mined again with a dictionary from the redo because rows predate a later DDL"
              + " (PRD-03 lag case).")
  long getLagReplays();

  @Description(kind = Description.Kind.COUNTER, value = "Snapshot chunks read (PRD-02).")
  long getSnapshotChunksRead();

  @Description(
      kind = Description.Kind.COUNTER,
      value = "Snapshot chunks published, after streaming passed their SCN.")
  long getSnapshotChunksPublished();

  @Description(kind = Description.Kind.COUNTER, value = "Snapshot rows published as op=r records.")
  long getSnapshotRowsPublished();

  @Description(
      kind = Description.Kind.COUNTER,
      value = "Snapshot chunks read again after an error, with a fresh SCN (SNAP-3, SNAP-4).")
  long getSnapshotChunkRetries();

  @Description(
      kind = Description.Kind.GAUGE,
      value = "Snapshot chunks read and waiting for streaming to pass their SCN.")
  long getSnapshotChunksPending();

  @Description(
      kind = Description.Kind.GAUGE,
      value = "Tables the running snapshot has still to read.")
  long getSnapshotTablesRemaining();

  @Description(
      kind = Description.Kind.COUNTER,
      value = "Transactions dropped by cdc.transaction.max.age.action=discard.")
  long getTransactionsDiscarded();

  @Description("Duration of the last mining step, in milliseconds.")
  long getLastStepMillis();

  @Description("Redo logs in the last step's window.")
  int getWindowLogs();

  @Description("SCN the engine has mined up to.")
  long getMinedToScn();

  @Description("Database SCN read at the last step: the end of what can be mined.")
  long getSafeEndScn();

  @Description("SafeEndScn minus MinedToScn.")
  long getScnLag();

  @Description("Transactions open in the buffer.")
  int getOpenTransactions();

  @Description("Changes held for open transactions, on heap and spilled.")
  long getBufferedEvents();

  @Description("Estimated heap bytes held by open transactions.")
  long getBufferHeapBytes();

  @Description("cdc.buffer.memory.max.bytes, for ratio alerts.")
  long getBufferMemoryMaxBytes();

  @Description(
      "First SCN of the oldest open transaction that is not journaled, or -1; the resume position"
          + " cannot pass it.")
  long getOldestOpenScn();

  @Description(kind = Description.Kind.COUNTER, value = "Transactions rolled back while buffered.")
  long getRolledBackTransactions();

  @Description(
      kind = Description.Kind.COUNTER,
      value = "Changes removed or downgraded by savepoint and statement rollbacks.")
  long getUndoneEvents();

  @Description(
      kind = Description.Kind.COUNTER,
      value = "Undo rows that matched no buffered change.")
  long getUnmatchedUndo();

  @Description("Open transactions spilled to disk.")
  int getSpilledTransactions();

  @Description("Bytes in spill files.")
  long getSpilledBytes();

  @Description("cdc.buffer.spill.max.bytes, for ratio alerts.")
  long getSpillMaxBytes();

  @Description("Open transactions written to the transaction journal.")
  int getJournaledTransactions();

  @Description(kind = Description.Kind.COUNTER, value = "Heartbeat records queued.")
  long getHeartbeatsSent();

  @Description(kind = Description.Kind.COUNTER, value = "Ops topic events queued.")
  long getOpsEvents();

  @Description(kind = Description.Kind.COUNTER, value = "Transaction journal chunks queued.")
  long getJournalChunks();

  @Description(kind = Description.Kind.COUNTER, value = "Transaction journal tombstones queued.")
  long getJournalTombstones();

  @Description(kind = Description.Kind.COUNTER, value = "DLQ records queued.")
  long getDlqRecords();

  @Description("Records waiting for Kafka Connect to poll.")
  int getQueueDepth();

  @Description("Commit time of the last transaction queued, epoch milliseconds, or -1.")
  long getLastCommitTimestampMillis();

  @Description(
      "Time between the commit of the last transaction queued and its queueing, in milliseconds.")
  long getMillisBehindSource();

  @Description("The 20 largest open transactions by bytes buffered (CORE-TX-8).")
  List<TransactionInfo> getLargestTransactions();
}
