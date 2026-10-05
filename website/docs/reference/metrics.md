---
title: Metrics
description: JMX attributes of each capture task and their Prometheus names.
---

# Metrics

Generated from `TaskMetricsMXBean` by `MetricsReferenceTest`; do not edit by hand.

Each capture task registers one MXBean, `sh.oso.cdc:type=task,server=<prefix>`, where the prefix is `cdc.topic.prefix`. The JMX Prometheus exporter configuration in `ops/jmx-exporter/oracle-cdc.yml` publishes every numeric attribute under the name below with a `server` label. A Grafana dashboard (`ops/grafana/oracle-cdc-connector.json`) and Prometheus alert rules (`ops/alerts/prometheus-rules.yaml`) use these names.

| Attribute | Prometheus name | Kind | Description |
|---|---|---|---|
| `BufferHeapBytes` | `oracle_cdc_buffer_heap_bytes` | gauge | Estimated heap bytes held by open transactions. |
| `BufferMemoryMaxBytes` | `oracle_cdc_buffer_memory_max_bytes` | gauge | cdc.buffer.memory.max.bytes, for ratio alerts. |
| `BufferedEvents` | `oracle_cdc_buffered_events` | gauge | Changes held for open transactions, on heap and spilled. |
| `DecodeFailures` | `oracle_cdc_decode_failures_total` | counter | Rows that could not be decoded and went to the DLQ (cdc.on.decode.error=dlq). |
| `DlqRecords` | `oracle_cdc_dlq_records_total` | counter | DLQ records queued. |
| `EventsApplied` | `oracle_cdc_events_applied_total` | counter | Mining events applied to the transaction buffer. |
| `HeartbeatsSent` | `oracle_cdc_heartbeats_sent_total` | counter | Heartbeat records queued. |
| `IdlePolls` | `oracle_cdc_idle_polls_total` | counter | Polls that found no new redo. |
| `JournalChunks` | `oracle_cdc_journal_chunks_total` | counter | Transaction journal chunks queued. |
| `JournalTombstones` | `oracle_cdc_journal_tombstones_total` | counter | Transaction journal tombstones queued. |
| `JournaledTransactions` | `oracle_cdc_journaled_transactions` | gauge | Open transactions written to the transaction journal. |
| `LagReplays` | `oracle_cdc_lag_replays_total` | counter | Steps mined again with a dictionary from the redo because rows predate a later DDL (PRD-03 lag case). |
| `LargestTransactions` | not exported (composite) | table | The 20 largest open transactions by bytes buffered (CORE-TX-8). |
| `LastCommitTimestampMillis` | `oracle_cdc_last_commit_timestamp_millis` | gauge | Commit time of the last transaction queued, epoch milliseconds, or -1. |
| `LastStepMillis` | `oracle_cdc_last_step_millis` | gauge | Duration of the last mining step, in milliseconds. |
| `LobInsertsMerged` | `oracle_cdc_lob_inserts_merged_total` | counter | LOB locator updates folded into their INSERT. |
| `LobRowsApplied` | `oracle_cdc_lob_rows_applied_total` | counter | LOB_WRITE, LOB_TRIM and LOB_ERASE rows applied. |
| `MillisBehindSource` | `oracle_cdc_millis_behind_source` | gauge | Time between the commit of the last transaction queued and its queueing, in milliseconds. |
| `MinedToScn` | `oracle_cdc_mined_to_scn` | gauge | SCN the engine has mined up to. |
| `OldestOpenScn` | `oracle_cdc_oldest_open_scn` | gauge | First SCN of the oldest open transaction that is not journaled, or -1; the resume position cannot pass it. |
| `OpenTransactions` | `oracle_cdc_open_transactions` | gauge | Transactions open in the buffer. |
| `OpsEvents` | `oracle_cdc_ops_events_total` | counter | Ops topic events queued. |
| `OrphansReleased` | `oracle_cdc_orphans_released_total` | counter | Open transactions released by orphan detection (CORE-TX-7). |
| `QueueDepth` | `oracle_cdc_queue_depth` | gauge | Records waiting for Kafka Connect to poll. |
| `Reconnects` | `oracle_cdc_reconnects_total` | counter | Reconnections after a transient database error (CDC-1001). |
| `RolledBackTransactions` | `oracle_cdc_rolled_back_transactions_total` | counter | Transactions rolled back while buffered. |
| `RowsMined` | `oracle_cdc_rows_mined_total` | counter | Rows returned by LogMiner, including rows for other tables in the same logs and transaction control rows. |
| `SafeEndScn` | `oracle_cdc_safe_end_scn` | gauge | Database SCN read at the last step: the end of what can be mined. |
| `ScnLag` | `oracle_cdc_scn_lag` | gauge | SafeEndScn minus MinedToScn. |
| `SessionRecycles` | `oracle_cdc_session_recycles_total` | counter | LogMiner sessions restarted at cdc.mining.session.max.age.ms. |
| `SnapshotChunkRetries` | `oracle_cdc_snapshot_chunk_retries_total` | counter | Snapshot chunks read again after an error, with a fresh SCN (SNAP-3, SNAP-4). |
| `SnapshotChunksPending` | `oracle_cdc_snapshot_chunks_pending` | gauge | Snapshot chunks read and waiting for streaming to pass their SCN. |
| `SnapshotChunksPublished` | `oracle_cdc_snapshot_chunks_published_total` | counter | Snapshot chunks published, after streaming passed their SCN. |
| `SnapshotChunksRead` | `oracle_cdc_snapshot_chunks_read_total` | counter | Snapshot chunks read (PRD-02). |
| `SnapshotRowsPublished` | `oracle_cdc_snapshot_rows_published_total` | counter | Snapshot rows published as op=r records. |
| `SnapshotTablesRemaining` | `oracle_cdc_snapshot_tables_remaining` | gauge | Tables the running snapshot has still to read. |
| `SpillMaxBytes` | `oracle_cdc_spill_max_bytes` | gauge | cdc.buffer.spill.max.bytes, for ratio alerts. |
| `SpilledBytes` | `oracle_cdc_spilled_bytes` | gauge | Bytes in spill files. |
| `SpilledTransactions` | `oracle_cdc_spilled_transactions` | gauge | Open transactions spilled to disk. |
| `StepCuts` | `oracle_cdc_step_cuts_total` | counter | Steps cut after a DDL that changed the captured object ids. |
| `StepRetries` | `oracle_cdc_step_retries_total` | counter | Steps discarded and mined again after a retriable LogMiner error (CDC-1002). |
| `StepTimeouts` | `oracle_cdc_step_timeouts_total` | counter | Steps cancelled at cdc.mining.query.timeout.ms; the next step covers fewer logs. |
| `Steps` | `oracle_cdc_steps_total` | counter | Mining steps applied. |
| `TransactionsCommitted` | `oracle_cdc_transactions_committed_total` | counter | Committed transactions handed to the sink. |
| `TransactionsDiscarded` | `oracle_cdc_transactions_discarded_total` | counter | Transactions dropped by cdc.transaction.max.age.action=discard. |
| `TransactionsSkipped` | `oracle_cdc_transactions_skipped_total` | counter | Committed transactions already delivered before a restart, skipped whole. |
| `UndoneEvents` | `oracle_cdc_undone_events_total` | counter | Changes removed or downgraded by savepoint and statement rollbacks. |
| `UnmatchedUndo` | `oracle_cdc_unmatched_undo_total` | counter | Undo rows that matched no buffered change. |
| `WindowLogs` | `oracle_cdc_window_logs` | gauge | Redo logs in the last step's window. |
