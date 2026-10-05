---
title: Ops topic
description: Versioned operational events the connector publishes about itself, and the internal topics it creates.
---

# Ops topic

The connector publishes events about its own behaviour to `${prefix}.cdc.ops` (configurable
with `cdc.ops.topic`). Operators read it to see what the connector did, and why it stopped,
without reading worker logs. The topic is not compacted; set its retention to suit your audit
needs.

## Record shape

Every record is keyed by the connector's server name, the topic prefix: the key schema is
`io.oso.cdc.ops.Key` with one string field, `server`. The value schema is `io.oso.cdc.ops.Event`
version 1.

| Field | Type | Meaning |
|---|---|---|
| `v` | int32 | Schema version, currently `1` |
| `type` | string | Event type, one of the names below |
| `ts_ms` | int64 | Wall-clock time the event was produced, milliseconds since the epoch |
| `server` | string | The topic prefix of the connector that wrote it |
| `resume_scn` | int64, optional | The connector's resume SCN when the event was produced |
| `details` | map of string to string, optional | Type-specific details, listed below; a detail with no value is left out |

Ops records ride the same queue as change records and carry a Kafka Connect source offset, so
committing one never moves the connector's position past an unacknowledged change. Under
exactly-once delivery they are part of the same Kafka transactions as the change records. The exception
is `offsets-set`, which `oracle-cdc-admin` writes directly while the connector is stopped; its
`resume_scn` is the new resume SCN.

## Event types

The table below is generated from the connector's code, so it lists exactly the events the
connector can write.

<!-- BEGIN GENERATED: ops event types (OpsTopicReferenceTest; do not edit by hand) -->

| Type | When | Details |
|---|---|---|
| `startup` | The task started and its start position is durable. | `resume_scn`, `last_commit` (absent on a fresh start), `database`, `version` |
| `stop` | The capture engine or the snapshot publisher stopped with an error; the task fails right after this record. | `exception`, `message`, `code` (for example CDC-2002; absent for an error without a code), `runbook`, `operator_action` |
| `ddl-seen` | A DDL statement by an owner that Oracle does not maintain was mined. | `pdb`, `owner`, `object`, `scn`, `sql` (truncated to 2,000 characters) |
| `ddl-applied` | A DDL gave a captured table a new schema version, or dropped or renamed it away. | `pdb`, `owner`, `object`, `scn`, `version` (the new version number, or removed), `columns` (absent when removed) |
| `dictionary-replay` | A step was mined again with a data dictionary from the redo, because rows of these tables were written before a later DDL on them. | `from_scn`, `to_scn`, `tables` |
| `dictionary-build` | A dictionary build into the redo ran, failed, or was switched off because the connector user cannot execute DBMS_LOGMNR_D. | `status` (built, failed or disabled), `millis` (when built), `message` (when failed or disabled) |
| `table-added` | A refresh of the object ids (after a CREATE TABLE, a rename or a refresh-tables signal) added a table matching the include patterns; it is captured from the next step. | `table`, `snapshot` (true when the table is snapshotted because it may already hold rows) |
| `table-removed` | A refresh of the object ids removed a table from the captured set (dropped, or renamed away). | `table` |
| `ids-refreshed` | The captured object ids were resolved again, after a CREATE TABLE, DROP TABLE or partition DDL, or a refresh-tables signal. | `owners` |
| `reconnected` | The database sessions were reopened after a transient error. | `cause` |
| `unsupported-row` | LogMiner marked a row of a captured table unsupported and `cdc.on.decode.error` is dlq. | `table`, `xid`, `scn`, `status`, `info` |
| `decode-error-dlq` | A row of a captured table could not be decoded and `cdc.on.decode.error` is dlq. | `table`, `xid`, `scn`, `operation`, `error` |
| `transaction-discarded` | A transaction open longer than `cdc.transaction.max.age.ms` was dropped because `cdc.transaction.max.age.action` is discard. | `xid`, `con_id`, `user`, `first_scn`, `last_scn`, `events`, `age_ms` |
| `transaction-orphan-released` | Orphan detection released a transaction that the database no longer knows, as a rollback. | `xid`, `con_id`, `user`, `client_id`, `first_scn`, `last_scn`, `events`, `absent_at_scn`, `reason` |
| `transaction-split` | In exactly-once mode, an Oracle transaction above `cdc.eos.split.max.records` or `cdc.eos.split.max.bytes` was delivered in several Kafka transactions. | `xid`, `commit_scn`, `events`, `kafka_transactions` |
| `snapshot-chunk-done` | A snapshot chunk was published. | `table`, `scn`, `rows` |
| `snapshot-complete` | A table's snapshot is complete, or the whole snapshot is complete or was stopped by a signal. | `table` (the table, or an asterisk for the whole snapshot), `stopped` (true when a snapshot-stop signal ended the snapshot), `skipped` (why the table was not read: it no longer exists, or a column type has no record mapping under `cdc.on.decode.error=dlq`) |
| `signal-ack` | A signal addressed to this connector was handled. | `id`, `type`, `outcome` (ok, rejected, unknown, invalid or failed), `message` (when there is one), `mined_to_scn` (log-state only), `open_transactions` (log-state only), `buffered_events` (log-state only), `oldest_open_scn` (log-state only), `largest` (log-state only), `snapshot_running` (log-state only) |

The following types are reserved for features that are not built yet. No code path writes them, so they do not appear on the topic: `position-committed`, `log-switch-detected`, `thread-state-changed`, `transaction-journaled`, `offsets-set`.

<!-- END GENERATED: ops event types -->

A `stop` event is the record of last resort when a task fails: its `code` leads to the
[error classes](error-classes.md) and its `runbook` to the page that explains the recovery.

## Internal topics

With `cdc.kafka.bootstrap.servers` set, the task creates the internal topics it needs before its
first record, each with one partition and the replication factor in
`cdc.internal.topic.replication.factor`:

| Topic | Default name | Cleanup policy |
|---|---|---|
| Ops | `${prefix}.cdc.ops` | delete |
| Heartbeat | `${prefix}.cdc.heartbeat` | delete, retention 24 hours |
| Signals | `${prefix}.cdc.signals` | delete |
| Schema | `${prefix}.cdc.schema` | compact |
| Transaction journal | `${prefix}.cdc.txjournal` | compact |
| Decode DLQ | `${prefix}.cdc.dlq` | delete; created only with `cdc.on.decode.error=dlq` or `cdc.transaction.max.age.action=discard` |
| Transaction metadata | `${prefix}.cdc.transactions` | delete; created only with `cdc.transactions.topic.enabled=true`, and nothing writes to it yet |

Topics that already exist are left as they are. Other client settings for the admin client and
for the readers of the schema and journal topics go under `cdc.kafka.*`, for example
`cdc.kafka.security.protocol`. Without broker access the connector relies on the worker's topic
creation (`topic.creation.enable`) or on topics you create in advance; the schema and journal
topics must then be compacted, and the transaction journal, the schema topic and signals are
not available.

## Decode DLQ topic

With `cdc.on.decode.error=dlq`, a row the connector cannot decode, or that LogMiner marks
unsupported for a captured table, goes to `cdc.dlq.topic` (default `${prefix}.cdc.dlq`) instead of
stopping the task, and so does a transaction discarded under `cdc.transaction.max.age.action=discard`.
Records are keyed by server and transaction id; the value schema `io.oso.cdc.dlq.Record` version 1
carries `kind` (`decode-error`, `unsupported-row` or `transaction-discarded`), the table, the redo
position (`scn`, `rs_id`, `ssn`), the operation, LogMiner's `status` and `info`, the raw `sql_redo`
and `sql_undo`, the exception class and message, and for a discarded transaction the user, first and
last SCN, event count and age. The raw SQL contains column values, so protect the topic like the
change topics.

Redo corruption (`MISSING_SCN`) is never sent to the DLQ: it always stops the task with
[CDC-3002](../operations/runbooks/corruption.md).
