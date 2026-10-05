---
title: Ops topic
description: Versioned operational events the connector publishes about itself.
---

# Ops topic

The connector publishes events about its own behaviour to `${prefix}.cdc.ops` (configurable
with `cdc.ops.topic`). Operators and the doctor read it to explain what the connector did without
reading worker logs. The topic is not compacted; set its retention to suit your audit needs.

## Record shape

Every record is keyed by the connector's server name (the topic prefix) and carries a versioned
value. The value schema is `io.oso.cdc.ops.Event` version 1.

| Field | Type | Meaning |
|---|---|---|
| `v` | int32 | Schema version, currently `1` |
| `type` | string | Event type, one of the names below |
| `ts_ms` | int64 | Wall-clock time the event was produced, milliseconds since the epoch |
| `server` | string | The topic prefix of the connector that wrote it |
| `resume_scn` | int64, optional | The connector's resume SCN when the event was produced |
| `details` | map of string to string, optional | Type-specific details, listed below |

Ops records ride the same queue as change records and carry a Kafka Connect source offset, so
committing one never moves the connector's position past an unacknowledged change.

## Event types

| Type | When | Details |
|---|---|---|
| `startup` | The task started and its start position is durable | `resume_scn`, `last_commit` (absent on a fresh start), `database`, `version` |
| `stop` | The capture engine stopped with an error; the task fails right after this record | `exception`, `message`, `code` (for example `CDC-2002`), `runbook`, `operator_action` |
| `ddl-seen` | A DDL statement was mined for a non-Oracle owner | `pdb`, `owner`, `object`, `scn`, `sql` (truncated to 2,000 characters) |
| `ddl-applied` | A DDL gave a captured table a new schema version, or dropped or renamed it away | `pdb`, `owner`, `object`, `scn`, `version` (the new version number, or `removed`), `columns` |
| `dictionary-replay` | A step was mined again with a data dictionary from the redo, because rows of these tables were written before a later DDL on them | `from_scn`, `to_scn`, `tables` |
| `dictionary-build` | A scheduled dictionary build into the redo ran, failed, or was switched off for lack of `EXECUTE ON DBMS_LOGMNR_D` | `status` (`built`, `failed` or `disabled`), `millis` when built, `message` otherwise |
| `ids-refreshed` | The pushed-down object ids were re-resolved after a CREATE TABLE, DROP TABLE or partition DDL | `owners` |
| `reconnected` | The database sessions were reopened after a transient error | `cause` |
| `unsupported-row` | LogMiner returned an unsupported row for a captured table under the DLQ policy | `table`, `xid`, `scn`, `status`, `info` |
| `decode-error-dlq` | A row could not be decoded and was written to the DLQ | `table`, `xid`, `scn`, `operation`, `error` |
| `transaction-orphan-released` | The orphan detector released a transaction absent from the database (ADR-0006) | `xid`, `con_id`, `user`, `client_id`, `first_scn`, `last_scn`, `events`, `absent_at_scn`, `reason` |
| `transaction-discarded` | A transaction older than `cdc.transaction.max.age.ms` was dropped under the `discard` action | `xid`, `con_id`, `user`, `first_scn`, `last_scn`, `events`, `age_ms` |
| `transaction-split` | In exactly-once mode, an Oracle transaction above the `cdc.eos.split.*` limits was delivered in several Kafka transactions | `xid`, `commit_scn`, `events`, `kafka_transactions` |

The following types are reserved for features that are not yet in a published release. They do not
appear on the topic today: `position-committed`, `log-switch-detected`, `thread-state-changed`,
`table-added`, `table-removed`, `transaction-journaled`, `snapshot-chunk-done`, `snapshot-complete`, `signal-ack`, `offsets-set`.

## Internal topics

With `cdc.kafka.bootstrap.servers` set, the task creates the internal topics it needs before its
first record: the ops and heartbeat topics with the `delete` cleanup policy and the schema and
transaction journal topics compacted. Other client settings for that admin client and for the
journal reader go under `cdc.kafka.*`, for example `cdc.kafka.security.protocol`. Without broker access the connector relies
on the worker's topic creation (`topic.creation.enable`) or on topics you create in advance; the
schema and journal topics must then be compacted.

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
