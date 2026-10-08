---
title: Error classes
description: The CDC error codes, what each means, whether the engine retries and where the runbook is.
---

# Error classes

Generated from `ErrorCode` by `ErrorClassesReferenceTest`; do not edit by hand.

Every condition that stops a capture task has a stable code, an operator action and a runbook. Kafka Connect shows the task's error message in the task status in the form `[CDC-2002] what happened Operator action: what to do Runbook: link`, and the `stop` event on the [ops topic](ops-topic.md) carries the code, the action and the link as separate fields. Codes are never reused or renumbered.

Codes marked as retried are handled inside the task first: the engine reconnects or mines the same range again, and the task stops with the code only when retrying has not helped. Every other code stops the task at once. Nothing is skipped in either case: once the cause is fixed and the task restarted, it resumes from its last acknowledged position.

| Code | Name | Retried | What the connector saw | Runbook |
|---|---|---|---|---|
| CDC-1001 | `TRANSIENT_DATABASE` | yes | The connection or the instance became unavailable. The engine reconnects with backoff and stops only when `cdc.retry.max.time.ms` runs out, or at once for an ORA code it does not classify. | [transient-database](../operations/runbooks/transient-database.md) |
| CDC-1002 | `MINING_STEP_RETRY` | yes | A LogMiner step failed with an error that mining the same range again fixes. The step is mined again; the task stops after more than 20 such failures in a row. | [mining-step-retry](../operations/runbooks/mining-step-retry.md) |
| CDC-2001 | `LOG_GAP` | no | The redo logs found for a thread do not cover the range to mine: a sequence is missing, or the first or last log falls inside the range. | [log-gap](../operations/runbooks/log-gap.md) |
| CDC-2002 | `LOG_PURGED` | no | A redo log the connector still needs is marked deleted in the catalog, or cannot be read although the catalog lists it. | [log-purged](../operations/runbooks/log-purged.md) |
| CDC-3001 | `DECODE` | no | A row of a captured table could not be decoded with `cdc.on.decode.error=fail`, or a captured table cannot be keyed or read. | [decode](../operations/runbooks/decode.md) |
| CDC-3002 | `CORRUPTION` | no | LogMiner reported MISSING_SCN, a spill file failed its integrity check, or the stored offset cannot be read. | [corruption](../operations/runbooks/corruption.md) |
| CDC-3003 | `LOB_TOO_LARGE` | no | A committed LOB value was larger than `cdc.lob.max.bytes` with `cdc.lob.oversize.action=fail`. | [lob-too-large](../operations/runbooks/lob-too-large.md) |
| CDC-4001 | `BUFFER_EXHAUSTED` | no | Spilled changes of open transactions exceeded `cdc.buffer.spill.max.bytes`, or writing to the spill directory failed. | [buffer-exhausted](../operations/runbooks/buffer-exhausted.md) |
| CDC-4002 | `JOURNAL_CORRUPTION` | no | At start, a journaled transaction is missing a chunk, or a journal record cannot be read with `cdc.journal.converter`. | [journal-corruption](../operations/runbooks/journal-corruption.md) |
| CDC-4003 | `MINING_STALLED` | no | A mining step over a single redo log timed out at `cdc.mining.query.timeout.ms` three times in a row. | [mining-stalled](../operations/runbooks/mining-stalled.md) |
| CDC-4004 | `TRANSACTION_TOO_OLD` | no | A transaction stayed open longer than `cdc.transaction.max.age.ms` with `cdc.transaction.max.age.action=fail`. | [transaction-too-old](../operations/runbooks/transaction-too-old.md) |
| CDC-5001 | `TOPOLOGY` | no | The stored offset belongs to another database (DBID or RESETLOGS changed), no valid archive destination can be mined, or the database has more than one enabled redo thread (RAC), which this release does not capture. | [topology](../operations/runbooks/topology.md) |
| CDC-5002 | `PRIVILEGE` | no | The database refused the connector user: a grant or a view is missing, or the login itself failed. | [privilege](../operations/runbooks/privilege.md) |
| CDC-6001 | `DICTIONARY_UNAVAILABLE` | no | Rows were written before a later DDL on their table, and no usable dictionary build in the redo or no exact schema version covers them. | [dictionary-unavailable](../operations/runbooks/dictionary-unavailable.md) |
| CDC-6002 | `UNSUPPORTED_DDL` | no | A DDL statement on a captured table could not be classified. | [unsupported-ddl](../operations/runbooks/unsupported-ddl.md) |
| CDC-6003 | `SCHEMA_MISMATCH` | no | At start, a schema version on the schema topic differs from the dictionary and no DDL ahead of the resume point explains it. | [schema-mismatch](../operations/runbooks/schema-mismatch.md) |
| CDC-6004 | `NAME_COLLISION` | no | Two columns of a table adjust to the same field name under `cdc.field.name.adjustment.mode`, or two tables route to the same topic only because characters Kafka does not allow became underscores. | [name-collision](../operations/runbooks/name-collision.md) |
| CDC-7001 | `ORPHAN_RELEASE_VIOLATION` | no | A COMMIT arrived for a transaction that orphan detection released or the age policy discarded. | [orphan-release-violation](../operations/runbooks/orphan-release-violation.md) |
| CDC-7002 | `ORPHAN_TRANSACTION` | no | A buffered transaction is no longer known to the database and `cdc.transaction.orphan.action=fail`. | [orphan-transaction](../operations/runbooks/orphan-transaction.md) |
| CDC-8001 | `SNAPSHOT_TOO_OLD` | no | A snapshot chunk still met ORA-01555 or ORA-08181 at the smallest chunk size. | [snapshot-too-old](../operations/runbooks/snapshot-too-old.md) |
