---
title: How capture works
description: LogMiner mining of uncommitted redo in steps, the connector's own transaction buffer, the redo address cursor and the no-silent-loss rule.
---

# How capture works

The connector runs one task with one LogMiner session. In a container database the session runs
at the root container and reads the redo of every PDB listed in `cdc.database.pdbs`; in a
non-container database it runs in the database itself. Nothing is written to the source database:
no flush table, no heartbeat table, no signal table.

## Mining in steps

The engine mines in steps. Each step covers the redo from its cursor up to the database's current
SCN, read in a window of whole redo logs:

1. **Log inventory.** Before every step the engine lists the logs covering the range for every
   enabled redo thread, preferring the archived copy at one archive destination and, in the default
   `online` capture mode, adding the online logs for the part not yet archived. It checks that the
   logs are continuous and present; a gap stops the task with
   [CDC-2001](../operations/runbooks/log-gap.md), a purged log with
   [CDC-2002](../operations/runbooks/log-purged.md). With `cdc.capture.mode=archive_only` only
   archived logs are mined, so a change becomes visible once its log is archived.
2. **Filter in the database.** The captured tables are resolved to their object ids per container,
   and the LogMiner query asks only for changes to those objects, transaction control rows and DDL.
   Users in `cdc.users.exclude` are filtered out in the same query, so their transactions never
   reach the buffer. A CREATE TABLE, a DROP TABLE or partition maintenance ends the step at that
   statement, the object ids are resolved again, and the next step starts right after it, so a new
   table that matches `cdc.tables.include` is captured from its first row.
3. **Read the whole step, then apply it.** The rows of a step are read and decoded before any of
   them changes the transaction buffer. A step that fails part way, for example because a log was
   switched while it was read, is discarded and mined again from the same cursor. A step is never
   half applied.
4. **Adapt the window.** A step that finishes faster than `cdc.mining.target.latency.ms` lets the
   next one cover twice as many logs, up to `cdc.mining.max.logs.per.step`. A step that times out
   at `cdc.mining.query.timeout.ms` is cancelled and the next covers half as many. The LogMiner
   session is restarted every `cdc.mining.session.max.age.ms` to release its memory.

Decoding turns each row's SQL_REDO into column values with the table's schema version. It is a
pure function of the row and the schema, so steps of 256 rows or more are decoded on
`cdc.mining.decode.threads` threads, and the results are applied in redo order.

Columns named by `cdc.columns.exclude` are dropped at this point, as soon as the parser has named
them and before their values are converted. An excluded value therefore never reaches the
transaction buffer, the spill files, the transaction journal, a record or a log line, and a value
the decoder could not convert cannot stop the task when its column is excluded. The schema topic
still holds the table's full layout (names and types, no values), so changing the filter needs no
schema rebuild. See [excluded columns](../reference/record-formats.md#excluded-columns).

## Uncommitted redo and the transaction buffer

LogMiner returns changes as they are written, before their transactions commit. The connector keeps
them in its own [transaction buffer](transaction-buffer-and-journal.md), one entry per
transaction, until LogMiner returns the COMMIT or ROLLBACK. A COMMIT publishes the transaction's
changes in order; a ROLLBACK drops them; a rollback to a savepoint arrives as undo rows that remove
the changes they reverse. Consumers therefore only ever see committed data, in commit order, and
never a rolled-back change.

Because the buffer is the connector's own, a long transaction does not force the connector to mine
its redo again and again: large transactions spill to disk, and long ones are journaled to Kafka so
that the restart position can move past their first change. Transactions that the database no
longer knows are found by [orphan detection](transaction-buffer-and-journal.md) rather than held
for ever.

## Where a step starts and ends

The cursor between steps is the redo byte address of the last row applied, not an SCN. That
matters because Oracle keeps the redo of an open transaction in a private strand until the
transaction commits, the strand fills or a log switch binds it, and the records keep the SCNs of
the original changes when they finally reach the log. A step bounded by SCN alone could pass over
those records and never see them. Because the redo log is append-only in redo byte address order,
selecting rows after the cursor's address catches every record that reached the log since the
previous step, whatever its SCN. The connector never writes to the source database to force a
flush.

The committed offset carries the same shape: the resume SCN for choosing logs, the redo byte
address to resume from, and the redo address of the last acknowledged commit, so a restart repeats
nothing and skips nothing. See [offsets and recovery](offsets-and-recovery.md).

## From a commit to Kafka

A committed transaction becomes one [record](../reference/record-formats.md) per change, each
carrying in its source offset the position a restart may use once that record is acknowledged.
Records wait in a bounded queue until Kafka Connect polls them. With
[exactly-once delivery](exactly-once.md) the connector also tells Connect where each Kafka
transaction may end, which is always at an Oracle commit boundary.

On a quiet database a heartbeat record every `cdc.heartbeat.interval.ms` carries the current
position, so committed offsets keep moving and the redo the connector needs stays recent.

## Errors

A transient database error (a dropped connection, a killed session, an instance restart, or a
pluggable database that is not open yet after a restart) makes the engine reopen its sessions with
backoff, within `cdc.retry.max.time.ms`, and mine the failed step again from the same cursor; see
[CDC-1001](../operations/runbooks/transient-database.md). A LogMiner error that mining the same
range again fixes, such as an online log switched or reused while it was read, discards the step
and mines it again without reconnecting ([CDC-1002](../operations/runbooks/mining-step-retry.md)).
Nothing of a failed step is ever applied. Every other unexpected
condition stops the task with a typed error: a code such as `CDC-2002`, an operator action, and a
link to the runbook for the code (see [error classes](../reference/error-classes.md)). An ORA code
the connector does not classify stops the task with the code in the message, rather than being
retried or ignored. No setting lets the connector continue past missing redo or corrupt redo. The
one choice offered is for rows that cannot be decoded: with `cdc.on.decode.error=dlq` such a row
goes to a dead letter topic with an ops event, instead of stopping the task, and is never dropped
without a trace.
