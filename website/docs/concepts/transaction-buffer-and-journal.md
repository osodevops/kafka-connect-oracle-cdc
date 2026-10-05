---
title: Transaction buffer and journal
description: Why offsets never pin on a long transaction and how large transactions leave the heap.
---

# Transaction buffer and journal

Changes are buffered per transaction, keyed by container and transaction id, until LogMiner reports
the COMMIT. A ROLLBACK discards the entry. A savepoint rollback arrives as undo rows; each one
removes the latest earlier change with the same ROWID, so partially rolled back transactions are
emitted exactly as Oracle committed them.

## Heap budget and spill

The buffer keeps changes on the heap up to `cdc.buffer.memory.max.bytes` (default 256 MiB). When
an addition takes it over the budget, the largest open transaction moves to an append-only file
under `cdc.buffer.spill.dir` and keeps appending there, so a single large transaction never grows
the heap again. Several transactions spill, largest first, until the heap is back within budget.

- The default spill directory is the worker's temporary directory plus `oracle-cdc-spill` and the
  connector name.
- Every frame in a spill file carries a CRC32. A damaged or truncated file stops the task with
  `CDC-3002` rather than emitting a shortened transaction.
- The spill files are not durable state. After a restart the connector re-mines open transactions
  from the position, and files left by an earlier run are removed when the task starts.
- At COMMIT a spilled transaction is read back from disk in redo order while its records are
  produced, so the heap stays within the budget whatever the transaction size. The file is deleted
  once the records are queued.
- The total on disk is capped by `cdc.buffer.spill.max.bytes` (default 10 GiB). Exceeding it stops
  the task with `CDC-4001`, naming the ten largest open transactions with their transaction id,
  user, first SCN, event count and bytes on heap and disk. A full spill volume produces the same
  typed stop with the operating system's message.

The buffer metrics report open transactions, buffered events, heap bytes, spilled transactions and
spilled bytes.

## Journal

A transaction open longer than `cdc.txjournal.threshold.ms` (default five minutes) or holding more
than `cdc.txjournal.threshold.events` changes (default 100,000) is journaled to the compacted topic
named by `cdc.txjournal.topic` (default `${prefix}.cdc.txjournal`). The journal records are emitted
from the task's poll() like change records, so under exactly-once delivery they share the Kafka
transaction with data and offsets, and in at-least-once mode Kafka Connect acknowledges them in
order with the data.

- The first chunk holds the transaction's whole history so far; every later mining step appends the
  changes since the previous chunk, undo rows included. Chunks are split at about
  `cdc.txjournal.chunk.max.bytes` (default 512 KiB).
- Once journaled, a transaction no longer pins the resume position at its start. The position is
  bounded by the transaction's last journaled record instead, so archived logs holding the
  beginning of a long transaction can be purged while it is open.
- At COMMIT the transaction's events are published and one tombstone per chunk follows, so the
  compacted topic forgets the transaction. A ROLLBACK writes the tombstones directly.
- At start the task reads the journal topic back with the converter named by
  `cdc.journal.converter`. The default reads JSON written with or without the schema envelope, so it
  matches a worker using the JSON converter in either mode; name the worker's converter class when it
  uses another format. The open transactions are rebuilt before mining resumes. Journaled
  records at or after the resume point in redo order are dropped because mining produces them again. Records
  written by a task start that never committed an offset carry a newer generation and are
  tombstoned. A gap in chunk numbers stops the task with `CDC-4002`.
- The journal needs `cdc.kafka.bootstrap.servers`. Without it the task logs a warning at start,
  journals nothing, and long transactions pin the resume position as before.

## Orphaned transactions

A transaction can disappear from the database without LogMiner ever returning its COMMIT or
ROLLBACK, for example when its session is killed in a way that leaves no end record in the redo
mined so far. Such a transaction would stay in the buffer and hold the resume position at its first
change for ever. Every `cdc.transaction.orphan.check.interval.ms` (five minutes by default) the
connector checks the transactions it has buffered for longer than that interval against
`GV$TRANSACTION`. A transaction is an orphan when it was absent in two consecutive checks, the
connector has mined past the SCN of the first absence without seeing its end, and the session named
by its START row no longer exists.

With `cdc.transaction.orphan.action=release` (the default) the connector drops an orphan as a
rollback and writes a `transaction-orphan-released` event to the ops topic. Its id joins the
released ledger carried in the offsets, so if a COMMIT for it arrives after all, the task stops with
[CDC-7001](../operations/runbooks/orphan-release-violation.md) rather than publish part of it. With
`fail` the task stops with [CDC-7002](../operations/runbooks/orphan-transaction.md) and the operator
decides.

Transactions with an all-zero transaction id, which LogMiner reports for some internal operations,
never own changes and never create a buffer entry.

## Long transactions

`cdc.transaction.max.age.ms` (default unlimited) bounds how long a transaction may stay open in the
buffer. On breach, `cdc.transaction.max.age.action=fail` stops the task with `CDC-4004`;
`discard` drops the transaction after writing a `transaction-discarded` event to the ops topic and a
record to the DLQ topic. A discarded transaction joins the released ledger carried in the offsets,
so if its COMMIT arrives later the task stops with `CDC-7001` instead of publishing the part of the
transaction mined after the discard. Nothing is ever discarded silently.
