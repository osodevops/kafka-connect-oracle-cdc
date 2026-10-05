---
title: Snapshots
description: SCN-anchored chunked snapshots that resume after a restart and interleave with streaming.
---

# Snapshots

A snapshot publishes the rows a captured table already holds, as `op=r` records, while the
connector streams the changes made meanwhile. On a connector's first start, `cdc.snapshot.mode`
decides whether it takes one:

| Mode | What happens |
|---|---|
| `initial` (default) | Every captured table is read, then the connector streams on. Streaming starts at the same moment: changes made during the snapshot are not missed. |
| `none` | No snapshot; the connector streams from the current SCN. |
| `snapshot_only` | Every captured table is read; then the task stays idle and does not stream. |
| `on_signal` | No snapshot by itself. Snapshots by signal are not in this release. |

A snapshot that the stored offset records as unfinished resumes in every mode. An offset written
before snapshots existed, or with a finished snapshot, starts none.

## How a table is read

Each table is cut into chunks of about `cdc.snapshot.chunk.rows` rows (default 100,000):

- by its primary key, or the key chosen with `cdc.key.*`, when every key column is NOT NULL and a
  number, character, DATE, TIMESTAMP or RAW column; composite keys are compared column by column;
- by ROWID ranges from the table's extents when it has no such key and is a heap table;
- as a single chunk otherwise, for example an index-organised table keyed by an unsupported type.

Up to `cdc.snapshot.threads` chunks of a table are read in parallel, on connections of their own,
all with `SELECT ... AS OF SCN` at one SCN taken just before them. One read lasts only as long as
one chunk, so the snapshot needs little undo. A filter for one table goes in
`cdc.snapshot.select.override.PDB.OWNER.TABLE` as a SQL condition, and
`cdc.snapshot.tables.order` lists tables to read first.

## Ordering with streaming

A chunk read at an SCN is published after every change committed before that SCN and before every
change committed at or after it. While a chunk is being read, streaming waits rather than publish a
later change first. A consumer that keeps the latest record per key therefore converges to the
table's contents, whatever ran during the snapshot. Up to `cdc.snapshot.max.pending.chunks` chunks
wait in memory for streaming to reach their SCN; reads pause beyond that.

## Records

Snapshot records have `op` `r`, no `before`, and a `source` block with `snapshot` set to `first`
for the first record, `last` for the last and `true` otherwise; `source.scn` is the chunk's SCN,
and `txId` and `commit_scn` are empty. They carry the header `cdc.snapshot`.

## Restarts

The offset records each table's progress: the key or ROWID where its next chunk starts, and the
tables that are done. A restart reads again only the chunks that were read but not yet acknowledged.

A chunk that fails is read again with a fresh SCN, up to `cdc.snapshot.chunk.retries` times; the
chunks before it are kept. When the undo for a chunk's SCN is gone (ORA-01555 or ORA-08181), the
chunk is read again at half the size, down to 1,000 rows; after that the task stops with
`CDC-8001`.

The `snapshot-chunk-done` and `snapshot-complete` events on the ops topic, and the `Snapshot*`
metrics, show progress.

## Limitations in this release

- A table whose key columns change during its snapshot is read with the key it had at the start.
- A keyless table with row movement enabled can have rows that move between ROWID ranges during the
  snapshot; `oracle-cdc-doctor` warns about such tables.
- Partitions are read through the table's key or ROWID ranges, not one partition at a time.
- Tables that start matching `cdc.tables.include` after the first start are streamed but not
  snapshotted.
