# PRD-02: Snapshots and Backfill

**Status:** Draft for implementation
**Module:** `kafka-connect-oracle-cdc`, package `sh.oso.connect.oracle.snapshot`
**Replaces:** Confluent `start.from=snapshot` and snapshot threads; Debezium blocking and incremental snapshots
**Depends on:** PRD-00 (position, interleaving with commits), PRD-03 (schema at SCN)
**Clean-room note:** The algorithm is our own, derived from the public SCN and flashback query semantics of Oracle Database. The DBLog paper describes the general idea of interleaving chunks with a change log ([DBLog](https://arxiv.org/abs/2010.12597)), and a September 2026 paper generalises it to reads tied to exact log positions ([Generalized DBLog](https://papers.cool/arxiv/2609.08160)).

---

## 1. Objective

Copy existing table contents into Kafka quickly, without long-running flashback queries, without writing to the source, resumably at chunk level, and while streaming continues, so that large initial loads do not end in ORA-01555 or a restart from zero (PP-09).

## 2. Scope

**In:** initial snapshot, snapshot of newly added tables, ad-hoc snapshot by signal (with optional predicate), recovery resnapshot after `OracleCdcPurgedException`, snapshot-only mode, parallel chunk reads, chunk-level resume, progress metrics.

**Out:** consistent single-SCN snapshot across all tables as a separate mode (the chunk algorithm gives a correct final state; cross-table point-in-time consistency is not a requirement users raised, and long flashback reads are the failure we are removing).

## 3. Algorithm: SCN-anchored chunks

For each table:

1. **Plan chunks.** Prefer key ranges on the primary key (or chosen unique key): sample boundaries with `SAMPLE BLOCK` or `NTILE` over the key; target `cdc.snapshot.chunk.rows` (default 100000). For tables without a usable key, chunk by ROWID ranges from `DBA_EXTENTS` (block ranges), which also suits partitioned and IOT tables. Composite keys use row-value comparisons (`(a, b) > (:a, :b)`) on the index order.
2. **Read a chunk.** On a snapshot connection, read `s = CURRENT_SCN` from `V$DATABASE`, then run `SELECT ... FROM t AS OF SCN :s WHERE <chunk range>`. The flashback window is only as long as one chunk read (seconds), so undo retention requirements are small.
3. **Hold the chunk.** Buffer the chunk rows (heap, spilling under the PRD-00 budget) tagged with `s`.
4. **Interleave.** The emitter emits commits in commit SCN order. When the next commit to emit has `commit_scn > s` (or the safe mined SCN passes `s` with no commits), emit the chunk rows as `op=r` before that commit. At that point the chunk represents exactly the table state at `s`, and every later change for those keys follows it in the stream, so per-key order in Kafka is correct without dropping or deduplicating rows.
5. **Record progress.** After the chunk's records are acknowledged (or committed in the Kafka transaction), the position records the chunk as done.

Correctness argument: a change committed at or before `s` is reflected in the chunk and was emitted earlier in the stream (or is reflected in the chunk only, if it happened before streaming started), and a change committed after `s` is emitted after the chunk. Consumers that materialise by key converge to the database state. The correctness oracle in `testing_strategy.md` checks this under concurrent writes.

Constraint: streaming must have mined at least to `s` before the chunk can be emitted. If streaming lags badly, chunk reads pause when held chunks exceed `cdc.snapshot.max.pending.chunks` (default `2 x threads`).

Before streaming has ever started (first deployment), the connector records `start_scn = CURRENT_SCN` before any chunk, starts mining from it, and applies the same rule.

## 4. Functional requirements

| ID | Requirement |
|---|---|
| SNAP-1 | `cdc.snapshot.mode`: `initial` (default; snapshot tables with no completed snapshot, then stream), `none` (stream from current SCN or `cdc.start.scn`), `snapshot_only` (snapshot then stop the task with status complete), `on_signal` (no automatic snapshot; signals only). There is no mode that can skip committed changes on restart (PP-02). |
| SNAP-2 | `cdc.snapshot.threads` (default 4) parallel chunk readers, across tables and within a table. |
| SNAP-3 | Chunk position in offsets: per table, completed chunk ranges compacted into a frontier plus an exception list; restart resumes at the first incomplete chunk. A failed chunk is retried up to `cdc.snapshot.chunk.retries` (default 5) with a fresh SCN, independent of other chunks ([dbz#2297](https://github.com/debezium/dbz/issues/2297)). |
| SNAP-4 | ORA-01555 or ORA-08181 on a chunk: retry the chunk with a new SCN and half the chunk size; after the size floor (1000 rows) fails, stop with guidance to raise `UNDO_RETENTION`. |
| SNAP-5 | `cdc.snapshot.fetch.size` (default 5000); `cdc.snapshot.select.overrides` per table (`SCHEMA.TABLE:<where clause>`) to exclude rows. |
| SNAP-6 | Signals (PRD-01 SRC-SIG): `snapshot` with tables and optional predicate; `snapshot-pause`, `snapshot-resume`, `snapshot-stop`. Read-only for the source; works in archive-only and standby modes. |
| SNAP-7 | Recovery resnapshot: `oracle-cdc-admin resnapshot --tables` (PRD-05) writes a signal and sets the position past the purged gap for those tables only; other tables are unaffected. This replaces renaming the connector (PP-03, PP-04). |
| SNAP-8 | Snapshot records use `op=r` (Debezium format) or `op_type=R` (Confluent format) with `source.snapshot` = `true`, `first`, `last`, or `incremental` for signal-driven snapshots. |
| SNAP-9 | Schema used for a chunk is the schema version at `s` (PRD-03). A DDL on a table between chunks causes remaining chunks to use the new schema; a DDL that changes the key restarts that table's snapshot. |
| SNAP-10 | Partitioned tables: chunk per partition when partitions exceed chunk size, with partition pruning (`PARTITION (p)` clause). |
| SNAP-11 | Standby (Phase 2): chunk reads may run on an Active Data Guard standby via `cdc.snapshot.database.url`, using `AS OF SCN` on the standby; the SCN must be at or below the standby's applied SCN. |
| SNAP-12 | Progress metrics per table: chunks total, done, failed, rows emitted, estimated time remaining; ops events per chunk and per table. |

## 5. Configuration

| Property | Type | Default | Description |
|---|---|---|---|
| `cdc.snapshot.mode` | enum | `initial` | See SNAP-1 |
| `cdc.snapshot.threads` | int | 4 | Parallel readers |
| `cdc.snapshot.chunk.rows` | int | 100000 | Target rows per chunk |
| `cdc.snapshot.chunk.retries` | int | 5 | Retries per chunk |
| `cdc.snapshot.fetch.size` | int | 5000 | JDBC fetch size |
| `cdc.snapshot.max.pending.chunks` | int | 8 | Held chunks before reads pause |
| `cdc.snapshot.select.overrides` | string | empty | Per-table predicates |
| `cdc.snapshot.database.url` | string | primary | Alternative read endpoint (Phase 2) |
| `cdc.snapshot.tables.order` | list | empty | Tables to snapshot first |

## 6. Non-functional requirements

- Snapshot read rate target: at least 50,000 rows per second per thread for narrow rows on reference hardware (validate in Phase 0).
- Undo requirement: no single flashback query longer than `cdc.snapshot.chunk.max.duration.ms` (default 120000); chunks are split if their read takes longer.
- No writes to the source database.

## 7. Acceptance criteria

- [ ] 200 GB, 160-table snapshot (matching the reported ORA-01555 case) completes with `UNDO_RETENTION` at 900 seconds under concurrent OLTP load.
- [ ] Killing the worker at 50 per cent of a 100 million row table resumes from the first incomplete chunk; no chunk is read twice except the one in flight.
- [ ] Materialised state after snapshot plus concurrent updates, deletes and key changes equals `SELECT ... AS OF SCN` at a later check SCN for every table (correctness oracle).
- [ ] A signal-driven snapshot of a table with a composite primary key of three columns completes at a rate within 20 per cent of a single-column key table of the same size.
- [ ] A table without a key is snapshotted by ROWID ranges when `cdc.key.missing=rowid`.
- [ ] Recovery resnapshot after a simulated purge restores only the affected tables.
