---
title: Offsets and recovery
description: The versioned position, what it encodes and how restart replays exactly the unacknowledged suffix.
---

# Offsets and recovery

**Status:** implemented for a single redo thread; the per-thread position vector for RAC comes
with Phase 2.

Every record the connector hands to Kafka Connect carries a position in its source offset. Kafka
Connect commits the offset of the latest acknowledged record, so the position only ever encodes what
the framework acknowledged: nothing is marked done because it was mined or queued.

## Fields

| Field | Meaning |
|---|---|
| `v` | Position version, currently `1` |
| `resume_scn` | The SCN from which logs are selected on restart; the lower of the mined-to SCN and the first capture of the oldest open transaction (the last journaled record for a journaled one) |
| `resume_rs_id`, `resume_ssn` | The redo byte address to resume from, inclusive. The redo log is append-only in this order while SCNs can arrive out of order, so this is the real cursor |
| `last_commit_scn`, `last_commit_xid`, `last_commit_thread` | The commit whose records were acknowledged last |
| `last_commit_rs_id`, `last_commit_ssn` | The redo byte address of that commit row; commit order for the skip rule is redo order |
| `event_index` | How many records of that commit were acknowledged |
| `journal_generation` | Incremented at every task start; journal chunks of a newer generation than the position were never acknowledged |
| `schema_epoch` | Reserved for the schema topic |
| `dbid`, `resetlogs_scn` | The database identity; a mismatch at start stops the task with `CDC-5001` |
| `released_xids` | Transactions the orphan detector released; a later COMMIT for one of them stops the task with `CDC-7001` |
| `snapshot` | Reserved for the snapshot frontier |

Unknown fields are kept and written back, so a one-version downgrade does not lose them.

## Restart

The engine mines from the resume point: LogMiner starts at the first SCN of the log holding it, and
rows are selected by redo byte address from the point inclusive. Open transactions are rebuilt from
redo, or from the journal topic when they were journaled. Commits that sort before the last
acknowledged commit in redo order are skipped whole; the acknowledged commit itself skips its first
`event_index` records; everything after is emitted. The result is exactly the unacknowledged suffix,
which the property test `ReplaySuffixPropertyTest` proves for random commit sequences and restart
points.

## Why the cursor is a redo byte address

Oracle keeps the redo of an open transaction in a private strand until the transaction commits, the
strand fills or a log switch binds it. The records keep the SCNs of the original changes when they
reach the log, so a cursor expressed as an SCN could pass over them and never see them. Selecting by
redo byte address catches every record that reached the log since the previous step. The evidence is
recorded in the repository under `oracle-cdc-core/src/main/resources/reference/redo-flush-lag.md`.
