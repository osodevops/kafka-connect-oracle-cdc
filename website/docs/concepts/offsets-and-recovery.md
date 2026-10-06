---
title: Offsets and recovery
description: The versioned position, what it encodes, how a restart replays exactly the unacknowledged suffix, and how to move it by hand.
---

# Offsets and recovery

Every record the connector hands to Kafka Connect carries a position in its source offset. Kafka
Connect commits the offset of the latest acknowledged record, so the position only ever encodes what
the framework acknowledged: nothing is marked done because it was mined or queued.

The position is per redo thread. Oracle RAC, which needs one position per thread, is not supported
yet.

## Fields

The offset partition is `{"server": "<prefix>"}`, where the prefix is `cdc.topic.prefix`. The
offset holds:

| Field | Meaning |
|---|---|
| `v` | Position version, currently `1` |
| `resume_scn` | The SCN from which logs are selected on restart; the lower of the mined-to SCN and the first change of the oldest open transaction (the last journaled change for a journaled one) |
| `resume_rs_id`, `resume_ssn` | The redo byte address to resume from, inclusive. The redo log is append-only in this order while SCNs can arrive out of order, so this is the real cursor |
| `last_commit_scn`, `last_commit_xid`, `last_commit_thread` | The commit whose records were acknowledged last |
| `last_commit_rs_id`, `last_commit_ssn` | The redo byte address of that commit row; commit order for the skip rule is redo order |
| `event_index` | How many records of that commit were acknowledged |
| `journal_generation` | Incremented at every task start; journal chunks of a newer generation than the position were never acknowledged |
| `schema_epoch` | Reserved; written as `0` |
| `dbid`, `resetlogs_scn` | The database identity; a mismatch at start stops the task with [CDC-5001](../operations/runbooks/topology.md) |
| `released_xids` | Transactions that orphan detection released or the age policy discarded; a later COMMIT for one of them stops the task with [CDC-7001](../operations/runbooks/orphan-release-violation.md) |
| `snapshot` | The snapshot progress as JSON text: whether the snapshot is complete, and per table whether it is done and the key or ROWID where its next chunk starts (see [snapshots](snapshots.md)); absent when no snapshot was ever started |
| `signal_offset` | The offset of the last signal handled on the signal topic, so a restart does not handle it again (see [signals](../operations/signals.md)) |
| `snapshot_pending` | Tables that joined the captured set while streaming and wait for their snapshot, so a restart does not lose them (see [multi-PDB capture](multi-pdb.md#tables-that-appear-later)) |
| `start_floor_scn`, `start_open_scn`, `start_open_xids` | Present only after a first start and until the first commit is acknowledged: the start SCN, the SCN the open transactions were read after, and those transactions. Below the start SCN only those transactions are kept, so a restart before the first acknowledged commit applies the same rule (see [first connector](../getting-started/first-connector.md#what-happens-at-the-first-start)) |

Fields the connector does not know are kept and written back, so a one-version downgrade does not
lose them. An offset with a newer `v` than the connector reads stops the task with
[CDC-3002](../operations/runbooks/corruption.md) rather than guessing.

## Restart

The engine mines from the resume point: LogMiner starts at the first SCN of the log holding it, and
rows are selected by redo byte address from the point inclusive. Open transactions are rebuilt from
redo, or from the journal topic when they were journaled. Commits that sort before the last
acknowledged commit in redo order are skipped whole; the acknowledged commit itself skips its first
`event_index` records; everything after is emitted. The result is exactly the unacknowledged suffix,
which the property test `ReplaySuffixPropertyTest` proves for random commit sequences and restart
points.

In at-least-once mode, Kafka Connect may have written records whose offsets it had not yet
committed when a task died; those records are delivered again after the restart. The `cdc.xid` and
`cdc.event_index` headers identify them. With [exactly-once delivery](exactly-once.md) the offsets
commit in the same Kafka transaction as the records, so nothing is delivered twice.

## Reading and moving the offset by hand

Kafka Connect 3.6 and later can read, stop and alter a connector's offsets through its REST API.
The runbooks use this procedure when the right recovery is to mine again from an earlier point or,
as a deliberate decision, to skip forward.

```bash
CONNECT=http://localhost:8083
NAME=orders-cdc
curl -s -X PUT "$CONNECT/connectors/$NAME/stop"     # the task stops; wait until the state is STOPPED
curl -s "$CONNECT/connectors/$NAME/offsets"          # the partition and the stored offset
# edit the offset as described below and save the request body as offset.json
curl -s -X PATCH -H 'Content-Type: application/json' --data @offset.json "$CONNECT/connectors/$NAME/offsets"
curl -s -X PUT "$CONNECT/connectors/$NAME/resume"
```

`offset.json` holds the partition and the whole offset as read, with your edits:

```json
{"offsets": [{"partition": {"server": "cdc"}, "offset": {"v": 1, "resume_scn": 4711000, "dbid": 1234567890, "resetlogs_scn": 1, "journal_generation": 7, "schema_epoch": 0}}]}
```

The edits that make sense:

- **Mine again from an earlier SCN.** Set `resume_scn` to the earlier SCN and remove
  `resume_rs_id` and `resume_ssn`. The redo from that SCN must still be available. Commits up to
  the last acknowledged one are still skipped, so nothing is delivered twice; to deliver those
  transactions again as well, also remove the five `last_commit_*` fields and set `event_index`
  to `0`.
- **Skip forward.** Set `resume_scn` to the later SCN and remove `resume_rs_id` and `resume_ssn`.
  Every change between the old position and the new one is not delivered, and a transaction that
  was open at the new SCN is delivered without its earlier changes. Choose an SCN at which no
  transaction on the captured tables was open, and reload the affected tables afterwards, for
  example with a [`snapshot` signal](../operations/signals.md). This is the one place where data
  is left out, and it happens only because an operator chose it.

Keep `dbid` and `resetlogs_scn` as they were read: they tie the offset to its database. Keep
`released_xids` too; it is what stops a late COMMIT of a released transaction from being published
in part.

[`oracle-cdc-admin offsets set`](../operations/admin.md) wraps these steps and refuses an SCN whose
redo is already purged.

## Why the cursor is a redo byte address

Oracle keeps the redo of an open transaction in a private strand until the transaction commits, the
strand fills or a log switch binds it. The records keep the SCNs of the original changes when they
reach the log, so a cursor expressed as an SCN could pass over them and never see them. Selecting by
redo byte address catches every record that reached the log since the previous step. The evidence is
recorded in the repository under `oracle-cdc-core/src/main/resources/reference/redo-flush-lag.md`.
