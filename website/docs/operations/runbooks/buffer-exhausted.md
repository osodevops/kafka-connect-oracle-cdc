---
title: "CDC-4001 Transaction buffer exhausted"
description: "Runbook for CDC-4001 BUFFER_EXHAUSTED: open transactions spilled more to disk than cdc.buffer.spill.max.bytes allows, or the spill volume failed."
slug: /runbooks/buffer-exhausted
---

# CDC-4001 Transaction buffer exhausted

**Code:** `CDC-4001` (`BUFFER_EXHAUSTED`). Not retried: the task stops at once.

## What the connector observed

The connector keeps the changes of every open transaction until LogMiner reports its COMMIT or
ROLLBACK. Up to `cdc.buffer.memory.max.bytes` (256 MiB by default) they stay on the heap; above that
the largest open transactions spill to files under `cdc.buffer.spill.dir`. One of two things
happened:

- The spill files together exceeded `cdc.buffer.spill.max.bytes` (10 GiB by default).
- Writing to the spill directory failed, for example because the volume is full.

The message names the ten largest open transactions with their transaction id, user, first SCN,
number of changes, and bytes on the heap and on disk.

## Why it stopped rather than continued

Every buffered change belongs to a transaction that may still commit. Dropping any of them to make
room would publish that transaction incomplete when it commits. The cap protects the worker's disk;
when it is reached, the operator decides what to do.

## Confirm the cause

Find the transactions named in the message and what runs them:

```sql
SELECT t.inst_id, s.sid, s.serial#, s.username, s.program, s.machine, t.start_time, t.start_scn,
       t.used_urec, t.used_ublk
FROM gv$transaction t JOIN gv$session s ON s.inst_id = t.inst_id AND s.taddr = t.addr
ORDER BY t.used_urec DESC;
```

On the worker, check the spill volume with `df -h` on the spill directory. On Strimzi the default
spill directory is under `/tmp`, a 5 MiB in-memory volume, so a single large transaction fills it;
see [the spill volume on Strimzi](../../getting-started/strimzi.md#spill-volume). Before the stop, the
`SpilledBytes`, `SpillMaxBytes`, `OpenTransactions` and `LargestTransactions` metrics, the
`OracleCdcSpillNearLimit` alert and a [`log-state` signal](../signals.md) show the same picture.

## Recover

1. Choose one or more of:
   - raise `cdc.buffer.spill.max.bytes`, after making sure the volume has room for it;
   - point `cdc.buffer.spill.dir` at a larger volume;
   - raise `cdc.buffer.memory.max.bytes`, together with the worker's heap;
   - have the application commit or roll back the transactions named in the message;
   - set `cdc.transaction.max.age.ms` so that a transaction open too long stops the task early
     ([CDC-4004](transaction-too-old.md)) or, with `cdc.transaction.max.age.action=discard`, is
     dropped with an ops event and a DLQ record instead of growing without bound.
2. Restart the task. It mines the open transactions again from its position, so their changes are
   rebuilt; the redo from their first change (or the journal, for journaled transactions) must
   still be available.

   ```bash
   curl -s -X POST "$CONNECT/connectors/$NAME/restart?includeTasks=true&onlyFailed=true"
   ```

Journaling (see [transaction buffer and journal](../../concepts/transaction-buffer-and-journal.md))
keeps long transactions from holding the resume position, but it does not reduce what the buffer
holds: a journaled transaction is still buffered until it ends.
