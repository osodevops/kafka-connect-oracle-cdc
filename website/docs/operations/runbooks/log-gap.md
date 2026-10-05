---
title: "CDC-2001 Redo log gap"
description: "Runbook for CDC-2001 LOG_GAP: the redo logs for a thread do not cover the SCN range the connector has to mine."
slug: /runbooks/log-gap
---

# CDC-2001 Redo log gap

**Code:** `CDC-2001` (`LOG_GAP`). Not retried: the task stops at once.

## What the connector observed

Before every mining step the connector lists the redo logs covering the range from its cursor to
the database's current SCN, for every redo thread that is enabled. It prefers the archived copies
at one archive destination (the one named in `cdc.archive.destination`, or the lowest valid local
destination), and in the default `online` capture mode adds the online logs for the part of the
range not yet archived. It then checks that the list is continuous, and it was not. The message
names the thread, the SCN range and what is wrong:

- no logs at all for the thread;
- the earliest log starts after the start SCN;
- a sequence is missing between two logs;
- the latest log ends before the end SCN.

The connector reads `V$ARCHIVED_LOG` for its one destination only. A log archived only to another
destination is not seen.

The steps below use Kafka Connect's REST API. [`oracle-cdc-admin`](../admin.md) wraps them:
`offsets set` refuses an SCN whose redo is purged, and `resnapshot` reads chosen tables again.

## Why it stopped rather than continued

A missing sequence holds changes the connector has not read. Mining the logs on either side of it
would publish everything except those changes, and nobody would know. The connector never chooses a
later start point by itself.

## Confirm the cause

```sql
SELECT dest_id, dest_name, status, type, destination FROM v$archive_dest_status WHERE status <> 'INACTIVE';
SELECT thread#, sequence#, first_change#, next_change#, dest_id, name, deleted, status
FROM v$archived_log
WHERE thread# = :thread AND next_change# > :start_scn AND first_change# <= :end_scn
ORDER BY sequence#, dest_id;
SELECT thread#, status, enabled, sequence# FROM v$thread;
```

Look for the missing sequence: it may exist at another destination, be marked deleted, or be absent
everywhere. `oracle-cdc-doctor check` (rule DOC-12) confirms which destination the connector reads.

## Recover

1. If another destination holds the full sequence, set `cdc.archive.destination` to that
   destination's name (for example `LOG_ARCHIVE_DEST_2`) and restart the task.
2. If a backup holds the missing log, restore it with RMAN so that `V$ARCHIVED_LOG` lists it at the
   destination the connector reads, confirm with the query above, and restart the task. Nothing is
   lost.
3. If the log is gone for good, the changes in it cannot be captured. The only way on is a
   deliberate skip: move the offset to the first SCN after the gap
   ([moving the offset by hand](../../concepts/offsets-and-recovery.md#reading-and-moving-the-offset-by-hand)),
   then reload the captured tables with a [`snapshot` signal](../signals.md). A snapshot republishes
   the rows that exist; rows deleted during the gap get no delete record, so downstream copies may
   need to be reconciled.

Restart the task with:

```bash
curl -s -X POST "$CONNECT/connectors/$NAME/restart?includeTasks=true&onlyFailed=true"
```

To prevent a recurrence, keep archived logs at the destination the connector reads for at least as
long as the connector could be stopped plus its lag, and delete them with RMAN rather than with
operating system commands.
