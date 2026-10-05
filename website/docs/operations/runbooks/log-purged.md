---
title: "CDC-2002 Archived log purged"
description: "Runbook for CDC-2002 LOG_PURGED: a redo log the connector still needs has been deleted or cannot be read."
slug: /runbooks/log-purged
---

# CDC-2002 Archived log purged

**Code:** `CDC-2002` (`LOG_PURGED`). Not retried: the task stops at once.

## What the connector observed

A redo log that the connector still needs is gone. One of these happened, and the message says
which:

- The catalog marks the archived log deleted (`DELETED = YES` in `V$ARCHIVED_LOG`), usually because
  RMAN deleted it under its retention policy: "Redo log ... is needed for SCN ... to ... but the
  catalog marks it deleted."
- The catalog still lists the file but LogMiner cannot open it when the connector adds it to the
  mining session (ORA-01284, ORA-00308, ORA-01285 or ORA-16226), usually because it was removed
  with operating system commands: "mining from ... failed with ORA-01284." Nothing of that step is
  applied.

The connector needs a log when it holds changes after the last acknowledged position, or the start
of a transaction that is still open and not journaled.

The message from this release may suggest `oracle-cdc-admin resnapshot`. That command is not
available yet; use the steps below.

## Why it stopped rather than continued

The log holds changes that were never delivered. Starting from a later log, as some connectors do,
would drop them without any trace. The connector never skips redo by itself.

## Confirm the cause

```sql
SELECT thread#, sequence#, first_change#, next_change#, name, deleted, status
FROM v$archived_log
WHERE thread# = :thread AND sequence# = :sequence;
```

On the database host, check whether the file named in `NAME` exists. RMAN's
`CROSSCHECK ARCHIVELOG ALL;` marks files that are missing on disk as expired.

Compare the connector's resume position with what is still available:

```sql
SELECT MIN(first_change#) FROM v$archived_log WHERE deleted = 'NO' AND status = 'A';
```

The `resume_scn` of the stored offset (`GET /connectors/{name}/offsets`) is below that SCN when the
connector fell further behind than the archive retention.

## Recover

1. If a backup holds the log, restore it with RMAN, confirm with the first query that it is listed
   and not deleted, and restart the task. Nothing is lost.

   ```bash
   curl -s -X POST "$CONNECT/connectors/$NAME/restart?includeTasks=true&onlyFailed=true"
   ```

2. If no copy exists, the changes in the purged range cannot be captured. The only way on is a
   deliberate skip: move the offset to the first SCN of the oldest log still available
   ([moving the offset by hand](../../concepts/offsets-and-recovery.md#reading-and-moving-the-offset-by-hand)),
   then reload the captured tables with a [`snapshot` signal](../signals.md). A snapshot republishes
   the rows that exist; rows deleted in the purged range get no delete record, so downstream copies
   may need to be reconciled.

To prevent a recurrence:

- Keep archived logs for at least as long as the connector could be stopped plus its lag (see
  [redo sizing and archive retention](../../database-setup/redo-sizing.md)).
- Set `cdc.kafka.bootstrap.servers` so long transactions are journaled and stop holding the resume
  position at their first change (see [transaction buffer and journal](../../concepts/transaction-buffer-and-journal.md)).
- Watch the `OldestOpenScn` and `MillisBehindSource` metrics. The shipped `OracleCdcBehindSource`
  and `OracleCdcNotAdvancing` alerts warn when the connector falls behind or stops moving; compare
  how far behind it is with your archive retention.
