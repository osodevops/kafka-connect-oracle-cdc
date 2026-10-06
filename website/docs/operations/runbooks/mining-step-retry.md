---
title: "CDC-1002 Mining step retry exhausted"
description: "Runbook for CDC-1002 MINING_STEP_RETRY: a LogMiner step kept failing with an error that mining the same range again normally fixes."
slug: /runbooks/mining-step-retry
---

# CDC-1002 Mining step retry exhausted

**Code:** `CDC-1002` (`MINING_STEP_RETRY`). Retried inside the task first; the task stops with
this code only after more than 20 failures of the same kind in a row.

## What the connector observed

A LogMiner step failed with ORA-00310, ORA-00334, ORA-01289, ORA-01291 or ORA-01368. These mean
that an online redo log was switched, overwritten or archived while it was being read (ORA-01368:
the header of a log the step registered no longer matches it), or that a log the step needs is
not yet visible. The connector discards everything the failed step read, counts it
in the `StepRetries` metric and mines the same range again at the next poll, which normally
succeeds once the log has been archived.

The task stopped because more than 20 steps in a row failed this way. The message names the SCN
range and the last Oracle error: "Mining ... failed ... times in a row with a retriable LogMiner
error".

## Why it stopped rather than continued

Nothing of a failed step is ever applied, so retrying is safe, but a range that keeps failing
points at a problem in the database that retrying will not fix: an archiver that is stuck or
failing, or a log that cannot be read. Moving past the range would skip the changes in it.

## Confirm the cause

Check that the archiver keeps up and that every log covering the range in the message is archived
and readable:

```sql
SELECT group#, thread#, sequence#, status, archived, first_change# FROM v$log ORDER BY thread#, sequence#;
SELECT dest_id, dest_name, status, error FROM v$archive_dest_status WHERE status <> 'INACTIVE';
SELECT thread#, sequence#, first_change#, next_change#, name, status, deleted
FROM v$archived_log
WHERE next_change# > :start_scn AND first_change# <= :end_scn
ORDER BY thread#, sequence#;
```

Many online groups with `ARCHIVED = NO`, a destination in `ERROR`, or a full archive volume confirm
an archiving problem. The database alert log shows ORA-00310, ORA-00334, ORA-01289 or ORA-01368 around the
time of the failures. The `Step retries, timeouts and reconnects` panel of the shipped Grafana
dashboard shows how often it happened before the stop.

## Recover

1. Fix archiving: free space on the archive destination, clear the destination error, and wait
   until every online log covering the range has `ARCHIVED = YES`.
2. If the online redo groups switch every minute or two under normal load, add groups or make them
   larger so that a log is not reused while it is mined (see
   [redo sizing and archive retention](../../database-setup/redo-sizing.md)).
3. Restart the failed task. It resumes from its last acknowledged position:

   ```bash
   curl -s -X POST "$CONNECT/connectors/$NAME/restart?includeTasks=true&onlyFailed=true"
   ```

If the step still fails with every log of the range archived and readable, report the message and
the Oracle version and release update in a GitHub issue.
