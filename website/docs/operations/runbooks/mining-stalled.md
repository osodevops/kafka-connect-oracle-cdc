---
title: "CDC-4003 Mining stalled"
description: "Runbook for CDC-4003 MINING_STALLED: mining a single redo log kept timing out."
slug: /runbooks/mining-stalled
---

# CDC-4003 Mining stalled

**Code:** `CDC-4003` (`MINING_STALLED`). Not retried further: the task stops.

## What the connector observed

Each mining step covers a window of redo logs and must finish within
`cdc.mining.query.timeout.ms` (ten minutes by default). A step that times out is cancelled and
discarded, the `StepTimeouts` metric counts it, and the next step covers half as many logs. A
timeout never stops the task by itself. This task stopped because the window was already down to a
single log and three steps in a row over it timed out. The message reads "Mining ... timed out
three times at a single log" with the SCN range.

## Why it stopped rather than continued

The connector cannot mine past a log it cannot read in time without skipping the changes in it, and
retrying the same query forever would hide the problem while the connector falls further behind.

## Confirm the cause

Look at the mining session while it runs, and at the size of the log in question:

```sql
SELECT s.sid, s.serial#, s.username, s.event, s.seconds_in_wait, l.opname, l.sofar, l.totalwork
FROM v$session s LEFT JOIN v$session_longops l ON l.sid = s.sid AND l.serial# = s.serial#
WHERE s.username = 'C##CDC';
SELECT thread#, sequence#, blocks * block_size / 1024 / 1024 AS mb, first_change#, next_change#
FROM v$archived_log WHERE :start_scn BETWEEN first_change# AND next_change# - 1;
```

A very large log, a database under heavy load, or a session waiting on I/O explain most cases. Redo
from tables the connector does not capture still has to be read: a bulk job on another table (a
truncate and reload, for example) can make one log far slower to mine than usual. The
`Last step duration` panel of the shipped Grafana dashboard and the `LastStepMillis` and
`WindowLogs` metrics show the trend before the stop.

## Recover

1. Raise `cdc.mining.query.timeout.ms` so that a single log can be mined in the time it needs.
2. Look for the source of unusual redo volume in that period and, where possible, change the job
   (a `MERGE` instead of a truncate and reload produces far less redo).
3. If the database was under exceptional load, restart once it has passed.
4. Restart the task. It resumes from its last acknowledged position:

   ```bash
   curl -s -X POST "$CONNECT/connectors/$NAME/restart?includeTasks=true&onlyFailed=true"
   ```

Very large online redo logs make each single-log step longer; see
[redo sizing and archive retention](../../database-setup/redo-sizing.md).
