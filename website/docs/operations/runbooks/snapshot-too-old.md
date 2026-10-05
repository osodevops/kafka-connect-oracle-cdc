---
title: "CDC-8001 Snapshot too old"
description: "Runbook for CDC-8001 SNAPSHOT_TOO_OLD."
slug: /runbooks/snapshot-too-old
---

# CDC-8001 Snapshot too old

**Code:** `CDC-8001` (`SNAPSHOT_TOO_OLD`). Raised while a snapshot reads a table, after the retries
described below.

## What the connector observed

A snapshot chunk's `SELECT ... AS OF SCN` failed with ORA-01555 or ORA-08181, and kept failing after
the chunk was halved down to 1,000 rows with a fresh SCN each time. The message names the table.

## Why it stopped rather than continued

Skipping the chunk would leave its rows out of the snapshot. Reading it without `AS OF SCN` would
break the ordering with streamed changes that keeps consumers correct.

## Confirm the cause

```sql
SELECT name, value FROM v$parameter WHERE name IN ('undo_retention', 'undo_management');
SELECT tuned_undoretention, maxquerylen, ssolderrcnt FROM v$undostat
WHERE ROWNUM <= 12 ORDER BY begin_time DESC;
```

`SSOLDERRCNT` above zero confirms snapshot-too-old errors in that period.

## Recover

Raise `UNDO_RETENTION` and give the undo tablespace room for it, or lower
`cdc.snapshot.chunk.rows`, then restart the task. The snapshot resumes at the chunk that failed;
chunks already published are not read again.

Restart the task with:

```bash
curl -s -X POST "$CONNECT/connectors/$NAME/restart?includeTasks=true&onlyFailed=true"
```
