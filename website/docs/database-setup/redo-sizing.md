---
title: Redo sizing and archive retention
description: How online redo group size and archive retention affect capture, and how to work out what your database needs.
---

# Redo sizing and archive retention

The connector reads redo through LogMiner, so how the database writes and keeps redo decides how
fresh the records are and how long the connector can be stopped without losing its place.

## Archive retention

The connector needs every redo log from its resume position onwards. If one is deleted before the
connector has read it, the task stops with [CDC-2002](../operations/runbooks/log-purged.md) rather
than skip it, and recovery means restoring the log from a backup or a deliberate skip with a
reload of the tables.

Keep archived logs at the destination the connector reads for at least:

- the longest time the connector may be stopped (a weekend, a change freeze, an outage of the
  Kafka Connect cluster), plus
- the connector's lag when it is running, plus
- the age of the oldest open transaction on the captured tables that is not journaled.

The last item is why journaling matters. Without `cdc.kafka.bootstrap.servers` a long transaction
holds the resume position at its first change for as long as it stays open. With it, a transaction
open longer than `cdc.txjournal.threshold.ms` is journaled to Kafka and stops holding the position
(see [transaction buffer and journal](../concepts/transaction-buffer-and-journal.md)).

Two practical points:

- Delete archived logs with RMAN and its retention policy, not with operating system commands. A
  file removed outside RMAN still shows `DELETED = NO` in `V$ARCHIVED_LOG`; the connector finds out
  only when LogMiner fails to open it.
- Prefer a local archive destination with its own free-space monitoring over a fast recovery area
  that can fill and halt the database. The connector reads one destination: the one named in
  `cdc.archive.destination`, or the lowest valid local one.

To see how far back the connector's position is, compare the `resume_scn` in its offset with the
oldest archived log still present:

```sql
SELECT MIN(first_change#) AS oldest_scn, MIN(first_time) AS oldest_time
FROM v$archived_log WHERE deleted = 'NO' AND status = 'A' AND dest_id = :dest_id;
SELECT SCN_TO_TIMESTAMP(:resume_scn) FROM dual;
```

## Online redo groups

- **Too small.** Frequent log switches mean more logs per step and more chances that a log is
  reused while it is mined, which the connector handles by mining the step again (`StepRetries`
  metric, [CDC-1002](../operations/runbooks/mining-step-retry.md) if it keeps happening). Oracle's
  usual guidance of a few log switches an hour at peak suits capture as well.
- **Too large.** In `archive_only` capture mode a change becomes visible only when its log is
  archived, so a large group delays records. Each mining step also reads whole logs, so a very
  large log makes a single step slower ([CDC-4003](../operations/runbooks/mining-stalled.md) if it
  cannot finish within `cdc.mining.query.timeout.ms`).
- Enough groups that the archiver always keeps up: a group is reused only after it is archived.

Count switches per hour over a busy week:

```sql
SELECT TRUNC(first_time, 'HH24') AS hour, thread#, COUNT(*) AS switches,
       ROUND(SUM(blocks * block_size) / 1024 / 1024) AS mb
FROM v$archived_log
WHERE first_time > SYSDATE - 7 AND dest_id = :dest_id
GROUP BY TRUNC(first_time, 'HH24'), thread#
ORDER BY hour, thread#;
```

## Redo the connector does not need

LogMiner reads all the redo in a log, including the redo of tables the connector does not capture.
The connector asks LogMiner only for the captured objects, which keeps the rows it has to handle
small, but the database still reads the whole log. A batch job that rewrites a large table outside
the captured set (a truncate and reload, for example) can make a few logs much slower to mine.
The `RowsMined`, `LastStepMillis` and `ScnLag` [metrics](../reference/metrics.md) show it.

## Not available yet

The doctor commands that measure this from the database itself, `redo-profile` (redo rates and
redo per table from a short LogMiner sample) and `sizing`, are not available yet. The queries above
give the same picture by hand.
