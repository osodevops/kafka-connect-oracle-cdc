---
title: Redo sizing and archive retention
description: How to size Oracle online redo groups and archive log retention so this LogMiner CDC connector can resume after downtime without losing redo it still needs.
---

import Faq from '@site/src/components/Faq';

# Redo sizing and archive retention

The connector reads redo through LogMiner only. It does not read redo files directly. How the
database writes and keeps redo decides how fresh the records are, and how long the connector can
be stopped without losing its place. Each mining step reads a window of whole redo logs. See
[how capture works](../concepts/how-capture-works.md).

**Quick answer.** Size each online redo group so the peak hour switches about four times. More than six switches in an hour is too frequent. Keep every archived log from the resume position for the longest planned stop, the connector's lag, and the age of the oldest open transaction that is not journaled. The connector reads that redo through LogMiner.

## How long should archived logs be kept?

The connector needs every redo log from its resume position onwards. If one is deleted before the
connector has read it, the task stops with [CDC-2002](../operations/runbooks/log-purged.md). It
never skips the missing log. Restore the log from a backup, or skip the gap on purpose and
reload the tables.

Keep archived logs at the destination the connector reads for at least:

- the longest time the connector may be stopped (a weekend, a change freeze, an outage of the
  Kafka Connect cluster), plus
- the connector's lag when it is running, plus
- the age of the oldest open transaction on the captured tables that is not journaled.

The last item is why journaling matters. Without `cdc.kafka.bootstrap.servers` a long transaction
holds the resume position at its first change for as long as it stays open. With it, a transaction
open longer than `cdc.txjournal.threshold.ms` (five minutes by default) is journaled to Kafka and
stops holding the position. See
[transaction buffer and journal](../concepts/transaction-buffer-and-journal.md).

[`oracle-cdc-doctor`](../operations/doctor.md) rule DOC-10 checks a related figure. Archived redo
must reach back at least `cdc.txjournal.threshold.ms` plus the planned maximum downtime
(`--max-downtime`, 24 hours by default). The sum is rounded up to whole hours. That covers the
planned stop and the time a transaction can pin the position before it is journaled. Running lag
is separate. Compare `MillisBehindSource` with the retention you keep.

A first start needs redo from further back as well. When the connector starts with no stored
offset, it reads the redo from the start of the oldest transaction open in the captured containers
at that moment, so that the transaction is published whole when it commits. Transactions of other
users and on other tables count too, because `GV$TRANSACTION` does not say which tables a
transaction touched. The worker log names the transaction. If its redo is already gone, the task
stops with [CDC-2002](../operations/runbooks/log-purged.md). Wait for that transaction to end, or
end it, and start the connector again.

Two practical points:

- Delete archived logs with RMAN and its retention policy, not with operating system commands.
  Oracle sets `DELETED` on `V$ARCHIVED_LOG` to `YES` only when an RMAN `DELETE` has removed the
  file from disk and from the control file and the recovery catalog. A file removed outside RMAN
  still shows `DELETED = NO`, so the catalog cannot tell the connector it is gone. The task stops
  with [CDC-2002](../operations/runbooks/log-purged.md) when LogMiner fails to open it, naming the
  file and advising `CROSSCHECK ARCHIVELOG ALL`. The `offsets set` and `resnapshot` commands of
  [`oracle-cdc-admin`](../operations/admin.md), and `takeover_scn.py`, add every archived log they
  rely on to a LogMiner session first, so they refuse such a file before they move a position.
- Prefer a local archive destination with its own free-space monitoring over a fast recovery area
  that can fill and halt the database. Oracle states that a disk full error while archiving is a
  fatal error and the database stops responding. Enable ARCHIVELOG with a local destination as
  part of [database setup](./index.md). The connector reads one destination: the one named in
  `cdc.archive.destination`, or the lowest valid local one
  ([capture properties](../reference/configuration/capture.md)). On Amazon RDS for Oracle, archived
  redo is deleted after `archivelog retention hours`, and the default of 0 removes it soon after
  it is archived ([Amazon RDS for Oracle](./amazon-rds.md)).

To see how far back the connector's position is, compare the `resume_scn` in its offset with the
oldest archived log still present:

```sql
SELECT MIN(first_change#) AS oldest_scn, MIN(first_time) AS oldest_time
FROM v$archived_log WHERE deleted = 'NO' AND status = 'A' AND dest_id = :dest_id;
SELECT SCN_TO_TIMESTAMP(:resume_scn) FROM dual;
```

## How large should online redo groups be?

Size the groups from the redo the database writes. There is no single size, in megabytes, that
fits every database. [`oracle-cdc-doctor sizing`](../operations/doctor.md#sizing) reports the
online log size that keeps the peak hour at about four switches. Rule DOC-9 warns when any thread
switches more than six times in an hour over the last seven days. Oracle's performance guide gives
a rough guide of switching log files at most once every 20 minutes, and treats redo log files from
100 MB to a few gigabytes as reasonable.

### What happens when online redo groups are too small?

Frequent log switches mean more logs in each mining step, and more chances that a log is reused
while it is mined. The connector discards that step and mines the same range again. The
`StepRetries` metric counts it, and the task stops with
[CDC-1002](../operations/runbooks/mining-step-retry.md) if the same failure keeps happening.

### What happens when online redo groups are too large?

In `archive_only` capture mode a change becomes visible only when its log is archived, so a large
group delays records. The default `online` mode also mines the current online log, so the wait for
archival does not apply. Each mining step still reads whole redo logs through LogMiner, so a very
large log makes a single step slower. The task stops with
[CDC-4003](../operations/runbooks/mining-stalled.md) if a single log cannot finish within
`cdc.mining.query.timeout.ms` (ten minutes by default) three times in a row.

### How many online redo groups are enough?

Keep enough groups that the archiver always keeps up. Oracle states that LGWR cannot reuse an
online redo log group until the group has been archived. When every group is still waiting to be
archived, the log switch waits, and commits wait with it.

### How do I count log switches?

Count switches per hour over a busy week, at the destination the connector reads:

```sql
SELECT TRUNC(first_time, 'HH24') AS hour, thread#, COUNT(*) AS switches,
       ROUND(SUM(blocks * block_size) / 1024 / 1024) AS mb
FROM v$archived_log
WHERE first_time > SYSDATE - 7 AND dest_id = :dest_id
GROUP BY TRUNC(first_time, 'HH24'), thread#
ORDER BY hour, thread#;
```

## Does LogMiner read redo the connector does not capture?

Yes. When the connector queries `V$LOGMNR_CONTENTS`, LogMiner reads the redo log files in the
session sequentially. Oracle states that a query predicate is applied to the rows LogMiner
returns, and that the mining engine does not filter on that predicate while it reads. The
connector's query asks only for the captured objects, plus transaction control rows and DDL, which
keeps the rows it handles small. The database still reads the redo of other tables in those logs.

A batch job that rewrites a large table outside the captured set (a truncate and reload, for
example) can make a few logs much slower to mine. Table-level supplemental logging can add further
redo on the tables you do capture. See [supplemental logging](./supplemental-logging.md). The
`LastStepMillis` and `ScnLag` [metrics](../reference/metrics.md) show a step that took longer, and
`RowsMined` counts the rows the query returned.
[`redo-profile`](../operations/doctor.md#redo-profile) names the tables the redo came from.

## How do I measure redo with the doctor?

[`oracle-cdc-doctor`](../operations/doctor.md) measures this from the database:

- `sizing` reports log switches per hour and thread, archive generation per day, the online log
  size that keeps the peak hour at about four switches, and the retention and archive space a
  given maximum downtime needs.
- `redo-profile` samples the newest archived logs with LogMiner and reports which tables the redo
  comes from, including the share that belongs to tables the connector does not capture. It flags
  truncate-and-reload and delete-and-reload jobs.
- `check` warns about frequent log switches (DOC-9, more than six in an hour on any thread) and
  archived redo that does not reach back far enough (DOC-10).

The queries above give the same picture by hand.

Sources, checked on 10 November 2026: Oracle Database 19c
[configuring a database for performance](https://docs.oracle.com/en/database/oracle/oracle-database/19/tgdba/configuring-database-for-performance.html)
(redo log file size),
[managing archived redo log files](https://docs.oracle.com/en/database/oracle/oracle-database/19/admin/managing-archived-redo-log-files.html),
[`V$ARCHIVED_LOG`](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/V-ARCHIVED_LOG.html),
[`V$TRANSACTION`](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/V-TRANSACTION.html),
[background processes](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/background-processes.html)
(ARCn) and
[LogMiner](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html).
Statements about the connector and the doctor come from this site.

<Faq
  items={[
    {
      question: 'How large should online redo logs be for this connector?',
      answer:
        'Size each group from the redo written in the peak hour so that hour switches about four times. oracle-cdc-doctor sizing reports that size, and rule DOC-9 warns when any thread switches more than six times in an hour over the last seven days. Oracle\'s rough guide is to switch log files at most once every 20 minutes, and it treats files from 100 MB to a few gigabytes as reasonable, without one size for every database. Source: https://docs.oracle.com/en/database/oracle/oracle-database/19/tgdba/configuring-database-for-performance.html',
    },
    {
      question: 'How long should archived redo be kept for CDC?',
      answer:
        'Keep every archived log from the resume position for the longest planned stop, plus the connector\'s lag while it is running, plus the age of the oldest open transaction that is not journaled. With cdc.kafka.bootstrap.servers set, a transaction open longer than cdc.txjournal.threshold.ms (five minutes by default) is journaled and stops pinning that position. Rule DOC-10 checks that archived redo reaches back at least that threshold plus the planned maximum downtime (24 hours by default), with the sum rounded up to whole hours.',
    },
    {
      question: 'What happens if an archived log the connector still needs is deleted?',
      answer:
        'The task stops with CDC-2002 and does not skip the missing redo. Restore the log from a backup and restart the task, or move the offset on purpose and reload the captured tables. A reload republishes rows that still exist and does not emit a delete for a row removed in the purged range.',
    },
    {
      question: 'Should archived logs be deleted with operating system commands?',
      answer:
        'No. Delete them with RMAN so the catalog records the deletion. Oracle sets V$ARCHIVED_LOG.DELETED to YES only when an RMAN DELETE command has removed the file from disk and from the control file and the recovery catalog. A file removed outside RMAN stays listed as not deleted, and the task stops with CDC-2002 when LogMiner cannot open it. Source: https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/V-ARCHIVED_LOG.html',
    },
    {
      question: 'Does a large online redo group delay change records?',
      answer:
        'It does in archive_only mode, because a change is mined only after its online log is archived. The default online mode also mines the current online log, so waiting for archival does not hold the record back. A very large log can still make one mining step too slow, and three timeouts on a single log stop the task with CDC-4003.',
    },
    {
      question: 'Does redo for tables outside the capture set slow mining down?',
      answer:
        'Yes. LogMiner reads the redo log files in the session sequentially, and Oracle states that the mining engine does not apply the query predicate while it reads. The connector\'s query asks only for captured objects, transaction control rows and DDL, so fewer rows are returned, but redo for other tables in those logs is still read. Source: https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html',
    },
  ]}
/>
