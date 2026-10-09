---
title: "CDC-5001 Topology change"
description: "Runbook for CDC-5001 TOPOLOGY: the stored offset belongs to another database incarnation, there is no valid archive destination to mine, or the database has more than one enabled redo thread."
slug: /runbooks/topology
---

# CDC-5001 Topology change

**Code:** `CDC-5001` (`TOPOLOGY`). Raised at task start, or during the run when a second redo
thread appears. Not retried.

## What the connector observed

One of these, and the message says which:

- **The offset belongs to another database.** Every offset records the database's `DBID` and
  `RESETLOGS_CHANGE#` (`dbid` and `resetlogs_scn`). At start they did not match the database the
  connector is connected to: "The stored offset belongs to database ... but the connector is
  connected to ...". Typical causes are a connector pointed at a different database (a clone, a
  refreshed test database), a database opened with RESETLOGS after point-in-time recovery or a
  flashback, or an offset copied from another connector.
- **The database changed incarnation while the task ran.** "After reconnecting, the database is
  ... but the position belongs to ...": the connection dropped and the database the task
  reconnected to had been opened with RESETLOGS, typically a Data Guard failover or a
  point-in-time recovery. Nothing of the new incarnation was mined. A failover can lose
  transactions the old primary had committed, and the connector may already have delivered
  them; compare before you resnapshot.
- **No archive destination to mine.** `cdc.archive.destination` names a destination that is not
  active, or, with the setting empty, no valid local archive destination exists.
- **More than one enabled redo thread (RAC).** "The database has N enabled redo threads (thread 1
  OPEN, thread 2 OPEN)" at start, or "Redo from thread N appeared" during the run when an
  instance's thread was enabled after the start. This release captures a single redo thread: the
  mining cursor and the commit order are per thread, so with two threads the connector would skip
  or repeat whole threads. It stops instead, and the step that carried the foreign redo is never
  acknowledged. A thread shown DISABLED by `V$THREAD`, left behind by an instance removed from the
  cluster, does not count.

The steps below use Kafka Connect's REST API; [`oracle-cdc-admin offsets`](../admin.md) shows and
sets the stored offset as well.

## Why it stopped rather than continued

A position is an SCN and a redo address in one database incarnation. In another incarnation the
same numbers point at different changes, so resuming would skip or repeat changes at random.
Without an archive destination, the connector cannot list the logs it must mine. With two
enabled redo threads, no position in the offset could say where each thread stands.

## Confirm the cause

```sql
SELECT dbid, name, resetlogs_change#, resetlogs_time, database_role, open_mode FROM v$database;
SELECT dest_id, dest_name, status, type, destination FROM v$archive_dest_status WHERE status <> 'INACTIVE';
SELECT thread#, status, enabled, sequence# FROM v$thread;
```

Compare `DBID` and `RESETLOGS_CHANGE#` with `dbid` and `resetlogs_scn` in the stored offset:

```bash
curl -s "$CONNECT/connectors/$NAME/offsets"
```

## Recover

- **Wrong database.** Point the connector's `cdc.database.*` settings back at the original database
  and restart the task.
- **The database was legitimately replaced or reset.** The old position means nothing in the new
  incarnation; start the connector again from scratch. Stop it, delete its offsets with
  `DELETE /connectors/{name}/offsets`, delete its transaction journal and schema topics so nothing
  from the old incarnation is read back, and resume it. The connector starts at the current SCN and
  takes a snapshot as set by `cdc.snapshot.mode` (with the default `initial`, every captured table
  is read again and published as `op=r` records). Changes made between the reset and the new start
  reach consumers only through that snapshot.
- **Archive destination.** Set `cdc.archive.destination` to the name of a valid destination (for
  example `LOG_ARCHIVE_DEST_1`), or leave it empty to use the lowest valid local destination, and
  make sure one is valid (`oracle-cdc-doctor check`, rule DOC-12). Then restart the task.

- **More than one enabled redo thread.** Run the connector against a single-instance database.
  If the second thread belongs to an instance that has left the cluster for good, a DBA can
  disable it (`ALTER DATABASE DISABLE THREAD n` once its redo is archived), after which the
  connector starts. RAC capture arrives with the per-thread position; `oracle-cdc-doctor check`
  reports the same condition as DOC-13.

```bash
curl -s -X PUT "$CONNECT/connectors/$NAME/stop"
curl -s -X DELETE "$CONNECT/connectors/$NAME/offsets"
curl -s -X PUT "$CONNECT/connectors/$NAME/resume"
curl -s -X POST "$CONNECT/connectors/$NAME/restart?includeTasks=true&onlyFailed=true"
```

The first three commands start from scratch; the last restarts a failed task after a configuration
fix.
