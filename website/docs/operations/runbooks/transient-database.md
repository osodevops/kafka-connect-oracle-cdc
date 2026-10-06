---
title: "CDC-1001 Transient database error"
description: "Runbook for CDC-1001 TRANSIENT_DATABASE: the database or the network was unavailable for longer than the retry budget, or returned an error the connector does not classify."
slug: /runbooks/transient-database
---

# CDC-1001 Transient database error

**Code:** `CDC-1001` (`TRANSIENT_DATABASE`). Retried inside the task first; the task stops with
this code only when retrying has not helped or cannot be judged safe.

## What the connector observed

A database call failed in a way that means the connection or the instance is not available: the
JDBC driver reported the connection as recoverable or transient, an I/O error occurred, or Oracle
returned one of ORA-00028, ORA-00031, ORA-03113, ORA-03114, ORA-03135, ORA-12170, ORA-12514,
ORA-12516, ORA-12528, ORA-12537, ORA-12541, ORA-12543, ORA-01033, ORA-01034, ORA-01089, ORA-01090,
ORA-01092, ORA-01109, ORA-16331, ORA-17002, ORA-17008, ORA-17410, ORA-17800, ORA-25408 or ORA-04068,
an ORA-00604 caused by one of these, or a code listed in `cdc.retry.extra.error.codes`.

ORA-16331 ("container ... is not open") deserves a note: LogMiner reads every container whose redo
lies in the range it mines, captured or not, so a closed pluggable database stops mining. It is
expected for a few moments after a restart, before the PDBs open.

The engine handles this without stopping: it discards the mining step in progress, reopens its
database sessions with backoff (about a second at first, doubling up to 30 seconds, with jitter),
mines again from the same cursor, writes a `reconnected` event to the ops topic and counts the
reconnect in the `Reconnects` metric. The task stops with `CDC-1001` in two cases only:

- The sessions could not be reopened within `cdc.retry.max.time.ms` (24 hours by default). The
  message reads "Could not open the ... connection within the retry budget of ... seconds."
- The database returned an ORA code that the connector does not classify. The message names the
  code and reads "The connector does not classify this error." The task stops at once in this
  case, because it cannot tell whether retrying is safe.

## Why it stopped rather than continued

Retrying a transient error never loses data: a failed step is never applied, so mining the same
range again returns the same rows. After the retry budget the connector stops so that a long outage
is visible as a failed task rather than a connector that silently does nothing. An error it does
not know may not be transient at all, so it stops and reports it rather than retry or skip.

## Confirm the cause

The Kafka Connect task status and the worker log hold the message with the ORA code. The ops topic
shows the `reconnected` events that came before the stop, with their cause.

From the database host, check that the instance and the listener are up and that the service the
connector uses is registered:

```sql
SELECT instance_name, status, database_status FROM v$instance;
SELECT name, open_mode, database_role FROM v$database;
```

```bash
lsnrctl status
tnsping <service>
```

For an unclassified code, look it up with `oerr ora <number>` on the database host.

## Recover

1. Restore the database, the listener or the network path. Check any load balancer or proxy
   between the worker and the database for idle timeouts that drop connections. For ORA-16331,
   open the pluggable database the message names and keep it open across restarts:

   ```sql
   ALTER PLUGGABLE DATABASE FREEPDB2 OPEN;
   ALTER PLUGGABLE DATABASE FREEPDB2 SAVE STATE;
   ```

2. Restart the failed task. It resumes from its last acknowledged position, so nothing is lost or
   repeated beyond what at-least-once delivery allows:

   ```bash
   curl -s -X POST "$CONNECT/connectors/$NAME/restart?includeTasks=true&onlyFailed=true"
   ```

3. If the code is unclassified but known to be transient in your environment, add it to
   `cdc.retry.extra.error.codes` (for example `ORA-12345`) and report it in a GitHub issue so it can
   be classified in a release.
4. If outages longer than a day are expected, raise `cdc.retry.max.time.ms`, and keep archived logs
   for at least the outage plus the connector's lag (see
   [redo sizing and archive retention](../../database-setup/redo-sizing.md)). The
   `OracleCdcReconnecting` alert in the shipped Prometheus rules warns about repeated reconnects
   before the budget runs out.
