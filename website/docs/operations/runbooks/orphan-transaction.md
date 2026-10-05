---
title: "CDC-7002 Orphan transaction"
description: "Runbook for CDC-7002 ORPHAN_TRANSACTION: a buffered transaction is no longer known to the database and the orphan action is fail."
slug: /runbooks/orphan-transaction
---

# CDC-7002 Orphan transaction

**Code:** `CDC-7002` (`ORPHAN_TRANSACTION`). Raised only when `cdc.transaction.orphan.action` is
`fail`. Not retried.

## What the connector observed

A buffered transaction older than `cdc.transaction.orphan.check.interval.ms` was absent from
`GV$TRANSACTION` in two consecutive checks, the connector had mined past the SCN of the first
absence without seeing its COMMIT or ROLLBACK, and the session named by its START row no longer
exists. The message names the transaction id, user, first SCN and number of changes.

## Why it stopped rather than continued

The transaction's changes are in the buffer and hold the resume position at its first change.
Oracle no longer knows the transaction, so no COMMIT or ROLLBACK will arrive, and the buffer would
hold it until the connector stops. With the `fail` action the operator decides what happens to
those changes; with the default `release` action the connector treats the transaction as rolled
back, records the decision on the ops topic, and stops with [CDC-7001](orphan-release-violation.md)
if a COMMIT for it arrives after all.

## Confirm the cause

```sql
SELECT inst_id, xidusn, xidslot, xidsqn, start_scn, status FROM gv$transaction;
```

The transaction from the message is not listed. Check the alert log for a killed session, a
crashed client or an instance restart around the transaction's first SCN, and the application's
own logs for the work it was doing.

## Recover

- If the transaction is truly gone, set `cdc.transaction.orphan.action=release` and restart the
  task: the transaction is dropped as a rollback, a `transaction-orphan-released` event goes to the
  ops topic, and the position moves on. You can set the action back to `fail` afterwards.
- If you believe the transaction did commit, find its COMMIT before releasing anything. Leave the
  action at `fail`, move the offset back to an SCN before the transaction's first change with the
  redo still available
  ([moving the offset by hand](../../concepts/offsets-and-recovery.md#reading-and-moving-the-offset-by-hand)),
  and resume, so the connector mines it whole again.

Restart the task with:

```bash
curl -s -X POST "$CONNECT/connectors/$NAME/restart?includeTasks=true&onlyFailed=true"
```
