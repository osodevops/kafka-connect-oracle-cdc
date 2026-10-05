---
title: "CDC-7002 Orphan transaction"
description: "Runbook for CDC-7002 ORPHAN_TRANSACTION."
slug: /runbooks/orphan-transaction
---

# CDC-7002 Orphan transaction

**Code:** `CDC-7002` (`ORPHAN_TRANSACTION`). Raised only when `cdc.transaction.orphan.action` is
`fail`.

## What the connector observed

A buffered transaction older than `cdc.transaction.orphan.check.interval.ms` was absent from
`GV$TRANSACTION` in two consecutive checks, the connector had mined past the SCN of the first
absence without seeing its COMMIT or ROLLBACK, and the session named by its START row no longer
exists. The message names the transaction id, user, first SCN and event count.

## Why it stopped rather than continued

The transaction's changes are in the buffer and pin the resume position. Oracle no longer knows the
transaction, so no COMMIT or ROLLBACK will ever arrive, and the buffer would hold it until the
connector stops. With the `fail` action the operator decides what happens to those changes; with
the default `release` action the connector discards them as a rollback and records the decision on
the ops topic.

## Confirm the cause

```sql
SELECT xidusn, xidslot, xidsqn, start_scn, status FROM gv$transaction;
```

The transaction from the message is not listed. Check the alert log for a session kill or an
instance restart around the transaction's first SCN.

## Recover

- Set `cdc.transaction.orphan.action=release` and restart the task: the transaction is discarded
  with a `transaction-orphan-released` event on the ops topic and the position moves on.
- If you believe the transaction did commit, reset the offsets to a position before its first SCN
  with `oracle-cdc-admin offsets set`, so the connector mines it whole.
