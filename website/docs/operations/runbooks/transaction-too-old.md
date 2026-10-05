---
title: "CDC-4004 Transaction too old"
description: "Runbook for CDC-4004 TRANSACTION_TOO_OLD."
slug: /runbooks/transaction-too-old
---

# CDC-4004 Transaction too old

**Code:** `CDC-4004` (`TRANSACTION_TOO_OLD`). Raised only when `cdc.transaction.max.age.ms` is set
and `cdc.transaction.max.age.action` is `fail`.

## What the connector observed

A buffered transaction has been open longer than `cdc.transaction.max.age.ms`. The message names the
transaction id, user, event count and first SCN.

## Why it stopped rather than continued

An open transaction pins the resume position at its first change (unless it is journaled) and holds
its changes in the buffer. You asked the connector to stop rather than decide for you what to do
with a transaction that old.

## Confirm the cause

```sql
SELECT s.sid, s.serial#, s.username, s.program, t.start_time, t.used_urec
FROM gv$transaction t JOIN gv$session s ON s.taddr = t.addr
WHERE t.xidusn = :usn AND t.xidslot = :slot AND t.xidsqn = :sqn;
```

## Recover

- Have the application commit or roll back the transaction, then restart the task; the buffered
  changes are published or dropped accordingly.
- Raise `cdc.transaction.max.age.ms` if the transaction is legitimate and the journal keeps the
  position moving.
- Set `cdc.transaction.max.age.action=discard` to drop such transactions: the connector writes a
  `transaction-discarded` event to the ops topic and a record to the DLQ topic, adds the transaction
  to the released ledger, and stops with `CDC-7001` if a COMMIT for it arrives later, because the
  discarded changes cannot be published any more.

Restart the task with:

```bash
curl -s -X POST "$CONNECT/connectors/$NAME/restart?includeTasks=true&onlyFailed=true"
```
