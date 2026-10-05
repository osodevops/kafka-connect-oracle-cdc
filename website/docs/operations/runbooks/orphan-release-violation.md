---
title: "CDC-7001 Orphan release violation"
description: "Runbook for CDC-7001 ORPHAN_RELEASE_VIOLATION."
slug: /runbooks/orphan-release-violation
---

# CDC-7001 Orphan release violation

**Code:** `CDC-7001` (`ORPHAN_RELEASE_VIOLATION`).

## What the connector observed

LogMiner returned a COMMIT for a transaction that the orphan detector had earlier released as
rolled back (`cdc.transaction.orphan.action=release`). The released transaction ids travel in the
connector offsets, so this is detected even after a restart.

## Why it stopped rather than continued

When the transaction was released, its earlier changes were discarded. Emitting the COMMIT now
would publish a partial transaction: only the changes mined after the release. The connector stops
instead of publishing an incomplete transaction.

## Confirm the cause

The ops topic holds a `transaction-orphan-released` event for the transaction id with the SCNs of
the two checks and the mined-to SCN. Compare them with the COMMIT SCN in the error message. A
release followed by a COMMIT means `GV$TRANSACTION` did not list an open transaction for the
duration of two checks, which points at a very long check interval combined with a database or
listener outage during which the probe queries failed over to a stale view.

## Recover

- Reset the offsets to a position before the transaction's first SCN with
  `oracle-cdc-admin offsets set` so the transaction is mined whole, then restart.
- Alternatively resnapshot the affected tables.
- Raise `cdc.transaction.orphan.check.interval.ms` or set `cdc.transaction.orphan.action=fail` so
  the next orphan stops the task for a manual decision.
