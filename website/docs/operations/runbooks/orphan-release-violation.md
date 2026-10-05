---
title: "CDC-7001 Orphan release violation"
description: "Runbook for CDC-7001 ORPHAN_RELEASE_VIOLATION: a COMMIT arrived for a transaction the connector had already released or discarded."
slug: /runbooks/orphan-release-violation
---

# CDC-7001 Orphan release violation

**Code:** `CDC-7001` (`ORPHAN_RELEASE_VIOLATION`). Not retried: the task stops at once.

## What the connector observed

LogMiner returned a COMMIT for a transaction whose buffered changes the connector had already let
go of, in one of two ways:

- Orphan detection released it as rolled back (`cdc.transaction.orphan.action=release`, the
  default), because the transaction was absent from `GV$TRANSACTION` in two checks, the connector
  had mined past the first absence, and the session named by its START row no longer existed.
- The long-transaction policy discarded it (`cdc.transaction.max.age.action=discard`) because it
  was open longer than `cdc.transaction.max.age.ms`.

The ids of released and discarded transactions travel in the connector's offsets
(`released_xids`, the most recent 256), so this is detected after a restart too. The message names
the transaction id and the COMMIT SCN.

## Why it stopped rather than continued

When the transaction was released or discarded, its earlier changes were dropped. Publishing the
COMMIT now would publish only the changes mined after that point: a partial transaction, which is
worse than none. The connector stops instead.

## Confirm the cause

The ops topic holds the decision: a `transaction-orphan-released` event (with `first_scn`,
`last_scn`, `absent_at_scn` and `reason`) or a `transaction-discarded` event (with `first_scn`,
`last_scn` and `age_ms`) for the same transaction id. A discarded transaction also has a record on
the DLQ topic. Compare the event's SCNs with the COMMIT SCN in the message.

A released transaction that later commits means the database still had it while the probe did not
see it, for example because the transaction ran on an instance or in a container the connector user
cannot see in `GV$TRANSACTION`. Check the user's `CONTAINER_DATA` with `oracle-cdc-doctor check`
(rule DOC-4).

## Recover

To publish the transaction whole, mine it again from its first change:

1. Stop the connector and read its offset
   ([moving the offset by hand](../../concepts/offsets-and-recovery.md#reading-and-moving-the-offset-by-hand)).
2. Set `resume_scn` to the `first_scn` from the ops event (or lower), remove `resume_rs_id` and
   `resume_ssn`, and remove the transaction's id from `released_xids`. Keep the `last_commit_*`
   fields, so transactions already delivered are skipped.
3. Write the offset back and resume. The redo from `first_scn` must still be available.

If that redo is gone, the transaction cannot be published whole. Move the offset past its COMMIT
SCN as a deliberate skip, remove the id from `released_xids`, and reload the tables the transaction
touched with a [`snapshot` signal](../signals.md).

To make a repeat less likely, raise `cdc.transaction.orphan.check.interval.ms` or set
`cdc.transaction.orphan.action=fail`, so that the next suspected orphan stops the task
([CDC-7002](orphan-transaction.md)) for a decision instead of being released; and raise or unset
`cdc.transaction.max.age.ms` if the discard policy caught a legitimate transaction.
