---
title: How capture works
description: LogMiner mining of uncommitted redo, step scheduling and the no-silent-loss rule.
---

# How capture works

**Status:** design from the product requirements; implementation lands in Phase 1. This page is
updated as the code arrives and the spike findings in `oracle-cdc-core/src/main/resources/reference`
are linked from it.

The engine mines redo with LogMiner in steps sized by log count, applying each step atomically; a step that fails midway is discarded and re-mined. Captured tables are pushed down to LogMiner by object id, with the step cut at any DDL that creates a new segment. Every unexpected condition stops the task with a typed error.

## Where a step starts and ends

A mining step reads the rows LogMiner returns for the logs covering the range from the cursor to the
current SCN. The cursor between steps is the redo byte address of the last row applied, not an SCN.
That matters because Oracle keeps the redo of an open transaction in a private strand until the
transaction commits, the strand fills or a log switch binds it, and the records keep the SCNs of the
original changes when they finally reach the log. A step bounded by SCN alone could pass over those
records and never see them. Because the redo log is append-only in redo byte address order, selecting
rows after the cursor's address catches every record that reached the log since the previous step,
whatever its SCN. The connector never writes to the source database to force a flush, so no flush
table is needed.

The committed offset carries the same shape: the resume SCN for choosing logs, the redo byte address
to resume from, and the redo address of the last acknowledged commit so a restart repeats nothing and
skips nothing.

## Database errors

A transient database error (a dropped connection, a killed session, an instance restart) makes
the engine reopen its sessions with backoff inside the retry budget and mine the failed step
again from the same cursor; nothing of a failed step is ever applied. An ORA code the connector
does not classify stops the task with the code in the message and an action to report it or,
when it is transient in your environment, add it to `cdc.retry.extra.error.codes`. The task then
restarts under the Connect restart policy.
