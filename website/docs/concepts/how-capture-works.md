---
title: How capture works
description: LogMiner mining of uncommitted redo, step scheduling and the no-silent-loss rule.
---

# How capture works

**Status:** design from the product requirements; implementation lands in Phase 1. This page is
updated as the code arrives and the spike findings in `oracle-cdc-core/src/main/resources/reference`
are linked from it.

The engine mines redo with LogMiner in steps sized by log count, applying each step atomically; a step that fails midway is discarded and re-mined. Captured tables are pushed down to LogMiner by object id, with the step cut at any DDL that creates a new segment. Every unexpected condition stops the task with a typed error.

## Database errors

A transient database error (a dropped connection, a killed session, an instance restart) makes
the engine reopen its sessions with backoff inside the retry budget and mine the failed step
again from the same cursor; nothing of a failed step is ever applied. An ORA code the connector
does not classify stops the task with the code in the message and an action to report it or,
when it is transient in your environment, add it to `cdc.retry.extra.error.codes`. The task then
restarts under the Connect restart policy.
