---
title: "CDC-3003 LOB too large"
description: "Runbook for CDC-3003 LOB_TOO_LARGE."
slug: /runbooks/lob-too-large
---

# CDC-3003 LOB too large

**Code:** `CDC-3003` (`LOB_TOO_LARGE`). Raised only when `cdc.lob.mode` is `inline` or `reselect`
and `cdc.lob.oversize.action` is `fail`.

## What the connector observed

A committed transaction wrote a CLOB, NCLOB or BLOB value larger than `cdc.lob.max.bytes`, or a
reselected value was larger than that limit. The message names the transaction, the commit SCN, the
table and column, and the size reached. Text is measured in UTF-8 bytes.

## Why it stopped rather than continued

The limit bounds the memory each value may take while the connector assembles it from redo, and
the size of the records it publishes. You asked the connector to stop rather than publish the
placeholder in place of a value. The check runs at commit, so a rolled-back transaction never stops
the task.

## Confirm the cause

```sql
SELECT id, DBMS_LOB.GETLENGTH(c) AS characters FROM app.docs
WHERE DBMS_LOB.GETLENGTH(c) > :limit;
```

## Recover

- Raise `cdc.lob.max.bytes` above the largest value you expect, and check that the Kafka producer and
  broker accept records of that size (`max.request.size`, `message.max.bytes`). Then restart the
  task; it resumes from its last acknowledged position.
- Set `cdc.lob.oversize.action=placeholder` to publish `cdc.unavailable.placeholder` for values above
  the limit instead. Consumers keep the previous value of a column that carries the placeholder.
- Set `cdc.lob.mode=skip` if the topic does not need LOB values at all.
