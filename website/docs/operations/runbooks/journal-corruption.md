---
title: "CDC-4002 Transaction journal corruption"
description: "Runbook for CDC-4002 JOURNAL_CORRUPTION: at start, the transaction journal topic could not be read back completely."
slug: /runbooks/journal-corruption
---

# CDC-4002 Transaction journal corruption

**Code:** `CDC-4002` (`JOURNAL_CORRUPTION`). Raised at task start. Not retried.

## What the connector observed

Long or large open transactions are journaled to the compacted topic `cdc.txjournal.topic` (by
default the topic prefix followed by `.cdc.txjournal`), so the resume position can move past their
first change. At start the task reads the topic back and rebuilds those transactions before mining
resumes. That failed in one of these ways, and the message says which:

- A journaled transaction is missing a chunk: "Journal for transaction ... is missing chunk ...".
  Chunks are numbered from zero and every one is needed.
- A record's key or value could not be read with the converter in `cdc.journal.converter`: "has an
  unreadable key" or "cannot be read".
- A chunk's content is damaged: it declares more frames or bytes than it holds, or a frame of an
  unknown kind.

Chunks written by a task start that never committed an offset, and chunks the connector will mine
again from its position, are not the problem: they are tombstoned at start as a matter of course.

## Why it stopped rather than continued

A transaction rebuilt without one of its chunks would be published at COMMIT with changes missing,
and the redo for those changes may no longer be needed by anything else, so it may be gone.

## Confirm the cause

Check the topic's configuration. It must be compacted only, with no time or size based deletion:

```bash
kafka-configs.sh --bootstrap-server "$BROKERS" --entity-type topics \
  --entity-name cdc.cdc.txjournal --describe --all | grep -E 'cleanup.policy|retention'
```

`cleanup.policy` must be `compact`, not `delete` or `compact,delete`. Check that nothing else
produces to the topic, and compare the worker's `key.converter` and `value.converter` with
`cdc.journal.converter`. The default converter reads JSON written with or without the schema
envelope; a worker using Avro or Protobuf needs its own converter class there, with its settings
under `cdc.journal.converter.*`.

If the transaction named in the message is still open, find where it started:

```sql
SELECT t.inst_id, t.xidusn, t.xidslot, t.xidsqn, t.start_scn, t.start_time
FROM gv$transaction t ORDER BY t.start_scn;
```

## Recover

- **Converter mismatch.** Set `cdc.journal.converter` (and `cdc.journal.converter.*`) to match the
  worker's converters and restart the task.
- **A lost or damaged chunk.** Fix the topic configuration first (`cleanup.policy=compact`). Then
  move the offset back to an SCN before the transaction's first change, with the redo from that SCN
  still available ([moving the offset by hand](../../concepts/offsets-and-recovery.md#reading-and-moving-the-offset-by-hand)).
  Keep the `last_commit_*` fields so that transactions already delivered are skipped. At start, every
  journal chunk at or after the new resume point is tombstoned and the transaction is mined again
  whole from the redo.
- If the redo from the transaction's start is gone as well, the transaction cannot be rebuilt. Move
  the offset forward past its COMMIT as a deliberate skip and reload the tables it touched with a
  [`snapshot` signal](../signals.md).

Restart the task with:

```bash
curl -s -X POST "$CONNECT/connectors/$NAME/restart?includeTasks=true&onlyFailed=true"
```
