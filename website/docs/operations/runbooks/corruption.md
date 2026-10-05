---
title: "CDC-3002 Corruption detected"
description: "Runbook for CDC-3002 CORRUPTION: LogMiner reported missing redo, a spill file failed its integrity check, or the stored offset cannot be read."
slug: /runbooks/corruption
---

# CDC-3002 Corruption detected

**Code:** `CDC-3002` (`CORRUPTION`). Not retried, and never sent to the DLQ: the task always stops.

## What the connector observed

Data the connector was about to rely on failed an integrity check. The message says which of three
places it was:

- **Redo.** LogMiner returned a `MISSING_SCN` row in the range being mined: it found a hole in the
  redo it was given. The message reads "LogMiner reported MISSING_SCN at ..." with the redo
  position.
- **A spill file.** An open transaction spilled to disk under `cdc.buffer.spill.dir` could not be
  read back intact: a frame failed its CRC32 check, the file ended early, or reading it failed. The
  message names the file.
- **The stored offset.** The offset Kafka Connect handed back at start is empty, has no format
  version, has a newer format version than this connector reads, or holds a snapshot block that is
  not valid.

The steps below use Kafka Connect's REST API; [`oracle-cdc-admin offsets`](../admin.md) shows and
sets the stored offset as well.

## Why it stopped rather than continued

Missing redo means changes the connector cannot see; mining past it would drop them. A damaged spill
file would publish a transaction with changes missing or altered. An offset the connector cannot
read could make it start anywhere. In each case continuing would publish data nobody could trust.

## Confirm the cause

For missing redo, check the database alert log for redo corruption around the SCN in the message,
and validate the archived logs that cover it with RMAN:

```sql
SELECT thread#, sequence#, first_change#, next_change#, name
FROM v$archived_log WHERE :scn BETWEEN first_change# AND next_change# - 1;
```

```bash
rman target / <<< "VALIDATE ARCHIVELOG SEQUENCE <sequence> THREAD <thread>;"
```

For a spill file, check the volume behind `cdc.buffer.spill.dir`: free space, disk errors in the
operating system log, and whether anything else writes to that directory.

For the offset, read it back:

```bash
curl -s "$CONNECT/connectors/$NAME/offsets"
```

## Recover

- **Missing redo.** Restore intact copies of the archived logs for the range from a backup and
  restart the task. If no intact copy exists, the changes in that range cannot be captured: move the
  offset past it as a deliberate skip
  ([moving the offset by hand](../../concepts/offsets-and-recovery.md#reading-and-moving-the-offset-by-hand))
  and reload the captured tables with a [`snapshot` signal](../signals.md).
- **A spill file.** Fix the volume, or point `cdc.buffer.spill.dir` at a healthy one, and restart
  the task. Spill files are not durable state: the task re-mines open transactions from its
  position and removes old spill files when it starts, so nothing is lost.
- **An offset with a newer format version.** Run the connector version that wrote it, or a newer
  one. A downgrade by more than one minor release is not supported.
- **An empty or unreadable offset.** Something other than this connector wrote it. If you know a
  good position, for example from the `resume_scn` of the last `startup` or `stop` event on the ops
  topic, write it back with the procedure above. Otherwise start again from scratch: stop the
  connector, delete its offsets with `DELETE /connectors/{name}/offsets`, delete its transaction
  journal and schema topics so nothing from the old position is read back, and resume. The
  connector then starts at the current SCN and takes a snapshot as set by `cdc.snapshot.mode`.

Restart the task with:

```bash
curl -s -X POST "$CONNECT/connectors/$NAME/restart?includeTasks=true&onlyFailed=true"
```
