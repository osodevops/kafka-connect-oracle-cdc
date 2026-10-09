---
title: Upgrade guide
description: What the connector stores between runs, how each store is versioned, and how to upgrade a running connector.
---

# Upgrade guide

From 0.1.1 to 0.1.2 nothing stored changes: the offset format and the internal topics are the
same, so replacing the plugin and restarting the workers is the whole upgrade. One behaviour
change to check first: 0.1.2 refuses to start (CDC-5001) on a database with more than one enabled
redo thread (RAC), in online mode on a standby, and on a mounted, logical or snapshot standby,
where 0.1.1 would have started; `oracle-cdc-doctor check` reports the same as DOC-13 and DOC-14.
This page says how
the connector's stored state is versioned, which is what makes upgrades and one-step downgrades
safe, and how to replace the plugin on a running worker.

## What the connector stores

| Store | Where | Versioned by |
|---|---|---|
| Position | Kafka Connect's offsets | The `v` field, currently `1` ([offsets and recovery](../concepts/offsets-and-recovery.md)) |
| Schema versions | The compacted schema topic | A format number in every record |
| Transaction journal | The compacted journal topic | A format number in every chunk, and the journal generation in the position |
| Ops events | The ops topic | The record schema version (`io.oso.cdc.ops.Event` version 1) |
| DLQ records | The DLQ topic | The record schema version (`io.oso.cdc.dlq.Record` version 1) |

A connector reads every position version up to its own and keeps the fields it does not know, so a
newer release reads an older offset and a downgrade by one minor release keeps what the newer
release wrote. An offset with a newer position version than the running connector reads stops the
task with [CDC-3002](runbooks/corruption.md) rather than guessing.

Spill files are not durable state: they are removed when a task starts, and the open transactions
are mined again.

## Replacing the plugin

1. Read the release notes for the new version.
2. Stop the connector: `PUT /connectors/{name}/stop`. Its position stays in the offsets.
3. Replace the plugin directory on every worker (or roll out a new image on Strimzi) and restart
   the workers.
4. Resume the connector: `PUT /connectors/{name}/resume`. The `startup` event on the ops topic
   records the new version and the position it resumed from.

The task resumes from its last acknowledged position, and open transactions are rebuilt from the
redo or from the journal, so nothing is lost across the upgrade.
