---
title: "Transaction buffer"
description: "Transaction buffer properties of the OSO CDC Connector for Oracle Database."
sidebar_position: 5
---

# Transaction buffer properties

Generated from the connector's `ConfigDef` by `ConfigDocsGeneratorTest`; do not edit by hand.

| Property | Type | Default | Importance | Description |
|---|---|---|---|---|
| `cdc.buffer.memory.max.bytes` | long | `268435456` | medium | Heap budget for buffered uncommitted changes; the largest transactions spill to disk above it. |
| `cdc.buffer.spill.dir` | string | none | low | Spill directory. Default: the worker's temporary directory plus the connector name. |
| `cdc.buffer.spill.max.bytes` | long | `10737418240` | low | Spill cap. Exceeding it stops the task with BufferExhaustedException naming the largest transactions. |
