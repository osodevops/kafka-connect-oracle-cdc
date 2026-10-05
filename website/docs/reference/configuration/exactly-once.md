---
title: "Exactly-once"
description: "Exactly-once properties of the OSO CDC Connector for Oracle Database."
sidebar_position: 14
---

# Exactly-once properties

Generated from the connector's `ConfigDef` by `ConfigDocsGeneratorTest`; do not edit by hand.

| Property | Type | Default | Importance | Description |
|---|---|---|---|---|
| `cdc.eos.batch.max.records` | int | `5000` | low | With exactly.once.support and transaction.boundary=connector: a Kafka transaction is committed at the first Oracle commit after this many records. |
| `cdc.eos.batch.max.ms` | long | `500` | low | A Kafka transaction is committed at the first Oracle commit after it has been open this long, or as soon as no more records are waiting. |
| `cdc.eos.batch.max.bytes` | long | `16777216` | low | A Kafka transaction is committed at the first Oracle commit after about this many bytes of change data (ADR-0007). |
| `cdc.eos.split.max.records` | int | `500000` | low | An Oracle transaction with more changes than this is split into consecutive Kafka transactions at exact event indexes, so none outlives transaction.max.timeout.ms; its records carry the cdc.split header and an ops event names it. |
| `cdc.eos.split.max.bytes` | long | `268435456` | low | An Oracle transaction is also split after about this many bytes of change data. |
