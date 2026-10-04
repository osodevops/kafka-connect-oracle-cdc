---
title: "Transaction journal"
description: "Transaction journal properties of the OSO CDC Connector for Oracle Database."
sidebar_position: 6
---

# Transaction journal properties

Generated from the connector's `ConfigDef` by `ConfigDocsGeneratorTest`; do not edit by hand.

| Property | Type | Default | Importance | Description |
|---|---|---|---|---|
| `cdc.txjournal.topic` | string | none | low | Compacted journal topic for long transactions. Default: $\{cdc.topic.prefix\}.cdc.txjournal. |
| `cdc.txjournal.threshold.ms` | long | `300000` | low | A transaction open longer than this is journaled and stops pinning the restart position. |
| `cdc.txjournal.threshold.events` | long | `100000` | low | A transaction with more buffered events than this is journaled. |
