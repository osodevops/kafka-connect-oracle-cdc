---
title: "Transactions"
description: "Transactions properties of the OSO CDC Connector for Oracle Database."
sidebar_position: 7
---

# Transactions properties

Generated from the connector's `ConfigDef` by `ConfigDocsGeneratorTest`; do not edit by hand.

| Property | Type | Default | Importance | Description |
|---|---|---|---|---|
| `cdc.transaction.max.age.ms` | long | `-1` | low | Long transaction limit; -1 is unlimited. On breach the action below applies and is never silent. |
| `cdc.transaction.max.age.action` | string | `fail` | low | fail stops the task; discard drops the transaction after writing its details to the ops topic and DLQ. |
| `cdc.transaction.orphan.check.interval.ms` | long | `300000` | low | Interval for checking buffered transactions against GV$TRANSACTION. |
| `cdc.transaction.orphan.action` | string | `release` | low | release treats a vanished transaction as rolled back (a later COMMIT for it is a stop); fail stops the task. |
