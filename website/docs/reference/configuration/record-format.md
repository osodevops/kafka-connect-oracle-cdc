---
title: "Record format"
description: "Record format properties of the OSO CDC Connector for Oracle Database."
sidebar_position: 12
---

# Record format properties

Generated from the connector's `ConfigDef` by `ConfigDocsGeneratorTest`; do not edit by hand.

| Property | Type | Default | Importance | Description |
|---|---|---|---|---|
| `cdc.output.format` | string | `debezium` | medium | Record envelope. Only debezium (before, after, source, op, ts_ms) is available; a Confluent-compatible flat format is not built yet. |
| `cdc.decimal.mode` | string | `precise` | medium | NUMBER handling: precise (Connect Decimal; unconstrained NUMBER and FLOAT as a variable scale decimal struct), string, or double. |
| `cdc.temporal.mode` | string | `adaptive` | medium | DATE, TIMESTAMP and INTERVAL handling: adaptive (Debezium semantic types sized to the column precision) or iso_string (ISO 8601 text). |
