---
title: "Record format"
description: "Record format properties of the OSO CDC Connector for Oracle Database."
sidebar_position: 11
---

# Record format properties

Generated from the connector's `ConfigDef` by `ConfigDocsGeneratorTest`; do not edit by hand.

| Property | Type | Default | Importance | Description |
|---|---|---|---|---|
| `cdc.output.format` | string | `debezium` | medium | Record envelope: debezium (before, after, source, op, ts_ms). The Confluent-compatible flat format arrives in Phase 2. |
| `cdc.decimal.mode` | string | `precise` | medium | NUMBER handling: precise (Connect Decimal; unconstrained NUMBER and FLOAT as a variable scale decimal struct), string, or double. |
| `cdc.temporal.mode` | string | `adaptive` | medium | DATE, TIMESTAMP and INTERVAL handling: adaptive (Debezium semantic types sized to the column precision) or iso_string (ISO 8601 text). |
