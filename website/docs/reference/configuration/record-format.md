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
| `cdc.schema.name.adjustment.mode` | string | `none` | medium | How the names of the per-table key, value and envelope schemas are adjusted for converters with strict naming rules, such as Avro, where each dot-separated part of a name must be letters, digits and underscores and must not start with a digit. none (default) uses the topic prefix, PDB, owner and table names as they are. avro replaces every other character, for example $, # or a space in a table name or a hyphen in the topic prefix, with an underscore, and puts an underscore before a part that starts with a digit. avro_unicode replaces every such character, and the underscore itself, with _u and the four hexadecimal digits of its UTF-16 code unit, so different names stay different. The fixed schema names (the source block, the transaction block, the semantic types) are already valid and never change. Schema Registry subjects named after the record follow the adjusted names. Same values and meaning as Debezium's schema.name.adjustment.mode. |
| `cdc.field.name.adjustment.mode` | string | `none` | medium | How the key and value field names taken from column names are adjusted, with the same modes as cdc.schema.name.adjustment.mode: none (default) keeps the column names, avro replaces each character not allowed in an Avro name with an underscore and puts an underscore before a leading digit, avro_unicode replaces each such character and the underscore with _u and four hexadecimal digits. Values are still read from the column they belong to. If two columns of a table adjust to the same field name, the task stops with CDC-6004 before it writes a record of that table. Same values and meaning as Debezium's field.name.adjustment.mode; avro matches Debezium 1.x sanitize.field.names=true. |
