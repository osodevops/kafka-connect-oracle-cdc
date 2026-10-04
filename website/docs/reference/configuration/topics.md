---
title: "Topics"
description: "Topics properties of the OSO CDC Connector for Oracle Database."
sidebar_position: 9
---

# Topics properties

Generated from the connector's `ConfigDef` by `ConfigDocsGeneratorTest`; do not edit by hand.

| Property | Type | Default | Importance | Description |
|---|---|---|---|---|
| `cdc.topic.prefix` | string | required | high | Prefix of every topic this connector writes; also the logical server name in the Debezium-compatible envelope and the offset partition. |
| `cdc.topic.template` | string | none | medium | Topic name template. Variables: $\{prefix\}, $\{pdb\}, $\{schema\}, $\{table\}, $\{database\}. Default $\{prefix\}.$\{pdb\}.$\{schema\}.$\{table\} in a CDB and $\{prefix\}.$\{schema\}.$\{table\} otherwise. Characters Kafka does not allow become underscores. |
| `cdc.tables.include` | list | required | high | Comma-separated regular expressions over PDB.SCHEMA.TABLE (CDB) or SCHEMA.TABLE (non-CDB) selecting the tables to capture. |
| `cdc.tables.exclude` | list | empty | medium | Comma-separated regular expressions removing tables from the included set. |
| `cdc.tables.case.sensitive` | boolean | `false` | low | Match table patterns case-sensitively. Oracle stores unquoted names in upper case. |
| `cdc.users.exclude` | list | empty | low | Oracle users whose transactions are dropped in the mining query, so they never create transactions in the buffer (for example a replication or GoldenGate user). |
| `cdc.key.missing` | string | `fail` | medium | What to do with a captured table that has neither a primary key nor a NOT NULL unique index: fail (at validation), rowid (key records by ROWID; a moved row changes its key) or none (no key). |
| `cdc.key.columns` | string | empty | low | Per-table key override as SCHEMA.TABLE:COL1,COL2;SCHEMA.OTHER:COL, taking precedence over the primary key. |
| `cdc.tombstones.on.delete` | boolean | `true` | medium | Emit a null-valued record after every delete so compacted topics drop the key. |
