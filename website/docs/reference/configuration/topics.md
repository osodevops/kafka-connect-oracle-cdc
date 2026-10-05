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
| `cdc.ops.topic` | string | `${prefix}.cdc.ops` | low | Topic for the connector's operational events (task start and stop, DDL seen, decode failures, reconnects, discarded or released transactions, signal acknowledgements); $\{prefix\} expands to the topic prefix. |
| `cdc.signals.topic` | string | `${prefix}.cdc.signals` | low | Topic the connector reads signals from (snapshot, refresh-tables, log-state). |
| `cdc.schema.topic` | string | `${prefix}.cdc.schema` | low | Compacted topic holding table schema versions. |
| `cdc.transactions.topic.enabled` | boolean | `false` | low | Write BEGIN and END records per Oracle transaction to the transaction metadata topic. |
| `cdc.transactions.topic` | string | `${prefix}.cdc.transactions` | low | Transaction metadata topic. |
| `cdc.kafka.bootstrap.servers` | string | none | medium | Bootstrap servers for the connector's own Kafka clients: the admin client that creates the internal topics with the right cleanup policy, and the readers of the schema and journal topics. Other client settings go under cdc.kafka.*. When unset the connector relies on the worker's topic creation and on pre-created topics. |
| `cdc.internal.topic.replication.factor` | short | `-1` | low | Replication factor for internal topics the connector creates; -1 uses the broker default. |
| `cdc.journal.converter` | string | `sh.oso.connect.oracle.journal.TolerantJsonConverter` | low | Converter the task uses to read the transaction journal topic back at start. The default reads JSON written with or without the schema envelope, so it matches a worker using the JSON converter in either mode. Set it to the worker's converter class when the worker uses another converter (Avro, Protobuf); settings for it go under cdc.journal.converter.*. |
| `cdc.heartbeat.interval.ms` | long | `10000` | medium | How often a heartbeat record carrying the current position is written when no change records flow, so Connect commits offsets on a quiet database and the start position of a new connector becomes durable at once. 0 disables periodic heartbeats; the start heartbeat is always written. |
| `cdc.heartbeat.topic` | string | `${prefix}.cdc.heartbeat` | low | Topic for heartbeat records; $\{prefix\} expands to the topic prefix. |
