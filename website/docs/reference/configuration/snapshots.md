---
title: "Snapshots"
description: "Snapshots properties of the OSO CDC Connector for Oracle Database."
sidebar_position: 10
---

# Snapshots properties

Generated from the connector's `ConfigDef` by `ConfigDocsGeneratorTest`; do not edit by hand.

| Property | Type | Default | Importance | Description |
|---|---|---|---|---|
| `cdc.snapshot.mode` | string | `initial` | high | initial reads every captured table's existing rows on the connector's first start, while streaming, then streams on; none only streams; snapshot_only reads the tables and then stays idle without streaming; on_signal starts no snapshot by itself. A snapshot that a stored offset records as unfinished resumes in every mode. |
| `cdc.snapshot.threads` | int | `4` | medium | Chunks of a table read in parallel, each on a connection of its own. |
| `cdc.snapshot.chunk.rows` | int | `100000` | medium | Target rows per chunk. Each chunk is one flashback query, so smaller chunks need less undo; up to cdc.snapshot.max.pending.chunks chunks are held in memory. |
| `cdc.snapshot.chunk.retries` | int | `5` | low | Times a failed chunk is read again with a fresh SCN before the task stops. |
| `cdc.snapshot.fetch.size` | int | `5000` | low | JDBC fetch size of chunk reads. |
| `cdc.snapshot.max.pending.chunks` | int | `8` | low | Chunks read but not yet published before reads pause; chunks wait until streaming has passed their SCN. |
| `cdc.snapshot.tables.order` | list | empty | low | Tables to read first, as PDB.OWNER.TABLE; the others follow in name order. A filter for one table's snapshot goes in cdc.snapshot.select.override.PDB.OWNER.TABLE as a SQL condition, for example cdc.snapshot.select.override.FREEPDB1.APP.ORDERS=STATUS &lt;&gt; 'ARCHIVED'. |
