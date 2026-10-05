---
title: Comparison
description: How the connector compares with Confluent's Oracle CDC Source and Debezium's Oracle connector, from their public documentation, and how to measure for yourself.
---

# Comparison

This page sets the connector beside the two connectors most teams consider for capturing Oracle
Database changes into Kafka with LogMiner: Confluent's Oracle CDC Source connector and the Debezium
Oracle connector. Statements about those connectors come from their public documentation and issue
trackers, linked below, as read on 4 and 5 October 2026 (Debezium 3.7). Statements about this
connector describe the code as it is today, not a roadmap. If you find something out of date,
please open an issue.

This project does not publish database benchmark figures. The [harness](#measure-it-yourself) is
published instead, so you can measure on your own database.

## Feature comparison

| Capability | OSO CDC Connector | Confluent Oracle CDC Source | Debezium Oracle connector |
|---|---|---|---|
| Licence | Apache-2.0 | Proprietary premium connector, Confluent enterprise licence | Apache-2.0 |
| Capture API | LogMiner | LogMiner | LogMiner, XStream or OpenLogReplicator |
| Delivery | Exactly-once, with one or more whole Oracle transactions per Kafka transaction; at-least-once otherwise | At-least-once | At-least-once; exactly-once through Kafka Connect, with Kafka transactions per poll batch (`transaction.boundary=poll`) |
| Tasks | One task; decoding runs on several threads | Task zero mines into a redo log topic; table work is spread across tasks | One task |
| Long-running transactions | Buffered by the connector, spilled to disk past a heap budget, journaled to Kafka so the restart position can move past them | Buffered in connector memory; a threshold with warn or discard | The restart position stays at the start of the oldest open transaction |
| Transactions the database lost | Orphan detection against `GV$TRANSACTION`, released or stopped by policy | Not documented | Reported to hold the offset back ([dbz#2683](https://github.com/debezium/dbz/issues/2683)) |
| Missing redo | Always stops with a typed error and a runbook | Not documented | Logs a warning and continues from a later point when redo of an open transaction is gone; a fail-fast option is requested in [dbz#2713](https://github.com/debezium/dbz/issues/2713) |
| Initial snapshot | Chunked, each chunk read as of its own SCN, resumable per chunk after a restart, interleaved with streaming | Parallel; an incomplete snapshot restarts from the beginning | Blocking or incremental |
| Snapshots on demand | `snapshot` signal on a Kafka topic, no writes to the source | New tables are detected automatically | Signals; read-only incremental snapshots for Oracle are an open request ([dbz#2606](https://github.com/debezium/dbz/issues/2606)) |
| Schema changes | Per-table versions on a compacted topic; rows written before a DDL decoded with a dictionary from the redo; renames supported | Several DDL forms unsupported, including rename table or column and add or drop constraints | Schema history topic with one partition and unlimited retention |
| LOB columns | Leave out, assemble inline from redo, or read again as of the commit SCN | Separate LOB topics | Inline, with `lob.enabled` |
| Several PDBs from one connector | Configured with `cdc.database.pdbs`, one mining session at the root (see [multi-PDB capture](../concepts/multi-pdb.md)) | One PDB per connector | One PDB per connector ([dbz#478](https://github.com/debezium/dbz/issues/478)) |
| Quiet databases | Heartbeat records carry the position; no writes to the source | Heartbeat topic | Heartbeats, with an action query that writes to the source where needed |
| Preflight and setup | `oracle-cdc-doctor`: checks the database and writes the DBA script | Configuration validation | Configuration validation |
| Oracle RAC | Not supported yet | Supported | Supported |
| Standby capture | Not supported yet | Not supported; must point to the primary | Physical standby in archive-log-only mode |
| Autonomous Database | Not supported yet | Not supported | Not documented |
| Commercial support | OSO ([enterprise support](../enterprise-support.md)) | Confluent | Red Hat, for its supported builds and configurations |

Sources: Confluent's [Oracle CDC Source overview](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/overview.html),
[configuration reference](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/configuration-properties.html)
and [best practices](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/best-practices.html);
Debezium's [exactly-once delivery](https://debezium.io/documentation/reference/stable/configuration/eos.html)
documentation, [3.7 release announcement](https://debezium.io/blog/2026/09/29/debezium-3-7-final-released/),
[Red Hat's supported configurations](https://access.redhat.com/articles/4938181) and the issues
linked in the table.

Where the connectors agree: all three can read redo through LogMiner (Debezium also offers other
adapters), need ARCHIVELOG mode and supplemental logging, and through LogMiner cannot capture what
LogMiner does not support, such as identity columns and some of the newest column types.

## What this connector does not do yet

- Oracle RAC, Amazon RDS for Oracle, Autonomous Database and standby capture.
- A record format compatible with Confluent's Oracle CDC Source, and separate LOB topics.
- Transaction metadata records and a schema change topic for consumers.
- `oracle-cdc-admin` and the doctor's redo profiler.

See the [status table](../intro.md#status) for everything that is available.

## Measure it yourself

Oracle's licence terms restrict publishing database benchmark results, so this project publishes
the means to measure, not results. Everything below is in the repository and runs on your own
database under its own licence.

- **A deterministic workload.** `bench workload` runs a seeded mix of inserts, updates (including
  key changes), deletes, LOB writes, savepoint and full rollbacks, multi-table transactions,
  occasional large transactions, truncates and column DDL over several sessions. The same seed and
  specification always produce the same statements. Every committed transaction writes one row to
  a ledger table inside the transaction. Specifications live in `bench/src/main/resources/workloads`.
- **A correctness oracle.** `bench check` consumes the change topics with `read_committed`,
  materialises them, and checks three things: each table's materialised state equals the database
  as of the highest commit SCN seen; the committed transactions in the ledger are exactly the
  transactions seen in Kafka; and within each transaction the event indexes are complete, with no
  duplicates and commit SCNs that never go backwards within a partition. It writes an evidence file
  with a SHA-256 digest. It reads this connector's topics.
- **A lab to run them in.** The [Docker Compose lab](../getting-started/quick-start.md) runs Oracle
  Database Free, Kafka and Connect; its `debezium` profile adds a Debezium worker on the same
  database and Kafka, so both connectors can mine the same workload side by side.
- **Metrics to measure with.** Latency and throughput come from the connector's
  [metrics](../reference/metrics.md) (`MillisBehindSource`, `RowsMined`, `TransactionsCommitted`,
  `LastStepMillis`) and from consumer lag on the change topics; memory from `BufferHeapBytes`,
  `SpilledBytes` and the worker's own JVM metrics.

```bash
java -jar bench/target/bench-*-cli.jar workload --url jdbc:oracle:thin:@//db:1521/PDB1 \
  --user workload --password '<password>' --spec bench/src/main/resources/workloads/long-tx.json --reset
java -jar bench/target/bench-*-cli.jar check --bootstrap-servers kafka:9092 \
  --url jdbc:oracle:thin:@//db:1521/PDB1 --user workload --password '<password>' \
  --topic-prefix cdc --pdb PDB1 --tables WL_T1,WL_T2,WL_T3 --out evidence.json
```

`bench check` exits with 0 when every check passes, 1 when one fails and 3 when the result is
inconclusive (for example when the database could no longer answer the `AS OF SCN` query).
