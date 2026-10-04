---
title: Introduction
description: Open-source change data capture from Oracle Database to Apache Kafka through LogMiner, built for no silent loss.
slug: /
sidebar_position: 1
---

# OSO CDC Connector for Oracle Database

**OSO CDC Connector for Oracle Database** is an open-source (Apache-2.0) Kafka Connect source
connector. It reads committed changes from Oracle Database redo through LogMiner and publishes
them to Apache Kafka. It is built to replace Confluent's proprietary Oracle CDC Source and to
give Debezium users on LogMiner a connector whose first rule is that no change is ever lost
silently.

## Status

The connector is under active development and has not yet made a release. This site documents
the design as it is implemented; every page says whether the capability it describes exists in
the current code, is being built, or is planned. Nothing on this site is a benchmark claim.

| Capability | Status |
|---|---|
| Database preflight (`oracle-cdc-doctor check`, `setup-sql`) | Available in the repository |
| Deterministic workload generator and ledger (`bench workload`) | Available in the repository |
| Docker Compose and Strimzi labs | Available in the repository |
| Capture engine, Debezium-compatible envelope, at-least-once delivery for one or more PDBs (no LOB values, no DDL replay yet) | Available in the repository (Phase 1a) |
| Transaction journal, spill to disk, orphan detection | Planned (Phase 1b) |
| Exactly-once delivery, snapshots, DDL replay, multi-PDB | Planned (Phase 1c) |
| RAC, Confluent record format, RDS, Autonomous Database | Planned (Phase 2 and 3) |

## Design in one paragraph

The engine mines uncommitted redo with LogMiner and keeps its own transaction buffer, so a long
running transaction never pins the connector's offsets and never fills the heap: large
transactions spill to disk and, past a threshold, to a durable journal topic in Kafka. Offsets
only ever encode what Kafka Connect acknowledged. Any condition the engine does not understand,
such as a missing archived log, a corrupt redo record or a column it cannot decode, stops the
task with a typed error and a runbook link rather than skipping data.

## Where to go next

- [Getting started](getting-started/index.md) runs the whole stack on a laptop.
- [Database setup](database-setup/index.md) prepares an Oracle database for capture.
- [oracle-cdc-doctor](operations/doctor.md) checks the database before the connector starts.
- [Enterprise support](enterprise-support.md) describes the production support subscription.

## Trademarks

This project is not affiliated with, endorsed by or sponsored by Oracle Corporation. Oracle is
a registered trademark of Oracle and/or its affiliates. The repository and plugin names use
"oracle" descriptively, as other connectors for Oracle Database do.
