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

The current release is 0.1.1, on [Maven Central](https://central.sonatype.com/namespace/sh.oso)
and the [GitHub release page](https://github.com/osodevops/kafka-connect-oracle-cdc/releases). It
is a preview: releases before 1.0 are tested on Oracle Database Free 23ai and 26ai, single
instance, and the 19c and 21c qualification and a 72-hour soak come before 1.0. Everything this
site describes as available is in that release and covered by its test suites (see
[getting started](getting-started/index.md)). Nothing on this site is a benchmark claim.

| Capability | Status |
|---|---|
| Streaming capture with LogMiner, Debezium-compatible records, at-least-once delivery | Available |
| [Exactly-once delivery](concepts/exactly-once.md), Kafka transactions at Oracle commit boundaries | Available |
| [Snapshots](concepts/snapshots.md): chunked, SCN-anchored, resumable, interleaved with streaming | Available |
| [Signals](operations/signals.md): snapshots on demand, pause, resume, stop, table refresh, state dump | Available |
| [Transaction buffer](concepts/transaction-buffer-and-journal.md) with spill to disk, Kafka transaction journal, orphan detection, long-transaction policy | Available |
| [Schema versions and DDL](concepts/schema-and-ddl.md): schema topic, rows written before a DDL decoded with a dictionary from the redo | Available |
| [LOB columns](reference/record-formats.md#lob-columns): skip, inline from redo, or reselect as of the commit SCN | Available |
| [Column exclusion](reference/record-formats.md#excluded-columns): named columns dropped before their values are read | Available |
| [Avro-safe names](reference/record-formats.md#avro-and-other-strict-naming-rules) for schemas and fields, with a typed stop on name collisions | Available |
| Decode DLQ, [ops topic](reference/ops-topic.md), heartbeats, archive-only mining | Available |
| [Metrics](reference/metrics.md), Prometheus exporter rules, [Grafana dashboard and alert rules](operations/dashboards-and-alerts.md) | Available |
| [`oracle-cdc-doctor`](operations/doctor.md): preflight checks, setup script, redo profile, sizing, lag explanation | Available |
| Several PDBs from one connector | See [multi-PDB capture](concepts/multi-pdb.md) |
| [`oracle-cdc-admin`](operations/admin.md): offsets, resnapshot, open transactions, journal inspection | Available |
| [Migration tools](migration/tools.md) from Confluent and Debezium: configuration translation, takeover SCN, cutover verification | Available |
| [Amazon RDS for Oracle](database-setup/amazon-rds.md) 19c non-CDB | Preview |
| Oracle RAC, RDS with the CDB architecture, Autonomous Database, standby capture | Not available yet |
| Confluent-compatible record format, transaction metadata records, schema change topic | Not available yet |

Oracle Database 19c and later is required. The test suites run against Oracle Database Free,
release 23.26.3;
see [installation](getting-started/installation.md#compatibility) for what is tested.

## Design in one paragraph

The engine mines uncommitted redo with LogMiner and keeps its own transaction buffer, so a long
running transaction never pins the connector's offsets and never fills the heap: large
transactions spill to disk and, past a threshold, to a durable journal topic in Kafka. Offsets
only ever encode what Kafka Connect acknowledged. Any condition the engine does not understand,
such as a missing archived log, a corrupt redo record or a column it cannot decode, stops the
task with a typed error and a [runbook](operations/runbooks/index.md) link rather than skipping
data.

## Where to go next

- [Getting started](getting-started/index.md) runs the whole stack on a laptop.
- [Database setup](database-setup/index.md) prepares an Oracle database for capture.
- [How capture works](concepts/how-capture-works.md) explains the engine.
- [Comparison](comparison/index.md) sets the connector beside Confluent's and Debezium's.
- [Migration](migration/tools.md) moves an existing Debezium or Confluent pipeline across.
- [Enterprise support](enterprise-support.md) describes the production support subscription.

## Trademarks

This project is not affiliated with, endorsed by or sponsored by Oracle Corporation. Oracle is
a registered trademark of Oracle and/or its affiliates. The repository and plugin names use
"oracle" descriptively, as other connectors for Oracle Database do.
