---
title: Open-Source Oracle CDC Connector for Kafka
sidebar_label: Introduction
description: An open-source (Apache-2.0) Kafka Connect source connector for Oracle change data capture using LogMiner. It publishes committed changes to Apache Kafka.
slug: /
sidebar_position: 1
---

import Faq from '@site/src/components/Faq';

# OSO CDC Connector for Oracle Database

Oracle change data capture records committed inserts, updates and deletes from Oracle Database
redo. The OSO CDC Connector for Oracle Database reads those changes through Oracle LogMiner and
publishes them to Apache Kafka as an open-source (Apache-2.0) Kafka Connect source connector.

It is built to replace Confluent's proprietary Oracle CDC Source and to give Debezium users on
LogMiner a connector whose first rule is that no change is ever lost silently.

## Status

The current release is 0.1.6, on [Maven Central](https://central.sonatype.com/namespace/sh.oso)
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
| [Amazon RDS for Oracle](database-setup/amazon-rds.md) 19c, non-CDB, and with the CDB architecture from 0.1.3 | Preview |
| Oracle RAC, Autonomous Database, standby capture | Not available yet |
| [Confluent-compatible format and separate LOB topics](reference/record-formats.md#confluent-compatible-format), same-topic migration | Available from 0.1.6 |
| Transaction metadata records, schema change topic | Not available yet |

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

## What is Oracle change data capture?

Change data capture (CDC) records each committed insert, update and delete so another system can
consume it as an event. On Oracle Database those changes are written to the redo stream. Teams
stream them to Apache Kafka to feed analytics, search, caches and other services without changing
the applications that write the data. The
[guide to Oracle change data capture](guides/oracle-cdc-to-kafka.md) walks through the practical
steps, and [how capture works](concepts/how-capture-works.md) describes this connector's engine.

The main capture approaches on Oracle Database are:

- **LogMiner.** [Oracle LogMiner](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html)
  is part of Oracle Database. It queries online and archived redo log files through SQL. This
  connector uses LogMiner only and does not read redo files directly.
- **Oracle GoldenGate.** GoldenGate captures and delivers database transactions for Oracle
  databases. Oracle lists it as a separately licensed product. A GoldenGate licence includes a
  licence for XStream, and using XStream also requires Oracle Database Enterprise Edition
  ([GoldenGate](https://docs.oracle.com/en/database/goldengate/core/26/ogglc/oracle-goldengate.html),
  [product list](https://docs.oracle.com/en/database/goldengate/core/26/ogglc/oracle-goldengate-products.html)).
- **XStream.** XStream is a set of Oracle Database components and APIs that stream logical change
  records to a client application. Using the XStream APIs requires a GoldenGate licence
  ([Introduction to XStream](https://docs.oracle.com/en/database/oracle/oracle-database/19/xstrm/introduction-to-xstream.html)).

## How is this different from Debezium, GoldenGate and Confluent?

- **Debezium's Oracle connector** is Apache-2.0 licensed and captures row-level changes, including
  tables added while it is running. LogMiner is the default adapter. XStream, which Debezium
  describes as a commercial component of Oracle GoldenGate, and OpenLogReplicator, which reads
  redo and archive logs directly, are alternatives. It writes the Debezium envelope. Delivery is
  at least once, and exactly-once delivery is available through Kafka Connect with a Kafka
  transaction per poll
  ([Oracle connector](https://debezium.io/documentation/reference/stable/connectors/oracle.html),
  [exactly-once delivery](https://debezium.io/documentation/reference/stable/configuration/eos.html),
  [licence](https://github.com/debezium/debezium/blob/main/LICENSE.txt)).
- **Confluent's Oracle CDC Source connector** uses LogMiner and is built to spread table work
  across tasks, with snapshots, separate LOB topics and a redo log topic written at least once.
  It is a Confluent Premium connector under the Confluent enterprise licence and needs an
  additional subscription
  ([overview](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/overview.html)).
- **Oracle GoldenGate** captures and delivers database transactions for Oracle databases. A
  GoldenGate licence includes a licence for XStream, and using XStream also requires Oracle
  Database Enterprise Edition. Publishing those changes to Apache Kafka is covered by the
  separately licensed Oracle GoldenGate for Big Data Targets product, which lists Apache Kafka
  as one target technology
  ([GoldenGate](https://docs.oracle.com/en/database/goldengate/core/26/ogglc/oracle-goldengate.html),
  [Big Data targets](https://docs.oracle.com/en/database/goldengate/core/26/ogglc/oracle-goldengate-big-data-targets.html)).

This connector is an Apache-2.0 Kafka Connect source and uses LogMiner only, so it does not need
GoldenGate or XStream. Release 0.1.6 writes Debezium-compatible records, and a
Confluent-compatible format from that same release. Delivery is at least once, or
[exactly once](concepts/exactly-once.md) with Kafka transactions at Oracle commit boundaries. The
[comparison](comparison/index.md) sets this connector beside Confluent's Oracle CDC Source and
Debezium's Oracle connector. The [migration tools](migration/tools.md) translate a configuration,
take over from a system change number and verify the cutover.

<Faq
  items={[
    {
      question: 'What is Oracle CDC?',
      answer:
        'Oracle CDC is change data capture from Oracle Database: the committed inserts, updates and deletes recorded in the redo stream. This connector reads those changes through Oracle LogMiner and publishes them to Apache Kafka.',
    },
    {
      question: 'Is there an open-source Oracle CDC connector for Kafka?',
      answer:
        'Yes. This connector is an open-source (Apache-2.0) Kafka Connect source for Oracle change data capture. It reads committed changes through LogMiner and publishes them to Apache Kafka.',
    },
    {
      question: 'Does this connector need Oracle GoldenGate or XStream?',
      answer:
        'No. It reads changes through Oracle LogMiner only and does not require Oracle GoldenGate or XStream.',
    },
    {
      question: 'Which Oracle Database versions are supported?',
      answer:
        'Oracle Database 19c and later is required, as a single instance, CDB or non-CDB. The test suites run against Oracle Database Free, release 23.26.3, a CDB with three PDBs. 19c and 21c are listed in the support policy for 1.0 and are not yet qualified.',
    },
    {
      question: "Can I migrate from Debezium or Confluent's Oracle CDC Source?",
      answer:
        'Yes. The current release includes tools that translate a Debezium or Confluent Oracle CDC Source configuration, take over from a system change number, and verify the cutover.',
    },
    {
      question: 'Does it support exactly-once delivery?',
      answer:
        'Yes. With exactly.once.source.support set to enabled on the Connect worker, exactly.once.support set to required, and transaction.boundary set to connector, a consumer with isolation.level=read_committed sees each change once and never part of an Oracle transaction. Without that mode the connector delivers at least once.',
    },
  ]}
/>

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
