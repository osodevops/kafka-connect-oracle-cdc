---
title: Oracle CDC and Replication Tools Compared
sidebar_label: Comparison
description: A fair, sourced comparison of Oracle CDC and replication tools, covering LogMiner, XStream, GoldenGate and connectors such as Debezium, Confluent and OSO.
---

import Faq from '@site/src/components/Faq';

# Oracle CDC and Replication Tools Compared

This page compares ways to capture changes from Oracle Database and the tools that publish them,
including to Apache Kafka. Facts about other products come from the vendor pages linked below, as
read on 27 October 2026. Facts about the OSO CDC Connector come from this site and describe release
0.1.6, which is the current release. This project is not affiliated with or endorsed by Oracle,
Confluent, the Debezium project, Striim or Qlik.

**Quick answer.** The main Oracle CDC tools read committed database changes and publish them to another system, often Apache Kafka. Capture usually goes through LogMiner, XStream, GoldenGate, or a reader of the redo logs. Open source Kafka Connect options include Debezium and the OSO connector. Commercial options include Confluent's Oracle CDC Source, GoldenGate, Striim and Qlik Replicate.

## Which capture methods can Oracle CDC tools use?

| Mechanism | What it is | Oracle licensing requirement | Notes |
|---|---|---|---|
| LogMiner | Part of Oracle Database. It queries online and archived redo log files through SQL, including `V$LOGMNR_CONTENTS`. | Included with all Oracle Database offerings, and with all Oracle AI Database offerings. A GoldenGate licence is required when GoldenGate supplemental logging is on. | Oracle defines that logging as `ENABLE_GOLDENGATE_REPLICATION` set to `TRUE`, and requires the licence whichever API consumes the redo, including LogMiner, XStream or a third-party tool. Oracle also states that LogMiner is intended as a debugging and analysis tool and is not intended for third-party replication in a production environment. Minimal supplemental logging is required to use it. |
| XStream | Oracle Database components and APIs. XStream Out reads data manipulation and data definition changes from the redo log and sends logical change records to a client application. | Licensed through Oracle GoldenGate. Oracle states that you must purchase that licence before using the XStream APIs. | Oracle Database Enterprise Edition is a prerequisite to license and use XStream. XStream Out streams logical change records in committed transaction order. |
| GoldenGate | Oracle's product for capturing and delivering database transactions for Oracle databases, using Extract and Replicat. | Separately licensed. A GoldenGate licence includes a licence for XStream and satisfies the database requirement when `ENABLE_GOLDENGATE_REPLICATION` is `TRUE`, subject to the Enterprise Edition prerequisite for XStream. | Oracle describes GoldenGate as its solution for replicating between Oracle and non-Oracle databases. Publishing those changes to Apache Kafka is a target of the separately licensed GoldenGate for Big Data Targets product, which does not include source capture. |
| Log-based redo readers | Third-party software that reads binary redo or archive logs directly, outside LogMiner, XStream and GoldenGate. | Oracle states that it does not authorise third parties to use the content and structure of those logs. | Oracle describes this approach as unsupported, and states that it may breach the customer's agreements with Oracle, because the reader reverse engineers log structures Oracle does not document. A later database patch can change the log format without notice. |

Sources, checked on 27 October 2026: Oracle's
[LogMiner guide](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html),
[19c licensing information](https://docs.oracle.com/en/database/oracle/oracle-database/19/dblic/Licensing-Information.html),
[current licensing information](https://docs.oracle.com/en/database/oracle/oracle-database/26/dblic/Licensing-Information.html),
[Introduction to XStream](https://docs.oracle.com/en/database/oracle/oracle-database/19/xstrm/introduction-to-xstream.html),
[GoldenGate licensing](https://docs.oracle.com/en/database/goldengate/core/26/ogglc/oracle-goldengate.html),
[GoldenGate for Big Data Targets](https://docs.oracle.com/en/database/goldengate/core/26/ogglc/oracle-goldengate-big-data-targets.html),
and [binary log readers](https://blogs.oracle.com/dataintegration/binary-log-readers).

This page reports those documents. It is not an Oracle licence ruling. Oracle licensing questions
are outside the scope of [enterprise support](../enterprise-support.md) for the OSO connector.

## How do the main Oracle CDC tools compare?

Each row starts from what that product's own pages present as its job. A fact that could not be
checked on a primary page is left out.

| Tool | What it is for | Capture method | Licence | Kafka Connect based | Notes |
|---|---|---|---|---|---|
| Debezium Oracle connector | Captures row-level changes, including tables added while the connector is running, and can use more than one Oracle adapter. | LogMiner by default. XStream or OpenLogReplicator are alternatives. | Open source, Apache-2.0. | Yes. A Kafka Connect plug-in, class `io.debezium.connector.oracle.OracleConnector`. | Debezium states that it is compatible with a standalone database and with Oracle Real Application Clusters. It describes XStream as a commercial component of GoldenGate. It states that OpenLogReplicator reads redo and archive logs directly. |
| Confluent Oracle CDC Source | Captures each row change through LogMiner and can spread table work across Kafka Connect tasks. | Oracle LogMiner, reading the redo log. | Commercial. A Confluent Premium connector under the Confluent enterprise licence, and it requires an additional subscription. A trial period of 30 days does not need a licence key. | Yes. A Kafka Connect source connector. | Confluent documents Oracle Real Application Clusters, a redo log topic, and parallel snapshots of partitioned tables. From Confluent Platform 6.0 it can assign frequently changing tables to different tasks. It does not terminate if LogMiner reports corrupted redo blocks. |
| Oracle GoldenGate | Captures and delivers database transactions for Oracle databases, and Oracle describes it for replication between Oracle and non-Oracle databases. | GoldenGate Extract and Replicat. A GoldenGate licence includes XStream. | Commercial. GoldenGate is licensed separately from Oracle Database. | GoldenGate for Distributed Applications and Analytics documents a Kafka Handler and a Kafka Connect Handler. | Apache Kafka is one target technology of the separately licensed GoldenGate for Big Data Targets product. That product licence covers targets only, not source capture. |
| Striim | Streams Oracle changes into Kafka from a Striim application, with a documented wizard that selects LogMiner and writes with Kafka Writer. | Oracle Reader uses LogMiner. OJet uses a log mining API. GG Trail Reader reads existing GoldenGate trail files when the Striim cluster is licensed for Oracle GoldenGate. | Commercial. The software is licensed under Striim's end user licence agreement, for the fees on an order form. Oracle Reader is a premium adapter. | Kafka delivery uses Striim's Kafka Writer adapter inside a Striim application. | Striim states that Oracle Reader and OJet can read a primary, a logical standby, or an Active Data Guard standby, and that Oracle Reader supports inserts, updates and deletes. Kafka Writer documents exactly-once or at-least-once recovery. |
| Qlik Replicate | Captures Oracle changes and delivers them to a target endpoint. Kafka is one documented target. | Oracle LogMiner, or Replicate Log Reader. Qlik describes Replicate Log Reader as its redo log reader. | Commercial. Qlik states that a valid licence is required, requested from the product and registered as a licence file. | Kafka is a Replicate target endpoint: broker servers, a target role, and topic settings on the Replicate server. | Qlik states that capturing changes from a pluggable database with LogMiner is not supported, and that LogMiner is not supported when replicating from Oracle 21c. Qlik also states that setting `ENABLE_GOLDENGATE_REPLICATION` has licensing implications that remain the customer's responsibility. |
| OSO CDC Connector | An open-source Kafka Connect source that publishes committed Oracle changes. A condition it cannot decode stops the task with a typed error. | Oracle LogMiner only. It does not read redo files directly and does not use GoldenGate or XStream. | Open source, Apache-2.0. | Yes. A Kafka Connect source connector, class `sh.oso.connect.oracle.OracleCdcSourceConnector`. | Release 0.1.6 is a preview. Exactly-once delivery is available when the Connect worker and the connector are set for it. Oracle Real Application Clusters, Autonomous Database and standby capture are not available yet. Documented setup asks for ARCHIVELOG mode and supplemental logging, and does not set `ENABLE_GOLDENGATE_REPLICATION`. The connector runs one task. |

Sources, checked on 27 October 2026:

- Debezium: [Oracle connector](https://debezium.io/documentation/reference/stable/connectors/oracle.html) and the [Apache-2.0 licence](https://github.com/debezium/debezium/blob/main/LICENSE.txt).
- Confluent: [Oracle CDC Source overview](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/overview.html).
- Oracle GoldenGate: [GoldenGate licensing](https://docs.oracle.com/en/database/goldengate/core/26/ogglc/oracle-goldengate.html), [Big Data targets](https://docs.oracle.com/en/database/goldengate/core/26/ogglc/oracle-goldengate-big-data-targets.html), the [Kafka handler](https://docs.oracle.com/en/database/goldengate/big-data/26/gadbd/apache-kafka.html), and [Kafka ingestion with GoldenGate for Distributed Applications and Analytics](https://docs.oracle.com/en/database/goldengate/big-data/26/gadbd/qs-realtime-message-ingestion-apache-kafka-goldengate-daa.html).
- Striim: [Oracle CDC readers](https://www.striim.com/docs/platform/en/oracle-database-cdc.html), [Oracle to Kafka](https://www.striim.com/docs/en/replicating-oracle-data-to-kafka.html), [Kafka Writer](https://www.striim.com/docs/platform/en/kafka-writer.html), the [end user licence agreement](https://www.striim.com/eula/), and [consumption terms](https://www.striim.com/cloud-consumption-terms/).
- Qlik: [redo access methods](https://help.qlik.com/en-US/replicate/May2026/Content/Replicate/Main/Oracle/setting_advanced_conneciton_params.htm), [LogMiner limitations](https://help.qlik.com/en-US/replicate/May2026/Content/Global_Common/Content/SharedReplicateHDD/Oracle/OracleDB_source_limitations.htm), [Kafka target](https://help.qlik.com/en-US/replicate/May2026/Content/Replicate/Main/Kafka/kafka_setting_general_properties.htm), and [requesting a licence](https://help.qlik.com/en-US/replicate/November2024/Content/Global_Common/Content/SharedEMReplicate/Server%20Settings/config_licReq.htm).
- OSO CDC Connector: the [introduction](../intro.md), [how capture works](../concepts/how-capture-works.md), [exactly-once delivery](../concepts/exactly-once.md), [database setup](../database-setup/index.md), and [installation](../getting-started/installation.md).

## Which open source Oracle CDC options exist?

Two of the tools above are open source.

Debezium's Oracle connector is published under the Apache-2.0 licence. It installs into Kafka
Connect and captures row-level inserts, updates and deletes. LogMiner is the default. XStream and
OpenLogReplicator are configuration choices. Debezium's description of OpenLogReplicator, which
reads redo and archive logs directly, sits next to Oracle's statement on binary log readers in
the table above. Whether that adapter is the kind of reader Oracle's blog discusses is a question
for Oracle and for counsel. This page does not decide it.

The OSO CDC Connector is the connector this site documents. It is published under the Apache-2.0
licence. It reads changes through LogMiner only and does not read redo files directly. Release
0.1.6 writes Debezium-compatible records, and a Confluent-compatible format from that same
release. The [status table](../intro.md#status) lists what is in that release and what is not
available yet, including Oracle Real Application Clusters.

## When is GoldenGate the right choice?

GoldenGate is the product Oracle documents for capturing and delivering transactions on an Oracle
database, and for replicating between Oracle and non-Oracle databases. A GoldenGate licence
includes XStream. Choose it when you want that Oracle product, when the capture path you need is
XStream or GoldenGate supplemental logging, or when you want the Kafka Handler or the Kafka
Connect Handler in GoldenGate for Distributed Applications and Analytics. Those handlers deliver
changes that GoldenGate has already captured. Apache Kafka as a target needs the separate
GoldenGate for Big Data Targets licence.

Oracle's LogMiner guide states that LogMiner is not intended for third-party replication in a
production environment. That is one reason teams look at GoldenGate. It is not, by itself, a
licence ruling for any third-party connector. Confluent's Oracle CDC Source uses LogMiner and is
a different product from Confluent's Oracle XStream CDC Source connector. The
[Confluent, LogMiner or GoldenGate](confluent-oracle-cdc-logminer-or-goldengate.md) page sets
those two Confluent connectors beside the licence documents.

## How do I choose an Oracle CDC tool?

Match the capture API to the Oracle licence you hold, then match the runtime to the platform you
already operate.

- For a Kafka Connect source that uses LogMiner, compare the Debezium, Confluent and OSO rows
  above, then the detailed pages below. The [harness](#measure-it-yourself) measures the OSO
  connector on your own database. This project does not publish database benchmark figures.
- For XStream, Oracle's licensing information requires a GoldenGate licence before using the
  APIs. Debezium documents an XStream adapter. Confluent documents a separate XStream connector.
- For Oracle's own capture product, or for a Kafka handler that runs inside GoldenGate, use the
  GoldenGate section above.
- For a replication server that writes to Kafka itself, see the Striim and Qlik rows and the
  vendor pages linked there.
- Support for the OSO connector is described in [enterprise support](../enterprise-support.md).

## Detailed comparisons

- [Confluent, LogMiner or GoldenGate](confluent-oracle-cdc-logminer-or-goldengate.md) answers
  which Oracle API Confluent's Oracle CDC Source uses, and how Confluent's separate XStream
  connector differs.
- [Migrating from Confluent Oracle CDC Source](../migration/from-confluent.md) describes the
  same-topic cutover, available from release 0.1.6.
- [Oracle CDC to Kafka](../guides/oracle-cdc-to-kafka.md) explains LogMiner-based capture and how
  to run it with this connector.

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
- **A soak harness.** `bench soak` runs a paced workload for as many hours as you choose against a
  running connector, pauses at fixed check points until the connector has caught up, runs the
  correctness oracle at each one, and samples the connector's metrics from the JMX exporter. The
  Docker Compose lab runs it with `make soak`. The figures it records are for your own use; this
  project does not publish them.
- **A lab to run them in.** The [Docker Compose lab](../getting-started/quick-start.md) runs Oracle
  Database Free, Kafka and Connect; its `debezium` profile adds a Debezium worker on the same
  database and Kafka, so both connectors can mine the same workload side by side.
- **Metrics to measure with.** Latency and throughput come from the connector's
  [metrics](../reference/metrics.md) (`MillisBehindSource`, `RowsMined`, `TransactionsCommitted`,
  `LastStepMillis`) and from consumer lag on the change topics; memory from `BufferHeapBytes`,
  `SpilledBytes` and the worker's own JVM metrics.

```bash
java -jar bench/target/bench-*-cli.jar workload --url jdbc:oracle:thin:@//db:1521/PDB1 \
  --user workload --password-env WORKLOAD_PASSWORD --spec bench/src/main/resources/workloads/long-tx.json --reset
java -jar bench/target/bench-*-cli.jar check --bootstrap-servers kafka:9092 \
  --url jdbc:oracle:thin:@//db:1521/PDB1 --user workload --password-env WORKLOAD_PASSWORD \
  --topic-prefix cdc --pdb PDB1 --tables WL_T1,WL_T2,WL_T3 --out evidence.json
```

`bench check` exits with 0 when every check passes, 1 when one fails and 3 when the result is
inconclusive (for example when the database could no longer answer the `AS OF SCN` query).

<Faq
  items={[
    {
      question: 'Is there an open source alternative to GoldenGate for Oracle CDC?',
      answer:
        'Yes. Debezium\'s Oracle connector and the OSO CDC Connector are both open source under the Apache-2.0 licence, and both can capture Oracle changes through LogMiner. Debezium can also use the XStream API or OpenLogReplicator, which Debezium states reads redo and archive logs directly. GoldenGate is a separately licensed Oracle product for capturing and delivering database transactions. Sources: https://debezium.io/documentation/reference/stable/connectors/oracle.html and https://github.com/debezium/debezium/blob/main/LICENSE.txt and https://docs.oracle.com/en/database/goldengate/core/26/ogglc/oracle-goldengate.html. The OSO statement is from this site\'s introduction.',
    },
    {
      question: 'Do I need an XStream or GoldenGate licence to capture Oracle changes?',
      answer:
        'Not for every capture method. Oracle states that LogMiner is available with all Oracle Database offerings and with all Oracle AI Database offerings. Using the XStream APIs requires a GoldenGate licence, and a GoldenGate licence includes XStream. When GoldenGate supplemental logging is on, which Oracle defines as ENABLE_GOLDENGATE_REPLICATION set to TRUE, a GoldenGate licence is required whichever API consumes the redo, including LogMiner, XStream or a third-party tool. The OSO connector uses LogMiner only, and its documented setup does not set that parameter. Sources: https://docs.oracle.com/en/database/oracle/oracle-database/19/dblic/Licensing-Information.html and https://docs.oracle.com/en/database/oracle/oracle-database/26/dblic/Licensing-Information.html and https://docs.oracle.com/en/database/oracle/oracle-database/19/xstrm/introduction-to-xstream.html.',
    },
    {
      question: 'What is the difference between LogMiner and XStream?',
      answer:
        'LogMiner queries online and archived redo log files through SQL, and Oracle includes it with database offerings unless GoldenGate supplemental logging is on. XStream is a set of Oracle Database components and APIs. XStream Out reads data manipulation and data definition changes from the redo log and sends logical change records to a client, and using those APIs requires a GoldenGate licence. Oracle also states that LogMiner is intended as a debugging and analysis tool and is not intended for third-party replication in a production environment. Sources: https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html and https://docs.oracle.com/en/database/oracle/oracle-database/19/xstrm/introduction-to-xstream.html and https://docs.oracle.com/en/database/oracle/oracle-database/19/dblic/Licensing-Information.html.',
    },
    {
      question: 'Can I stream Oracle changes into Kafka without Confluent Platform?',
      answer:
        'Yes. This site\'s installation guide documents the connector on Apache Kafka Connect: unzip the plugin into a directory on plugin.path and restart the worker. The same page lists Apache Kafka Connect, Strimzi, Confluent Platform 7.6 and later, and Amazon MSK Connect. The quick start runs Oracle Database Free, Apache Kafka in KRaft mode and a Kafka Connect worker. The connector class is sh.oso.connect.oracle.OracleCdcSourceConnector.',
    },
    {
      question: 'Is the OSO connector open source?',
      answer:
        'Yes. The repository licence is the Apache License, Version 2.0, and this site\'s introduction describes the connector as an open-source Apache-2.0 Kafka Connect source. It reads changes through Oracle LogMiner only and does not read redo files directly.',
    },
  ]}
/>

## Trademarks

This project is not affiliated with, endorsed by or sponsored by Oracle Corporation or Confluent.
Oracle is a registered trademark of Oracle and/or its affiliates. Debezium, Striim, Qlik and other
names may be trademarks of their respective owners. The repository and plugin names use "oracle"
descriptively, as other connectors for Oracle Database do.
