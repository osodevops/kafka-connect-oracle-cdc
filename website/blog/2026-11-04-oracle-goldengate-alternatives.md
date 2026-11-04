---
title: Oracle GoldenGate Alternatives for Kafka (2026)
title_meta: Oracle GoldenGate Alternatives for Kafka
description: A fair comparison of Oracle GoldenGate alternatives for streaming Oracle changes into Kafka, with licensing notes for open-source and commercial tools.
slug: oracle-goldengate-alternatives
date: 2026-11-04
authors: [oso]
tags: [goldengate, kafka, comparison]
---

import Faq from '@site/src/components/Faq';

**Quick answer.** Oracle GoldenGate captures and delivers transactions through trail files, including between Oracle and non-Oracle databases, and it can publish to Kafka under a separate Big Data licence. For a Kafka Connect pipeline, compare LogMiner connectors such as Debezium, Confluent's Oracle CDC Source and the OSO CDC Connector, plus Striim and Qlik Replicate. XStream requires a GoldenGate licence.

{/* truncate */}

The notes below were checked against the vendor pages linked here on 4 November 2026. Statements about the OSO CDC Connector come from this site and describe release 0.1.6. This project is not affiliated with or endorsed by Oracle, Confluent, the Debezium project, Striim or Qlik. This page reports those documents. It is not an Oracle licence ruling.

## Why do teams look for a GoldenGate alternative?

Oracle GoldenGate is the product Oracle documents for this job. Oracle's GoldenGate FAQ describes it as a data integration and replication solution that captures, transforms and delivers transactional changes across heterogeneous systems in real time. Extract captures changes from the source redo or transaction logs and writes them to trail files. A distribution path reads the trail, and Replicat applies the changes, so capture and delivery can run independently. The same FAQ describes low latency as the result of a tuned deployment: integrated Extract with parallel Replicat, separate trail files for capture and apply, and fast storage, database memory and a stable network under them. GoldenGate is Oracle's own product, documented and licensed by Oracle.

Teams still look elsewhere when the shape of the pipeline does not match the product.

- **Licensing.** GoldenGate is licensed separately from Oracle Database. A GoldenGate licence includes XStream, subject to an Oracle Database Enterprise Edition prerequisite, and it satisfies the database requirement when `ENABLE_GOLDENGATE_REPLICATION` is `TRUE`. Publishing to Apache Kafka is a target of the separately licensed GoldenGate for Big Data Targets product. That licence covers targets only, one technology per licence, and does not include source capture.
- **Operational footprint.** A GoldenGate path is Extract, trail files, distribution and then Replicat or a Big Data handler. That is a replication deployment of its own, with trail management beside the database and beside Kafka.
- **A Kafka-first runtime.** Teams that already operate Kafka Connect often want capture to be a connector task, with offsets stored by Connect, rather than a GoldenGate deployment that then publishes. The [comparison](/comparison/) sets the main Oracle CDC tools side by side, and the [Oracle CDC to Kafka](/guides/oracle-cdc-to-kafka) guide walks through a LogMiner connector on Connect.

## How do the alternatives compare?

Each row uses only what that product's own pages stated on 4 November 2026. Throughput figures stay in those vendor documents. This project publishes a harness for your own database and does not publish database benchmark results.

| Tool | Capture method | Kafka Connect connector | Licence | Oracle licensing |
|---|---|---|---|---|
| Oracle GoldenGate | Extract captures changes from the source transaction logs and writes trail files. Replicat, or a Big Data handler, delivers them. | Oracle documents a Kafka Handler and a Kafka Connect Handler that stream from a trail. Oracle describes the Kafka Connect Handler as a Kafka Connect source connector. Capture itself remains Extract. | Commercial. Licensed separately from Oracle Database. | A GoldenGate licence includes XStream and covers `ENABLE_GOLDENGATE_REPLICATION` set to `TRUE`. Enterprise Edition is a prerequisite for XStream. Apache Kafka as a target needs a separate GoldenGate for Big Data Targets licence, which does not include source capture. |
| XStream | XStream Out reads data manipulation and data definition changes from the redo log and sends logical change records to a client, in committed transaction order. | No. XStream is a set of database components and APIs. A client application attaches to an outbound server. | Licensed through Oracle GoldenGate. Oracle states that you must purchase that licence before using the XStream APIs. | Oracle Database Enterprise Edition is a prerequisite to licence and use XStream. The [Introduction to XStream](https://docs.oracle.com/en/database/oracle/oracle-database/19/xstrm/introduction-to-xstream.html) states the same purchase requirement. |
| Debezium Oracle connector | LogMiner by default. Set `database.connection.adapter` to `logminer`. XStream (`xstream`) and OpenLogReplicator (`olr`) are alternatives. Debezium states that OpenLogReplicator reads redo and archive logs directly. | Yes. A Kafka Connect source, class `io.debezium.connector.oracle.OracleConnector`. | Open source, Apache-2.0. | The LogMiner adapter uses a feature Oracle includes with database offerings unless GoldenGate supplemental logging is on. Debezium states that the XStream adapter requires a GoldenGate licence, and that installing GoldenGate is not required for that adapter. |
| Confluent Oracle CDC Source | Oracle LogMiner. The connector queries `V$LOGMNR_CONTENTS` and reads the database redo log. | Yes. A Kafka Connect source, class `io.confluent.connect.oracle.cdc.OracleCdcSourceConnector`. | Commercial. A Confluent Premium connector under the Confluent enterprise licence, and it requires an additional subscription. A trial period of 30 days does not need a licence key. | Confluent documents LogMiner, ARCHIVELOG mode and supplemental logging. Oracle states that LogMiner is included with database offerings unless GoldenGate supplemental logging is on. |
| Striim | Oracle Reader uses LogMiner. OJet uses a log mining API. GG Trail Reader reads existing GoldenGate trail files when the Striim cluster is licensed for Oracle GoldenGate. | No. Kafka delivery uses Kafka Writer, including Kafka SmartMap, inside a Striim application. | Commercial. The software is licensed, not sold, under Striim's end user licence agreement, for the fees on an order form. | GG Trail Reader depends on a GoldenGate trail and on the Striim cluster being licensed for Oracle GoldenGate. |
| Qlik Replicate | Oracle LogMiner, or Replicate Log Reader. Qlik describes Replicate Log Reader as its high-speed redo log reader. | No. Kafka is a target endpoint on the Replicate server: a target role, broker servers and topic settings. | Commercial. Qlik states that you must have a valid licence, supplied as a licence file and registered in the product. | Qlik states that setting `ENABLE_GOLDENGATE_REPLICATION` has licensing implications that remain the customer's responsibility. |
| OSO CDC Connector | Oracle LogMiner only. It does not read redo files directly and does not use GoldenGate or XStream. | Yes. A Kafka Connect source, class `sh.oso.connect.oracle.OracleCdcSourceConnector`. The connector runs one task. | Open source, Apache-2.0. | [Database setup](/database-setup) asks for ARCHIVELOG mode, supplemental logging and a LogMiner user. It does not set `ENABLE_GOLDENGATE_REPLICATION`. |

Sources for the table, checked on 4 November 2026:

- Oracle GoldenGate and XStream: [GoldenGate licensing](https://docs.oracle.com/en/database/goldengate/core/26/ogglc/oracle-goldengate.html), [replication components and trails](https://docs.oracle.com/en/database/goldengate/core/26/coredoc/overview-components-data-replication-oracle-goldengate.html), [GoldenGate FAQ](https://docs.oracle.com/en/database/goldengate/core/26/coredoc/ogg-frequently-asked-questions.html), [Big Data targets](https://docs.oracle.com/en/database/goldengate/core/26/ogglc/oracle-goldengate-big-data-targets.html), [Kafka handlers](https://docs.oracle.com/en/database/goldengate/big-data/26/gadbd/apache-kafka-target.html), [Database Licensing Information](https://docs.oracle.com/en/database/oracle/oracle-database/26/dblic/Licensing-Information.html) and [Introduction to XStream](https://docs.oracle.com/en/database/oracle/oracle-database/19/xstrm/introduction-to-xstream.html).
- Debezium: [Oracle connector](https://debezium.io/documentation/reference/stable/connectors/oracle.html) and the [Apache-2.0 licence](https://github.com/debezium/debezium/blob/main/LICENSE.txt).
- Confluent: [Oracle CDC Source overview](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/overview.html) and the [quick start](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/getting-started.html), which shows the connector class.
- Striim: [Oracle CDC readers](https://www.striim.com/docs/platform/en/oracle-database-cdc.html), [Kafka Writer](https://www.striim.com/docs/platform/en/kafka-writer.html) and the [end user licence agreement](https://www.striim.com/eula/).
- Qlik: [redo access methods](https://help.qlik.com/en-US/replicate/May2026/Content/Replicate/Main/Oracle/setting_advanced_conneciton_params.htm), [Kafka target](https://help.qlik.com/en-US/replicate/May2026/Content/Replicate/Main/Kafka/kafka_setting_general_properties.htm) and [registering a licence](https://help.qlik.com/en-US/replicate/May2026/Content/Global_Common/Content/SharedEMReplicate/Server%20Settings/config_licSettings.htm).
- OSO CDC Connector: the [introduction](/), [how capture works](/concepts/how-capture-works), [database setup](/database-setup) and [exactly-once delivery](/concepts/exactly-once). The connector class and the single task are in the connector source.

A few limits that sit beside the table, each from the same pages:

- Debezium states that its Oracle connector is compatible with a standalone database and with Oracle Real Application Clusters. Confluent states that the Oracle CDC Source connector supports Oracle Real Application Clusters. Striim states that Oracle Reader and OJet can read RAC and pluggable databases.
- Qlik states that capturing changes from a pluggable database with LogMiner is not supported, and that LogMiner is not supported when replicating from Oracle 21c. Qlik does not recommend LogMiner when replicating from a RAC environment.
- For the OSO connector, release 0.1.6 is a preview. The [introduction](/#status) lists Oracle Real Application Clusters, Autonomous Database and standby capture as not available yet. Exactly-once delivery is available when the Connect worker and the connector are set for it. A condition the engine cannot decode, including missing redo, stops the task rather than skipping the change.

Oracle's [LogMiner guide](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html) states that LogMiner is intended as a debugging tool and is not intended for third-party replication in a production environment. That sentence is one reason teams stay with GoldenGate. It is not, by itself, a licence ruling for any third-party connector. Debezium's description of OpenLogReplicator, which reads redo and archive logs directly, sits next to Oracle's statement on [binary log readers](https://blogs.oracle.com/dataintegration/binary-log-readers). Whether that adapter is the reader Oracle describes is a question for Oracle and for counsel. This page does not decide it. The OSO connector does not use that approach.

## Which alternative fits Kafka-first architectures?

Choose the runtime that matches where you want capture to live.

A Kafka Connect source is the closest fit when the pipeline is already Connect. Three of the tools above install that way:

- Debezium's Oracle connector, with LogMiner as the default adapter. The [migration from Debezium](/migration/from-debezium) guide describes moving a LogMiner pipeline onto this connector while keeping the existing topics.
- Confluent's Oracle CDC Source connector, which uses LogMiner and can spread table work across tasks from Confluent Platform 6.0. It does not terminate if LogMiner reports corrupted redo blocks. The [Confluent, LogMiner or GoldenGate](/comparison/confluent-oracle-cdc-logminer-or-goldengate) page sets that connector beside Confluent's separate XStream connector and the licence documents.
- The OSO CDC Connector, which uses LogMiner only, runs as one Connect task, and stops the task on a change it cannot decode. The [Oracle CDC to Kafka](/guides/oracle-cdc-to-kafka) guide is the end-to-end setup.

GoldenGate fits when you want Oracle's capture product, heterogeneous replication through trail files, or a Kafka handler that runs on data GoldenGate has already captured. The handler publishes from the trail. It does not remove the need for Extract, and Apache Kafka as a target still needs the Big Data Targets licence.

Striim and Qlik Replicate fit when you want a replication server that owns capture and writes to Kafka itself. They are not Kafka Connect source connectors. Striim's Kafka Writer documents exactly-once or at-least-once recovery. Qlik documents Kafka as a target endpoint with JSON or Avro messages and a topic per table or a single topic.

XStream fits when a client application, such as Debezium's XStream adapter, should receive logical change records from an outbound server. It is not a Kafka product. Using the APIs requires a GoldenGate licence.

## What licensing and cost considerations matter?

There are no prices on this page. Cost follows from which licence each path requires, and the vendor documents are the source for that. Confirm the current wording with Oracle, or with the other vendor, before you buy.

**GoldenGate and XStream.** Oracle's [Database Licensing Information](https://docs.oracle.com/en/database/oracle/oracle-database/26/dblic/Licensing-Information.html) states that XStream is licensed through Oracle GoldenGate and that you must purchase a licence for that product before using the XStream APIs. The same manual states that LogMiner is available with all Oracle AI Database offerings, and that a GoldenGate licence is required when LogMiner is used with GoldenGate supplemental logging turned on. Oracle defines that feature as `ENABLE_GOLDENGATE_REPLICATION` set to `TRUE`, and states that for on-premises capture a GoldenGate licence is then required whichever API consumes the redo, including LogMiner, XStream or a third-party tool. The [GoldenGate licensing](https://docs.oracle.com/en/database/goldengate/core/26/ogglc/oracle-goldengate.html) page states that a GoldenGate licence includes XStream, with Enterprise Edition as a prerequisite, and that the licence satisfies the database requirement when `ENABLE_GOLDENGATE_REPLICATION` is `TRUE`.

**Kafka as a GoldenGate target.** [GoldenGate for Big Data Targets](https://docs.oracle.com/en/database/goldengate/core/26/ogglc/oracle-goldengate-big-data-targets.html) includes Apache Kafka and Confluent Kafka among the target technologies. The licence includes targets only, one technology per licence, and does not include source capture. Streaming an existing trail to Kafka is the [Kafka Handler and the Kafka Connect Handler](https://docs.oracle.com/en/database/goldengate/big-data/26/gadbd/apache-kafka-target.html).

**Debezium.** The connector is Apache-2.0. The default adapter is LogMiner. Debezium's XStream section states that you must have a licence for the GoldenGate product to use the XStream API, and that installing GoldenGate is not required. Debezium also states that Oracle JSON types need `ENABLE_GOLDENGATE_REPLICATION` set to `TRUE`, which Oracle treats as GoldenGate supplemental logging.

**Confluent's Oracle CDC Source.** Confluent's overview states that this is a Premium connector subject to the Confluent enterprise licence and that it requires an additional subscription. The same page states that you can use it for a trial period of 30 days without a licence key, and that after 30 days you must purchase a connector subscription. That is the connector's own licence. It is separate from any Oracle licence. The [comparison of Confluent's LogMiner and XStream connectors](/comparison/confluent-oracle-cdc-logminer-or-goldengate) collects the Oracle documents next to Confluent's prerequisites.

**Striim and Qlik.** Striim's end user licence agreement grants a licence for the fees on an order form, and the software may require licence keys. Qlik's current help states that you must have a valid licence to work with Qlik Replicate, provided as a licence file and registered in the console. Neither page publishes a price.

**The OSO connector.** The connector is Apache-2.0. There is no connector licence key. Oracle licensing questions, GoldenGate and XStream are outside [enterprise support](/enterprise-support) for this connector. Support hours on that page are 08:00 to 18:00 UK time, Monday to Friday, excluding UK public holidays.

## How should you evaluate an alternative?

Work through the capture API, the runtime and the failure behaviour before you compare features.

- Match the capture API to the Oracle licence you hold, including whether `ENABLE_GOLDENGATE_REPLICATION` must be `TRUE`.
- Decide whether capture should run as a Kafka Connect connector or in a separate replication server. The [comparison](/comparison/) is the hub for that choice.
- Check the vendor's documented support for your topology: pluggable databases, Oracle Real Application Clusters, standby databases and the database version. For this connector, the [introduction](/#status) lists Real Application Clusters, Autonomous Database and standby capture as not available yet.
- Check the delivery guarantee in the release you will run. For this connector, exactly-once delivery is in release 0.1.6 when the worker and the connector are set for it, as described in [exactly-once delivery](/concepts/exactly-once).
- Check what happens when an archived log is missing or a change cannot be decoded. This connector stops the task with a typed error. Confluent's Oracle CDC Source overview states that the connector does not terminate if LogMiner reports corrupted redo blocks.
- Read the licence pages linked above before you buy. Oracle licensing questions are outside enterprise support for this connector.
- If you are leaving Debezium's LogMiner adapter, use the [migration from Debezium](/migration/from-debezium) guide, which keeps the existing topics and continues from a takeover SCN.
- Measure on your own database. The [comparison](/comparison/#measure-it-yourself) explains the harness this project publishes. It does not publish database benchmark figures.

<Faq
  items={[
    {
      question: 'Is there a free alternative to Oracle GoldenGate?',
      answer:
        'Yes. Debezium\'s Oracle connector and the OSO CDC Connector are published under the Apache-2.0 licence, and both can capture Oracle changes through LogMiner. Oracle includes LogMiner with database offerings unless GoldenGate supplemental logging is turned on. Debezium\'s XStream adapter still requires a GoldenGate licence. Sources: https://github.com/debezium/debezium/blob/main/LICENSE.txt and https://debezium.io/documentation/reference/stable/connectors/oracle.html and https://docs.oracle.com/en/database/oracle/oracle-database/26/dblic/Licensing-Information.html. The OSO statement is from this site\'s introduction.',
    },
    {
      question: 'Does Debezium replace GoldenGate?',
      answer:
        'No. Debezium is a Kafka Connect source connector that captures row changes, with LogMiner as the default adapter, and it is not Oracle\'s product for heterogeneous replication. Its XStream adapter requires a GoldenGate licence, and Oracle states that LogMiner is not intended for third-party replication in a production environment. Sources: https://debezium.io/documentation/reference/stable/connectors/oracle.html and https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html.',
    },
    {
      question: 'Can GoldenGate write to Kafka?',
      answer:
        'Yes. Oracle documents a Kafka Handler and a Kafka Connect Handler that stream change data from a GoldenGate trail to Kafka topics. Apache Kafka is one target of the separately licensed GoldenGate for Big Data Targets product, and that licence does not include source capture. Sources: https://docs.oracle.com/en/database/goldengate/big-data/26/gadbd/apache-kafka-target.html and https://docs.oracle.com/en/database/goldengate/core/26/ogglc/oracle-goldengate-big-data-targets.html.',
    },
    {
      question: 'Is XStream part of GoldenGate licensing?',
      answer:
        'Yes. Oracle\'s Database Licensing Information states that XStream is licensed through Oracle GoldenGate and that you must purchase that licence before using the XStream APIs. A GoldenGate licence includes XStream, and Oracle Database Enterprise Edition is a prerequisite. Sources: https://docs.oracle.com/en/database/oracle/oracle-database/26/dblic/Licensing-Information.html and https://docs.oracle.com/en/database/goldengate/core/26/ogglc/oracle-goldengate.html and https://docs.oracle.com/en/database/oracle/oracle-database/19/xstrm/introduction-to-xstream.html.',
    },
    {
      question: 'What does GoldenGate do better than LogMiner-based tools?',
      answer:
        'Oracle documents GoldenGate for capturing and delivering transactions, including between Oracle and non-Oracle databases, with Extract writing changes to trail files so capture and delivery can run independently. Oracle also states that LogMiner is intended as a debugging tool and is not intended for third-party replication in a production environment. Sources: https://docs.oracle.com/en/database/oracle/oracle-database/19/xstrm/introduction-to-xstream.html and https://docs.oracle.com/en/database/goldengate/core/26/coredoc/overview-components-data-replication-oracle-goldengate.html and https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html.',
    },
  ]}
/>

## Trademarks

This project is not affiliated with, endorsed by or sponsored by Oracle Corporation, Confluent, the Debezium project, Striim or Qlik. Oracle is a registered trademark of Oracle and/or its affiliates. Other names may be trademarks of their respective owners.
