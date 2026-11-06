---
title: Debezium Oracle vs OSO CDC Connector
sidebar_label: Debezium Oracle
description: A fair comparison of Debezium's Oracle connector and the OSO CDC Connector, covering LogMiner, licence, exactly-once delivery, the doctor CLI and migration.
---

import Faq from '@site/src/components/Faq';

# Debezium Oracle vs OSO CDC Connector

## Quick answer

The Debezium Oracle connector and the OSO CDC Connector are both Apache-2.0 Kafka Connect sources that can capture Oracle changes through LogMiner. Debezium also offers XStream and OpenLogReplicator adapters, and it documents Real Application Clusters. The OSO connector is LogMiner only, with a doctor CLI, takeover tools and optional exactly-once delivery when the Connect worker supports it.

The notes below were checked on 6 November 2026. Statements about Debezium come from the pages linked here. Statements about the OSO connector come from this site and describe release 0.1.6, which is the current release and a preview. This project is not affiliated with or endorsed by Oracle or the Debezium project.

## What does the Debezium Oracle connector do well?

Debezium's Oracle connector captures row-level inserts, updates and deletes, including tables added while the connector is running. You can limit capture to chosen schemas and tables. It installs as a Kafka Connect plug-in. The connector class is `io.debezium.connector.oracle.OracleConnector`.

The same reference documents a wide capture and operations surface:

- LogMiner is the default adapter. XStream and OpenLogReplicator are configuration choices, described in the next section.
- The first start takes a consistent snapshot. Incremental snapshots read historical data in chunks.
- A database schema history topic records DDL so the connector can rebuild table structure after a restart. Schema change events can also go to a topic named from `topic.prefix`. Transaction metadata is optional.
- Signals include `drop-transaction` and `log-pending-transactions`. A heartbeat topic can move offsets forward on a quiet database. Streaming metrics are exposed over JMX, including `MilliSecondsBehindSource`.
- An Oracle database may be a standalone instance or Oracle Real Application Clusters. Debezium states that the connector is compatible with both. The property `rac.nodes` is required for a RAC deployment.

The Debezium repository publishes the Apache License, Version 2.0.

Sources: [Debezium Oracle connector](https://debezium.io/documentation/reference/stable/connectors/oracle.html) and the [Apache-2.0 licence](https://github.com/debezium/debezium/blob/main/LICENSE.txt).

| | Debezium Oracle connector | OSO CDC Connector |
|---|---|---|
| Capture method | LogMiner by default (`logminer`, or `logminer_unbuffered` for committed changes only). XStream (`xstream`) and OpenLogReplicator (`olr`) are alternatives. | Oracle LogMiner only. It does not read redo files directly and does not use GoldenGate or XStream. |
| Connector class | `io.debezium.connector.oracle.OracleConnector` | `sh.oso.connect.oracle.OracleCdcSourceConnector` |
| Licence | Apache-2.0 | Apache-2.0 |
| Kafka Connect based | Yes. A Kafka Connect source connector. | Yes. A Kafka Connect source connector. It runs one task. |
| Oracle RAC | Compatible with a standalone instance and with Oracle RAC. `rac.nodes` is required for a RAC deployment. | Not available yet. Capture is a single instance. More than one enabled redo thread is refused at start. |
| Exactly-once | At least once by default. Exactly-once through Kafka Connect when every worker sets `exactly.once.source.support=enabled` and the connector sets `exactly.once.support=required`. `transaction.boundary` stays `poll`. | At least once by default. Exactly-once when every worker sets `exactly.once.source.support=enabled`, the connector sets `exactly.once.support=required`, and `transaction.boundary` is `connector`. |
| Operational tooling | In-memory LogMiner transaction buffer by default, with optional Infinispan persistence to disk. Signals, a heartbeat topic and JMX metrics. A processing failure stops the connector, or skips the event when configured to do so. | `oracle-cdc-doctor`, an ops topic, a dead-letter topic, a transaction buffer that spills to disk, and a durable journal topic. Migration tools translate a Debezium configuration, take over from an SCN and verify the cutover. |

Each cell is taken from the sources linked in the sections below. A fact that is not on those pages is left out.

## How do capture methods differ?

Debezium selects the adapter with `database.connection.adapter`.

- `logminer` is the default. The connector receives committed and in-flight changes, and it buffers events until the matching commit or rollback. Debezium states that LogMiner reads redo and archive logs and streams the raw changes.
- `logminer_unbuffered` asks LogMiner for committed changes only. The connector then has no internal transaction buffer. Debezium states that this mode places more processing and memory load on the source database.
- `olr` uses OpenLogReplicator. Debezium states that OpenLogReplicator reads Oracle changes directly from the redo and archive logs. The [comparison hub](./index.md) records Oracle's statement on binary log readers next to that description.
- `xstream` uses Oracle XStream, which Debezium describes as a commercial component of Oracle GoldenGate.

The OSO CDC Connector reads changes through Oracle LogMiner only. It does not read redo or archive files directly, and it does not use GoldenGate or XStream. [How capture works](../concepts/how-capture-works.md) describes one task and one LogMiner session, and states that nothing is written to the source database: no flush table, no heartbeat table and no signal table. Debezium documents a LogMiner flush table (`log.mining.flush.table.name`, default `LOG_MINING_FLUSH`) and an optional `heartbeat.action.query` that inserts or updates a row. The [Oracle CDC to Kafka](../guides/oracle-cdc-to-kafka.md) guide walks through LogMiner capture with this connector. The [Confluent, LogMiner or GoldenGate](./confluent-oracle-cdc-logminer-or-goldengate.md) page is the matching comparison for Confluent's connectors.

## How do licensing and Oracle requirements compare?

Both connectors are open source under the Apache-2.0 licence. Debezium's text is the [repository licence](https://github.com/debezium/debezium/blob/main/LICENSE.txt). The OSO statement is the [introduction](../intro.md) on this site.

Oracle's licensing documents, read on 6 November 2026, separate LogMiner from XStream.

- The [19c licensing information](https://docs.oracle.com/en/database/oracle/oracle-database/19/dblic/Licensing-Information.html) states that LogMiner is available with all Oracle Database offerings. When LogMiner is used with GoldenGate supplemental logging turned on, a GoldenGate licence is required.
- The [current licensing information](https://docs.oracle.com/en/database/oracle/oracle-database/26/dblic/Licensing-Information.html) states the same rule for all Oracle AI Database offerings.
- Both pages define GoldenGate supplemental logging as `ENABLE_GOLDENGATE_REPLICATION` set to `TRUE`. For on-premises capture, a GoldenGate licence is then required whichever API consumes the redo, including LogMiner, XStream or a third-party tool.
- The same pages state that XStream is licensed through Oracle GoldenGate and that you must purchase that licence before using the XStream APIs. The [Introduction to XStream](https://docs.oracle.com/en/database/oracle/oracle-database/19/xstrm/introduction-to-xstream.html) states the same requirement. A [GoldenGate licence](https://docs.oracle.com/en/database/goldengate/core/26/ogglc/oracle-goldengate.html) includes a licence for XStream. Oracle Database Enterprise Edition is a prerequisite to license and use XStream.

Debezium's preparation for LogMiner puts the database in ARCHIVELOG mode and enables supplemental logging. That preparation does not set `ENABLE_GOLDENGATE_REPLICATION`. Debezium does state a separate case: capturing `JSON` columns with the LogMiner adapter requires that parameter set to `TRUE`, and Debezium states that Oracle support for `JSON` is a premium feature available through a GoldenGate licence.

The OSO [database setup](../database-setup/index.md) asks for ARCHIVELOG mode, minimal supplemental logging and supplemental logging on captured tables. It does not set `ENABLE_GOLDENGATE_REPLICATION`. Doctor rule DOC-5 treats `JSON` columns as a blocking finding, so this connector does not capture them by turning that parameter on.

Oracle's [LogMiner guide](https://docs.oracle.com/en/database/oracle/oracle-database/26/sutil/oracle-logminer-utility.html) states that LogMiner is intended as a debugging tool and is not intended for third-party replication in a production environment. This page reports that sentence. It is not an Oracle licence ruling. Oracle licensing questions are outside the scope of [enterprise support](../enterprise-support.md) for the OSO connector.

## How do delivery guarantees and operational tooling compare?

Debezium provides at-least-once delivery: no change is missed, and a record can be delivered more than once. The Oracle connector is one of the source connectors that can take part in Kafka Connect exactly-once delivery. Kafka must run distributed, on Kafka Connect 3.3.0 or later. Every worker needs `exactly.once.source.support=enabled`, and the connector needs `exactly.once.support=required`. For every Debezium source connector, `transaction.boundary` must be `poll`, which is already the default. Debezium states that it does not add its own deduplication layer, and that open issues in the Kafka transaction protocol may affect that guarantee.

On the default LogMiner adapter, Debezium keeps an in-memory buffer of running transactions (`log.mining.buffer.type=memory`). That buffer is not kept across a restart. `infinispan_embedded` and `infinispan_remote` persist the buffer to disk. `log.mining.transaction.retention.ms` defaults to `0`, which retains a transaction until commit or rollback. A positive value discards the whole transaction, and the connector emits no messages for it. `event.processing.failure.handling.mode` defaults to `fail`, which stops the connector. `warn` and `skip` skip the event.

The OSO connector also delivers at least once unless exactly-once is turned on. With `exactly.once.source.support=enabled` on the worker, in distributed mode, `exactly.once.support=required`, and `transaction.boundary=connector`, the connector decides where each Kafka transaction ends. A consumer with `isolation.level=read_committed` then sees each change once, and never part of an Oracle transaction. A Kafka transaction ends after the last record of an Oracle transaction, or after a heartbeat, ops or dead-letter record written between Oracle transactions. The bounds are `cdc.eos.batch.max.records`, `cdc.eos.batch.max.bytes` and `cdc.eos.batch.max.ms`. See [exactly-once delivery](../concepts/exactly-once.md).

Open transactions stay in a buffer until LogMiner reports the commit. The buffer spills large transactions to disk, and those spill files are removed after a restart. A transaction open longer than `cdc.txjournal.threshold.ms` (default five minutes), or holding more than `cdc.txjournal.threshold.events` changes (default 100,000), is written to a durable journal topic. [Transaction buffer and journal](../concepts/transaction-buffer-and-journal.md) describes both. The default age limit is unlimited (`cdc.transaction.max.age.ms=-1`). When a limit is set, `cdc.transaction.max.age.action` defaults to `fail`, which stops the task. `discard` drops the transaction after writing its details to the [ops topic](../reference/ops-topic.md) and the dead-letter topic. `cdc.on.decode.error` defaults to `fail`. `dlq` routes an undecodable row to the dead-letter topic with an ops event. The configuration reference states that corruption and a missing SCN always stop the task.

`oracle-cdc-doctor` checks the database named in a connector configuration, writes the setup SQL, profiles redo, sizes archive retention and explains lag. The rules and exit codes are on the [doctor](../operations/doctor.md) page. Rule DOC-13 reports Oracle RAC, more than one enabled redo thread, as a blocking finding, and the task refuses that shape at start.

## When should you choose Debezium, and when OSO?

Choose the Debezium Oracle connector when the documented adapter or topology is the one you need.

- You want XStream or OpenLogReplicator, not only LogMiner.
- You run Oracle Real Application Clusters. Debezium documents that compatibility and the `rac.nodes` property. This connector does not support RAC yet.
- You want Debezium's schema change topic or transaction metadata records. The [status table](../intro.md#status) lists both as not available yet on the OSO connector.
- You want to keep running the Debezium connector you already operate, with its snapshot modes, signals and JMX metrics.

Choose the OSO connector when you are on a single instance and LogMiner is the capture path you want.

- Release 0.1.6 reads through LogMiner only, writes a Debezium-compatible envelope by default, and does not read redo files directly.
- You want `oracle-cdc-doctor` before cutover, an ops topic, and a dead-letter topic for a row the connector could not decode when `cdc.on.decode.error` is `dlq`. The default is to stop the task.
- You want exactly-once delivery aligned to Oracle commit boundaries, which needs `transaction.boundary=connector` and a worker that supports exactly-once.
- You are leaving the Debezium LogMiner adapter and want the migration tools in the next section.

Oracle RAC, Autonomous Database and standby capture are not available yet. Production support for this connector is the [enterprise support](../enterprise-support.md) subscription. GitHub issues and discussions are best effort, with no commitment. The support page is the source of truth for those commitments.

## Can I migrate from Debezium to OSO?

Yes, from the LogMiner adapters, using the [migration from Debezium](../migration/from-debezium.md) guide and three of the [migration tools](../migration/tools.md).

- `migrate_from_debezium.py` rewrites the configuration as `cdc.*` properties and reports every source property as mapped, mapped with a change, dropped or manual.
- `takeover_scn.py` reads the stopped Debezium offset and writes `cdc.start.scn`, with `cdc.snapshot.mode=none`, so the new connector starts at that SCN.
- `verify_cutover.py` compares the database at a check SCN with the topics and writes an evidence file.

The new connector class is `sh.oso.connect.oracle.OracleCdcSourceConnector`. Table topic names stay: `cdc.topic.template` keeps Debezium's prefix, schema and table names, and `cdc.output.format` stays `debezium`. The schema change topic does not stay. This connector keeps schema versions on its own topic.

The takeover uses the LogMiner offset field `scn`. Debezium's XStream and OpenLogReplicator offsets carry no position the tool can use. The database must be a single instance, and the archived logs from that SCN must still exist. No committed change is skipped. Changes committed after the takeover SCN, up to Debezium's last commit, are delivered again.

<Faq
  items={[
    {
      question: 'Is the Debezium Oracle connector open source?',
      answer:
        'Yes. The Debezium repository publishes the Apache License, Version 2.0. The Oracle connector is a Kafka Connect plug-in, and its class is io.debezium.connector.oracle.OracleConnector. Sources: https://github.com/debezium/debezium/blob/main/LICENSE.txt and https://debezium.io/documentation/reference/stable/connectors/oracle.html',
    },
    {
      question: 'Does Debezium use LogMiner or GoldenGate for Oracle?',
      answer:
        'It uses LogMiner by default, and it can use other adapters. database.connection.adapter defaults to logminer. logminer_unbuffered streams committed changes only. OpenLogReplicator, set with olr, reads redo and archive logs directly. XStream, set with xstream, is a commercial component of Oracle GoldenGate, and using the XStream APIs requires a GoldenGate licence. Sources: https://debezium.io/documentation/reference/stable/connectors/oracle.html and https://docs.oracle.com/en/database/oracle/oracle-database/19/xstrm/introduction-to-xstream.html and https://docs.oracle.com/en/database/oracle/oracle-database/19/dblic/Licensing-Information.html',
    },
    {
      question: 'How does the OSO connector differ from Debezium for Oracle CDC?',
      answer:
        'The OSO connector reads changes through Oracle LogMiner only and does not read redo files directly. It does not offer the XStream or OpenLogReplicator adapters. Release 0.1.6 includes oracle-cdc-doctor, an ops topic, a transaction journal and migration tools, and exactly-once delivery when the Connect worker is set for it with transaction.boundary set to connector. Oracle RAC is not available yet. Debezium documents several adapters and compatibility with Oracle RAC. Sources: this site introduction at https://kafkacdcconnector.com/ and https://debezium.io/documentation/reference/stable/connectors/oracle.html',
    },
    {
      question: 'Can I keep my Kafka topics when moving from Debezium to OSO?',
      answer:
        'Yes, for the table topics. The migration keeps Debezium topic names and writes the Debezium-compatible envelope. The schema change topic does not stay. The new connector starts at a takeover SCN taken from the Debezium offset, so committed changes are not skipped, and changes between that SCN and the last Debezium commit are delivered again. The takeover applies to the LogMiner adapters only. Source: https://kafkacdcconnector.com/migration/from-debezium',
    },
    {
      question: 'Do I need a GoldenGate licence for either connector when using LogMiner?',
      answer:
        'Not for ordinary LogMiner use. Oracle states that LogMiner is available with all Oracle Database offerings, and with all Oracle AI Database offerings, unless GoldenGate supplemental logging is turned on. That feature is ENABLE_GOLDENGATE_REPLICATION set to TRUE, and an on-premises GoldenGate licence is then required whichever API consumes the redo, including LogMiner. Debezium prepares LogMiner with ARCHIVELOG mode and supplemental logging, and states that JSON capture on the LogMiner adapter requires that parameter and a GoldenGate licence. The OSO connector setup does not set the parameter. This is not an Oracle licence ruling. Sources: https://docs.oracle.com/en/database/oracle/oracle-database/19/dblic/Licensing-Information.html and https://docs.oracle.com/en/database/oracle/oracle-database/26/dblic/Licensing-Information.html and https://debezium.io/documentation/reference/stable/connectors/oracle.html',
    },
    {
      question: 'Does the OSO connector support Oracle RAC yet?',
      answer:
        'No. The status table lists Oracle RAC as not available yet, and the connector captures a single instance. Doctor rule DOC-13 treats more than one enabled redo thread as a blocking finding, and the task refuses it at start. Debezium documents compatibility with a standalone instance and with Oracle RAC. Sources: https://kafkacdcconnector.com/ and https://kafkacdcconnector.com/operations/doctor and https://debezium.io/documentation/reference/stable/connectors/oracle.html',
    },
  ]}
/>

## Trademarks

This project is not affiliated with, endorsed by or sponsored by Oracle Corporation or the Debezium project. Oracle is a registered trademark of Oracle and/or its affiliates. Debezium and other names may be trademarks of their respective owners. The repository and plugin names use "oracle" descriptively, as other connectors for Oracle Database do.
