---
title: Oracle CDC to Kafka with Kafka Connect
sidebar_label: Oracle Database change data capture to Kafka
description: Stream Oracle Database changes to Apache Kafka with a Kafka Connect source connector that reads committed inserts, updates and deletes through LogMiner.
---

import Faq from '@site/src/components/Faq';

# Oracle CDC to Kafka with Kafka Connect

Change data capture (CDC) turns every committed insert, update and delete in a database into an
event that other systems can consume. With Oracle Database as the source and Apache Kafka as the
destination, CDC feeds analytics platforms, search indexes, caches, microservices and replicas
without changing the applications that write to the database. This guide explains how CDC from
Oracle Database works, what makes it hard to get right, and how to run it with the OSO CDC
Connector.

## How do I stream Oracle changes to Kafka?

**Quick answer.** Enable ARCHIVELOG mode and supplemental logging, and create a mining user with the LogMiner grants. Deploy a Kafka Connect source connector that reads committed inserts, updates and deletes through Oracle LogMiner and writes each change to a Kafka topic. One such connector is the OSO CDC Connector, which uses LogMiner only and never reads redo files directly.

A minimal configuration for one table in one pluggable database is below. The connector class is
`sh.oso.connect.oracle.OracleCdcSourceConnector`. Each `cdc.*` name is a setting of this
connector. The [first connector](../getting-started/first-connector.md) guide explains where to
connect, how the table pattern is matched, and what happens at the first start. The
[configuration reference](../reference/configuration/index.md) lists every setting.

```json
{
  "name": "orders-cdc",
  "config": {
    "connector.class": "sh.oso.connect.oracle.OracleCdcSourceConnector",
    "cdc.database.host": "db.example.internal",
    "cdc.database.port": "1521",
    "cdc.database.service": "ORCLCDB",
    "cdc.database.user": "c##cdc",
    "cdc.database.password": "${file:/opt/kafka/secrets/oracle.properties:password}",
    "cdc.database.pdbs": "ORCLPDB1",
    "cdc.tables.include": "ORCLPDB1\\.APP\\.ORDERS",
    "cdc.topic.prefix": "app"
  }
}
```

The steps in [How do I run it?](#how-do-i-run-it) cover the database, a local lab, installation,
metrics and the runbooks.

## What are the ways to capture changes from Oracle Database?

| Approach | How it works | Trade-offs |
|---|---|---|
| Query polling | Select rows changed since the last poll, using a timestamp or version column | Misses deletes and intermediate states; needs the column on every table; adds query load |
| Triggers | Triggers copy each change to a shadow table that is then read | Adds work to every write transaction; schema changes need trigger changes |
| LogMiner | Reads the changes from the redo log through Oracle's `DBMS_LOGMNR` package | Included in all editions; needs ARCHIVELOG mode and supplemental logging; the connector must handle open transactions, redo retention and schema changes |
| XStream and GoldenGate | Oracle's replication products stream logical change records | Separately licensed ([Oracle XStream guide](https://docs.oracle.com/database/121/XSTRM/xstrm_intro.htm)) |
| Reading redo files directly | Third-party parsers of the binary redo format | Oracle states these are unsupported and may breach its licence terms ([Oracle blog](https://blogs.oracle.com/dataintegration/binary-log-readers)) |

LogMiner is the capture interface Oracle provides with the database at no extra cost, and it sees
every change, including deletes, without touching the applications. The OSO CDC Connector uses
LogMiner only and never reads redo files directly.

## How does LogMiner-based CDC work?

Every change Oracle makes is first written to the redo log. In ARCHIVELOG mode, filled online redo
logs are copied to archived logs before they are reused. LogMiner reads online and archived logs
and returns their contents as rows of `V$LOGMNR_CONTENTS`: one row per change, with the SQL that
redoes it, the transaction it belongs to, its System Change Number (SCN) and its position in the
redo.

With minimal supplemental logging, the redo also carries what LogMiner needs to rebuild rows; with
table-level ALL COLUMNS logging, an update or delete carries the whole row before the change, not
only the changed columns. See [supplemental logging](../database-setup/supplemental-logging.md).

A CDC connector on LogMiner mines the redo in steps, decodes each row into column values, groups
changes by transaction, and publishes a transaction's changes when its COMMIT arrives. The details
of how this connector does that are in [how capture works](../concepts/how-capture-works.md).

## What makes Oracle change data capture hard?

**Open transactions.** LogMiner returns changes before their transactions commit. A connector must
hold them until the COMMIT or ROLLBACK, and must be able to restart without losing the ones it held.
The usual answer, restarting from the start of the oldest open transaction, means a single long
transaction holds the restart point back for as long as it is open. This connector spills large
transactions to disk and journals long ones to Kafka, so the restart point moves on
([transaction buffer and journal](../concepts/transaction-buffer-and-journal.md)).

**Redo retention.** The connector needs every redo log from its restart point onwards. If archived
logs are deleted first, the changes in them are gone. A connector that quietly continues from a
later log loses data without anyone noticing; this one stops with a typed error and a
[runbook](../operations/runbooks/log-purged.md). Size retention for the longest outage plus lag
([redo sizing](../database-setup/redo-sizing.md)).

**Schema changes.** LogMiner decodes redo with the current data dictionary. Rows written before a
DDL on their table come back with generic column names when they are mined later. This connector
keeps schema versions per table and mines such ranges again with a dictionary stored in the redo
([schema and DDL](../concepts/schema-and-ddl.md)).

**Initial load.** A new consumer usually needs the rows that already exist, not just new changes,
and the two must line up. This connector reads tables in chunks, each as of one SCN, and publishes
each chunk in the right place among the streamed changes, so a consumer that keeps the latest
record per key converges to the table ([snapshots](../concepts/snapshots.md)).

**Delivery guarantees.** A Kafka Connect source connector delivers at least once by default: after
a crash some records can arrive twice. With Kafka Connect's exactly-once source support, this
connector commits Kafka transactions at Oracle commit boundaries, so a `read_committed` consumer
sees each change once and never part of an Oracle transaction
([exactly-once delivery](../concepts/exactly-once.md)).

**Large values.** CLOB, NCLOB and BLOB changes are written to the redo in pieces, and some values
are not in the redo at all. See [LOB columns](../reference/record-formats.md#lob-columns) for the
choices.

**Quiet databases.** If the captured tables do not change, a connector's committed position does
not move, and the redo it would need on restart grows old. This connector writes heartbeat records
that carry its position, without writing anything to the source database.

## How do I run it?

1. **Prepare the database.** ARCHIVELOG mode, supplemental logging, and a mining user with the
   LogMiner grants. `oracle-cdc-doctor setup-sql` writes the script for your DBA and
   `oracle-cdc-doctor check` confirms the result ([database setup](../database-setup/index.md)).
2. **Try it locally.** The [Docker Compose lab](../getting-started/quick-start.md) runs Oracle
   Database Free, Kafka and Kafka Connect with the connector on a laptop.
3. **Install the plugin** on your Connect workers ([installation](../getting-started/installation.md))
   and register a [first connector](../getting-started/first-connector.md).
4. **Monitor it.** Export the [metrics](../reference/metrics.md) to Prometheus and load the shipped
   [dashboard and alerts](../operations/dashboards-and-alerts.md). Read the
   [ops topic](../reference/ops-topic.md) to see what the connector did and why.
5. **Know the runbooks.** Every condition that stops the connector has a code and a
   [runbook](../operations/runbooks/index.md) that explains the recovery.

## Which Kafka Connect Oracle connectors exist?

Confluent's Oracle CDC Source connector uses Oracle LogMiner to read the database redo log,
querying `V$LOGMNR_CONTENTS`, and publishes each change as a record on an Apache Kafka topic. It
is built to spread table work across tasks, to snapshot partitioned tables in parallel, and to
write large object columns to separate topics. It supports Oracle Real Application Clusters. It is
a Confluent Premium connector under the Confluent enterprise licence and needs an additional
subscription. Confluent documents a trial period of 30 days without a licence key
([overview](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/overview.html)).

Debezium's Oracle connector captures row-level inserts, updates and deletes and writes them to one
Kafka topic per table. It is licensed under Apache-2.0. LogMiner is the default adapter. It can
also use XStream, which Debezium describes as a commercial component of Oracle GoldenGate, or
OpenLogReplicator, which reads redo and archive logs directly. Debezium documents compatibility
with a standalone database and with Oracle Real Application Clusters. Debezium states that the
XStream API requires a GoldenGate licence, and Oracle states the same for the XStream APIs
([Oracle connector](https://debezium.io/documentation/reference/stable/connectors/oracle.html),
[licence](https://github.com/debezium/debezium/blob/main/LICENSE.txt),
[Introduction to XStream](https://docs.oracle.com/en/database/oracle/oracle-database/19/xstrm/introduction-to-xstream.html)).

The OSO CDC Connector for Oracle Database is the connector this site documents. It is an
Apache-2.0 Kafka Connect source. It reads changes through LogMiner only, does not read redo files
directly, and does not use GoldenGate or XStream. Oracle Real Application Clusters is not
supported yet. See the [introduction](../intro.md) and the
[comparison](../comparison/index.md).

| Connector | Capture mechanism | Connector licence | Notes |
|---|---|---|---|
| Confluent Oracle CDC Source | Oracle LogMiner, through `V$LOGMNR_CONTENTS` | Confluent Premium connector, Confluent enterprise licence | Spreads table work across tasks, snapshots partitioned tables in parallel, writes large objects to separate topics, and supports Oracle Real Application Clusters |
| Debezium Oracle connector | LogMiner by default. XStream or OpenLogReplicator are alternatives | Apache-2.0 | Row-level changes on a topic per table. Standalone databases and Oracle Real Application Clusters. XStream requires a GoldenGate licence |
| OSO CDC Connector | Oracle LogMiner only | Apache-2.0 | Publishes committed changes to Kafka through LogMiner. Does not read redo files directly and does not use GoldenGate or XStream. Oracle Real Application Clusters is not supported yet |

If you are choosing between connectors, or moving from one, the [comparison](../comparison/index.md)
sets this connector beside Confluent's Oracle CDC Source and Debezium's Oracle connector, and the
[benchmark harness](../comparison/index.md#measure-it-yourself) lets you measure them on your own
database. [Confluent, LogMiner or GoldenGate](../comparison/confluent-oracle-cdc-logminer-or-goldengate.md)
answers which Oracle API the Confluent connectors use.

Sources, checked on 26 October 2026: Confluent's
[Oracle CDC Source overview](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/overview.html);
Debezium's [Oracle connector](https://debezium.io/documentation/reference/stable/connectors/oracle.html)
and [Apache-2.0 licence](https://github.com/debezium/debezium/blob/main/LICENSE.txt); Oracle's
[19c licensing information](https://docs.oracle.com/en/database/oracle/oracle-database/19/dblic/Licensing-Information.html)
and [Introduction to XStream](https://docs.oracle.com/en/database/oracle/oracle-database/19/xstrm/introduction-to-xstream.html).
Statements about the OSO connector come from this site.

<Faq
  items={[
    {
      question: 'Can Kafka Connect capture changes from Oracle Database?',
      answer:
        'Yes. A Kafka Connect source connector can read committed inserts, updates and deletes from Oracle Database through Oracle LogMiner and write them to Kafka topics. The database needs ARCHIVELOG mode, supplemental logging and a mining user with the LogMiner grants. Oracle states that LogMiner is available with all Oracle Database offerings, unless GoldenGate supplemental logging is turned on. Source: https://docs.oracle.com/en/database/oracle/oracle-database/19/dblic/Licensing-Information.html',
    },
    {
      question: 'Do I need GoldenGate to stream Oracle changes to Kafka?',
      answer:
        'No. Streaming through LogMiner does not require Oracle GoldenGate. Oracle states that LogMiner is available with all Oracle Database offerings, and that a GoldenGate licence is required when GoldenGate supplemental logging is turned on or when a client uses the XStream APIs. The OSO CDC Connector uses LogMiner only, and its documented setup does not set ENABLE_GOLDENGATE_REPLICATION. Sources: https://docs.oracle.com/en/database/oracle/oracle-database/19/dblic/Licensing-Information.html and https://docs.oracle.com/en/database/oracle/oracle-database/19/xstrm/introduction-to-xstream.html',
    },
    {
      question: 'Which Oracle versions does the OSO connector support?',
      answer:
        'Oracle Database 19c and later is required, as a single instance, CDB or non-CDB. The doctor refuses older releases. The test suites run against Oracle Database Free, release 23.26.3, a CDB with three PDBs. 19c and 21c are listed in the support policy for 1.0 and are not yet qualified.',
    },
    {
      question: 'Is the OSO connector open source?',
      answer:
        'Yes. The OSO CDC Connector for Oracle Database is published under the Apache-2.0 licence.',
    },
    {
      question: 'Does it work with Confluent Platform or plain Apache Kafka Connect?',
      answer:
        'Yes. The installation guide lists Apache Kafka Connect 3.6 and later and Confluent Platform 7.6 and later as supported platforms. The test suites run on Apache Kafka Connect. Strimzi and Amazon MSK Connect are also listed, and exactly-once delivery on Amazon MSK Connect has not been verified.',
    },
  ]}
/>
