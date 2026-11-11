---
title: Oracle Database Replication Methods Compared
title_meta: Oracle Database Replication Methods
description: "Data Guard, GoldenGate, materialised views, XStream, LogMiner-based CDC and AWS DMS: when you need a physical standby versus an event stream into Kafka."
slug: oracle-database-replication-methods
date: 2026-11-11
authors: [oso]
tags: [replication, data-guard, kafka]
---

import Faq from '@site/src/components/Faq';

**Quick answer.** Oracle replication either keeps a standby for recovery or ships committed changes to another system. Data Guard applies redo to a physical standby. GoldenGate, materialised views and XStream move logical changes, including to non-Oracle databases. LogMiner-based CDC and AWS DMS publish an event stream to Kafka. Choose a standby to recover the same database, and CDC when consumers need events.

{/* truncate */}

The notes below were checked against the vendor pages linked here on 11 November 2026. Statements about the OSO CDC Connector come from this site and describe release 0.1.6. This project is not affiliated with or endorsed by Oracle or Amazon Web Services. This page reports those documents. It is not an Oracle licence ruling. Oracle licensing questions are outside [enterprise support](/enterprise-support).

The [comparison](/comparison/) lists the Oracle replication tools that publish changes to Kafka. This post stays with the methods, and with the choice between a physical standby and an event stream.

## What are the ways to replicate an Oracle database?

Oracle database replication is several methods that copy data for different jobs. Some keep a second Oracle database ready to take over. Some deliver rows to another database product. Some record each committed change so a downstream system, often Apache Kafka, can consume it. The [introduction](/) describes the connector this site documents, which is one Kafka option among those methods.

| Method | What it produces | When it fits |
|---|---|---|
| Data Guard physical standby | A block-for-block replica, maintained by Redo Apply | You need the same database back after an outage |
| Data Guard logical standby | SQL rebuilt from redo and applied on an open database | You need a standby whose storage and indexes can differ |
| GoldenGate | Committed transactions, stored in trail files and applied by Replicat | You need another database, a subset of tables, or writes on both sides |
| Read-only materialised views | A query result, refreshed from a master table | You need a scheduled read-only subset inside Oracle |
| XStream | Logical change records sent to a client application | Your own program should receive changes in commit order |
| LogMiner | Redo, queried through SQL | A CDC connector should turn committed changes into events |
| AWS DMS | A full load plus ongoing changes into a supported target | You want a managed copy, including into PostgreSQL or Kafka |

### What does Data Guard do well?

Oracle Data Guard keeps one or more standby databases so a primary can survive an outage. Oracle states that Data Guard is included in Enterprise Edition. A physical standby is an exact, block-for-block copy. Redo Apply maintains it by applying redo with the database recovery mechanisms, and it supports the data types, DDL and DML the primary supports. A logical standby starts as a copy and can later differ in structure. SQL Apply turns received redo into SQL and executes it. The logical standby stays open, and the tables SQL Apply maintains are available only for read-only operations while they are updated. A snapshot standby receives and archives redo without applying it, so you can open a temporary read/write copy for a test and later convert it back, discarding local updates and applying the redo the primary sent.

The [licensing table](https://docs.oracle.com/en/database/oracle/oracle-database/26/dblic/Licensing-Information.html) includes Redo Apply and SQL Apply for Enterprise Edition, and not for Standard Edition 2 or Oracle AI Database Free. The same table marks Oracle Active Data Guard as an extra-cost option for Enterprise Edition. Active Data Guard is what lets Redo Apply stay active while the physical standby is open read-only, which Oracle calls real-time query. Far Sync and real-time cascading standbys require that option on Enterprise Edition. A GoldenGate licence includes Active Data Guard, and Enterprise Edition is a prerequisite for that inclusion. Oracle's availability guide describes Active Data Guard as the way to offload read-only work to a standby that is still applying changes.

### What does GoldenGate do well?

Oracle GoldenGate captures, transforms and delivers committed transactions across heterogeneous systems. Extract reads the source redo or transaction logs and writes trail files. A distribution path moves the trail. Replicat applies the changes, so capture and delivery can run independently. Oracle documents this for migrations, reporting, and for bi-directional replication between two or more databases that both accept writes. Because that movement is asynchronous, Oracle also documents conflict detection and resolution when the same row can change in more than one database. A logical standby does not reshape rows. Oracle says a logical standby does not replicate a subset of columns or add columns, and that GoldenGate is the product for that kind of reporting copy.

GoldenGate is licensed separately from Oracle Database. A GoldenGate licence includes XStream and Active Data Guard, subject to an Enterprise Edition prerequisite, and it satisfies the database requirement when `ENABLE_GOLDENGATE_REPLICATION` is `TRUE`. Publishing to Apache Kafka is not included in that core licence. The restricted Kafka use included with GoldenGate is only for communication to Oracle Stream Analytics. Apache Kafka as a general target is one technology of the separately licensed GoldenGate for Big Data Targets product. That licence covers targets only, one technology per licence, and does not include source capture. The [GoldenGate alternatives](/blog/oracle-goldengate-alternatives) post collects the Kafka handler notes.

Oracle GoldenGate for PostgreSQL documents capture and delivery of initial load and transactional DML into PostgreSQL, including data that came from another database GoldenGate supports. The same page states that DDL replication is not supported for PostgreSQL.

### What do materialised views do well?

A read-only materialised view holds a complete or partial copy of a master table at one point in time. Applications can query the local copy, and they send changes to the master. A refresh brings in what changed after the previous refresh. A fast refresh uses a materialised view log and applies the changed rows. A complete refresh rebuilds the view. Views in a refresh group refresh together, so they share one transactionally consistent point in time. That is a good fit for a scheduled read-only subset, including a subset of rows or columns, on another Oracle database.

It is not a continuous redo stream. The copy matches the master as of the last refresh. Oracle also desupported Advanced Replication in its entirety starting in Oracle Database 12c release 2 (12.2). That desupport covers multimaster replication, updatable materialised views and deployment templates. Read-only materialised views remain supported as basic replication. Oracle recommends GoldenGate to replace Advanced Replication.

### What does XStream provide?

XStream is a set of database components and APIs. XStream Out reads data manipulation and data definition changes from the redo log and sends logical change records to a client, in committed transaction order. XStream In lets a client send change records into an Oracle database for apply. Oracle describes this for independent software vendors who need a programmatic feed, including replication between Oracle and non-Oracle data stores, and states that GoldenGate is the complete product for that replication. Using the XStream APIs requires a GoldenGate licence, and Enterprise Edition is a prerequisite. XStream is not itself a Kafka product.

### What does LogMiner provide?

LogMiner queries online and archived redo log files through SQL. Oracle includes it with all Oracle AI Database offerings. You must enable at least minimal supplemental logging before the redo is useful to LogMiner. When GoldenGate supplemental logging is on, which Oracle defines as `ENABLE_GOLDENGATE_REPLICATION` set to `TRUE`, a GoldenGate licence is required whichever API consumes the redo, including LogMiner, XStream or a third-party tool.

Oracle also states that LogMiner is intended as a debugging tool and is not intended for third-party replication in a production environment. That sentence is why Data Guard and GoldenGate remain the products Oracle documents for a standby and for heterogeneous replication. It is not, by itself, a licence ruling for any third-party connector. Connectors that publish to Kafka still use this SQL interface. The [comparison](/comparison/) lists those tools. This connector is one of them, and the next section says what it does and what it does not do.

### What does AWS DMS replicate?

AWS Database Migration Service can use an Oracle database as a source and migrate to any target AWS DMS supports. The target list includes PostgreSQL and Apache Kafka, both Amazon MSK and self-managed Kafka. With Kafka as the target, AWS DMS publishes each source record as JSON. For ongoing replication from Oracle, AWS DMS uses LogMiner by default. Its other method, Binary Reader, reads and parses the raw redo log files directly. That work runs on an AWS DMS replication instance, which publishes to the Kafka cluster configured as the target.

## Physical vs logical replication

Physical replication and logical replication keep different kinds of copy. Data Guard and GoldenGate both start from redo. A materialised view starts from a refresh of its master table.

### When is a physical standby the right copy?

Choose a physical standby when the outcome you want is the same database. Redo Apply is media recovery, so the standby matches the primary block for block, including data types and DDL, and the primary and the physical standby can reverse roles. Oracle describes switchover and failover for planned and unplanned outages, and states that storage-level physical corruptions on the primary are not propagated to a physical standby. Oracle also states that a primary and a physical standby are physically exact copies, so RMAN backups are interchangeable.

A physical standby can be open read-only. Redo Apply continues during that read-only access only with Active Data Guard. Without that option, a physical standby is still the disaster-recovery copy. It is not an event stream, and it is not a PostgreSQL or Kafka target.

### When is logical replication the right copy?

Choose logical replication when the copy should not be a block-for-block twin. SQL Apply keeps an Oracle standby that can use different indexes, partitioning and storage, and that can support a rolling upgrade. GoldenGate goes further: another database product, a subset of tables, transformations, and bi-directional writes, with conflict handling because both sides can change the same row. Read-only materialised views copy a subset on a refresh schedule. XStream hands logical change records to a client. Change data capture publishes each committed change as an event and leaves apply to the consumer.

Logical replication does not give you a physical standby. A Kafka topic is not a database you can open if the primary fails. If that is the requirement, Data Guard is the method. If consumers need events, CDC is the method.

## Real-time replication to Kafka with CDC

Oracle real time replication means committed changes leave the source as they happen, rather than on a refresh schedule. In Data Guard, real-time apply does that for a standby: apply services can apply redo as the standby redo log is filled, when the standby has a standby redo log and is in ARCHIVELOG mode. By default, apply waits until that log is archived. A materialised view waits until the next refresh.

For Kafka, real time means a change becomes a record after its transaction commits. The record is an event, not a recovered data file.

### How does a LogMiner connector publish events?

A Kafka Connect source reads committed inserts, updates and deletes through LogMiner and writes one record per change. The [Oracle CDC to Kafka](/guides/oracle-cdc-to-kafka) guide walks through that path. LogMiner returns changes before their transactions commit. This connector keeps them in its own transaction buffer and publishes a transaction when the commit arrives, so consumers see committed data only. [How capture works](/concepts/how-capture-works) describes the mining steps, the redo byte address cursor and that rule.

The OSO CDC Connector is the Kafka event-stream option on this site. It is an Apache-2.0 Kafka Connect source, class `sh.oso.connect.oracle.OracleCdcSourceConnector`, and it runs as one task. It uses LogMiner only. It does not read redo files directly, and it does not use GoldenGate or XStream. The documented [database setup](/database-setup) asks for ARCHIVELOG mode and supplemental logging. It does not set `ENABLE_GOLDENGATE_REPLICATION`. Release 0.1.6 is a preview. Exactly-once delivery is available when the Connect worker and the connector are set for it, as described in [exactly-once delivery](/concepts/exactly-once). A condition the engine cannot decode, including missing redo, stops the task rather than skipping the change. The [status table](/#status) lists Oracle Real Application Clusters, Autonomous Database and standby capture as not available yet.

The connector is not a physical standby and it is not a bi-directional replicator. It does not apply redo, it does not open a second database, and it does not write changes back to Oracle. Pair it with Data Guard when you also need failover. Use GoldenGate when you need Oracle's product for applying changes to another database or for writes on both sides.

### How do GoldenGate and AWS DMS reach Kafka?

GoldenGate reaches Kafka from a trail that Extract has already written. Apache Kafka is a Big Data Targets technology, under the separate target licence described above, and that licence does not include source capture. AWS DMS reaches Kafka by publishing JSON from its replication instance. Ongoing Oracle capture there is LogMiner unless you select Binary Reader, which reads the redo files directly. This connector does not use that method.

Neither path is a physical standby. Both can sit beside Data Guard: the standby protects the database, and the event stream feeds consumers.

## How do you choose?

Match the method to the copy you need, then pick a product that implements that method.

- **The same database after an outage.** Use a Data Guard physical standby. Add Active Data Guard when the standby must stay open read-only while Redo Apply runs, or when you need a feature the licensing table places under that option, such as Far Sync.
- **An Oracle standby with different indexes or a rolling upgrade, without reshaping rows.** Use a Data Guard logical standby. Oracle points to GoldenGate when the copy must drop columns, add columns or leave Oracle.
- **Another database, a subset, or writes on both sides.** Use GoldenGate, including delivery into PostgreSQL where that page documents DML. Budget for a separate Big Data Targets licence if Apache Kafka is the target. The [alternatives post](/blog/oracle-goldengate-alternatives) is the Kafka-specific note.
- **A read-only subset on a schedule, still in Oracle.** Use read-only materialised views. Do not plan a new multimaster or updatable materialised view deployment. Oracle has desupported Advanced Replication and recommends GoldenGate instead.
- **A client that receives logical change records from the database.** Use XStream, and purchase the GoldenGate licence Oracle requires before using the APIs.
- **An event stream into Kafka.** Use CDC. On Kafka Connect, that is a LogMiner source connector. The [comparison](/comparison/) sets the connectors side by side, and the [Oracle CDC to Kafka](/guides/oracle-cdc-to-kafka) guide is the setup for this one. This connector is that event stream. It is not the standby in the first bullet, and it is not the bi-directional replicator in the GoldenGate bullet.
- **A managed service into PostgreSQL or JSON in Kafka.** Use AWS DMS. Read its LogMiner and Binary Reader notes before you choose how it reads redo. Binary Reader reads redo files directly. This connector does not.

Read the licence pages before you buy. Confirm the current wording with Oracle, or with AWS, for the edition and the options you will run. This page does not decide an Oracle licence question.

Sources, checked on 11 November 2026:

- Data Guard: [getting started](https://docs.oracle.com/en/database/oracle/oracle-database/26/sbydb/getting-started-with-oracle-data-guard.html), [apply services](https://docs.oracle.com/en/database/oracle/oracle-database/26/sbydb/oracle-data-guard-redo-apply-services.html), [availability overview](https://docs.oracle.com/en/database/oracle/oracle-database/26/haovw/oracle-data-guard.html) and [when to use Active Data Guard or GoldenGate](https://docs.oracle.com/en/database/oracle/oracle-database/26/haovw/best-practice-oracle-active-data-guard-and-oracle-goldengate.html).
- Licensing: [Database Licensing Information](https://docs.oracle.com/en/database/oracle/oracle-database/26/dblic/Licensing-Information.html), [GoldenGate licensing](https://docs.oracle.com/en/database/goldengate/core/26/ogglc/oracle-goldengate.html) and [Big Data targets](https://docs.oracle.com/en/database/goldengate/core/26/ogglc/oracle-goldengate-big-data-targets.html).
- GoldenGate behaviour: [FAQ](https://docs.oracle.com/en/database/goldengate/core/26/coredoc/ogg-frequently-asked-questions.html), [replication components](https://docs.oracle.com/en/database/goldengate/core/26/coredoc/overview-components-data-replication-oracle-goldengate.html), [bi-directional replication](https://docs.oracle.com/en/database/goldengate/core/26/ggsol/active-active.html) and [PostgreSQL](https://docs.oracle.com/en/database/goldengate/core/26/coredoc/prepare-postgresql.html).
- Materialised views and Advanced Replication: [read-only materialised view concepts](https://docs.oracle.com/en/database/oracle/oracle-database/26/admin/read-only-materialized-view-concepts.html) and [desupported features](https://docs.oracle.com/en/database/oracle/oracle-database/26/upgrd/oracle-database-changes-deprecations-desupports.html).
- XStream and LogMiner: [Introduction to XStream](https://docs.oracle.com/en/database/oracle/oracle-database/19/xstrm/introduction-to-xstream.html) and [LogMiner](https://docs.oracle.com/en/database/oracle/oracle-database/26/sutil/oracle-logminer-utility.html).
- AWS DMS: [Oracle source](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.Oracle.html), [targets](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Introduction.Targets.html) and [Kafka target](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.Kafka.html).
- OSO CDC Connector: the [introduction](/), [how capture works](/concepts/how-capture-works), [database setup](/database-setup) and [exactly-once delivery](/concepts/exactly-once).

<Faq
  items={[
    {
      question: 'What is the difference between Data Guard and GoldenGate?',
      answer:
        'Data Guard maintains a standby copy of an Oracle database for availability and disaster recovery. GoldenGate captures committed transactions and delivers them to another system, including a non-Oracle database. A physical standby is a block-for-block copy kept by Redo Apply. GoldenGate writes changes to trail files and can replicate a subset or run bi-directional replication between databases that both accept writes. Sources: https://docs.oracle.com/en/database/oracle/oracle-database/26/sbydb/getting-started-with-oracle-data-guard.html and https://docs.oracle.com/en/database/goldengate/core/26/coredoc/ogg-frequently-asked-questions.html and https://docs.oracle.com/en/database/goldengate/core/26/ggsol/active-active.html.',
    },
    {
      question: 'Can Oracle replicate to PostgreSQL or Kafka?',
      answer:
        'Yes. Oracle GoldenGate documents delivery of transactional DML into PostgreSQL, and a separate Big Data Targets licence includes Apache Kafka as a target. AWS DMS documents Oracle as a source and both PostgreSQL and Apache Kafka as targets. A Data Guard physical standby remains an Oracle database. The OSO CDC Connector publishes committed changes to Kafka through LogMiner and does not apply them to PostgreSQL. Sources: https://docs.oracle.com/en/database/goldengate/core/26/coredoc/prepare-postgresql.html and https://docs.oracle.com/en/database/goldengate/core/26/ogglc/oracle-goldengate-big-data-targets.html and https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Introduction.Targets.html.',
    },
    {
      question: 'Is CDC the same as replication?',
      answer:
        'No. Replication maintains another copy of the data, such as a Data Guard standby or a GoldenGate target database. Change data capture records each committed insert, update and delete so another system can consume it as an event. A stream into Kafka can feed a replica, and the stream itself is not a standby database.',
    },
    {
      question: 'What is real-time replication in Oracle?',
      answer:
        'It means committed changes are shipped as they happen, rather than copied on a schedule. Data Guard real-time apply applies redo as the standby redo log is filled, when the standby has a standby redo log and is in ARCHIVELOG mode. A read-only materialised view matches its master only as of the last refresh. A LogMiner CDC connector publishes a change after the transaction commits. Sources: https://docs.oracle.com/en/database/oracle/oracle-database/26/sbydb/oracle-data-guard-redo-apply-services.html and https://docs.oracle.com/en/database/oracle/oracle-database/26/admin/read-only-materialized-view-concepts.html.',
    },
    {
      question: 'Is Active Data Guard required for a physical standby?',
      answer:
        'No. Oracle states that Data Guard is included in Enterprise Edition, and the licensing table includes Redo Apply for that edition. Active Data Guard is an extra-cost option. You need it for real-time query, where Redo Apply stays active while the physical standby is open read-only, and for other features that table lists under the option, such as Far Sync. Sources: https://docs.oracle.com/en/database/oracle/oracle-database/26/haovw/oracle-data-guard.html and https://docs.oracle.com/en/database/oracle/oracle-database/26/dblic/Licensing-Information.html.',
    },
    {
      question: 'Does the OSO connector replace Data Guard or GoldenGate?',
      answer:
        'No. The connector is a Kafka Connect source that reads committed changes through LogMiner and publishes them to Kafka. It does not keep a physical standby, it does not apply changes to another database, and it is not a bi-directional replicator. Release 0.1.6 is a preview. Use Data Guard when you need the database back after an outage, and GoldenGate when you need the Oracle product that applies changes to another database. The connector statement is from this site introduction at https://kafkacdcconnector.com/.',
    },
  ]}
/>

## Trademarks

This project is not affiliated with, endorsed by or sponsored by Oracle Corporation or Amazon Web Services. Oracle is a registered trademark of Oracle and/or its affiliates. Other names may be trademarks of their respective owners.
