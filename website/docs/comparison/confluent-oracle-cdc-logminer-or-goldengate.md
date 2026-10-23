---
title: Does Confluent's Oracle CDC use GoldenGate?
sidebar_label: Confluent, LogMiner or GoldenGate
description: Confluent's Oracle CDC Source uses LogMiner, not GoldenGate. A separate Confluent connector uses XStream, which Oracle includes in a GoldenGate licence.
---

import Faq from '@site/src/components/Faq';

# Does Confluent's Oracle CDC use GoldenGate?

## Quick answer

No. Confluent's Oracle CDC Source connector reads Oracle Database changes through Oracle LogMiner, not through GoldenGate or XStream. Confluent also offers a separate Oracle XStream CDC Source connector that uses the XStream API. Oracle licenses XStream as part of GoldenGate. Confluent states that users of that XStream connector do not need an additional Oracle licence.

The notes below were checked against Confluent's and Oracle's documentation on 23 October 2026. Statements about those connectors come from the pages linked here. Statements about the OSO connector come from this site. This project is not affiliated with or endorsed by Confluent or Oracle.

## Which Oracle API does Confluent's Oracle CDC Source connector use?

Confluent's Oracle CDC Source connector captures each change to rows in an Oracle database and publishes those changes as records on Apache Kafka topics. On Confluent Platform it queries `V$LOGMNR_CONTENTS`, writes each raw redo event to a redo log topic at least once, and can place large object columns on separate topics. From Confluent Platform 6.0 it can assign frequently changing tables to different tasks. It supports Oracle Real Application Clusters, parallel snapshots of partitioned tables, and a heartbeat topic for quiet databases. The fully managed connector on Confluent Cloud captures inserts, updates and deletes in the same way.

Both editions state that the connector uses Oracle LogMiner to read the database redo log. On Confluent Platform the connector class is `io.confluent.connect.oracle.cdc.OracleCdcSourceConnector`. The documented database prerequisites are ARCHIVELOG mode, supplemental logging, and a database user who can use LogMiner and select from the captured tables. Those prerequisites do not set `ENABLE_GOLDENGATE_REPLICATION`, and they do not mention XStream.

GoldenGate appears on the Platform overview in one place. The topic and key template variables are similar to the Oracle GoldenGate Kafka Connect template variables, which simplify migrating from Oracle GoldenGate to this connector. That is a naming aid for a move off GoldenGate. It is not the capture API.

Sources: [Oracle CDC Source for Confluent Platform](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/overview.html), [quick start](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/getting-started.html), [database prerequisites](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/prereqs-validation.html), and [Oracle CDC Source for Confluent Cloud](https://docs.confluent.io/cloud/current/connectors/cc-oracle-cdc-source/cc-oracle-cdc-source.html).

## What is Confluent's Oracle XStream CDC Source connector?

Confluent publishes a second connector, the Oracle XStream CDC Source connector, separate from the LogMiner connector above. It captures row changes and represents them as Kafka records. It uses Oracle's XStream API. XStream Out reads data manipulation and data definition changes from the redo log, converts them into logical change records, and an outbound server sends those records to the connector. One outbound server accepts one active client, so each connector needs its own outbound server. The connector can snapshot several tables at once on multiple threads, and it can run capture on a downstream database so the mining work stays off the source.

On Confluent Cloud the connector class is `io.confluent.connect.oracle.xstream.cdc.OracleXStreamSourceConnector`. The Platform connector is a Confluent Premium connector under the Confluent enterprise licence. Confluent offers a trial period of 30 days and does not permit this connector under the Developer licence.

Sources: [Oracle XStream CDC Source for Confluent Platform](https://docs.confluent.io/kafka-connectors/oracle-xstream-cdc-source/current/overview.html), [XStream database prerequisites](https://docs.confluent.io/kafka-connectors/oracle-xstream-cdc-source/current/prereqs-validation.html), and [Oracle XStream CDC Source for Confluent Cloud](https://docs.confluent.io/cloud/current/connectors/cc-oracle-xstream-cdc-source/cc-oracle-xstream-cdc-source.html).

## Do I need a GoldenGate licence?

The two Confluent connectors are different, and Confluent and Oracle do not describe the XStream case in the same way.

For the Oracle CDC Source connector, Confluent documents LogMiner. Oracle's [19c licensing information](https://docs.oracle.com/en/database/oracle/oracle-database/19/dblic/Licensing-Information.html) states that LogMiner is available with all Oracle Database offerings. The [current licensing information](https://docs.oracle.com/en/database/oracle/oracle-database/26/dblic/Licensing-Information.html) states the same rule for Oracle AI Database offerings. Both state that a GoldenGate licence is required when LogMiner is used with GoldenGate supplemental logging turned on. Oracle defines that feature as `ENABLE_GOLDENGATE_REPLICATION` set to `TRUE`, and states that an on-premises GoldenGate licence is then required whichever API consumes the redo, including LogMiner, XStream or a third party tool. Confluent's prerequisites for the LogMiner connector do not set that parameter.

For the Oracle XStream CDC Source connector, Confluent's prerequisites do set `enable_goldengate_replication` to `TRUE`. The [Introduction to XStream](https://docs.oracle.com/en/database/oracle/oracle-database/19/xstrm/introduction-to-xstream.html) states that using the XStream APIs requires purchasing a licence for Oracle GoldenGate. Oracle's licensing information states that XStream is licensed through GoldenGate and that you must purchase that licence before using the XStream APIs. A [GoldenGate licence](https://docs.oracle.com/en/database/goldengate/core/26/ogglc/oracle-goldengate.html) includes a licence for XStream, and Oracle Database Enterprise Edition is a prerequisite for licensing and using XStream.

Confluent's end user terms, on the [Platform XStream overview](https://docs.confluent.io/kafka-connectors/oracle-xstream-cdc-source/current/overview.html) and on [Confluent Cloud](https://docs.confluent.io/cloud/current/connectors/cc-oracle-xstream-cdc-source/cc-oracle-xstream-cdc-source-features.html), state the other half. XStream technology is licensed as part of the Oracle GoldenGate family. Confluent has an agreement with Oracle that allows Confluent to use this technology with its products. Confluent therefore states that users do not need to buy additional licences from Oracle to use XStream with this connector.

This page reports those documents. It is not an Oracle licence ruling. Oracle licensing questions are outside the scope of [enterprise support](../enterprise-support.md) for this connector.

## How does the OSO connector compare?

The OSO CDC Connector for Oracle Database is the open-source connector this site documents. It is published under the Apache-2.0 licence. It reads changes through Oracle LogMiner and does not read redo files directly. It does not use GoldenGate or XStream. The current release is 0.1.6. [Database setup](../database-setup/index.md) asks for ARCHIVELOG mode and supplemental logging, and does not set `ENABLE_GOLDENGATE_REPLICATION`. [How capture works](../concepts/how-capture-works.md) describes the LogMiner session. The default [migration from Confluent's Oracle CDC Source connector](../migration/from-confluent.md) keeps the existing table topics from release 0.1.6. The [comparison](./index.md) covers delivery, snapshots and the limits still open. Oracle RAC is one of those limits: Confluent documents RAC support for both connectors above, and this connector does not support RAC yet.

| | Confluent Oracle CDC Source | Confluent Oracle XStream CDC Source | OSO CDC Connector |
|---|---|---|---|
| Capture mechanism | Oracle LogMiner, through `V$LOGMNR_CONTENTS` | Oracle XStream Out API | Oracle LogMiner |
| Licence of the connector | Confluent Premium connector, Confluent enterprise licence | Confluent Premium connector, Confluent enterprise licence; excluded from the Developer licence | Apache-2.0 |
| Oracle licence implication | Prerequisites use LogMiner and do not set `ENABLE_GOLDENGATE_REPLICATION`. Oracle states that LogMiner is available with all database offerings unless GoldenGate supplemental logging is on. | Prerequisites set `enable_goldengate_replication` to `TRUE`. Oracle licenses the XStream APIs through GoldenGate. Confluent states that its agreement means no additional Oracle licence for this connector. | LogMiner only. The connector does not use GoldenGate or XStream, and its documented setup does not set `ENABLE_GOLDENGATE_REPLICATION`. |

Each cell is taken from the sources linked in the sections above, and from the [introduction](../intro.md) for this connector. If a linked page has moved, please open an issue.

<Faq
  items={[
    {
      question: "Does Confluent's Oracle CDC Source connector use GoldenGate?",
      answer:
        "No. Confluent's Oracle CDC Source overview states that the connector uses Oracle LogMiner to read the database redo log. That page mentions GoldenGate only because the topic and key template variables resemble the Oracle GoldenGate Kafka Connect templates, which simplify a migration from GoldenGate. Source: https://docs.confluent.io/kafka-connectors/oracle-cdc/current/overview.html",
    },
    {
      question: "Does Confluent's Oracle CDC Source connector use LogMiner or XStream?",
      answer:
        "It uses LogMiner. Confluent documents a separate Oracle XStream CDC Source connector for the XStream API. Sources: https://docs.confluent.io/kafka-connectors/oracle-cdc/current/overview.html and https://docs.confluent.io/kafka-connectors/oracle-xstream-cdc-source/current/overview.html",
    },
    {
      question: "What is Confluent's Oracle XStream CDC Source connector?",
      answer:
        "It is a separate Confluent Premium connector that reads changes with Oracle's XStream API. XStream Out turns data manipulation and data definition changes into logical change records and an outbound server sends them to the connector. Source: https://docs.confluent.io/kafka-connectors/oracle-xstream-cdc-source/current/overview.html",
    },
    {
      question: "Do I need a GoldenGate licence for Confluent's XStream connector?",
      answer:
        "Oracle and Confluent give different answers. Oracle states that using the XStream APIs requires a GoldenGate licence, and Confluent's setup sets enable_goldengate_replication to TRUE. Confluent states that its agreement with Oracle means users do not need to buy an additional Oracle licence to use XStream with this connector. Sources: https://docs.oracle.com/en/database/oracle/oracle-database/19/dblic/Licensing-Information.html and https://docs.confluent.io/kafka-connectors/oracle-xstream-cdc-source/current/overview.html",
    },
    {
      question: "Does Confluent's LogMiner connector set ENABLE_GOLDENGATE_REPLICATION?",
      answer:
        "No. Confluent's database prerequisites for the Oracle CDC Source connector call for ARCHIVELOG mode, supplemental logging, and a user who can use LogMiner. They do not set ENABLE_GOLDENGATE_REPLICATION. Source: https://docs.confluent.io/kafka-connectors/oracle-cdc/current/prereqs-validation.html",
    },
    {
      question: "Does the OSO connector need GoldenGate, or does it read redo files?",
      answer:
        "No. This site's introduction states that the connector reads changes through Oracle LogMiner only, does not read redo files directly, and does not require Oracle GoldenGate or XStream. It is open source under the Apache-2.0 licence.",
    },
  ]}
/>

## Trademarks

This project is not affiliated with, endorsed by or sponsored by Oracle Corporation or Confluent. Oracle is a registered trademark of Oracle and/or its affiliates. Other names may be trademarks of their respective owners.
