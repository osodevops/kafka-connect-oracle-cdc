---
title: Enterprise support
description: Production support for the connector from OSO, the team that builds it; what it covers, response targets and how to get it.
slug: /enterprise-support
sidebar_position: 90
---

# Enterprise support

The connector is open source and free to run. If you run it in production and want someone
accountable for it, OSO, the team that builds and maintains it, offers an annual **Enterprise
support subscription**. There are no licence keys and nothing changes in the software: the
subscription buys support and maintenance commitments, and if it ends the connector keeps
running.

## Community or enterprise

| | Community | Enterprise support subscription |
|---|---|---|
| Who | Everyone | Subscribers |
| Channel | [GitHub issues](https://github.com/osodevops/kafka-connect-oracle-cdc/issues) and [discussions](https://github.com/osodevops/kafka-connect-oracle-cdc/discussions) | support@oso.sh, the OSO support portal, and a shared Slack Connect or Microsoft Teams channel |
| Response | Best effort, no commitment | P1 within 60 minutes with a workaround or recovery path within four hours, P2 within four hours, P3 one business day, P4 two business days |
| Who answers | Maintainers, as time allows | The engineers who write the code, directly |
| Fixes | Next release | Patch releases for your P1 and P2 defects |

Business hours are 08:00 to 18:00 UK time, Monday to Friday, excluding UK public holidays.
There is no staffed 24x7 desk; out-of-hours cover is a priced option.

## P1 for this connector

- The connector has stopped and no workaround restores capture
- The resume SCN is approaching archive retention and redo is at risk of being purged
- Records are suspected lost, duplicated or corrupted
- Mining is causing measurable production impact on the source database

## What the subscription includes

| Component | Detail |
|---|---|
| Maintained releases | Security patches, dependency updates and critical defect fixes for the supported versions |
| Oracle release readiness | Each Oracle Database release update in the supported matrix is tested before customers apply it |
| Kafka Connect compatibility | Verification on Apache Kafka, Amazon MSK Connect, Strimzi and Confluent Platform |
| `oracle-cdc-doctor` findings review | Help interpreting findings, redo sizing advice and DBA script review |
| Migration guidance | From Confluent Oracle CDC Source and Debezium Oracle, including cutover verification |
| Onboarding review | Configuration, grants, offsets, journal and runbook review, and an upgrade to the current release |
| Roadmap | Your issues and feature requests are prioritised and reviewed each quarter |

## What is not covered

- Operating the Oracle database itself, Oracle licensing questions, GoldenGate and XStream
- Confluent's and Debezium's connectors
- Kafka platform operation, available from OSO as a separate service

Supported versions: the current and previous minor release of the connector; Oracle Database
versions per the published matrix; Kafka Connect 3.6 and later.

OSO supports the connector. Oracle supports your database. LogMiner is a feature of Oracle
Database included in all editions.

[Contact OSO](https://oso.sh/contact/) to discuss a subscription.
