---
title: Enterprise support
description: Production support for the connector from OSO, the team that builds it; what it covers, response targets and how to get it.
slug: /enterprise-support
sidebar_position: 90
---

# Enterprise support

The connector is open source under the Apache-2.0 licence and free to run. If you run it in
production and want someone accountable for it, OSO, the team that builds and maintains it, offers
an annual **Enterprise support subscription**. There are no licence keys and nothing changes in the
software: the connector has no phone-home, and if a subscription ends the connector keeps running.

The commitments on this page are the ones in the repository's `SUPPORT.md`, which is the source of
truth. A subscriber's schedule may extend them but never reduces them.

## How to get help

| Channel | Who | Commitment |
|---|---|---|
| [GitHub issues](https://github.com/osodevops/kafka-connect-oracle-cdc/issues) and [discussions](https://github.com/osodevops/kafka-connect-oracle-cdc/discussions) | Everyone | Best effort, no commitment |
| support@oso.sh and the support portal | Subscribers | Response targets below |
| Shared Slack Connect or Microsoft Teams channel | Subscribers | Included |
| security@oso.sh or GitHub private vulnerability reporting | Everyone | See `SECURITY.md` |
| sales@oso.sh | Prospective subscribers | Scoping call |

Subscribers' questions are answered by the engineers who write the code.

When you open a ticket, include the connector version, the Oracle Database version and release
update, the Kafka Connect version and platform, the connector configuration with credentials
removed, the `oracle-cdc-doctor check` report, and the Connect task status and logs.

## Hours

08:00 to 18:00 UK time, Monday to Friday, excluding UK public holidays. There is no staffed 24x7
desk. Out-of-hours P1 cover is available as a priced option.

## Priorities and response targets

| Priority | Definition | Initial response | Workaround or recovery path |
|---|---|---|---|
| P1 | The connector has stopped and no workaround restores capture; the resume SCN is approaching archive retention (risk of purge); records are suspected lost, duplicated or corrupted; mining is causing measurable production impact on the source database | 60 minutes | 4 hours |
| P2 | Degraded capture (lag beyond the agreed target, snapshots failing) with a workaround | 4 hours | 1 business day |
| P3 | A defect or question that is not urgent | 1 business day | 3 business days |
| P4 | Enhancement request | 2 business days | 5 business days |

Every P1 gets a root
cause analysis within five business days. Escalation runs from the engineer to the lead engineer
to the CTO. Subscribers receive a monthly report and a quarterly service review.

## What the subscription covers

- The connector itself.
- Review of `oracle-cdc-doctor` findings, redo sizing advice and review of the DBA scripts.
- Oracle release update and upgrade readiness: each release update in the supported matrix is
  tested before subscribers apply it.
- Migration guidance from Confluent Oracle CDC Source and Debezium Oracle, and the migration and
  verification tools.

## What it does not cover

- Operating the Oracle database itself, and Oracle licensing questions.
- GoldenGate and XStream products.
- Confluent's and Debezium's connectors.
- Kafka platform operation, which OSO offers as a separate service.

OSO supports the connector. Oracle supports your database. LogMiner is a feature of Oracle Database
included in all editions.

## Supported versions

- Connector: the current and the previous minor release.
- Oracle Database: per the support matrix; 19c, 21c and 23ai at 1.0. What the test suites run
  today is listed under [compatibility](getting-started/installation.md#compatibility).
- Kafka Connect 3.6 and later on Apache Kafka, Strimzi, Confluent Platform 7.6 and later, and
  Amazon MSK Connect; Java 17 and 21.

## Security fixes

The targets in `SECURITY.md`: acknowledgement within two business days, assessment within five,
and critical and high severity issues fixed or mitigated within ten business days.

## Continuity

Releases are automated, at least two OSO engineers hold release rights, and the source is public,
so there is nothing to escrow.

[Contact OSO](https://oso.sh/contact/) to discuss a subscription.
