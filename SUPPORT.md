# Support, lifecycle and maintenance

The OSO CDC Connector for Oracle Database is Apache-2.0 open source. OSO also offers an annual
Enterprise support subscription. A subscriber's schedule may extend these terms but never reduces
them.

## How to get help

| Channel | Who | Commitment |
|---|---|---|
| GitHub issues and discussions | Everyone | Best effort, no commitment |
| support@oso.sh and the support portal | Subscribers | Response targets below |
| Shared Slack Connect or Microsoft Teams channel | Subscribers | Included |
| security@oso.sh or GitHub private vulnerability reporting | Everyone | See `SECURITY.md` |
| sales@oso.sh | Prospective subscribers | Scoping call |

When you open a ticket, include the connector version, Oracle Database version and release
update, Kafka Connect version and platform, the connector configuration with credentials
redacted, the `oracle-cdc-doctor check` report, and the Connect task status and logs.

## Scope

Included: the connector, `oracle-cdc-doctor` findings review, redo sizing advice, Oracle release
update and upgrade readiness (each release update in the supported matrix is tested before
subscribers apply it), DBA script review, migration guidance from Confluent Oracle CDC and
Debezium Oracle, and the migration and verification tools.

Excluded: operating the Oracle database itself, Oracle licensing questions, GoldenGate and
XStream products, Confluent's and Debezium's connectors, and Kafka platform operation (available
as a separate service).

Statement on Oracle support: OSO supports the connector. Oracle supports your database. LogMiner
is a feature of Oracle Database included in all editions.

## Hours

08:00 to 18:00 UK time, Monday to Friday, excluding UK public holidays. There is no staffed 24x7
desk. Out-of-hours P1 cover is available as a priced option.

## Priorities and response targets

| Priority | Definition | Initial response | Workaround or recovery path |
|---|---|---|---|
| P1 | Connector stopped with no workaround; `resume.scn` age approaching archive retention (risk of purge); records suspected lost, duplicated or corrupted; mining causing measurable production impact on the source database | 60 minutes | 4 hours |
| P2 | Degraded capture (lag beyond agreed target, snapshots failing) with a workaround | 4 hours | 1 business day |
| P3 | Non-urgent defect or question | 1 business day | 3 business days |
| P4 | Enhancement request | 2 business days | 5 business days |

Every P1 gets a root cause analysis within five business days. Escalation runs engineer, then
lead engineer, then CTO. Subscribers receive a monthly report and a quarterly service review.

## Supported versions

- Connector: the current and the previous minor release.
- Oracle Database: per the published support matrix on the docs site (19c, 21c, 23ai at 1.0).
- Kafka Connect 3.6 and later on Apache Kafka, Strimzi, Confluent Platform 7.6 and later, and
  Amazon MSK Connect; Java 17 and 21.

## Security fixes

Same targets as `SECURITY.md`: acknowledgement within two business days, assessment within five,
critical and high fixed or mitigated within ten business days.

## Maintenance and continuity

Automated release process; at least two OSO engineers hold release rights; the source is public,
so there is nothing to escrow; the connector has no phone-home and no licence keys.
