# OSO CDC Connector for Oracle Database

Open-source (Apache-2.0) change data capture source connector for Apache Kafka Connect that
reads Oracle Database redo through LogMiner.

**Status: preview.** The current release is 0.1.1, on
[Maven Central](https://central.sonatype.com/namespace/sh.oso) (group `sh.oso`) and the
[GitHub releases](https://github.com/osodevops/kafka-connect-oracle-cdc/releases) page, which carries
the plugin ZIP. Releases before 1.0 are tested on Oracle Database Free 23ai and 26ai, single
instance; the 19c and 21c qualification and a 72-hour soak come before 1.0. Everything listed below
is in that release and covered by its test suites. The user documentation is at
[osodevops.github.io/kafka-connect-oracle-cdc](https://osodevops.github.io/kafka-connect-oracle-cdc/)
(sources in [`website/`](website/)); the
product requirements and design decisions are in [`docs/`](docs/README_index.md).

## What it does

- **No silent loss.** One capture path; any condition the engine does not understand, such as a
  missing archived log or a row it cannot decode, stops the task with a typed error code and a
  runbook; offsets only encode what Kafka Connect acknowledged; Debezium Oracle data-loss issues
  are reproduced as regression tests.
- **Long transactions without pinned offsets.** A transaction buffer that spills to local disk past
  a heap budget, and a transaction journal on a compacted Kafka topic, so a long transaction does
  not hold the restart position back.
- **Exactly-once delivery** with Kafka transactions at Oracle commit boundaries (Kafka Connect
  KIP-618).
- **SCN-anchored chunked snapshots** that resume after a restart and run alongside streaming, and
  snapshots on demand through a signal topic, with no writes to the source database.
- **Schema versions and DDL**, including rows written before a later DDL, decoded with a
  dictionary from the redo.
- **Multi-PDB capture** from one LogMiner session.
- **Debezium-compatible records**, column exclusion, and Avro-safe schema and field names.
- **Operability built in.** `oracle-cdc-doctor` (preflight checks, setup script, redo profile,
  sizing, lag explanation), `oracle-cdc-admin` (offsets, resnapshot, open transactions, journal
  inspection), task metrics with a Grafana dashboard and Prometheus alerts, and a runbook per
  error code.
- **Migration tools** from Debezium Oracle and Confluent Oracle CDC Source: configuration
  translation, a takeover SCN, and cutover verification.

Not available yet: Oracle RAC, Amazon RDS for Oracle, Autonomous Database, capture from a standby,
and a record format compatible with Confluent's Oracle CDC Source.

## Building

```bash
./mvnw clean verify -DskipE2E      # quality gates and unit tests, no Docker
./mvnw clean package -DskipTests   # plugin ZIP in kafka-connect-oracle-cdc/target
```

Docker-backed tests against Oracle Database Free live in `e2e-tests`; see `CLAUDE.md` for the
commands and `docs/testing_strategy.md` for the tiers. To run the connector on a laptop with
Oracle Database Free, Kafka and Kafka Connect in Docker, use the Compose lab
(`make -C lab/local/compose up register`, see [`lab/local/compose`](lab/local/compose/README.md)).

## Support

Community support is through GitHub issues and discussions. OSO offers an enterprise support
subscription; see [SUPPORT.md](SUPPORT.md). Security issues: see [SECURITY.md](SECURITY.md).

## Licence and trademarks

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

Oracle and Java are registered trademarks of Oracle and/or its affiliates. This project is not
affiliated with or endorsed by Oracle. Apache Kafka is a trademark of the Apache Software
Foundation.
