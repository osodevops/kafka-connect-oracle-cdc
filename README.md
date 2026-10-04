# OSO CDC Connector for Oracle Database

Open-source (Apache-2.0) change data capture source connector for Apache Kafka Connect that
reads Oracle Database redo through LogMiner.

**Status: pre-release, under active development.** Nothing here is production ready yet. The
product requirements live in [`docs/`](docs/README_index.md).

## What it will do

- **No silent loss.** One capture path, typed stop conditions, offsets that only encode what was
  delivered, and every known Debezium Oracle data-loss issue as a regression test.
- **Durable transaction journal.** Long-running transactions are journaled to a compacted Kafka
  topic, so the restart position is never pinned to the oldest open transaction.
- **Bounded memory.** In-heap buffer with automatic spill to local disk, sized by one property.
- **SCN-anchored chunked snapshots.** Short flashback reads per chunk, resumable, read-only,
  running alongside streaming.
- **Exactly-once delivery** aligned to Oracle transaction boundaries (Kafka Connect KIP-618).
- **Multi-PDB capture** from one LogMiner session.
- **Operability built in.** `oracle-cdc-doctor` preflight and redo profiler, one latency target
  instead of tuning knobs, offsets managed through the Connect REST API, Grafana dashboard.
- **Drop-in migration** from Confluent Oracle CDC Source and Debezium Oracle, with cutover
  verification.

## Building

```bash
mvn clean verify -DskipE2E      # quality gates and unit tests, no Docker
mvn clean package -DskipTests   # plugin ZIP in kafka-connect-oracle-cdc/target
```

Docker-backed tests against Oracle Database Free live in `e2e-tests`; see `CLAUDE.md` for the
commands and `docs/testing_strategy.md` for the tiers.

## Support

Community support is through GitHub issues and discussions. OSO offers an enterprise support
subscription; see [SUPPORT.md](SUPPORT.md). Security issues: see [SECURITY.md](SECURITY.md).

## Licence and trademarks

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

Oracle and Java are registered trademarks of Oracle and/or its affiliates. This project is not
affiliated with or endorsed by Oracle. Apache Kafka is a trademark of the Apache Software
Foundation.
