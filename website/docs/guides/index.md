---
title: Guides
description: Practical guides on change data capture from Oracle Database to Apache Kafka.
---

# Guides

Practical guides on capturing changes from Oracle Database into Apache Kafka. They explain the
problems in general terms and link to the reference pages for how this connector handles each one.

- [Oracle Database change data capture to Kafka](oracle-cdc-to-kafka.md): the ways to capture
  changes, how LogMiner-based capture works, what makes it hard, and how to run it.

Related pages that answer common questions directly:

- [Comparison with Confluent's and Debezium's connectors](../comparison/index.md), with a harness to
  measure them yourself.
- [Supplemental logging](../database-setup/supplemental-logging.md) and
  [redo sizing and archive retention](../database-setup/redo-sizing.md).
- [Exactly-once delivery from Oracle Database to Kafka](../concepts/exactly-once.md).
- The [runbooks](../operations/runbooks/index.md), one per error the connector can stop with,
  including ORA-01555 during snapshots and missing or purged redo logs.
