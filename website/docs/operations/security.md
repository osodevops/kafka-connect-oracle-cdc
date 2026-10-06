---
title: Credentials and sensitive data
description: How the connector handles database and Kafka credentials and row values, which topics and files hold row data, and what to protect.
---

# Credentials and sensitive data

The connector reads every change to the tables it captures, so its records, some of its internal
topics and its spill files hold row data. This page says where credentials and row values go and
what to protect. To report a vulnerability, follow the
[security policy](https://github.com/osodevops/kafka-connect-oracle-cdc/blob/main/SECURITY.md).

## Credentials

- `cdc.database.password` and `cdc.database.tls.truststore.password` are Kafka Connect `PASSWORD`
  settings and never appear in the connector's logs. Kafka Connect's REST API returns a
  connector's configuration as it was written, so pass secrets through a config provider, for
  example `${file:/opt/kafka/secrets/oracle.properties:password}`, rather than as literals.
- Encrypt the database connection with `cdc.database.tls.truststore.location` (and its type and
  password), or with an Oracle wallet in `cdc.database.wallet.location`. `cdc.database.url` takes a
  full TNS descriptor, and `cdc.database.connection.properties` passes further driver settings.
- The connector's own Kafka clients take their security settings under `cdc.kafka.*`, for example
  `cdc.kafka.security.protocol`. Pass secrets such as `cdc.kafka.sasl.jaas.config` through a config
  provider too.
- The mining user needs the grants that `oracle-cdc-doctor setup-sql` writes
  ([database setup](../database-setup/index.md)), including `SELECT ANY TABLE` and
  `FLASHBACK ANY TABLE` for snapshots and LOB reselect. Use it for the connector only.
- `oracle-cdc-doctor` and `oracle-cdc-admin` read the connector's configuration, pass the
  passwords to their clients and never print them. The [migration tools](../migration/tools.md#secrets)
  never write a secret to their output, reports or logs.

## Row values in logs, task status and ops events

Row values never appear in the worker log, the task status or the ops topic by default. When a
change cannot be decoded, the message names the table and the redo position and says that the
value is withheld, rather than quoting the literal or the SQL_REDO text around a parse error.

`cdc.log.sensitive.data=true` includes those values in the message and the worker log. Set it only
while you diagnose a [decode failure](runbooks/decode.md), restart the task, and set it back once
the cause is known. Ops events never carry row values, whatever the setting, and values of columns
named in `cdc.columns.exclude` are never included.

## Columns that must not reach Kafka

`cdc.columns.exclude` keeps named columns out of the connector altogether: their values are
dropped when a change is decoded, before they are converted, and snapshots and LOB reselect never
select them. They never reach the transaction buffer, the spill files, the journal, a record or a
log line, and dead letter records for a table the patterns may match carry no SQL text. Set the
patterns before the first start, because records already in Kafka are not rewritten. See
[excluded columns](../reference/record-formats.md#excluded-columns).

## Where row data is stored

| Place | What it holds | Protect it |
|---|---|---|
| Change topics | Every captured row value, in `before` and `after` | With Kafka ACLs, as you would the tables |
| Transaction journal topic (`${prefix}.cdc.txjournal`) | The changes of long or large open transactions until they end | Like the change topics |
| Decode DLQ topic (`${prefix}.cdc.dlq`) | The raw SQL_REDO and SQL_UNDO of rows that could not be decoded, and discarded transactions | Like the change topics |
| Spill directory (`cdc.buffer.spill.dir`) | Changes of large open transactions, on the worker's disk until they end | A volume only the worker can read; a file is deleted once its transaction's records are queued, and leftovers when the task starts |
| Schema topic (`${prefix}.cdc.schema`) | Table layouts: column names and types, no values | As you would the table definitions |
| Ops, heartbeat and signal topics | Positions, events and commands, no row values | See below for the signal topic |

## The signal topic

A [`snapshot` signal](signals.md) may carry a predicate, a SQL condition the connector adds to its
snapshot queries and runs as the mining user. Give write access to the signal topic only to the
operators who may start snapshots.
