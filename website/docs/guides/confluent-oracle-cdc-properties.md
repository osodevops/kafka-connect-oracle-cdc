---
title: Confluent Oracle CDC Properties Mapped
sidebar_label: Confluent Oracle CDC properties
description: How io.confluent.connect.oracle.cdc.OracleCdcSourceConnector properties map to the OSO CDC Connector for Oracle Database, including filters and start SCN.
---

import Faq from '@site/src/components/Faq';

# Confluent Oracle CDC Properties Mapped

This page maps each configuration property of Confluent's Oracle CDC Source connector to the
OSO CDC Connector for Oracle Database. It is a property reference. The cutover procedure, the
offset handover and the commands that run the migration tool stay on
[Migrating from Confluent Oracle CDC Source](../migration/from-confluent.md) and
[Migration tools](../migration/tools.md). OSO property names link to the
[configuration reference](../reference/configuration/index.md) where that reference documents
them. The same questions are answered, at a higher level, in the [guides](index.md) and in the
[comparison](../comparison/index.md).

**Quick answer.** `io.confluent.connect.oracle.cdc.OracleCdcSourceConnector` is the `connector.class` value for Confluent's Oracle CDC Source connector. The OSO connector class is `sh.oso.connect.oracle.OracleCdcSourceConnector`. The migration tool copies some settings unchanged, rewrites others, and drops or flags the rest for review. The table below is that translation, and the migration pages explain how to run the tool.

## What is io.confluent.connect.oracle.cdc.OracleCdcSourceConnector?

`io.confluent.connect.oracle.cdc.OracleCdcSourceConnector` is the `connector.class` value
Confluent documents for the Oracle CDC Source connector on Confluent Platform. Confluent's quick
start sets that class, then names the Oracle host, port, SID, pluggable database and the tables
to capture
([quick start](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/getting-started.html)).

Confluent describes the connector as capturing each change to rows in a database and representing
those changes as change event records in Apache Kafka topics. It uses Oracle LogMiner to read the
database redo log. An include expression and an exclude expression choose the tables. Each table
is written to a topic named by `table.topic.name.template`. On Confluent Platform 6.0 and later
the connector can spread tables across tasks and rebalance them by throughput. It can snapshot
partitioned tables in parallel. It can write BLOB, CLOB and NCLOB values to separate large-object
topics. It supports Oracle Real Application Clusters. Confluent states that it is a Premium
connector under the Confluent enterprise licence, that it requires an additional subscription,
and that a trial of 30 days does not need a licence key
([overview](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/overview.html)).

The property names below are the ones in Confluent's configuration reference for that Platform
connector
([configuration properties](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/configuration-properties.html)).
Confluent Cloud uses a different `connector.class` value, `OracleCdcSource`. The migration tool
accepts `io.confluent.connect.oracle.cdc.OracleCdcSourceConnector` and does not accept the Cloud
class name.

The OSO connector class is `sh.oso.connect.oracle.OracleCdcSourceConnector`. It reads changes
through LogMiner only and does not read redo files directly. It does not use GoldenGate or
XStream. It runs one task. Oracle Real Application Clusters is not supported yet. The connector
is published under the Apache-2.0 licence. Release 0.1.6 includes the Confluent-compatible record
format and the migration tool that applies the table below. OSO is not affiliated with or
endorsed by Confluent. Oracle is a registered trademark of Oracle and/or its affiliates.

Sources, checked on 30 October 2026: Confluent's
[quick start](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/getting-started.html),
[overview](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/overview.html) and
[configuration reference](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/configuration-properties.html).
Statements about the OSO connector and about the migration tool come from this repository.

## Confluent to OSO property mapping

The table is the rule set in the migration tool, including every property that tool maps, drops
or marks for manual review. The default record format is Confluent (`--format=confluent`), which
keeps table topics and flat records. `--format=debezium` is named in the notes where the target
changes. A blank value is dropped, because there is nothing to carry over, except
`table.topic.name.template` and `lob.topic.name.template`, whose blank values are meaningful.
A literal password is replaced with a mask. A config provider reference is kept.

Classifications match the tool: mapped, mapped with a change, dropped, and manual.

| Confluent property | OSO property | Notes |
|---|---|---|
| `connector.class` | `sh.oso.connect.oracle.OracleCdcSourceConnector` | Mapped with a change. The Confluent class is replaced by the OSO class. |
| `name` | `name` | Mapped with a change. The new connector has its own name, because Kafka Connect keeps offsets per connector name and this connector does not read the Confluent offset format. The default name is the source name with `-oso` appended, after any character outside letters, digits, `.`, `_` and `-` is replaced with `-`. The name must differ from the source name. |
| `tasks.max` | `tasks.max` | Mapped when the source value is `1`. Mapped with a change otherwise. The tool writes `1`. A higher value is accepted by the connector and ignored: one task mines every captured table with one LogMiner session. |
| `oracle.server` | [`cdc.database.host`](../reference/configuration/database.md) | Mapped. The value is copied unchanged. |
| `oracle.port` | [`cdc.database.port`](../reference/configuration/database.md) | Mapped. The value is copied unchanged. |
| `oracle.username` | [`cdc.database.user`](../reference/configuration/database.md) | Mapped. The value is copied unchanged. In a container database this must be a common user with the grants from `oracle-cdc-doctor setup-sql`. |
| `oracle.password` | [`cdc.database.password`](../reference/configuration/database.md) | Mapped. A config provider reference is copied unchanged. A literal password is replaced with a mask and is not copied. |
| `oracle.sid` | [`cdc.database.sid`](../reference/configuration/database.md) | Mapped when the value is not blank. The value is copied unchanged. |
| `oracle.service.name` | [`cdc.database.service`](../reference/configuration/database.md) | Mapped when the value is not blank. The value is copied unchanged. |
| `oracle.pdb.name` | [`cdc.database.pdbs`](../reference/configuration/database.md) | Mapped. The value is copied unchanged. One pluggable database name. |
| `table.inclusion.regex` | [`cdc.tables.include`](../reference/configuration/topics.md) | Mapped with a change. See [How does table.inclusion.regex map to OSO?](#how-does-tableinclusionregex-map-to-oso). |
| `table.exclusion.regex` | [`cdc.tables.exclude`](../reference/configuration/topics.md) | Mapped with a change. The same rewrite as `table.inclusion.regex`, written to `cdc.tables.exclude`. See [How does table.exclusion.regex map to OSO?](#how-does-tableexclusionregex-map-to-oso). |
| `table.topic.name.template` | [`cdc.topic.template`](../reference/configuration/topics.md) | Mapped with a change. `${schemaName}` becomes `${schema}`, `${tableName}` becomes `${table}`, `${databaseName}` becomes `${pdb}` when `oracle.pdb.name` is set and `${database}` otherwise, `${fullyQualifiedTableName}` becomes those three segments, `${emptyString}` becomes empty, and `${connectorName}` becomes the source connector name. A leading `${prefix}.` is added only with `--format=debezium`. A blank value, a backslash, or any other variable is manual. When the property is absent, the tool translates Confluent's default, `${fullyQualifiedTableName}`. |
| `redo.log.topic.name` | none | Dropped. There is no redo log topic. The tool reports that the redo topic stops receiving data at the cutover. |
| `start.from` | [`cdc.snapshot.mode`](../reference/configuration/snapshots.md) | Mapped with a change. The value is not copied. The tool sets `cdc.snapshot.mode` to `none`. See [How does start.scn map to OSO?](#how-does-startscn-map-to-oso). |
| `key.template` | [`cdc.output.confluent.key.template`](../reference/configuration/confluent-compatible-format.md) | Mapped in the default Confluent format. The value is copied unchanged. With `--format=debezium` the property is manual, because record keys are structs of the key columns and consumers of Confluent keys must change. |
| `max.batch.size` | [`cdc.poll.max.records`](../reference/configuration/task.md) | Mapped. The value is copied unchanged. |
| `query.timeout.ms` | [`cdc.mining.query.timeout.ms`](../reference/configuration/mining.md) | Mapped with a change. The value is copied. It applies to each mining step. On expiry the step is retried with a smaller window. |
| `max.retry.time.ms` | [`cdc.retry.max.time.ms`](../reference/configuration/errors-and-retries.md) | Mapped. The value is copied unchanged. |
| `redo.log.poll.interval.ms` | none | Dropped. The mining window adapts to [`cdc.mining.target.latency.ms`](../reference/configuration/mining.md), default two seconds. The Confluent interval is not written. |
| `snapshot.row.fetch.size` | [`cdc.snapshot.fetch.size`](../reference/configuration/snapshots.md) | Mapped. The value is copied unchanged. |
| `redo.log.row.fetch.size` | [`cdc.mining.fetch.size`](../reference/configuration/mining.md) | Mapped. The value is copied unchanged. |
| `poll.linger.ms` | [`cdc.poll.linger.ms`](../reference/configuration/task.md) | Mapped. The value is copied unchanged. |
| `snapshot.threads.per.task` | [`cdc.snapshot.threads`](../reference/configuration/snapshots.md) | Mapped. The value is copied unchanged. |
| `heartbeat.interval.ms` | [`cdc.heartbeat.interval.ms`](../reference/configuration/topics.md) | Mapped when the value is a whole number above zero: the value is copied. Mapped with a change when it is zero or negative: the interval is not written, and heartbeats stay at the connector default of every 10 seconds. Those heartbeats never write to the source database. A value that is not a whole number is manual. |
| `heartbeat.topic.name` | none | Dropped. Heartbeat records go to [`cdc.heartbeat.topic`](../reference/configuration/topics.md), default `${prefix}.cdc.heartbeat`, in the connector's own format. The Confluent topic name is not written. |
| `use.transaction.begin.for.mining.session` | none | Dropped. Restart from the oldest open transaction is always on. |
| `log.mining.transaction.age.threshold.ms` | [`cdc.transaction.max.age.ms`](../reference/configuration/transactions.md) | Dropped when the threshold is zero or negative: there is no age limit, which is also the connector default. Mapped when the breached action is `discard` and the threshold is above zero: the value is written, and a value below 1000 is raised to 1000. Mapped with a change for any other action, including `warn`: the threshold is not written, because there is no warn-only age limit. Long transactions are kept, spilled to disk and journaled. A value that is not a whole number is manual. |
| `log.mining.transaction.threshold.breached.action` | [`cdc.transaction.max.age.action`](../reference/configuration/transactions.md) | Uses the same rule as the age threshold. Mapped to `discard` when the action is `discard` and the threshold is above zero. A discarded transaction's details go to the ops topic and the dead-letter topic. Dropped when the threshold is zero or negative. Mapped with a change, and not written, for any other action. |
| `redo.log.corruption.topic` | none | Dropped. Corrupt redo stops the task with a typed error and an ops event. It is never skipped. |
| `behavior.on.dictionary.mismatch` | [`cdc.on.decode.error`](../reference/configuration/errors-and-retries.md) | Mapped to `fail` when both this property and `behavior.on.unparsable.statement` are `fail` (the default). Mapped with a change to `dlq` when either value is `log`: undecodable rows go to the dead-letter topic, and the tool says to watch that topic. One OSO setting covers both Confluent properties. Any other value is manual. |
| `behavior.on.unparsable.statement` | [`cdc.on.decode.error`](../reference/configuration/errors-and-retries.md) | The same single target as `behavior.on.dictionary.mismatch`. |
| `oracle.dictionary.mode` | none | Dropped. The connector mines with the online catalog and replays a step with a dictionary from the redo when a row predates a later DDL. See [`cdc.dictionary.build.interval.ms`](../reference/configuration/mining.md). The Confluent mode is not written. |
| `log.mining.archive.destination.name` | [`cdc.archive.destination`](../reference/configuration/capture.md) | Mapped when the value is not blank. The value is copied unchanged. |
| `record.buffer.mode` | none | Dropped. The connector buffers in its own transaction buffer. |
| `max.batch.timeout.ms` | none | Dropped. Deprecated in the source, and not used. |
| `max.buffer.size` | none | Dropped. The connector buffers in its own transaction buffer. |
| `lob.topic.name.template` | [`cdc.lob.mode`](../reference/configuration/lobs.md), [`cdc.lob.topic.template`](../reference/configuration/topics.md) | Mapped in the default Confluent format when the template is not blank. The tool sets `cdc.lob.mode` to `topic` and rewrites the template: `${columnName}` becomes `${column}`, and the table-name variables are rewritten as for `table.topic.name.template`. It also reports that separate large-object topics need stable column keys, complete values and enough undo for an AS OF commit reselect. OSO moves large-object state when the row key changes. Confluent 2.15.2 left the previous state in place. A blank template is dropped, and large-object columns stay out of the records (`cdc.lob.mode=skip`). A non-blank template with `--format=debezium`, or a variable the tool does not know, is manual. |
| `enable.large.lob.object.support` | none | Dropped. [`cdc.lob.max.bytes`](../reference/configuration/lobs.md) limits large-object values, default 1 MiB. The Confluent flag is not written. |
| `log.sensitive.data` | [`cdc.log.sensitive.data`](../reference/configuration/errors-and-retries.md) | Mapped. The value is copied unchanged. |
| `numeric.mapping` | [`cdc.output.confluent.numeric.mapping`](../reference/configuration/confluent-compatible-format.md) | Mapped in the default Confluent format. The value is copied, except `best_fit` (any case) which is written as `best_fit_or_decimal`. With `--format=debezium`, `none`, `best_fit` and `best_fit_or_decimal` are mapped with a change to [`cdc.decimal.mode`](../reference/configuration/record-format.md) `precise`. Any other mode in that format is manual. |
| `numeric.default.scale` | [`cdc.output.confluent.numeric.default.scale`](../reference/configuration/confluent-compatible-format.md) | Mapped in the default Confluent format. The value is copied. A value that needs rounding to fit the scale stops the task with CDC-3001, including during snapshots. With `--format=debezium`, `127` is dropped, because an unconstrained NUMBER keeps its own scale, and any other scale is manual. |
| `oracle.date.mapping` | [`cdc.output.confluent.date.mapping`](../reference/configuration/confluent-compatible-format.md) | Mapped in the default Confluent format. The value is copied unchanged. With `--format=debezium` the property is manual. |
| `emit.tombstone.on.delete` | [`cdc.tombstones.on.delete`](../reference/configuration/topics.md) | Mapped. The value is copied unchanged. When the property is absent, the tool sets `cdc.tombstones.on.delete` to `false`, which is Confluent's default. |
| `oracle.fan.events.enable` | none | Dropped when the value is not `true`. Manual when it is `true`: Oracle Real Application Clusters is not supported in this release, so there are no FAN events to follow. The tool does not write [`cdc.database.fan.enabled`](../reference/configuration/database.md), which the configuration reference lists as reserved. |
| `table.task.reconfig.checking.interval.ms` | none | Dropped. One task captures every table, so there is no table placement to rebalance. |
| `table.rps.logging.interval.ms` | none | Dropped. Per-table rates are JMX metrics. |
| `log.mining.end.scn.deviation.ms` | none | Dropped. A Real Application Clusters setting. Real Application Clusters is not supported in this release. |
| `output.table.name.field` | [`cdc.output.confluent.table.name.field`](../reference/configuration/confluent-compatible-format.md) | Mapped in the default Confluent format. The value is copied unchanged. With `--format=debezium` the property is manual. |
| `output.scn.field` | [`cdc.output.confluent.scn.field`](../reference/configuration/confluent-compatible-format.md) | Mapped in the default Confluent format. The value is copied unchanged. With `--format=debezium` the property is manual. |
| `output.commit.scn.field` | [`cdc.output.confluent.commit.scn.field`](../reference/configuration/confluent-compatible-format.md) | Mapped in the default Confluent format. The value is copied unchanged. With `--format=debezium` the property is manual. |
| `output.op.type.field` | [`cdc.output.confluent.op.type.field`](../reference/configuration/confluent-compatible-format.md) | Mapped in the default Confluent format. The value is copied unchanged. With `--format=debezium` the property is manual. |
| `output.op.ts.field` | [`cdc.output.confluent.op.ts.field`](../reference/configuration/confluent-compatible-format.md) | Mapped in the default Confluent format. The value is copied unchanged. With `--format=debezium` the property is manual. |
| `output.current.ts.field` | [`cdc.output.confluent.current.ts.field`](../reference/configuration/confluent-compatible-format.md) | Mapped in the default Confluent format. The value is copied unchanged. With `--format=debezium` the property is manual. |
| `output.row.id.field` | [`cdc.output.confluent.row.id.field`](../reference/configuration/confluent-compatible-format.md) | Mapped in the default Confluent format. The value is copied unchanged. With `--format=debezium` the property is manual. |
| `output.username.field` | [`cdc.output.confluent.username.field`](../reference/configuration/confluent-compatible-format.md) | Mapped in the default Confluent format. The value is copied unchanged. With `--format=debezium` the property is manual. |
| `output.redo.field` | [`cdc.output.confluent.redo.field`](../reference/configuration/confluent-compatible-format.md) | Mapped in the default Confluent format. The value is copied unchanged. With `--format=debezium` the property is manual. |
| `output.undo.field` | [`cdc.output.confluent.undo.field`](../reference/configuration/confluent-compatible-format.md) | Mapped in the default Confluent format. The value is copied unchanged. With `--format=debezium` the property is manual. |
| `output.before.state.field` | [`cdc.output.confluent.before.state.field`](../reference/configuration/confluent-compatible-format.md) | Mapped in the default Confluent format. The value is copied unchanged. With `--format=debezium` the property is manual. |
| `output.op.type.read.value` | [`cdc.output.confluent.op.type.read.value`](../reference/configuration/confluent-compatible-format.md) | Mapped in the default Confluent format. The value is copied unchanged. With `--format=debezium` the property is manual. |
| `output.op.type.insert.value` | [`cdc.output.confluent.op.type.insert.value`](../reference/configuration/confluent-compatible-format.md) | Mapped in the default Confluent format. The value is copied unchanged. With `--format=debezium` the property is manual. |
| `output.op.type.update.value` | [`cdc.output.confluent.op.type.update.value`](../reference/configuration/confluent-compatible-format.md) | Mapped in the default Confluent format. The value is copied unchanged. With `--format=debezium` the property is manual. |
| `output.op.type.delete.value` | [`cdc.output.confluent.op.type.delete.value`](../reference/configuration/confluent-compatible-format.md) | Mapped in the default Confluent format. The value is copied unchanged. With `--format=debezium` the property is manual. |
| `output.op.type.truncate.value` | [`cdc.output.confluent.op.type.truncate.value`](../reference/configuration/confluent-compatible-format.md) | Mapped in the default Confluent format. The value is copied unchanged. With `--format=debezium` the property is manual. |
| any other `output.*` | none | Manual in the default Confluent format: the metadata name is not one of the fields or operation values above. With `--format=debezium`, every `output.*` property is manual. |
| `redo.log.startup.polling.limit.ms` | none | Dropped. There is no redo log topic, so its startup wait does not apply. |
| `snapshot.by.table.partitions` | none | Dropped. Snapshots are split into chunks of `cdc.snapshot.chunk.rows` rows. |
| `oracle.validation.result.fetch.size` | none | Dropped. Validation is the doctor's fast mode. |
| `redo.log.row.poll.fields.include` | none | Dropped. There is no redo log topic, so its column lists do not apply. |
| `redo.log.row.poll.fields.exclude` | none | Dropped. There is no redo log topic, so its column lists do not apply. |
| `redo.log.row.poll.username.include` | none | Manual when the value is not blank. There is no user include filter in this release. Only [`cdc.users.exclude`](../reference/configuration/topics.md) exists. |
| `redo.log.row.poll.username.exclude` | [`cdc.users.exclude`](../reference/configuration/topics.md) | Mapped when the value is not blank. The value is copied unchanged. |
| `db.timezone` | none | Dropped when the value is `UTC`, `Z`, `+00:00`, `GMT` or `ETC/UTC`, in any case. DATE and TIMESTAMP values are read as UTC. Any other zone is manual: there is no time zone setting in this release. |
| `db.timezone.date` | none | The same rule as `db.timezone`. |
| `oracle.supplemental.log.level` | none | Dropped. `oracle-cdc-doctor check` verifies supplemental logging for each captured table. See [supplemental logging](../database-setup/supplemental-logging.md). |
| `ldap.url` | [`cdc.database.url`](../reference/configuration/database.md) | Mapped with a change. The tool writes `jdbc:oracle:thin:@` followed by the LDAP naming URL. LDAP authentication settings are not translated. |
| `ldap.security.principal` | none | Manual. LDAP authentication is not translated. Use the database user and password, or a wallet. |
| `ldap.security.credentials` | none | Manual. LDAP authentication is not translated. Use the database user and password, or a wallet. |
| `oracle.ssl.truststore.file` | [`cdc.database.tls.truststore.location`](../reference/configuration/database.md) | Mapped when the value is not blank. The value is copied unchanged. |
| `oracle.ssl.truststore.password` | [`cdc.database.tls.truststore.password`](../reference/configuration/database.md) | Mapped when the value is not blank. A config provider reference is copied unchanged. A literal password is replaced with a mask and is not copied. |
| `oracle.kerberos.cache.file` | none | Manual when the value is not blank. Kerberos authentication is not built yet. The configuration reference lists [`cdc.database.kerberos.ccache`](../reference/configuration/database.md) as reserved, and the tool does not copy the cache path into it. |
| `retry.error.codes` | [`cdc.retry.extra.error.codes`](../reference/configuration/errors-and-retries.md) | Mapped with a change. Each entry is written as `ORA-` and five digits, so `123` and `ORA-123` both become `ORA-00123`. An entry that is not an Oracle error number is manual. |
| `enable.metrics.collection` | none | Dropped. JMX metrics are always on. |
| `confluent.license` | none | Dropped. The licence is not used. The tool asks you to remove it from the configuration and the secret store once no other Confluent premium connector uses it. |
| `redo.log.initial.delay.interval.ms` | none | Dropped. Streaming starts with the task. |
| `redo.log.consumer.*` | none | Dropped. There is no redo log topic, so its consumer settings do not apply. This includes `redo.log.consumer.bootstrap.servers`, `redo.log.consumer.fetch.min.bytes`, `redo.log.consumer.max.partition.fetch.bytes`, `redo.log.consumer.fetch.max.bytes`, `redo.log.consumer.max.poll.records`, `redo.log.consumer.request.timeout.ms`, `redo.log.consumer.receive.buffer.bytes` and `redo.log.consumer.send.buffer.bytes`, and any other name that starts with `redo.log.consumer.`. |
| `confluent.topic` and `confluent.topic.*` | none | Dropped. Confluent licence topic settings are not used. This includes `confluent.topic`, `confluent.topic.bootstrap.servers`, `confluent.topic.replication.factor`, `confluent.topic.security.protocol`, `confluent.topic.ssl.truststore.location`, `confluent.topic.ssl.truststore.password`, `confluent.topic.ssl.keystore.location`, `confluent.topic.ssl.keystore.password` and `confluent.topic.ssl.key.password`, and any other name that starts with `confluent.topic`. |
| `connection.pool.*` | none | Dropped. The connector manages its own connections. Driver options are not copied into [`cdc.database.connection.properties`](../reference/configuration/database.md). This includes `connection.pool.initial.size`, `connection.pool.min.size`, `connection.pool.max.size`, `connection.pool.login.timeout.ms`, `connection.pool.connection.wait.timeout.ms`, `connection.pool.validation.on.borrow.enabled`, `connection.pool.validation.sql`, `connection.pool.max.ttl.ms`, `connection.pool.abandoned.timeout.ms`, `connection.pool.inactive.timeout.ms`, `connection.pool.check.interval.timeout.ms`, `connection.pool.validation.timeout.ms`, `connection.pool.trust.idle.time.ms`, `connection.pool.max.reuse.time.ms`, `connection.pool.max.reuse.count`, `connection.pool.property.cycle.interval.ms`, `connection.pool.property.max.statements`, `connection.pool.harvest.trigger.count`, `connection.pool.harvest.max.count` and `connection.pool.fast.failover.enabled`, and any other name that starts with `connection.pool.`. |
| `key.converter` and `key.converter.*` | the same name | Mapped. A Kafka Connect framework property, copied unchanged. |
| `value.converter` and `value.converter.*` | the same name | Mapped. A Kafka Connect framework property, copied unchanged. |
| `header.converter` and `header.converter.*`, `transforms` and `transforms.*`, `predicates` and `predicates.*`, `errors.*`, `topic.creation.*`, `producer.override.*`, `consumer.override.*`, `admin.override.*`, `config.action.reload` and `offsets.storage.topic` | the same name | Mapped. Kafka Connect framework properties, copied unchanged. A value that names an `io.debezium` class is kept, with a reminder that those jars must stay on the plugin path or be replaced. `errors.tolerance=all` is kept, with a reminder that `none` is the recommended value so a conversion problem stops the task. |
| `exactly.once.support`, `transaction.boundary` and `transaction.boundary.*` | `exactly.once.support` and `transaction.boundary` | Mapped with a change. The source values are not copied. When the target Connect cluster has exactly-once source support enabled, the tool writes `exactly.once.support=required` and `transaction.boundary=connector`. Otherwise it leaves them unset and reports how to enable them. See [exactly-once delivery](../concepts/exactly-once.md). |
| any other property | none | Manual. The tool does not know the property. Check what it did in the source connector and set the corresponding `cdc.*` property by hand, or leave it out. |

### Settings the tool adds

These settings are not copies of a Confluent property. The tool writes them while it translates
a configuration. A value that a row above already set is left as it is.

| Setting | What the tool writes |
|---|---|
| [`cdc.output.format`](../reference/configuration/record-format.md) | `confluent` by default, or `debezium` when `--format=debezium` is set. |
| [`cdc.topic.prefix`](../reference/configuration/topics.md) | The `--topic-prefix` value, or the source connector name with disallowed characters replaced by underscores, or `oracle-cdc` when there is no source name. It names internal topics and the offset partition. Table topics stay unprefixed in the default format. |
| [`cdc.topic.template`](../reference/configuration/topics.md) | The translation of `table.topic.name.template`, or of `${fullyQualifiedTableName}` when that property is absent. |
| [`cdc.tables.case.sensitive`](../reference/configuration/topics.md) | `true`, when an include or exclude pattern was written. Confluent matches table expressions case-sensitively. The connector default without this setting is `false`. |
| [`cdc.key.missing`](../reference/configuration/topics.md) | `none` in the default Confluent format, so a table without a column key keeps a null key. |
| [`cdc.tombstones.on.delete`](../reference/configuration/topics.md) | `false` when `emit.tombstone.on.delete` was not set. |
| [`cdc.lob.mode`](../reference/configuration/lobs.md) | `skip` when a large-object topic template did not select `topic`. |
| [`cdc.journal.converter`](../reference/configuration/topics.md) | In the default Confluent format, the value converter class and each `value.converter.*` setting, when both converters are set, the class and the settings match, and the value converter class does not end with `JsonConverter`. |
| [`cdc.decimal.mode`](../reference/configuration/record-format.md) | `precise` in the Debezium format, when `numeric.mapping` did not already set it. |
| [`cdc.snapshot.mode`](../reference/configuration/snapshots.md) | `none`. A takeover streams from the previous connector's position and does not snapshot. |
| [`cdc.start.scn`](../reference/configuration/capture.md) | The `--start-scn` value, when that option is passed. Without it the property is left unset. |

## How does start.scn map to OSO?

Confluent's configuration reference does not define a property named `start.scn`. The property
that chooses the first position in the redo log is `start.from`. Confluent documents the valid
values `snapshot` (the default), `current` and `force_current`, and the same page says the
property specifies an Oracle system change number or a database timestamp in the form
`yyyy-MM-dd HH:mm:SS`
([configuration properties](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/configuration-properties.html)).

The migration tool does not read an SCN out of `start.from`. It maps every value of `start.from`
to `cdc.snapshot.mode=none`, because the takeover streams from the stopped connector's position
and does not snapshot. The OSO start position is
[`cdc.start.scn`](../reference/configuration/capture.md). The tool writes it only when it is given
that SCN with `--start-scn`. Otherwise it leaves `cdc.start.scn` unset and reports that
`takeover_scn.py` must read the stopped connector's offset, check that the archived logs from
that SCN still exist, and write `cdc.start.scn`. The connector honours `cdc.start.scn` only while
it has no stored offset. An empty value starts at the current SCN. If the archived logs from the
SCN are already gone when the task starts, the task stops with CDC-2002.

```json
"start.from": "snapshot"
```

The tool writes:

```json
"cdc.snapshot.mode": "none"
```

After the takeover, the same configuration also contains `cdc.start.scn`. The number is the SCN
from the stopped connector's offset. The value below is illustrative only.

```json
"cdc.snapshot.mode": "none",
"cdc.start.scn": "1000"
```

How the offset is read, and why the new connector must be stopped rather than only paused, is on
[Migrating from Confluent Oracle CDC Source](../migration/from-confluent.md).

## How does table.inclusion.regex map to OSO?

`table.inclusion.regex` maps to [`cdc.tables.include`](../reference/configuration/topics.md).
Confluent matches the expression case-sensitively against fully qualified names in the data
dictionary. For a pluggable database the name includes the pluggable database, the schema and
the table. For a non-container database it includes the SID, the schema and the table
([configuration properties](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/configuration-properties.html)).

The tool removes a leading `^` and a trailing `$`. When `oracle.pdb.name` is set, the rest of
the expression is kept, because Confluent already matches `PDB.SCHEMA.TABLE`, which is what
`cdc.tables.include` matches. When there is no pluggable database, top-level alternatives,
separated by `|`, are rewritten one by one. A pair of parentheses around a single alternative
is removed, and a leading database segment that matches `oracle.sid` or `oracle.service.name`,
or a wildcard segment, is removed, because the connector then matches `SCHEMA.TABLE`. A comma
in the expression is manual, because the connector splits its pattern list on every comma. If
the database segment cannot be removed, the property is manual.
Whenever an include or exclude pattern is written, the tool also sets
[`cdc.tables.case.sensitive`](../reference/configuration/topics.md) to `true`.

With a pluggable database, this Confluent fragment:

```json
"oracle.pdb.name": "ORCLPDB1",
"table.inclusion.regex": "ORCLPDB1[.]C##MYUSER[.].*"
```

is written as:

```json
"cdc.database.pdbs": "ORCLPDB1",
"cdc.tables.include": "ORCLPDB1[.]C##MYUSER[.].*",
"cdc.tables.case.sensitive": "true"
```

Without a pluggable database, a leading SID is removed. This fragment:

```json
"oracle.sid": "ORCL",
"table.inclusion.regex": "ORCL[.]APP[.]ORDERS"
```

is written as:

```json
"cdc.database.sid": "ORCL",
"cdc.tables.include": "APP[.]ORDERS",
"cdc.tables.case.sensitive": "true"
```

If `oracle.pdb.name` is absent, the tool assumes a non-container database. When the tables
actually live in the root of a container database, `cdc.tables.include` has to be rewritten by
hand.

## How does table.exclusion.regex map to OSO?

`table.exclusion.regex` maps to [`cdc.tables.exclude`](../reference/configuration/topics.md) by
the same rewrite as `table.inclusion.regex`. Confluent's exclusion expression is also
case-sensitive and uses the same fully qualified names. A pluggable-database expression is kept.
A non-container expression loses its leading database segment. The case-sensitive flag is set
when either pattern is written.

```json
"oracle.pdb.name": "ORCLPDB1",
"table.exclusion.regex": "ORCLPDB1[.]C##MYUSER[.]AUDIT_.*"
```

is written as:

```json
"cdc.database.pdbs": "ORCLPDB1",
"cdc.tables.exclude": "ORCLPDB1[.]C##MYUSER[.]AUDIT_.*",
"cdc.tables.case.sensitive": "true"
```

A comma in the exclusion expression is manual, for the same reason as in the include expression.

## Which Confluent properties have no OSO equivalent?

The tool's dropped and manual rows are the properties with no copied equivalent. Dropped means
the value is not written into the OSO configuration. Manual means the tool will not choose a
target, and the report asks for a decision. The table above is the full list. The groups below
are the ones that never become an OSO setting, and the values that stay manual even when a
neighbouring value is mapped.

Dropped for any non-blank value:

- `redo.log.topic.name`, `redo.log.consumer.*`, `redo.log.startup.polling.limit.ms`, `redo.log.row.poll.fields.include` and `redo.log.row.poll.fields.exclude`, because there is no redo log topic.
- `redo.log.poll.interval.ms`, `heartbeat.topic.name`, `use.transaction.begin.for.mining.session`, `redo.log.corruption.topic`, `oracle.dictionary.mode`, `record.buffer.mode`, `max.batch.timeout.ms` and `max.buffer.size`.
- `enable.large.lob.object.support`, `table.task.reconfig.checking.interval.ms`, `table.rps.logging.interval.ms`, `log.mining.end.scn.deviation.ms` and `snapshot.by.table.partitions`.
- `oracle.validation.result.fetch.size`, `oracle.supplemental.log.level`, `enable.metrics.collection`, `redo.log.initial.delay.interval.ms`, `confluent.license`, `confluent.topic`, `confluent.topic.*` and `connection.pool.*`.

Manual whenever the value is set:

- `ldap.security.principal` and `ldap.security.credentials`. LDAP authentication is not translated.
- `redo.log.row.poll.username.include`. There is no user include filter.
- `oracle.kerberos.cache.file`. Kerberos authentication is not built yet.
- `oracle.fan.events.enable` when the value is `true`. Real Application Clusters is not supported yet. Any other value is dropped.
- `db.timezone` and `db.timezone.date` when the zone is not one of the UTC spellings in the table. A UTC spelling is dropped, because DATE and TIMESTAMP values are read as UTC.
- Any property the tool does not know, and any `output.*` name outside the mapped field and operation list.

Manual for a particular value, while other values of the same property are mapped:

- A comma in `table.inclusion.regex` or `table.exclusion.regex`, or a database segment the tool cannot remove.
- A blank `table.topic.name.template`, a template that contains a backslash, or a template variable with no equivalent. The same variable limit applies to a non-blank `lob.topic.name.template`.
- `key.template`, `oracle.date.mapping`, `output.*`, a non-blank `lob.topic.name.template`, and `numeric.mapping` modes other than `none`, `best_fit` and `best_fit_or_decimal`, when `--format=debezium` is set. `numeric.default.scale` other than `127` is manual in that format. `127` is dropped.
- `retry.error.codes` entries that are not Oracle error numbers, and numeric properties whose value is not a whole number.

`start.from` does have a target, `cdc.snapshot.mode=none`. It does not have a target named
`start.scn`. The SCN is [`cdc.start.scn`](../reference/configuration/capture.md), as described
above.

## What does connector.class look like before and after?

A Confluent Platform configuration uses the Confluent class. Confluent's quick start sets
`tasks.max` to `2`. The names here illustrate the tool's default renaming.

```json
{
  "name": "orders-cdc",
  "config": {
    "connector.class": "io.confluent.connect.oracle.cdc.OracleCdcSourceConnector",
    "tasks.max": "2"
  }
}
```

The translated fragment is:

```json
{
  "name": "orders-cdc-oso",
  "config": {
    "connector.class": "sh.oso.connect.oracle.OracleCdcSourceConnector",
    "tasks.max": "1"
  }
}
```

The rest of a real configuration is the table above, plus the settings the tool adds. Record
shape, topic names and the Confluent-compatible format are described in
[Confluent-compatible records](confluent-compatible-records.md). On the default path the tool
keeps the existing table topics and flat records. `--format=debezium` writes new prefixed topics
and a different envelope, so consumers have to move. That choice, and the steps that stop the
old connector and start the new one, are on
[Migrating from Confluent Oracle CDC Source](../migration/from-confluent.md).

<Faq
  items={[
    {
      question: 'What is the connector.class for the Confluent Oracle CDC Source connector?',
      answer:
        'The connector.class value is io.confluent.connect.oracle.cdc.OracleCdcSourceConnector. Confluent\'s quick start for Confluent Platform sets that class on the Oracle CDC Source connector. The OSO connector class is sh.oso.connect.oracle.OracleCdcSourceConnector. Source: https://docs.confluent.io/kafka-connectors/oracle-cdc/current/getting-started.html',
    },
    {
      question: 'What is the OSO equivalent of start.scn?',
      answer:
        'The OSO property is cdc.start.scn. Confluent\'s configuration reference does not define a property named start.scn. The related setting is start.from, which the migration tool maps to cdc.snapshot.mode=none without copying the value. takeover_scn.py writes cdc.start.scn from the stopped connector\'s offset, and the connector honours it only while no offset is stored.',
    },
    {
      question: 'What is the OSO equivalent of table.inclusion.regex?',
      answer:
        'The OSO property is cdc.tables.include. The migration tool rewrites the expression as patterns over PDB.SCHEMA.TABLE, or SCHEMA.TABLE when there is no PDB, and sets cdc.tables.case.sensitive to true. table.exclusion.regex is rewritten the same way into cdc.tables.exclude.',
    },
    {
      question: 'Can I convert a Confluent Oracle CDC config automatically?',
      answer:
        'Yes. migrate_from_confluent.py reads a saved Confluent Oracle CDC Source configuration and classifies every property as mapped, mapped with a change, dropped, or manual. The cutover steps, including the SCN takeover, are on the migration from Confluent page.',
    },
    {
      question: 'Do the topic names or record formats change?',
      answer:
        'Not on the default path. The migration tool keeps Confluent\'s table topics, flat records, key templates and Schema Registry subjects. Passing --format=debezium writes new prefixed topics and a Debezium envelope with before, after, source and op, so consumers must move. This path is available from release 0.1.6.',
    },
  ]}
/>
