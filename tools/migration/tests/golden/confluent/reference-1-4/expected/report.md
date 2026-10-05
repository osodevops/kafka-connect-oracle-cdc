# Migration report: Confluent Oracle CDC Source connector to the OSO CDC Connector

| Item | Value |
|---|---|
| Tool | `migrate_from_confluent.py` <version> |
| Source connector | `confluent-oracle-cdc` |
| New connector | `confluent-oracle-cdc-oso` |
| Exactly-once check | disabled: This worker does not have exactly-once source support enabled. |
| Exit code | 2, success with manual follow-ups |

98 source properties: 20 mapped, 9 mapped with a change, 55 dropped and 14 needing manual action. 8 follow-ups.

## Follow-ups

1. This release publishes the Debezium-compatible envelope only (`before`, `after`, `source`, `op`); the Confluent-compatible flat format (`cdc.output.format=confluent`) is not built yet. Records change shape and keys become structs of the key columns, so consumers and schemas of the existing table topics break. Point `cdc.topic.template` at new topic names and move consumers to the new format, or wait for the Confluent-compatible format before cutting over.
2. Record keys are structs of the key columns (Debezium style). Confluent's key template (default `${primaryKeyStructOrValue}`, a plain value for a single-column key) has no equivalent; consumers and compacted topics keyed by the old form must change. (`key.template`)
3. DATE columns are published as `io.debezium.time.Timestamp` (milliseconds) with the time of day kept, or as ISO text with `cdc.temporal.mode=iso_string`. Consumers that read Confluent's DATE mapping must change. (`oracle.date.mapping`)
4. Part of Confluent's record format, which this release does not produce. See the record format follow-up. (`output.current.ts.field`, `output.op.ts.field`, `output.op.type.delete.value`, `output.op.type.field`, `output.op.type.insert.value`, `output.op.type.read.value`, `output.op.type.truncate.value`, `output.op.type.update.value`, `output.row.id.field`, `output.scn.field`, `output.table.name.field`, `output.username.field`)
5. Confluent's documentation describes `${databaseName}` and `${fullyQualifiedTableName}` loosely. Compare the topic names the translated `cdc.topic.template` produces with the existing topics before cutting over.
6. The redo log topic (`redo.log.topic.name`, default `${connectorName}-${databaseName}-redo-log`) stops receiving data at the cutover (MIG-5). Move or retire its consumers, then delete it after the agreed retention.
7. Set `cdc.start.scn` before the new connector first runs: stop the old connector, then run `takeover_scn.py`, which reads its offset, checks the archived logs and writes `cdc.start.scn` into this configuration. Without it the new connector starts at the current SCN and changes made since the old connector stopped are lost.
8. The target Connect cluster does not have exactly-once source support enabled (This worker does not have exactly-once source support enabled.), so the connector delivers at least once. To deliver exactly once, set `exactly.once.source.support=enabled` on every worker, then add `exactly.once.support=required` and `transaction.boundary=connector`.

## Settings added by the translator

| Property | Value | Reason |
|---|---|---|
| `name` | `confluent-oracle-cdc-oso` | The new connector's name. |
| `cdc.output.format` | `debezium` | The only format in this release; Confluent's flat format is not built yet. |
| `cdc.topic.prefix` | `confluent-oracle-cdc` | Names the connector's internal topics and its offset partition; Confluent table topic names do not use it. |
| `cdc.tables.case.sensitive` | `true` | Confluent matches table expressions case-sensitively (MIG-3). |
| `cdc.lob.mode` | `skip` | LOB columns stay out of the records, as before. |

## Source properties

| Source property | Value | Classification | Target | Note |
|---|---|---|---|---|
| `behavior.on.dictionary.mismatch` | `fail` | mapped | `cdc.on.decode.error` = `fail` |  |
| `behavior.on.unparsable.statement` | `fail` | mapped | `cdc.on.decode.error` = `fail` |  |
| `confluent.license` | `********` | dropped |   | Empty in the source, so there is nothing to carry over. |
| `confluent.topic.bootstrap.servers` | `kafka:9092` | dropped |   | Confluent licence topic settings; not used. |
| `confluent.topic.replication.factor` | `3` | dropped |   | Confluent licence topic settings; not used. |
| `connection.pool.initial.size` | `0` | dropped |   | The connector manages its own connections (MIG-5); driver options go in `cdc.database.connection.properties`. |
| `connection.pool.max.size` | `1` | dropped |   | The connector manages its own connections (MIG-5); driver options go in `cdc.database.connection.properties`. |
| `connection.pool.min.size` | `0` | dropped |   | The connector manages its own connections (MIG-5); driver options go in `cdc.database.connection.properties`. |
| `connector.class` | `io.confluent.connect.oracle.cdc.OracleCdcSourceConnector` | mapped-with-change | `connector.class` = `sh.oso.connect.oracle.OracleCdcSourceConnector` | Replaced by the OSO CDC Connector class. |
| `db.timezone` | `UTC` | dropped |   | DATE and TIMESTAMP values are read as UTC. |
| `db.timezone.date` | `UTC` | dropped |   | DATE and TIMESTAMP values are read as UTC. |
| `emit.tombstone.on.delete` | `false` | mapped | `cdc.tombstones.on.delete` = `false` |  |
| `enable.large.lob.object.support` | `false` | dropped |   | `cdc.lob.max.bytes` limits LOB values (default 1 MiB). |
| `enable.metrics.collection` | `false` | dropped |   | JMX metrics are always on. |
| `heartbeat.interval.ms` | `0` | mapped-with-change |   | Not copied: the connector's heartbeats (default every 10 seconds) keep offsets moving on a quiet database and never write to the source database. |
| `heartbeat.topic.name` | `${connectorName}-${databaseName}-heartbeat-topic` | dropped |   | Heartbeat records go to `cdc.heartbeat.topic` (default `${prefix}.cdc.heartbeat`) in the connector's own format. |
| `key.converter` | `io.confluent.connect.avro.AvroConverter` | mapped | `key.converter` = `io.confluent.connect.avro.AvroConverter` | Kafka Connect framework property, copied unchanged. |
| `key.template` | `${primaryKeyStructOrValue}` | manual |   | Record keys are structs of the key columns (Debezium style). Confluent's key template (default `${primaryKeyStructOrValue}`, a plain value for a single-column key) has no equivalent; consumers and compacted topics keyed by the old form must change. |
| `ldap.security.credentials` | `********` | dropped |   | Empty in the source, so there is nothing to carry over. |
| `ldap.security.principal` | (empty) | dropped |   | Empty in the source, so there is nothing to carry over. |
| `ldap.url` | (empty) | dropped |   | Empty in the source, so there is nothing to carry over. |
| `lob.topic.name.template` | (empty) | dropped |   | LOB columns were not captured; `cdc.lob.mode=skip` keeps them out of the records. |
| `log.mining.archive.destination.name` | (empty) | dropped |   | Empty in the source, so there is nothing to carry over. |
| `log.mining.end.scn.deviation.ms` | `0` | dropped |   | A RAC setting; RAC is not supported in this release. |
| `log.mining.transaction.age.threshold.ms` | `-1` | dropped |   | No age limit, which is also the connector's default. |
| `log.mining.transaction.threshold.breached.action` | `warn` | dropped |   | No age limit, which is also the connector's default. |
| `log.sensitive.data` | `false` | mapped | `cdc.log.sensitive.data` = `false` |  |
| `max.batch.size` | `1000` | mapped | `cdc.poll.max.records` = `1000` |  |
| `max.batch.timeout.ms` | `60000` | dropped |   | Deprecated in the source; not used. |
| `max.buffer.size` | `0` | dropped |   | The connector buffers in its own transaction buffer. |
| `max.retry.time.ms` | `86400000` | mapped | `cdc.retry.max.time.ms` = `86400000` |  |
| `numeric.default.scale` | `127` | dropped |   | Unconstrained NUMBER values are published as a variable scale decimal, with their own scale. |
| `numeric.mapping` | `none` | mapped-with-change | `cdc.decimal.mode` = `precise` | Kept exact (MIG-3). Confluent published every NUMBER as a Connect Decimal (bytes); `precise` publishes NUMBER(p,0) up to 18 digits as integers, other constrained NUMBER as Decimal and unconstrained NUMBER as a variable scale decimal struct. |
| `oracle.date.mapping` | `date` | manual |   | DATE columns are published as `io.debezium.time.Timestamp` (milliseconds) with the time of day kept, or as ISO text with `cdc.temporal.mode=iso_string`. Consumers that read Confluent's DATE mapping must change. |
| `oracle.dictionary.mode` | `auto` | dropped |   | The connector mines with the online catalog and replays a step with a dictionary from the redo when a row predates a later DDL (`cdc.dictionary.build.*`). |
| `oracle.fan.events.enable` | `false` | dropped |   | Off in the source as well. |
| `oracle.kerberos.cache.file` | (empty) | dropped |   | Empty in the source, so there is nothing to carry over. |
| `oracle.password` | `${file:/opt/secrets/oracle.properties:password}` | mapped | `cdc.database.password` = `${file:/opt/secrets/oracle.properties:password}` |  |
| `oracle.pdb.name` | `ORCLPDB1` | mapped | `cdc.database.pdbs` = `ORCLPDB1` |  |
| `oracle.port` | `1521` | mapped | `cdc.database.port` = `1521` |  |
| `oracle.server` | `oracle.example.com` | mapped | `cdc.database.host` = `oracle.example.com` |  |
| `oracle.service.name` | `ORCLCDB` | mapped | `cdc.database.service` = `ORCLCDB` |  |
| `oracle.sid` | `ORCLCDB` | mapped | `cdc.database.sid` = `ORCLCDB` |  |
| `oracle.ssl.truststore.file` | (empty) | dropped |   | Empty in the source, so there is nothing to carry over. |
| `oracle.ssl.truststore.password` | `********` | dropped |   | Empty in the source, so there is nothing to carry over. |
| `oracle.supplemental.log.level` | `full` | dropped |   | `oracle-cdc-doctor check` verifies supplemental logging per captured table. |
| `oracle.username` | `C##MYUSER` | mapped | `cdc.database.user` = `C##MYUSER` | In a CDB this must be a common user with the grants from `oracle-cdc-doctor setup-sql`. |
| `oracle.validation.result.fetch.size` | `5000` | dropped |   | Validation is the doctor's fast mode. |
| `output.before.state.field` | (empty) | dropped |   | Empty in the source, so there is nothing to carry over. |
| `output.commit.scn.field` | (empty) | dropped |   | Empty in the source, so there is nothing to carry over. |
| `output.current.ts.field` | `current_ts` | manual |   | Part of Confluent's record format, which this release does not produce. See the record format follow-up. |
| `output.op.ts.field` | `op_ts` | manual |   | Part of Confluent's record format, which this release does not produce. See the record format follow-up. |
| `output.op.type.delete.value` | `D` | manual |   | Part of Confluent's record format, which this release does not produce. See the record format follow-up. |
| `output.op.type.field` | `op_type` | manual |   | Part of Confluent's record format, which this release does not produce. See the record format follow-up. |
| `output.op.type.insert.value` | `I` | manual |   | Part of Confluent's record format, which this release does not produce. See the record format follow-up. |
| `output.op.type.read.value` | `R` | manual |   | Part of Confluent's record format, which this release does not produce. See the record format follow-up. |
| `output.op.type.truncate.value` | `T` | manual |   | Part of Confluent's record format, which this release does not produce. See the record format follow-up. |
| `output.op.type.update.value` | `U` | manual |   | Part of Confluent's record format, which this release does not produce. See the record format follow-up. |
| `output.redo.field` | (empty) | dropped |   | Empty in the source, so there is nothing to carry over. |
| `output.row.id.field` | `row_id` | manual |   | Part of Confluent's record format, which this release does not produce. See the record format follow-up. |
| `output.scn.field` | `scn` | manual |   | Part of Confluent's record format, which this release does not produce. See the record format follow-up. |
| `output.table.name.field` | `table` | manual |   | Part of Confluent's record format, which this release does not produce. See the record format follow-up. |
| `output.undo.field` | (empty) | dropped |   | Empty in the source, so there is nothing to carry over. |
| `output.username.field` | `username` | manual |   | Part of Confluent's record format, which this release does not produce. See the record format follow-up. |
| `poll.linger.ms` | `5000` | mapped | `cdc.poll.linger.ms` = `5000` |  |
| `query.timeout.ms` | `300000` | mapped-with-change | `cdc.mining.query.timeout.ms` = `300000` | Applies to each mining step; on expiry the step is retried with a smaller window. |
| `record.buffer.mode` | `connector` | dropped |   | The connector buffers in its own transaction buffer. |
| `redo.log.consumer.bootstrap.servers` | `kafka:9092` | dropped |   | There is no redo log topic, so its consumer settings do not apply. |
| `redo.log.consumer.fetch.max.bytes` | `52428800` | dropped |   | There is no redo log topic, so its consumer settings do not apply. |
| `redo.log.consumer.fetch.min.bytes` | `1` | dropped |   | There is no redo log topic, so its consumer settings do not apply. |
| `redo.log.consumer.max.partition.fetch.bytes` | `1048576` | dropped |   | There is no redo log topic, so its consumer settings do not apply. |
| `redo.log.consumer.max.poll.records` | `500` | dropped |   | There is no redo log topic, so its consumer settings do not apply. |
| `redo.log.consumer.receive.buffer.bytes` | `65536` | dropped |   | There is no redo log topic, so its consumer settings do not apply. |
| `redo.log.consumer.request.timeout.ms` | `30000` | dropped |   | There is no redo log topic, so its consumer settings do not apply. |
| `redo.log.consumer.send.buffer.bytes` | `131072` | dropped |   | There is no redo log topic, so its consumer settings do not apply. |
| `redo.log.corruption.topic` | (empty) | dropped |   | Empty in the source, so there is nothing to carry over. |
| `redo.log.initial.delay.interval.ms` | `0` | dropped |   | Streaming starts with the task. |
| `redo.log.poll.interval.ms` | `1000` | dropped |   | The mining window adapts to `cdc.mining.target.latency.ms` (default two seconds). |
| `redo.log.row.fetch.size` | `1000` | mapped | `cdc.mining.fetch.size` = `1000` |  |
| `redo.log.row.poll.fields.exclude` | (empty) | dropped |   | Empty in the source, so there is nothing to carry over. |
| `redo.log.row.poll.fields.include` | (empty) | dropped |   | Empty in the source, so there is nothing to carry over. |
| `redo.log.row.poll.username.exclude` | `GGADMIN` | mapped | `cdc.users.exclude` = `GGADMIN` |  |
| `redo.log.row.poll.username.include` | (empty) | dropped |   | Empty in the source, so there is nothing to carry over. |
| `redo.log.startup.polling.limit.ms` | `300000` | dropped |   | There is no redo log topic, so its consumer settings do not apply. |
| `redo.log.topic.name` | `${connectorName}-${databaseName}-redo-log` | dropped |   | There is no redo log topic; see the follow-up. |
| `retry.error.codes` | (empty) | dropped |   | Empty in the source, so there is nothing to carry over. |
| `snapshot.by.table.partitions` | `false` | dropped |   | Snapshots are split into chunks of `cdc.snapshot.chunk.rows` rows. |
| `snapshot.row.fetch.size` | `2000` | mapped | `cdc.snapshot.fetch.size` = `2000` |  |
| `snapshot.threads.per.task` | `4` | mapped | `cdc.snapshot.threads` = `4` |  |
| `start.from` | `snapshot` | mapped-with-change | `cdc.snapshot.mode` = `none` | The takeover starts the new connector at the Confluent position (`cdc.start.scn` from takeover_scn.py), so no snapshot runs: `cdc.snapshot.mode=none`. |
| `table.exclusion.regex` | `ORCLPDB1[.]C##MYUSER[.]ORDERS_TMP` | mapped-with-change | `cdc.tables.exclude` = `ORCLPDB1[.]C##MYUSER[.]ORDERS_TMP` | Rewritten as patterns over `PDB.SCHEMA.TABLE` (or `SCHEMA.TABLE` without a PDB); matching stays case-sensitive (`cdc.tables.case.sensitive=true`). |
| `table.inclusion.regex` | `ORCLPDB1[.]C##MYUSER[.](ORDERS\|CUSTOMERS)` | mapped-with-change | `cdc.tables.include` = `ORCLPDB1[.]C##MYUSER[.](ORDERS\|CUSTOMERS)` | Rewritten as patterns over `PDB.SCHEMA.TABLE` (or `SCHEMA.TABLE` without a PDB); matching stays case-sensitive (`cdc.tables.case.sensitive=true`). |
| `table.rps.logging.interval.ms` | `60000` | dropped |   | Per-table rates are JMX metrics. |
| `table.task.reconfig.checking.interval.ms` | `300000` | dropped |   | One task captures every table, so there is no table placement to rebalance. |
| `table.topic.name.template` | `${databaseName}.${schemaName}.${tableName}` | mapped-with-change | `cdc.topic.template` = `${pdb}.${schema}.${table}` | Template variables translated; check the result against the existing topic names. |
| `tasks.max` | `8` | mapped-with-change | `tasks.max` = `1` | Reduced to one (MIG-5). Confluent spread table topics over tasks that all read its redo log topic; this connector mines every table in one task with one LogMiner session, so more tasks would add nothing. A higher value is accepted and ignored. |
| `use.transaction.begin.for.mining.session` | `true` | dropped |   | Always on: the connector restarts from the oldest open transaction. |
| `value.converter` | `io.confluent.connect.avro.AvroConverter` | mapped | `value.converter` = `io.confluent.connect.avro.AvroConverter` | Kafka Connect framework property, copied unchanged. |

## Next steps

1. Resolve the follow-ups, above all the record format, then run `oracle-cdc-doctor check --config config.json`.
2. Stop the Confluent connector with `PUT /connectors/confluent-oracle-cdc/stop`; pausing is not enough, the offset must be final.
3. Run `takeover_scn.py --target-config config.json` to read the Confluent offset, check the archived logs and write `cdc.start.scn` into the configuration.
4. Create `confluent-oracle-cdc-oso` from that configuration. With no stored offset it starts streaming at `cdc.start.scn`.
5. Point dashboards and alerts at the connector's JMX metrics; the names differ from Confluent's (see the metrics reference).
6. Remove the redo log topic and the old connector's other topics after the agreed retention.
