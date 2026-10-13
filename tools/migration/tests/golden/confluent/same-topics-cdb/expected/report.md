# Migration report: Confluent Oracle CDC Source connector to the OSO CDC Connector

| Item | Value |
|---|---|
| Tool | `migrate_from_confluent.py` <version> |
| Source connector | `orders-cdc` |
| New connector | `orders-cdc-oso` |
| Exactly-once check | not checked: no --connect-url given |
| Exit code | 2, success with manual follow-ups |

24 source properties: 19 mapped, 5 mapped with a change, 0 dropped and 0 needing manual action. 6 follow-ups.

## Follow-ups

1. Keep the existing topics and consumers. Check the documented differences: exact Decimal scales (rounding stops CDC-3001), additional cdc.* headers, SQL suppression with column exclusions, LOB key moves, and optional OSO truncate records.
2. Separate LOB topics require stable column keys, complete values and sufficient undo for AS OF commit reselect. OSO moves LOB state on key changes; Confluent 2.15.2 did not. (`lob.topic.name.template`)
3. OSO refuses NUMBER values needing rounding to fit numeric.default.scale, including snapshots (CDC-3001); Confluent snapshots rounded HALF_EVEN. Increase the scale before cutover when needed. (`numeric.default.scale`)
4. The redo log topic (`redo.log.topic.name`, default `${connectorName}-${databaseName}-redo-log`) stops receiving data at the cutover (MIG-5). Move or retire its consumers, then delete it after the agreed retention.
5. If the worker uses Avro or Protobuf, configure cdc.journal.converter and cdc.journal.converter.* to match before enabling cdc.kafka.bootstrap.servers.
6. Exactly-once delivery was not configured because the target Connect cluster was not checked (no --connect-url given). Run the translator again with `--connect-url`, or add `exactly.once.support=required` and `transaction.boundary=connector` yourself once the workers run with `exactly.once.source.support=enabled`.

## Settings added by the translator

| Property | Value | Reason |
|---|---|---|
| `name` | `orders-cdc-oso` | The new connector's name. |
| `cdc.output.format` | `confluent` | Preserves table topics, flat rows, key templates and Schema Registry subjects. |
| `cdc.topic.prefix` | `orders-cdc` | Names internal topics and the offset partition; table topics stay unchanged with Confluent output and are prefixed with Debezium output. |
| `cdc.tables.case.sensitive` | `true` | Confluent matches table expressions case-sensitively (MIG-3). |
| `cdc.key.missing` | `none` | Confluent writes null keys for tables without a column key. |
| `cdc.snapshot.mode` | `none` | A takeover streams from the old connector's position, without a snapshot. |
| `cdc.start.scn` | `1000` | The old connector's position (--start-scn); honoured while the new connector has no stored offset. |

## Source properties

| Source property | Value | Classification | Target | Note |
|---|---|---|---|---|
| `behavior.on.dictionary.mismatch` | `fail` | mapped | `cdc.on.decode.error` = `fail` |  |
| `behavior.on.unparsable.statement` | `fail` | mapped | `cdc.on.decode.error` = `fail` |  |
| `connector.class` | `io.confluent.connect.oracle.cdc.OracleCdcSourceConnector` | mapped-with-change | `connector.class` = `sh.oso.connect.oracle.OracleCdcSourceConnector` | Replaced by the OSO CDC Connector class. |
| `emit.tombstone.on.delete` | `true` | mapped | `cdc.tombstones.on.delete` = `true` |  |
| `heartbeat.interval.ms` | `30000` | mapped | `cdc.heartbeat.interval.ms` = `30000` |  |
| `key.template` | `${primaryKeyStruct}` | mapped | `cdc.output.confluent.key.template` = `${primaryKeyStruct}` | Confluent-compatible record setting preserved. |
| `lob.topic.name.template` | `${connectorName}.lob.${fullyQualifiedTableName}.${columnName}` | mapped | `cdc.lob.mode` = `topic`<br/>`cdc.lob.topic.template` = `orders-cdc.lob.${pdb}.${schema}.${table}.${column}` | Existing LOB topics and byte encodings preserved. |
| `log.mining.transaction.age.threshold.ms` | `7200000` | mapped-with-change |   | Not copied: the connector has no warn-only age limit. Long transactions are kept, spilled to disk and journaled, and their age shows in the metrics; set `cdc.transaction.max.age.ms` to stop or discard instead. |
| `log.mining.transaction.threshold.breached.action` | `fail` | mapped-with-change |   | Not copied: the connector has no warn-only age limit. Long transactions are kept, spilled to disk and journaled, and their age shows in the metrics; set `cdc.transaction.max.age.ms` to stop or discard instead. |
| `numeric.default.scale` | `4` | mapped | `cdc.output.confluent.numeric.default.scale` = `4` | Confluent-compatible record setting preserved. |
| `numeric.mapping` | `precision_only` | mapped | `cdc.output.confluent.numeric.mapping` = `precision_only` | Confluent-compatible record setting preserved. |
| `oracle.date.mapping` | `timestamp` | mapped | `cdc.output.confluent.date.mapping` = `timestamp` | Confluent-compatible record setting preserved. |
| `oracle.password` | `${file:/secrets/oracle.properties:password}` | mapped | `cdc.database.password` = `${file:/secrets/oracle.properties:password}` |  |
| `oracle.pdb.name` | `ORCLPDB1` | mapped | `cdc.database.pdbs` = `ORCLPDB1` |  |
| `oracle.port` | `1521` | mapped | `cdc.database.port` = `1521` |  |
| `oracle.server` | `db` | mapped | `cdc.database.host` = `db` |  |
| `oracle.sid` | `ORCLCDB` | mapped | `cdc.database.sid` = `ORCLCDB` |  |
| `oracle.username` | `C##MYUSER` | mapped | `cdc.database.user` = `C##MYUSER` | In a CDB this must be a common user with the grants from `oracle-cdc-doctor setup-sql`. |
| `output.before.state.field` | `before` | mapped | `cdc.output.confluent.before.state.field` = `before` | Confluent-compatible record setting preserved. |
| `output.commit.scn.field` | `meta.commit` | mapped | `cdc.output.confluent.commit.scn.field` = `meta.commit` | Confluent-compatible record setting preserved. |
| `output.op.type.field` | `header:operation` | mapped | `cdc.output.confluent.op.type.field` = `header:operation` | Confluent-compatible record setting preserved. |
| `table.inclusion.regex` | `ORCLPDB1\.APP\..*` | mapped-with-change | `cdc.tables.include` = `ORCLPDB1\.APP\..*` | Rewritten as patterns over `PDB.SCHEMA.TABLE` (or `SCHEMA.TABLE` without a PDB); matching stays case-sensitive (`cdc.tables.case.sensitive=true`). |
| `table.topic.name.template` | `${fullyQualifiedTableName}` | mapped-with-change | `cdc.topic.template` = `${pdb}.${schema}.${table}` | Template variables translated; check the result against the existing topic names. |
| `tasks.max` | `1` | mapped | `tasks.max` = `1` |  |

## Next steps

1. Resolve the follow-ups, above all the record format, then run `oracle-cdc-doctor check --config config.json`.
2. Stop the Confluent connector with `PUT /connectors/orders-cdc/stop`; pausing is not enough, the offset must be final.
3. Run `takeover_scn.py --target-config config.json` to read the Confluent offset, check the archived logs and write `cdc.start.scn` into the configuration.
4. Create `orders-cdc-oso` from that configuration. With no stored offset it starts streaming at `cdc.start.scn`.
5. Point dashboards and alerts at the connector's JMX metrics; the names differ from Confluent's (see the metrics reference).
6. Retire the old redo log and internal topics after the agreed retention; keep table and LOB topics for the Confluent same-topics path.
