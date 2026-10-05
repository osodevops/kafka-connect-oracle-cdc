# Migration report: Confluent Oracle CDC Source connector to the OSO CDC Connector

| Item | Value |
|---|---|
| Tool | `migrate_from_confluent.py` <version> |
| Source connector | `SimpleOracleCDC` |
| New connector | `SimpleOracleCDC-oso` |
| Exactly-once check | not checked: no --connect-url given |
| Exit code | 2, success with manual follow-ups |

16 source properties: 7 mapped, 4 mapped with a change, 4 dropped and 1 needing manual action. 7 follow-ups.

## Follow-ups

1. This release publishes the Debezium-compatible envelope only (`before`, `after`, `source`, `op`); the Confluent-compatible flat format (`cdc.output.format=confluent`) is not built yet. Records change shape and keys become structs of the key columns, so consumers and schemas of the existing table topics break. Point `cdc.topic.template` at new topic names and move consumers to the new format, or wait for the Confluent-compatible format before cutting over.
2. The translator does not know this property. Check what it did in the source connector and set the corresponding `cdc.*` property by hand, or leave it out. (`_table.topic.name.template_`)
3. Confluent's documentation describes `${databaseName}` and `${fullyQualifiedTableName}` loosely. Compare the topic names the translated `cdc.topic.template` produces with the existing topics before cutting over.
4. The redo log topic (`redo.log.topic.name`, default `${connectorName}-${databaseName}-redo-log`) stops receiving data at the cutover (MIG-5). Move or retire its consumers, then delete it after the agreed retention.
5. Without `oracle.pdb.name` the translator assumed a non-CDB database and removed the database name from the table expressions. If the tables live in the root container of a CDB, rewrite `cdc.tables.include` by hand.
6. Set `cdc.start.scn` before the new connector first runs: stop the old connector, then run `takeover_scn.py`, which reads its offset, checks the archived logs and writes `cdc.start.scn` into this configuration. Without it the new connector starts at the current SCN and changes made since the old connector stopped are lost.
7. Exactly-once delivery was not configured because the target Connect cluster was not checked (no --connect-url given). Run the translator again with `--connect-url`, or add `exactly.once.support=required` and `transaction.boundary=connector` yourself once the workers run with `exactly.once.source.support=enabled`.

## Settings added by the translator

| Property | Value | Reason |
|---|---|---|
| `name` | `SimpleOracleCDC-oso` | The new connector's name. |
| `cdc.output.format` | `debezium` | The only format in this release; Confluent's flat format is not built yet. |
| `cdc.topic.prefix` | `SimpleOracleCDC` | Names the connector's internal topics and its offset partition; Confluent table topic names do not use it. |
| `cdc.tables.case.sensitive` | `true` | Confluent matches table expressions case-sensitively (MIG-3). |
| `cdc.tombstones.on.delete` | `false` | Confluent's default (`emit.tombstone.on.delete=false`), pinned (MIG-3). |
| `cdc.decimal.mode` | `precise` | Confluent's default `numeric.mapping=none` kept numbers exact; `precise` does too (MIG-3). NUMBER(p,0) up to 18 digits become integers rather than Decimal bytes. |
| `cdc.lob.mode` | `skip` | LOB columns stay out of the records, as before. |

## Source properties

| Source property | Value | Classification | Target | Note |
|---|---|---|---|---|
| `_table.topic.name.template_` | `Using template vars to set change event topic for each table` | manual |   | The translator does not know this property. Check what it did in the source connector and set the corresponding `cdc.*` property by hand, or leave it out. |
| `confluent.topic.replication.factor` | `1` | dropped |   | Confluent licence topic settings; not used. |
| `connection.pool.max.size` | `20` | dropped |   | The connector manages its own connections (MIG-5); driver options go in `cdc.database.connection.properties`. |
| `connector.class` | `io.confluent.connect.oracle.cdc.OracleCdcSourceConnector` | mapped-with-change | `connector.class` = `sh.oso.connect.oracle.OracleCdcSourceConnector` | Replaced by the OSO CDC Connector class. |
| `oracle.password` | `${file:/secrets/oracle.properties:password}` | mapped | `cdc.database.password` = `${file:/secrets/oracle.properties:password}` |  |
| `oracle.port` | `1521` | mapped | `cdc.database.port` = `1521` |  |
| `oracle.server` | `oracle` | mapped | `cdc.database.host` = `oracle` |  |
| `oracle.sid` | `ORCLDB` | mapped | `cdc.database.sid` = `ORCLDB` |  |
| `oracle.username` | `MYUSER` | mapped | `cdc.database.user` = `MYUSER` | In a CDB this must be a common user with the grants from `oracle-cdc-doctor setup-sql`. |
| `redo.log.consumer.bootstrap.servers` | `kafka:9092` | dropped |   | There is no redo log topic, so its consumer settings do not apply. |
| `redo.log.row.fetch.size` | `1` | mapped | `cdc.mining.fetch.size` = `1` |  |
| `redo.log.topic.name` | `oracle-redo-log-topic` | dropped |   | There is no redo log topic; see the follow-up. |
| `start.from` | `snapshot` | mapped-with-change | `cdc.snapshot.mode` = `none` | The takeover starts the new connector at the Confluent position (`cdc.start.scn` from takeover_scn.py), so no snapshot runs: `cdc.snapshot.mode=none`. |
| `table.inclusion.regex` | `ORCLDB[.]MYUSER[.](ORDERS\|CUSTOMERS)\|ORCLDB[.]MYUSER[.]INVOICES` | mapped-with-change | `cdc.tables.include` = `MYUSER[.](ORDERS\|CUSTOMERS),MYUSER[.]INVOICES` | Rewritten as patterns over `PDB.SCHEMA.TABLE` (or `SCHEMA.TABLE` without a PDB); matching stays case-sensitive (`cdc.tables.case.sensitive=true`). |
| `table.topic.name.template` | `${databaseName}.${schemaName}.${tableName}` | mapped-with-change | `cdc.topic.template` = `${database}.${schema}.${table}` | Template variables translated; check the result against the existing topic names. |
| `tasks.max` | `1` | mapped | `tasks.max` = `1` |  |

## Next steps

1. Resolve the follow-ups, above all the record format, then run `oracle-cdc-doctor check --config config.json`.
2. Stop the Confluent connector with `PUT /connectors/SimpleOracleCDC/stop`; pausing is not enough, the offset must be final.
3. Run `takeover_scn.py --target-config config.json` to read the Confluent offset, check the archived logs and write `cdc.start.scn` into the configuration.
4. Create `SimpleOracleCDC-oso` from that configuration. With no stored offset it starts streaming at `cdc.start.scn`.
5. Point dashboards and alerts at the connector's JMX metrics; the names differ from Confluent's (see the metrics reference).
6. Remove the redo log topic and the old connector's other topics after the agreed retention.
