# Migration report: Confluent Oracle CDC Source connector to the OSO CDC Connector

| Item | Value |
|---|---|
| Tool | `migrate_from_confluent.py` <version> |
| Source connector | `orders-cdc` |
| New connector | `orders-cdc-oso` |
| Exactly-once check | enabled: the worker accepted exactly.once.support=required |
| Exit code | 2, success with manual follow-ups |

22 source properties: 9 mapped, 7 mapped with a change, 0 dropped and 6 needing manual action. 10 follow-ups.

## Follow-ups

1. This release publishes the Debezium-compatible envelope only (`before`, `after`, `source`, `op`); the Confluent-compatible flat format (`cdc.output.format=confluent`) is not built yet. Records change shape and keys become structs of the key columns, so consumers and schemas of the existing table topics break. Point `cdc.topic.template` at new topic names and move consumers to the new format, or wait for the Confluent-compatible format before cutting over.
2. The source logged and skipped rows it could not parse. The connector never skips silently: such rows go to the DLQ topic (`cdc.dlq.topic`) with an ops event. Make sure someone watches the DLQ. (`behavior.on.dictionary.mismatch`)
3. DATE and TIMESTAMP values are read as UTC in this release; there is no time zone setting. Consumers that expect another zone must convert. (`db.timezone`)
4. The LDAP naming URL became `cdc.database.url`. Test the connection with `oracle-cdc-doctor check`; LDAP authentication settings are not translated. (`ldap.url`)
5. LOB topics are not built yet (MIG-5). Until they are, `cdc.lob.mode=inline` publishes LOB values inside the row records; consumers of the LOB topics must change. (`lob.topic.name.template`)
6. `numeric.mapping=best_fit_or_double` has no exact equivalent. `cdc.decimal.mode` offers `precise`, `string` (every NUMBER as text) and `double` (every NUMBER as a double); choose one. (`numeric.mapping`)
7. RAC is not supported in this release, so there are no FAN events to follow. Do not migrate a RAC database yet. (`oracle.fan.events.enable`)
8. Kerberos authentication is not built yet; use a password or a wallet. (`oracle.kerberos.cache.file`)
9. The template uses variables with no equivalent: `${staticMap[APP.ORDERS=orders]}`. `cdc.topic.template` knows `${prefix}`, `${pdb}`, `${schema}`, `${table}` and `${database}`. (`table.topic.name.template`)
10. The redo log topic (`redo.log.topic.name`, default `${connectorName}-${databaseName}-redo-log`) stops receiving data at the cutover (MIG-5). Move or retire its consumers, then delete it after the agreed retention.

## Settings added by the translator

| Property | Value | Reason |
|---|---|---|
| `name` | `orders-cdc-oso` | The new connector's name. |
| `cdc.output.format` | `debezium` | The only format in this release; Confluent's flat format is not built yet. |
| `cdc.topic.prefix` | `orders` | Names the connector's internal topics and its offset partition; Confluent table topic names do not use it. |
| `cdc.tables.case.sensitive` | `true` | Confluent matches table expressions case-sensitively (MIG-3). |
| `cdc.tombstones.on.delete` | `false` | Confluent's default (`emit.tombstone.on.delete=false`), pinned (MIG-3). |
| `cdc.decimal.mode` | `precise` | Confluent's default `numeric.mapping=none` kept numbers exact; `precise` does too (MIG-3). NUMBER(p,0) up to 18 digits become integers rather than Decimal bytes. |
| `cdc.lob.mode` | `skip` | LOB columns stay out of the records, as before. |
| `cdc.snapshot.mode` | `none` | A takeover streams from the old connector's position, without a snapshot. |
| `cdc.start.scn` | `2169287` | The old connector's position (--start-scn); honoured while the new connector has no stored offset. |
| `exactly.once.support` | `required` | The target Connect cluster has exactly-once source support enabled (MIG-4). |
| `transaction.boundary` | `connector` | The target Connect cluster has exactly-once source support enabled (MIG-4). |

## Source properties

| Source property | Value | Classification | Target | Note |
|---|---|---|---|---|
| `behavior.on.dictionary.mismatch` | `log` | mapped-with-change | `cdc.on.decode.error` = `dlq` | Undecodable rows go to the DLQ topic. |
| `behavior.on.unparsable.statement` | `fail` | mapped-with-change | `cdc.on.decode.error` = `dlq` | One decode setting for both cases; the other property asked to log and skip. |
| `connector.class` | `io.confluent.connect.oracle.cdc.OracleCdcSourceConnector` | mapped-with-change | `connector.class` = `sh.oso.connect.oracle.OracleCdcSourceConnector` | Replaced by the OSO CDC Connector class. |
| `db.timezone` | `Europe/London` | manual |   | DATE and TIMESTAMP values are read as UTC in this release; there is no time zone setting. Consumers that expect another zone must convert. |
| `heartbeat.interval.ms` | `30000` | mapped | `cdc.heartbeat.interval.ms` = `30000` |  |
| `ldap.url` | `ldap://ldap.example.com:389/orclpdb1,cn=OracleContext,dc=example,dc=com` | mapped-with-change | `cdc.database.url` = `jdbc:oracle:thin:@ldap://ldap.example.com:389/orclpdb1,cn=OracleContext,dc=example,dc=com` | Connect through LDAP naming in the JDBC URL. |
| `lob.topic.name.template` | `${tableName}.${columnName}` | manual |   | LOB topics are not built yet (MIG-5). Until they are, `cdc.lob.mode=inline` publishes LOB values inside the row records; consumers of the LOB topics must change. |
| `log.mining.transaction.age.threshold.ms` | `7200000` | mapped | `cdc.transaction.max.age.ms` = `7200000` | A discarded transaction's details go to the ops topic and the DLQ. |
| `log.mining.transaction.threshold.breached.action` | `discard` | mapped | `cdc.transaction.max.age.action` = `discard` | A discarded transaction's details go to the ops topic and the DLQ. |
| `numeric.mapping` | `best_fit_or_double` | manual |   | `numeric.mapping=best_fit_or_double` has no exact equivalent. `cdc.decimal.mode` offers `precise`, `string` (every NUMBER as text) and `double` (every NUMBER as a double); choose one. |
| `oracle.fan.events.enable` | `true` | manual |   | RAC is not supported in this release, so there are no FAN events to follow. Do not migrate a RAC database yet. |
| `oracle.kerberos.cache.file` | `/tmp/krb5cc` | manual |   | Kerberos authentication is not built yet; use a password or a wallet. |
| `oracle.password` | `${file:/secrets/oracle.properties:password}` | mapped | `cdc.database.password` = `${file:/secrets/oracle.properties:password}` |  |
| `oracle.pdb.name` | `ORCLPDB1` | mapped | `cdc.database.pdbs` = `ORCLPDB1` |  |
| `oracle.port` | `1521` | mapped | `cdc.database.port` = `1521` |  |
| `oracle.server` | `db` | mapped | `cdc.database.host` = `db` |  |
| `oracle.sid` | `ORCLCDB` | mapped | `cdc.database.sid` = `ORCLCDB` |  |
| `oracle.username` | `C##MYUSER` | mapped | `cdc.database.user` = `C##MYUSER` | In a CDB this must be a common user with the grants from `oracle-cdc-doctor setup-sql`. |
| `retry.error.codes` | `12345, ORA-1089` | mapped-with-change | `cdc.retry.extra.error.codes` = `ORA-12345,ORA-01089` | Written as ORA-nnnnn codes. |
| `table.inclusion.regex` | `ORCLPDB1\.APP\..*` | mapped-with-change | `cdc.tables.include` = `ORCLPDB1\.APP\..*` | Rewritten as patterns over `PDB.SCHEMA.TABLE` (or `SCHEMA.TABLE` without a PDB); matching stays case-sensitive (`cdc.tables.case.sensitive=true`). |
| `table.topic.name.template` | `${staticMap[APP.ORDERS=orders]}` | manual |   | The template uses variables with no equivalent: `${staticMap[APP.ORDERS=orders]}`. `cdc.topic.template` knows `${prefix}`, `${pdb}`, `${schema}`, `${table}` and `${database}`. |
| `tasks.max` | `2` | mapped-with-change | `tasks.max` = `1` | Reduced to one (MIG-5). Confluent spread table topics over tasks that all read its redo log topic; this connector mines every table in one task with one LogMiner session, so more tasks would add nothing. A higher value is accepted and ignored. |

## Next steps

1. Resolve the follow-ups, above all the record format, then run `oracle-cdc-doctor check --config config.json`.
2. Stop the Confluent connector with `PUT /connectors/orders-cdc/stop`; pausing is not enough, the offset must be final.
3. Run `takeover_scn.py --target-config config.json` to read the Confluent offset, check the archived logs and write `cdc.start.scn` into the configuration.
4. Create `orders-cdc-oso` from that configuration. With no stored offset it starts streaming at `cdc.start.scn`.
5. Point dashboards and alerts at the connector's JMX metrics; the names differ from Confluent's (see the metrics reference).
6. Remove the redo log topic and the old connector's other topics after the agreed retention.
