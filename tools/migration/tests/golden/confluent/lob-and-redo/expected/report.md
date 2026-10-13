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

1. This release publishes the Debezium-compatible envelope for the selected --format=debezium (`before`, `after`, `source`, `op`). Records change shape and keys become structs, so use new prefixed topics and move consumers. Choose --format=confluent to keep the existing table topics, flat records and key templates.
2. The source logged and skipped rows it could not parse. The connector never skips silently: such rows go to the DLQ topic (`cdc.dlq.topic`) with an ops event. Make sure someone watches the DLQ. (`behavior.on.dictionary.mismatch`)
3. DATE and TIMESTAMP values are read as UTC in this release; there is no time zone setting. Consumers that expect another zone must convert. (`db.timezone`)
4. The LDAP naming URL became `cdc.database.url`. Test the connection with `oracle-cdc-doctor check`; LDAP authentication settings are not translated. (`ldap.url`)
5. Separate LOB topics require --format=confluent (MIG-5). With Debezium output, choose inline or reselect row values and change the LOB topic consumers. (`lob.topic.name.template`)
6. `numeric.mapping=best_fit_or_double` has no exact equivalent. `cdc.decimal.mode` offers `precise`, `string` (every NUMBER as text) and `double` (every NUMBER as a double); choose one. (`numeric.mapping`)
7. RAC is not supported in this release, so there are no FAN events to follow. Do not migrate a RAC database yet. (`oracle.fan.events.enable`)
8. Kerberos authentication is not built yet; use a password or a wallet. (`oracle.kerberos.cache.file`)
9. The template uses variables with no equivalent: `${staticMap[APP.ORDERS=orders]}`. `cdc.topic.template` knows `${prefix}`, `${pdb}`, `${schema}`, `${table}` and `${database}`. (`table.topic.name.template`)
10. The redo log topic (`redo.log.topic.name`, default `${connectorName}-${databaseName}-redo-log`) stops receiving data at the cutover (MIG-5). Move or retire its consumers, then delete it after the agreed retention.

## Settings added by the translator

| Property | Value | Reason |
|---|---|---|
| `name` | `orders-cdc-oso` | The new connector's name. |
| `cdc.output.format` | `debezium` | Explicitly selected Debezium-compatible envelope and new topics. |
| `cdc.topic.prefix` | `orders` | Names internal topics and the offset partition; table topics stay unchanged with Confluent output and are prefixed with Debezium output. |
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
| `lob.topic.name.template` | `${tableName}.${columnName}` | manual |   | Separate LOB topics require --format=confluent (MIG-5). With Debezium output, choose inline or reselect row values and change the LOB topic consumers. |
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
6. Retire the old redo log and internal topics after the agreed retention; keep table and LOB topics for the Confluent same-topics path.
