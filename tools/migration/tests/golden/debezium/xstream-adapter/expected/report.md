# Migration report: Debezium Oracle connector to the OSO CDC Connector

| Item | Value |
|---|---|
| Tool | `migrate_from_debezium.py` <version> |
| Source connector | `xstream-cdc` |
| New connector | `xstream-cdc-oso` |
| Exactly-once check | disabled: This worker does not have exactly-once source support enabled. |
| Exit code | 2, success with manual follow-ups |

14 source properties: 6 mapped, 6 mapped with a change, 1 dropped and 1 needing manual action. 4 follow-ups.

## Follow-ups

1. The source used the `xstream` adapter. This connector reads redo only through LogMiner: grant the LogMiner privileges and check supplemental logging with `oracle-cdc-doctor check` before migrating. The Debezium offset of this adapter may carry no SCN that `takeover_scn.py` can use. (`database.connection.adapter`)
2. Debezium published schema change events to the topic named after `topic.prefix`; this connector does not. Move or retire the consumers of that topic.
3. Set `cdc.start.scn` before the new connector first runs: stop the old connector, then run `takeover_scn.py`, which reads its offset, checks the archived logs and writes `cdc.start.scn` into this configuration. Without it the new connector starts at the current SCN and changes made since the old connector stopped are lost.
4. The target Connect cluster does not have exactly-once source support enabled (This worker does not have exactly-once source support enabled.), so the connector delivers at least once. To deliver exactly once, set `exactly.once.source.support=enabled` on every worker, then add `exactly.once.support=required` and `transaction.boundary=connector`.

## Settings added by the translator

| Property | Value | Reason |
|---|---|---|
| `tasks.max` | `1` | The connector always runs one task. |
| `cdc.topic.template` | `${prefix}.${schema}.${table}` | Keeps Debezium's topic names, `prefix.SCHEMA.TABLE`. The connector's own default adds the PDB name in a CDB. |
| `cdc.output.format` | `debezium` | The Debezium-compatible envelope, so consumers keep working (MIG-2). |
| `cdc.decimal.mode` | `precise` | Debezium's default, pinned (MIG-3). |
| `cdc.temporal.mode` | `adaptive` | Debezium's default, pinned (MIG-3). |
| `cdc.tombstones.on.delete` | `true` | Debezium's default, pinned (MIG-3). |
| `cdc.lob.mode` | `skip` | Debezium's default (`lob.enabled=false`), pinned (MIG-3). |
| `cdc.key.missing` | `none` | Debezium published tables without a primary key with a null key; the connector would otherwise reject them at validation (MIG-3). |

## Source properties

| Source property | Value | Classification | Target | Note |
|---|---|---|---|---|
| `connector.class` | `io.debezium.connector.oracle.OracleConnector` | mapped-with-change | `connector.class` = `sh.oso.connect.oracle.OracleCdcSourceConnector` | Replaced by the OSO CDC Connector class. |
| `database.connection.adapter` | `xstream` | manual |   | The source used the `xstream` adapter. This connector reads redo only through LogMiner: grant the LogMiner privileges and check supplemental logging with `oracle-cdc-doctor check` before migrating. The Debezium offset of this adapter may carry no SCN that `takeover_scn.py` can use. |
| `database.dbname` | `ORCLCDB` | mapped | `cdc.database.service` = `ORCLCDB` | Debezium connects with this name as the service name; in a CDB it names the root. |
| `database.hostname` | `xs-db` | mapped | `cdc.database.host` = `xs-db` |  |
| `database.out.server.name` | `dbzxout` | dropped |   | XStream outbound server; the connector reads redo through LogMiner. |
| `database.password` | `${env:ORACLE_PASSWORD}` | mapped | `cdc.database.password` = `${env:ORACLE_PASSWORD}` |  |
| `database.pdb.name` | `ORCLPDB1` | mapped | `cdc.database.pdbs` = `ORCLPDB1` |  |
| `database.port` | `1521` | mapped | `cdc.database.port` = `1521` |  |
| `database.user` | `c##xstrmadmin` | mapped | `cdc.database.user` = `c##xstrmadmin` | In a CDB this must be a common user with the grants from `oracle-cdc-doctor setup-sql`. |
| `log.mining.session.max.ms` | `0` | mapped-with-change |   | Not copied: the connector restarts the LogMiner session every `cdc.mining.session.max.age.ms` (default one hour) to release PGA memory. |
| `name` | `xstream-cdc` | mapped-with-change | `name` = `xstream-cdc-oso` | The new connector has its own name: Kafka Connect keeps offsets per connector name, and this connector does not read the Debezium offset format. |
| `schema.include.list` | `APP` | mapped-with-change | `cdc.tables.include` = `ORCLPDB1\.APP\..+` | Rewritten into `cdc.tables.include` or `cdc.tables.exclude` as patterns over `PDB.SCHEMA.TABLE` (or `SCHEMA.TABLE` without a PDB). |
| `snapshot.mode` | `no_data` | mapped-with-change | `cdc.snapshot.mode` = `none` | The takeover starts the new connector at the Debezium position (`cdc.start.scn` from takeover_scn.py), so no snapshot runs: `cdc.snapshot.mode=none`. For a fresh deployment instead, `no_data` corresponds to `none`. |
| `topic.prefix` | `xs` | mapped-with-change | `cdc.topic.prefix` = `xs`<br/>`cdc.topic.template` = `${prefix}.${schema}.${table}` | Topic prefix and logical server name. `cdc.topic.template` is pinned so the topic names stay `prefix.SCHEMA.TABLE`. |

## Next steps

1. Resolve the follow-ups, then run `oracle-cdc-doctor check --config config.json`.
2. Stop the Debezium connector with `PUT /connectors/xstream-cdc/stop`; pausing is not enough, the offset must be final.
3. Run `takeover_scn.py --target-config config.json` to read the Debezium offset, check the archived logs and write `cdc.start.scn` into the configuration.
4. Create `xstream-cdc-oso` from that configuration. With no stored offset it starts streaming at `cdc.start.scn`.
5. After the overlap has passed, run `verify_cutover.py` and archive the evidence file.
6. Point dashboards and alerts at the connector's JMX metrics; the names differ from Debezium's (see the metrics reference).
7. Keep the Debezium schema history topic until the cutover is verified, then delete it and the old connector's other internal topics after the agreed retention.
