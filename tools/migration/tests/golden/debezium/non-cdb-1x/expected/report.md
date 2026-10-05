# Migration report: Debezium Oracle connector to the OSO CDC Connector

| Item | Value |
|---|---|
| Tool | `migrate_from_debezium.py` <version> |
| Source connector | `legacy-oracle` |
| New connector | `legacy-oracle-oso` |
| Exactly-once check | not checked: no --connect-url given |
| Exit code | 2, success with manual follow-ups |

20 source properties: 8 mapped, 10 mapped with a change, 2 dropped and 0 needing manual action. 4 follow-ups.

## Follow-ups

1. The schema history topic is no longer needed: the connector keeps table schema versions in its own compacted schema topic (`cdc.schema.topic`). Keep the history topic until the cutover is verified, then delete it.
2. Debezium published schema change events to the topic named after `topic.prefix`; this connector does not. Move or retire the consumers of that topic.
3. Set `cdc.start.scn` before the new connector first runs: stop the old connector, then run `takeover_scn.py`, which reads its offset, checks the archived logs and writes `cdc.start.scn` into this configuration. Without it the new connector starts at the current SCN and changes made since the old connector stopped are lost.
4. Exactly-once delivery was not configured because the target Connect cluster was not checked (no --connect-url given). Run the translator again with `--connect-url`, or add `exactly.once.support=required` and `transaction.boundary=connector` yourself once the workers run with `exactly.once.source.support=enabled`.

## Settings added by the translator

| Property | Value | Reason |
|---|---|---|
| `cdc.topic.template` | `${prefix}.${schema}.${table}` | Keeps Debezium's topic names, `prefix.SCHEMA.TABLE`. The connector's own default adds the PDB name in a CDB. |
| `cdc.output.format` | `debezium` | The Debezium-compatible envelope, so consumers keep working (MIG-2). |
| `cdc.lob.mode` | `skip` | Debezium's default (`lob.enabled=false`), pinned (MIG-3). |
| `cdc.key.missing` | `none` | Debezium published tables without a primary key with a null key; the connector would otherwise reject them at validation (MIG-3). |
| `cdc.schema.name.adjustment.mode` | `avro` | Debezium 1.x adjusted schema names for Avro by default; pinned so the record names, and the schemas registered under the existing subjects, stay the same. |

## Source properties

| Source property | Value | Classification | Target | Note |
|---|---|---|---|---|
| `connector.class` | `io.debezium.connector.oracle.OracleConnector` | mapped-with-change | `connector.class` = `sh.oso.connect.oracle.OracleCdcSourceConnector` | Replaced by the OSO CDC Connector class. |
| `database.dbname` | `LEGACY` | mapped | `cdc.database.service` = `LEGACY` | Debezium connects with this name as the service name; in a CDB it names the root. |
| `database.history.kafka.bootstrap.servers` | `kafka:9092` | dropped |   | Schema history is replaced by the connector's schema topic, created on start. |
| `database.history.kafka.topic` | `legacy.history` | dropped |   | Schema history is replaced by the connector's schema topic, created on start. |
| `database.hostname` | `legacy-db` | mapped | `cdc.database.host` = `legacy-db` |  |
| `database.password` | `${file:/etc/kafka/oracle.properties:password}` | mapped | `cdc.database.password` = `${file:/etc/kafka/oracle.properties:password}` |  |
| `database.port` | `1521` | mapped | `cdc.database.port` = `1521` |  |
| `database.server.name` | `legacy` | mapped-with-change | `cdc.topic.prefix` = `legacy`<br/>`cdc.topic.template` = `${prefix}.${schema}.${table}` | Topic prefix and logical server name. `cdc.topic.template` is pinned so the topic names stay `prefix.SCHEMA.TABLE`. |
| `database.user` | `dbzuser` | mapped | `cdc.database.user` = `dbzuser` | In a CDB this must be a common user with the grants from `oracle-cdc-doctor setup-sql`. |
| `decimal.handling.mode` | `string` | mapped | `cdc.decimal.mode` = `string` |  |
| `heartbeat.interval.ms` | `0` | mapped-with-change |   | Not copied: the connector's heartbeats (default every 10 seconds) keep offsets moving on a quiet database and never write to the source database. |
| `log.mining.archive.log.only.mode` | `true` | mapped-with-change | `cdc.capture.mode` = `archive_only` | Mines archived logs only, never online logs. |
| `log.mining.transaction.retention.hours` | `4` | mapped-with-change | `cdc.transaction.max.age.ms` = `14400000`<br/>`cdc.transaction.max.age.action` = `discard` | A transaction open longer than this is discarded, as Debezium did, but its details go to the ops topic and the DLQ. `cdc.transaction.max.age.action=fail` stops instead. |
| `name` | `legacy-oracle` | mapped-with-change | `name` = `legacy-oracle-oso` | The new connector has its own name: Kafka Connect keeps offsets per connector name, and this connector does not read the Debezium offset format. |
| `schema.whitelist` | `FINANCE,HR` | mapped-with-change | `cdc.tables.include` = `FINANCE\..+,HR\..+` | Rewritten into `cdc.tables.include` or `cdc.tables.exclude` as patterns over `PDB.SCHEMA.TABLE` (or `SCHEMA.TABLE` without a PDB). |
| `snapshot.mode` | `schema_only` | mapped-with-change | `cdc.snapshot.mode` = `none` | The takeover starts the new connector at the Debezium position (`cdc.start.scn` from takeover_scn.py), so no snapshot runs: `cdc.snapshot.mode=none`. For a fresh deployment instead, `schema_only` corresponds to `none`. |
| `table.blacklist` | `FINANCE\.AUDIT_.*` | mapped-with-change | `cdc.tables.exclude` = `FINANCE\.AUDIT_.*` | Rewritten into `cdc.tables.include` or `cdc.tables.exclude` as patterns over `PDB.SCHEMA.TABLE` (or `SCHEMA.TABLE` without a PDB). |
| `tasks.max` | `1` | mapped | `tasks.max` = `1` |  |
| `time.precision.mode` | `adaptive_time_microseconds` | mapped-with-change | `cdc.temporal.mode` = `adaptive` | Oracle Database has no TIME type, so this behaves as `adaptive`. |
| `tombstones.on.delete` | `false` | mapped | `cdc.tombstones.on.delete` = `false` |  |

## Next steps

1. Resolve the follow-ups, then run `oracle-cdc-doctor check --config config.properties`.
2. Stop the Debezium connector with `PUT /connectors/legacy-oracle/stop`; pausing is not enough, the offset must be final.
3. Run `takeover_scn.py --target-config config.properties` to read the Debezium offset, check the archived logs and write `cdc.start.scn` into the configuration.
4. Create `legacy-oracle-oso` from that configuration. With no stored offset it starts streaming at `cdc.start.scn`.
5. After the overlap has passed, run `verify_cutover.py` and archive the evidence file.
6. Point dashboards and alerts at the connector's JMX metrics; the names differ from Debezium's (see the metrics reference).
7. Keep the Debezium schema history topic until the cutover is verified, then delete it and the old connector's other internal topics after the agreed retention.
