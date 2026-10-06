# Migration report: Debezium Oracle connector to the OSO CDC Connector

| Item | Value |
|---|---|
| Tool | `migrate_from_debezium.py` <version> |
| Source connector | `inventory-connector` |
| New connector | `inventory-connector-oso` |
| Exactly-once check | enabled: the worker accepted exactly.once.support=required |
| Exit code | 2, success with manual follow-ups |

48 source properties: 18 mapped, 11 mapped with a change, 17 dropped and 2 needing manual action. 7 follow-ups.

## Follow-ups

1. Write the columns this property kept out in `cdc.columns.exclude`, as regular expressions over `PDB.SCHEMA.TABLE.COLUMN` (or `SCHEMA.TABLE.COLUMN` without a PDB); the patterns are not rewritten for you because the name forms differ. Key columns cannot be excluded. Until it is set, every column of a captured table is published. (`column.exclude.list`)
2. Set `cdc.database.password` to a config provider reference, for example `${file:/etc/kafka-connect/secrets.properties:cdc.database.password}`. The source held a literal secret, which is never copied. (`database.password`)
3. `event.processing.failure.handling.mode=warn` skipped rows that could not be decoded. The connector never skips silently: such rows go to the DLQ topic (`cdc.dlq.topic`) with an ops event. Make sure someone watches the DLQ. (`event.processing.failure.handling.mode`)
4. There is no user include filter in this release; only `cdc.users.exclude`. List the users to leave out instead. (`log.mining.username.include.list`)
5. The schema history topic is no longer needed: the connector keeps table schema versions in its own compacted schema topic (`cdc.schema.topic`). Keep the history topic until the cutover is verified, then delete it.
6. Signals are read from a Kafka topic (`cdc.signals.topic`) in this connector's own command format, never from a table. Move any automation that writes to the signal table, and drop the table after the cutover. (`signal.data.collection`)
7. Debezium published schema change events to the topic named after `topic.prefix`; this connector does not. Move or retire the consumers of that topic.

## Settings added by the translator

| Property | Value | Reason |
|---|---|---|
| `name` | `inventory-connector-oso` | The new connector's name. |
| `cdc.topic.template` | `${prefix}.${schema}.${table}` | Keeps Debezium's topic names, `prefix.SCHEMA.TABLE`. The connector's own default adds the PDB name in a CDB. |
| `cdc.output.format` | `debezium` | The Debezium-compatible envelope, so consumers keep working (MIG-2). |
| `cdc.key.missing` | `none` | Debezium published tables without a primary key with a null key; the connector would otherwise reject them at validation (MIG-3). |
| `cdc.start.scn` | `324567890` | The old connector's position (--start-scn); honoured while the new connector has no stored offset. |
| `exactly.once.support` | `required` | The target Connect cluster has exactly-once source support enabled (MIG-4). |
| `transaction.boundary` | `connector` | The target Connect cluster has exactly-once source support enabled (MIG-4). |

## Source properties

| Source property | Value | Classification | Target | Note |
|---|---|---|---|---|
| `archive.destination.name` | `LOG_ARCHIVE_DEST_1` | mapped | `cdc.archive.destination` = `LOG_ARCHIVE_DEST_1` |  |
| `binary.handling.mode` | `bytes` | dropped |   | Binary columns are published as bytes, the only representation in this release. |
| `column.exclude.list` | `INVENTORY\.CUSTOMERS\.SSN` | manual |   | Write the columns this property kept out in `cdc.columns.exclude`, as regular expressions over `PDB.SCHEMA.TABLE.COLUMN` (or `SCHEMA.TABLE.COLUMN` without a PDB); the patterns are not rewritten for you because the name forms differ. Key columns cannot be excluded. Until it is set, every column of a captured table is published. |
| `connector.class` | `io.debezium.connector.oracle.OracleConnector` | mapped-with-change | `connector.class` = `sh.oso.connect.oracle.OracleCdcSourceConnector` | Replaced by the OSO CDC Connector class. |
| `database.connection.adapter` | `logminer` | dropped |   | The connector reads redo only through LogMiner and buffers open transactions itself, spilling to disk and journaling long ones. |
| `database.dbname` | `ORCLCDB` | mapped | `cdc.database.service` = `ORCLCDB` | Debezium connects with this name as the service name; in a CDB it names the root. |
| `database.hostname` | `oracle.example.com` | mapped | `cdc.database.host` = `oracle.example.com` |  |
| `database.password` | `********` | mapped | `cdc.database.password` = `********` |  |
| `database.pdb.name` | `ORCLPDB1` | mapped | `cdc.database.pdbs` = `ORCLPDB1` |  |
| `database.port` | `1521` | mapped | `cdc.database.port` = `1521` |  |
| `database.url` | `jdbc:oracle:thin:@//oracle.example.com:1521/ORCLCDB` | mapped | `cdc.database.url` = `jdbc:oracle:thin:@//oracle.example.com:1521/ORCLCDB` |  |
| `database.user` | `c##dbzuser` | mapped | `cdc.database.user` = `c##dbzuser` | In a CDB this must be a common user with the grants from `oracle-cdc-doctor setup-sql`. |
| `decimal.handling.mode` | `precise` | mapped | `cdc.decimal.mode` = `precise` |  |
| `event.processing.failure.handling.mode` | `warn` | mapped-with-change | `cdc.on.decode.error` = `dlq` | Undecodable rows go to the DLQ topic. |
| `heartbeat.action.query` | `INSERT INTO c##dbzuser.heartbeat VALUES (sysdate)` | dropped |   | Not needed: heartbeats carry the position without writing to the source database, so offsets advance on a quiet database. |
| `heartbeat.interval.ms` | `10000` | mapped | `cdc.heartbeat.interval.ms` = `10000` |  |
| `lob.enabled` | `true` | mapped-with-change | `cdc.lob.mode` = `inline` | LOB values are assembled from redo; values the redo does not carry are published as `cdc.unavailable.placeholder`. `reselect` also reads them from the table. |
| `log.mining.archive.destination.name` | `LOG_ARCHIVE_DEST_1` | mapped | `cdc.archive.destination` = `LOG_ARCHIVE_DEST_1` |  |
| `log.mining.archive.log.only.mode` | `false` | mapped | `cdc.capture.mode` = `online` |  |
| `log.mining.batch.size.default` | `20000` | dropped |   | Mining tuning of the Debezium connector; the connector sizes its window adaptively from `cdc.mining.target.latency.ms` and spills open transactions to disk. |
| `log.mining.batch.size.max` | `100000` | dropped |   | Mining tuning of the Debezium connector; the connector sizes its window adaptively from `cdc.mining.target.latency.ms` and spills open transactions to disk. |
| `log.mining.batch.size.min` | `1000` | dropped |   | Mining tuning of the Debezium connector; the connector sizes its window adaptively from `cdc.mining.target.latency.ms` and spills open transactions to disk. |
| `log.mining.buffer.drop.on.stop` | `false` | dropped |   | Mining tuning of the Debezium connector; the connector sizes its window adaptively from `cdc.mining.target.latency.ms` and spills open transactions to disk. |
| `log.mining.buffer.type` | `memory` | dropped |   | Mining tuning of the Debezium connector; the connector sizes its window adaptively from `cdc.mining.target.latency.ms` and spills open transactions to disk. |
| `log.mining.query.filter.mode` | `in` | dropped |   | Captured tables are always filtered by object id in the mining query. |
| `log.mining.sleep.time.default.ms` | `1000` | dropped |   | Mining tuning of the Debezium connector; the connector sizes its window adaptively from `cdc.mining.target.latency.ms` and spills open transactions to disk. |
| `log.mining.sleep.time.increment.ms` | `200` | dropped |   | Mining tuning of the Debezium connector; the connector sizes its window adaptively from `cdc.mining.target.latency.ms` and spills open transactions to disk. |
| `log.mining.sleep.time.max.ms` | `3000` | dropped |   | Mining tuning of the Debezium connector; the connector sizes its window adaptively from `cdc.mining.target.latency.ms` and spills open transactions to disk. |
| `log.mining.sleep.time.min.ms` | `0` | dropped |   | Mining tuning of the Debezium connector; the connector sizes its window adaptively from `cdc.mining.target.latency.ms` and spills open transactions to disk. |
| `log.mining.strategy` | `online_catalog` | dropped |   | The connector mines with the online catalog and replays a step with a dictionary from the redo when a row predates a later DDL. |
| `log.mining.transaction.retention.ms` | `3600000` | mapped-with-change | `cdc.transaction.max.age.ms` = `3600000`<br/>`cdc.transaction.max.age.action` = `discard` | A transaction open longer than this is discarded, as Debezium did, but its details go to the ops topic and the DLQ. `cdc.transaction.max.age.action=fail` stops instead. |
| `log.mining.username.exclude.list` | `GGADMIN` | mapped | `cdc.users.exclude` = `GGADMIN` |  |
| `log.mining.username.include.list` | `APPUSER` | manual |   | There is no user include filter in this release; only `cdc.users.exclude`. List the users to leave out instead. |
| `rac.nodes` | (empty) | dropped |   | Empty in the source, so there is nothing to carry over. |
| `schema.exclude.list` | `INVENTORY_TMP` | mapped-with-change | `cdc.tables.exclude` = `ORCLPDB1\.INVENTORY\.ORDERS_ARCHIVE,ORCLPDB1\.INVENTORY_TMP\..+` | Rewritten into `cdc.tables.include` or `cdc.tables.exclude` as patterns over `PDB.SCHEMA.TABLE` (or `SCHEMA.TABLE` without a PDB). |
| `schema.history.internal.kafka.bootstrap.servers` | `kafka:9092` | mapped-with-change | `cdc.kafka.bootstrap.servers` = `kafka:9092` | The connector's internal topics (schema versions, ops events, signals, transaction journal) use the brokers the schema history used. |
| `schema.history.internal.kafka.topic` | `schema-changes.inventory` | dropped |   | Schema history is replaced by the connector's schema topic, created on start. |
| `schema.include.list` | `INVENTORY,SALES` | mapped-with-change | `cdc.tables.include` = `ORCLPDB1\.(?=(?:INVENTORY\|SALES)\.)INVENTORY\.CUSTOMERS,ORCLPDB1\.(?=(?:INVENTORY\|SALES)\.)INVENTORY\.ORDERS,ORCLPDB1\.(?=(?:INVENTORY\|SALES)\.)SALES\.INVOICES` | Rewritten into `cdc.tables.include` or `cdc.tables.exclude` as patterns over `PDB.SCHEMA.TABLE` (or `SCHEMA.TABLE` without a PDB). |
| `signal.data.collection` | `ORCLPDB1.C##DBZUSER.DEBEZIUM_SIGNAL` | dropped |   | The signal table is replaced by the signal topic. |
| `snapshot.max.threads` | `2` | mapped | `cdc.snapshot.threads` = `2` |  |
| `snapshot.mode` | `initial` | mapped-with-change | `cdc.snapshot.mode` = `none` | The takeover starts the new connector at the Debezium position (`cdc.start.scn` from takeover_scn.py), so no snapshot runs: `cdc.snapshot.mode=none`. For a fresh deployment instead, `initial` corresponds to `initial`. |
| `table.exclude.list` | `INVENTORY\.ORDERS_ARCHIVE` | mapped-with-change | `cdc.tables.exclude` = `ORCLPDB1\.INVENTORY\.ORDERS_ARCHIVE,ORCLPDB1\.INVENTORY_TMP\..+` | Rewritten into `cdc.tables.include` or `cdc.tables.exclude` as patterns over `PDB.SCHEMA.TABLE` (or `SCHEMA.TABLE` without a PDB). |
| `table.include.list` | `INVENTORY\.CUSTOMERS,INVENTORY\.ORDERS,ORCLPDB1\.SALES\.INVOICES` | mapped-with-change | `cdc.tables.include` = `ORCLPDB1\.(?=(?:INVENTORY\|SALES)\.)INVENTORY\.CUSTOMERS,ORCLPDB1\.(?=(?:INVENTORY\|SALES)\.)INVENTORY\.ORDERS,ORCLPDB1\.(?=(?:INVENTORY\|SALES)\.)SALES\.INVOICES` | Rewritten into `cdc.tables.include` or `cdc.tables.exclude` as patterns over `PDB.SCHEMA.TABLE` (or `SCHEMA.TABLE` without a PDB). |
| `tasks.max` | `1` | mapped | `tasks.max` = `1` |  |
| `time.precision.mode` | `adaptive` | mapped | `cdc.temporal.mode` = `adaptive` |  |
| `tombstones.on.delete` | `true` | mapped | `cdc.tombstones.on.delete` = `true` |  |
| `topic.prefix` | `server1` | mapped-with-change | `cdc.topic.prefix` = `server1`<br/>`cdc.topic.template` = `${prefix}.${schema}.${table}` | Topic prefix and logical server name. `cdc.topic.template` is pinned so the topic names stay `prefix.SCHEMA.TABLE`. |
| `unavailable.value.placeholder` | `__debezium_unavailable_value` | mapped | `cdc.unavailable.placeholder` = `__debezium_unavailable_value` |  |

## Next steps

1. Resolve the follow-ups, then run `oracle-cdc-doctor check --config config.json`.
2. Stop the Debezium connector with `PUT /connectors/inventory-connector/stop`; pausing is not enough, the offset must be final.
3. Run `takeover_scn.py --target-config config.json` to read the Debezium offset, check the archived logs and write `cdc.start.scn` into the configuration.
4. Create `inventory-connector-oso` from that configuration. With no stored offset it starts streaming at `cdc.start.scn`.
5. After the overlap has passed, run `verify_cutover.py` and archive the evidence file.
6. Point dashboards and alerts at the connector's JMX metrics; the names differ from Debezium's (see the metrics reference).
7. Keep the Debezium schema history topic until the cutover is verified, then delete it and the old connector's other internal topics after the agreed retention.
