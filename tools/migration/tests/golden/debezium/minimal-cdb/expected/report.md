# Migration report: Debezium Oracle connector to the OSO CDC Connector

| Item | Value |
|---|---|
| Tool | `migrate_from_debezium.py` <version> |
| Source connector | `orders-cdc` |
| New connector | `orders-cdc-oso` |
| Exactly-once check | enabled: the worker accepted exactly.once.support=required |
| Exit code | 2, success with manual follow-ups |

17 source properties: 11 mapped, 4 mapped with a change, 2 dropped and 0 needing manual action. 2 follow-ups.

## Follow-ups

1. The schema history topic is no longer needed: the connector keeps table schema versions in its own compacted schema topic (`cdc.schema.topic`). Keep the history topic until the cutover is verified, then delete it.
2. Set `cdc.start.scn` before the new connector first runs: stop the old connector, then run `takeover_scn.py`, which reads its offset, checks the archived logs and writes `cdc.start.scn` into this configuration. Without it the new connector starts at the current SCN and changes made since the old connector stopped are lost.

## Settings added by the translator

| Property | Value | Reason |
|---|---|---|
| `name` | `orders-cdc-oso` | The new connector's name. |
| `cdc.topic.template` | `${prefix}.${schema}.${table}` | Keeps Debezium's topic names, `prefix.SCHEMA.TABLE`. The connector's own default adds the PDB name in a CDB. |
| `cdc.output.format` | `debezium` | The Debezium-compatible envelope, so consumers keep working (MIG-2). |
| `cdc.decimal.mode` | `precise` | Debezium's default, pinned (MIG-3). |
| `cdc.temporal.mode` | `adaptive` | Debezium's default, pinned (MIG-3). |
| `cdc.tombstones.on.delete` | `true` | Debezium's default, pinned (MIG-3). |
| `cdc.lob.mode` | `skip` | Debezium's default (`lob.enabled=false`), pinned (MIG-3). |
| `cdc.key.missing` | `none` | Debezium published tables without a primary key with a null key; the connector would otherwise reject them at validation (MIG-3). |
| `cdc.snapshot.mode` | `none` | A takeover streams from the old connector's position, without a snapshot. |
| `exactly.once.support` | `required` | The target Connect cluster has exactly-once source support enabled (MIG-4). |
| `transaction.boundary` | `connector` | The target Connect cluster has exactly-once source support enabled (MIG-4). |

## Source properties

| Source property | Value | Classification | Target | Note |
|---|---|---|---|---|
| `connector.class` | `io.debezium.connector.oracle.OracleConnector` | mapped-with-change | `connector.class` = `sh.oso.connect.oracle.OracleCdcSourceConnector` | Replaced by the OSO CDC Connector class. |
| `database.dbname` | `ORCLCDB` | mapped | `cdc.database.service` = `ORCLCDB` | Debezium connects with this name as the service name; in a CDB it names the root. |
| `database.hostname` | `db.prod.internal` | mapped | `cdc.database.host` = `db.prod.internal` |  |
| `database.password` | `${file:/opt/kafka/secrets/oracle.properties:password}` | mapped | `cdc.database.password` = `${file:/opt/kafka/secrets/oracle.properties:password}` |  |
| `database.pdb.name` | `ORCLPDB1` | mapped | `cdc.database.pdbs` = `ORCLPDB1` |  |
| `database.port` | `1521` | mapped | `cdc.database.port` = `1521` |  |
| `database.user` | `c##dbzuser` | mapped | `cdc.database.user` = `c##dbzuser` | In a CDB this must be a common user with the grants from `oracle-cdc-doctor setup-sql`. |
| `include.schema.changes` | `false` | dropped |   | Off in the source as well. |
| `key.converter` | `io.confluent.connect.avro.AvroConverter` | mapped | `key.converter` = `io.confluent.connect.avro.AvroConverter` | Kafka Connect framework property, copied unchanged. |
| `key.converter.schema.registry.url` | `http://schema-registry:8081` | mapped | `key.converter.schema.registry.url` = `http://schema-registry:8081` | Kafka Connect framework property, copied unchanged. |
| `schema.history.internal.kafka.bootstrap.servers` | `kafka-1:9092,kafka-2:9092` | mapped-with-change | `cdc.kafka.bootstrap.servers` = `kafka-1:9092,kafka-2:9092` | The connector's internal topics (schema versions, ops events, signals, transaction journal) use the brokers the schema history used. |
| `schema.history.internal.kafka.topic` | `orders.schema-history` | dropped |   | Schema history is replaced by the connector's schema topic, created on start. |
| `table.include.list` | `APP.ORDERS,APP.ORDER_LINES` | mapped-with-change | `cdc.tables.include` = `ORCLPDB1\.APP.ORDERS,ORCLPDB1\.APP.ORDER_LINES` | Rewritten into `cdc.tables.include` or `cdc.tables.exclude` as patterns over `PDB.SCHEMA.TABLE` (or `SCHEMA.TABLE` without a PDB). |
| `tasks.max` | `1` | mapped | `tasks.max` = `1` |  |
| `topic.prefix` | `orders` | mapped-with-change | `cdc.topic.prefix` = `orders`<br/>`cdc.topic.template` = `${prefix}.${schema}.${table}` | Topic prefix and logical server name. `cdc.topic.template` is pinned so the topic names stay `prefix.SCHEMA.TABLE`. |
| `value.converter` | `io.confluent.connect.avro.AvroConverter` | mapped | `value.converter` = `io.confluent.connect.avro.AvroConverter` | Kafka Connect framework property, copied unchanged. |
| `value.converter.schema.registry.url` | `http://schema-registry:8081` | mapped | `value.converter.schema.registry.url` = `http://schema-registry:8081` | Kafka Connect framework property, copied unchanged. |

## Next steps

1. Resolve the follow-ups, then run `oracle-cdc-doctor check --config config.json`.
2. Stop the Debezium connector with `PUT /connectors/orders-cdc/stop`; pausing is not enough, the offset must be final.
3. Run `takeover_scn.py --target-config config.json` to read the Debezium offset, check the archived logs and write `cdc.start.scn` into the configuration.
4. Create `orders-cdc-oso` from that configuration. With no stored offset it starts streaming at `cdc.start.scn`.
5. After the overlap has passed, run `verify_cutover.py` and archive the evidence file.
6. Point dashboards and alerts at the connector's JMX metrics; the names differ from Debezium's (see the metrics reference).
7. Keep the Debezium schema history topic until the cutover is verified, then delete it and the old connector's other internal topics after the agreed retention.
