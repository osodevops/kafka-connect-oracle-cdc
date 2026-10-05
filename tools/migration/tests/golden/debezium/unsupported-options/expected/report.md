# Migration report: Debezium Oracle connector to the OSO CDC Connector

| Item | Value |
|---|---|
| Tool | `migrate_from_debezium.py` <version> |
| Source connector | `kitchen-sink` |
| New connector | `kitchen-sink-oso` |
| Exactly-once check | unknown: validation request failed: PUT /connector-plugins/sh.oso.connect.oracle.OracleCdcSourceConnector/config/validate returned HTTP 404: Failed to find any class that implements Connector |
| Exit code | 2, success with manual follow-ups |

37 source properties: 12 mapped, 8 mapped with a change, 5 dropped and 12 needing manual action. 16 follow-ups.

## Follow-ups

1. `binary.handling.mode=hex` has no equivalent: binary columns are published as bytes in this release. Consumers that expect text must decode the bytes. (`binary.handling.mode`)
2. A setting of the custom converter `boolean`; custom converters are not supported. (`boolean.type`)
3. Column masking is not supported. (`column.mask.with.12.chars`)
4. Custom converters are not supported; type mapping follows `cdc.decimal.mode` and `cdc.temporal.mode`. (`converters`)
5. `errors.tolerance=all` lets Connect skip records it cannot convert. For change data capture `none` is recommended, so a conversion problem stops the task instead of losing a change. (`errors.tolerance`)
6. Intervals as strings are only available with `cdc.temporal.mode=iso_string`, which also turns dates and timestamps into text. Choose one by hand. (`interval.handling.mode`)
7. Post processors are not supported. For LOB values the redo does not carry, use `cdc.lob.mode=reselect`. (`post.processors`)
8. Every record carries the `transaction` block (id, total order, data collection order), but BEGIN and END records on a transaction topic are not written in this release. Consumers of the Debezium transaction topic must wait or change. (`provide.transaction.metadata`)
9. The source published truncate records; this release does not. Consumers that act on truncates must be told by other means. (`skipped.operations`)
10. The connector takes one row filter per table as a SQL condition, in `cdc.snapshot.select.override.PDB.OWNER.TABLE`, not a whole statement. Rewrite it by hand if a later snapshot needs it. (`snapshot.select.statement.overrides`, `snapshot.select.statement.overrides.APP.ORDERS`)
11. `time.precision.mode=connect` has no equivalent. The connector offers `adaptive` (Debezium semantic types sized to the column precision) and `iso_string`; consumers that read the old representation must change. (`time.precision.mode`)
12. Transforms, predicates or converters name `io.debezium` classes. Keep the Debezium plugin (or its transform and converter jars) on the worker's plugin path, or replace them; the record envelope they expect is unchanged. (`transforms.unwrap.type`)
13. The translator does not know this property. Check what it did in the source connector and set the corresponding `cdc.*` property by hand, or leave it out. (`unknown.vendor.setting`)
14. Debezium published schema change events to the topic named after `topic.prefix`; this connector does not. Move or retire the consumers of that topic.
15. Set `cdc.start.scn` before the new connector first runs: stop the old connector, then run `takeover_scn.py`, which reads its offset, checks the archived logs and writes `cdc.start.scn` into this configuration. Without it the new connector starts at the current SCN and changes made since the old connector stopped are lost.
16. Exactly-once delivery was not configured because the target Connect cluster could not be checked (validation request failed: PUT /connector-plugins/sh.oso.connect.oracle.OracleCdcSourceConnector/config/validate returned HTTP 404: Failed to find any class that implements Connector). Run the translator again with `--connect-url`, or add `exactly.once.support=required` and `transaction.boundary=connector` yourself once the workers run with `exactly.once.source.support=enabled`.

## Settings added by the translator

| Property | Value | Reason |
|---|---|---|
| `name` | `kitchen-sink-oso` | The new connector's name. |
| `cdc.topic.template` | `${prefix}_${schema}_${table}` | Keeps Debezium's topic names, `prefix.SCHEMA.TABLE`. The connector's own default adds the PDB name in a CDB. |
| `cdc.output.format` | `debezium` | The Debezium-compatible envelope, so consumers keep working (MIG-2). |
| `cdc.decimal.mode` | `precise` | Debezium's default, pinned (MIG-3). |
| `cdc.temporal.mode` | `adaptive` | Debezium's default, pinned (MIG-3). |
| `cdc.tombstones.on.delete` | `true` | Debezium's default, pinned (MIG-3). |
| `cdc.lob.mode` | `skip` | Debezium's default (`lob.enabled=false`), pinned (MIG-3). |
| `cdc.key.missing` | `none` | Debezium published tables without a primary key with a null key; the connector would otherwise reject them at validation (MIG-3). |
| `cdc.snapshot.mode` | `none` | A takeover streams from the old connector's position, without a snapshot. |

## Source properties

| Source property | Value | Classification | Target | Note |
|---|---|---|---|---|
| `binary.handling.mode` | `hex` | manual |   | `binary.handling.mode=hex` has no equivalent: binary columns are published as bytes in this release. Consumers that expect text must decode the bytes. |
| `boolean.type` | `io.debezium.connector.oracle.converters.NumberOneToBooleanConverter` | manual |   | A setting of the custom converter `boolean`; custom converters are not supported. |
| `column.mask.with.12.chars` | `APP.CUSTOMERS.EMAIL` | manual |   | Column masking is not supported. |
| `connector.class` | `io.debezium.connector.oracle.OracleConnector` | mapped-with-change | `connector.class` = `sh.oso.connect.oracle.OracleCdcSourceConnector` | Replaced by the OSO CDC Connector class. |
| `converters` | `boolean` | manual |   | Custom converters are not supported; type mapping follows `cdc.decimal.mode` and `cdc.temporal.mode`. |
| `custom.metric.tags` | `env=prod` | dropped |   | Metric names differ; see the metrics reference for dashboards and alerts. |
| `database.dbname` | `ORCLCDB` | mapped | `cdc.database.service` = `ORCLCDB` | Debezium connects with this name as the service name; in a CDB it names the root. |
| `database.hostname` | `db` | mapped | `cdc.database.host` = `db` |  |
| `database.password` | `${vault:secret/data/oracle:password}` | mapped | `cdc.database.password` = `${vault:secret/data/oracle:password}` |  |
| `database.pdb.name` | `ORCLPDB1` | mapped | `cdc.database.pdbs` = `ORCLPDB1` |  |
| `database.port` | `1521` | mapped | `cdc.database.port` = `1521` |  |
| `database.user` | `c##dbzuser` | mapped | `cdc.database.user` = `c##dbzuser` | In a CDB this must be a common user with the grants from `oracle-cdc-doctor setup-sql`. |
| `driver.javax.net.ssl.trustStore` | `/etc/ssl/truststore.jks` | mapped | `cdc.database.tls.truststore.location` = `/etc/ssl/truststore.jks` |  |
| `driver.javax.net.ssl.trustStoreType` | `PKCS12` | mapped | `cdc.database.tls.truststore.type` = `PKCS12` |  |
| `driver.oracle.net.ssl_server_dn_match` | `true` | mapped-with-change | `cdc.database.connection.properties` = `oracle.net.ssl_server_dn_match=true` | Added to `cdc.database.connection.properties`. |
| `driver.oracle.net.wallet_location` | `(SOURCE=(METHOD=file)(METHOD_DATA=(DIRECTORY=/opt/wallet)))` | mapped-with-change | `cdc.database.wallet.location` = `/opt/wallet` | The wallet directory. |
| `errors.max.retries` | `5` | dropped |   | Transient database errors are retried for `cdc.retry.max.time.ms` (default one day). |
| `errors.tolerance` | `all` | mapped | `errors.tolerance` = `all` | Kafka Connect framework property, copied unchanged. |
| `interval.handling.mode` | `string` | manual |   | Intervals as strings are only available with `cdc.temporal.mode=iso_string`, which also turns dates and timestamps into text. Choose one by hand. |
| `log.mining.flush.table.name` | `LOG_MINING_FLUSH` | dropped |   | The connector never writes to the source database; drop the flush table after the cutover. |
| `max.batch.size` | `4096` | mapped | `cdc.poll.max.records` = `4096` |  |
| `max.queue.size` | `16384` | dropped |   | Records queue in batches of `cdc.poll.max.records`. |
| `message.key.columns` | `APP.ORDERS:ORDER_ID,REGION;APP\.ORDER_LINES:LINE_ID` | mapped-with-change | `cdc.key.columns` = `APP.ORDERS:ORDER_ID,REGION;APP.ORDER_LINES:LINE_ID` | Rewritten as `SCHEMA.TABLE:COL1,COL2` entries in upper case. |
| `poll.interval.ms` | `500` | dropped |   | `cdc.poll.linger.ms` sets how long a poll waits for the first record. |
| `post.processors` | `reselector` | manual |   | Post processors are not supported. For LOB values the redo does not carry, use `cdc.lob.mode=reselect`. |
| `provide.transaction.metadata` | `true` | manual |   | Every record carries the `transaction` block (id, total order, data collection order), but BEGIN and END records on a transaction topic are not written in this release. Consumers of the Debezium transaction topic must wait or change. |
| `skipped.operations` | `none` | manual |   | The source published truncate records; this release does not. Consumers that act on truncates must be told by other means. |
| `snapshot.select.statement.overrides` | `APP.ORDERS` | manual |   | The connector takes one row filter per table as a SQL condition, in `cdc.snapshot.select.override.PDB.OWNER.TABLE`, not a whole statement. Rewrite it by hand if a later snapshot needs it. |
| `snapshot.select.statement.overrides.APP.ORDERS` | `SELECT * FROM APP.ORDERS WHERE STATUS <> 'X'` | manual |   | The connector takes one row filter per table as a SQL condition, in `cdc.snapshot.select.override.PDB.OWNER.TABLE`, not a whole statement. Rewrite it by hand if a later snapshot needs it. |
| `table.include.list` | `APP\.ORDERS` | mapped-with-change | `cdc.tables.include` = `ORCLPDB1\.APP\.ORDERS` | Rewritten into `cdc.tables.include` or `cdc.tables.exclude` as patterns over `PDB.SCHEMA.TABLE` (or `SCHEMA.TABLE` without a PDB). |
| `tasks.max` | `3` | mapped-with-change | `tasks.max` = `1` | The connector always runs one task, as the Debezium Oracle connector does. |
| `time.precision.mode` | `connect` | manual |   | `time.precision.mode=connect` has no equivalent. The connector offers `adaptive` (Debezium semantic types sized to the column precision) and `iso_string`; consumers that read the old representation must change. |
| `topic.delimiter` | `_` | mapped-with-change | `cdc.topic.template` = `${prefix}_${schema}_${table}` | Used as the separator in `cdc.topic.template`. |
| `topic.prefix` | `sink` | mapped-with-change | `cdc.topic.prefix` = `sink`<br/>`cdc.topic.template` = `${prefix}_${schema}_${table}` | Topic prefix and logical server name. `cdc.topic.template` is pinned so the topic names stay `prefix.SCHEMA.TABLE`. |
| `transforms` | `unwrap` | mapped | `transforms` = `unwrap` | Kafka Connect framework property, copied unchanged. |
| `transforms.unwrap.type` | `io.debezium.transforms.ExtractNewRecordState` | mapped | `transforms.unwrap.type` = `io.debezium.transforms.ExtractNewRecordState` | Kafka Connect framework property, copied unchanged. |
| `unknown.vendor.setting` | `x` | manual |   | The translator does not know this property. Check what it did in the source connector and set the corresponding `cdc.*` property by hand, or leave it out. |

## Next steps

1. Resolve the follow-ups, then run `oracle-cdc-doctor check --config config.json`.
2. Stop the Debezium connector with `PUT /connectors/kitchen-sink/stop`; pausing is not enough, the offset must be final.
3. Run `takeover_scn.py --target-config config.json` to read the Debezium offset, check the archived logs and write `cdc.start.scn` into the configuration.
4. Create `kitchen-sink-oso` from that configuration. With no stored offset it starts streaming at `cdc.start.scn`.
5. After the overlap has passed, run `verify_cutover.py` and archive the evidence file.
6. Point dashboards and alerts at the connector's JMX metrics; the names differ from Debezium's (see the metrics reference).
7. Keep the Debezium schema history topic until the cutover is verified, then delete it and the old connector's other internal topics after the agreed retention.
