---
title: Migrating from Confluent Oracle CDC Source
description: What the translator and the takeover do for a Confluent Oracle CDC Source connector, and why the record format decides how to cut over.
---

# Migrating from Confluent Oracle CDC Source

`migrate_from_confluent.py` translates a Confluent Oracle CDC Source connector configuration and
`takeover_scn.py` gives the new connector `cdc.start.scn` from the Confluent offset (see
[Migration tools](tools.md)). Confluent property names and behaviour are taken from Confluent's
public documentation only.

**Record format.** The Confluent-compatible flat record format (`cdc.output.format=confluent`) and
LOB topics are not built yet. The new connector publishes the Debezium-compatible envelope
(`before`, `after`, `source`, `op`), and record keys are structs of the key columns. Consumers and
Schema Registry subjects of the existing table topics would break, so until the Confluent-compatible
format is available, point `cdc.topic.template` at new topic names and move the consumers to the
new format. The translator's report says this as its first follow-up.

## 1. Translate the configuration

```bash
uv run python migrate_from_confluent.py --input confluent-connector.json \
    --output oso-connector.json --report migration-report.md \
    --connect-url http://connect:8083 --topic-prefix orders
```

`--topic-prefix` sets `cdc.topic.prefix`, which names the connector's internal topics and its
offset partition; it defaults to the Confluent connector's name. Every source property is
classified as mapped, mapped with a change, dropped (with the reason) or manual (with an
instruction); empty values are dropped. The main translations:

| Confluent property | Translation |
|---|---|
| `oracle.server`, `oracle.port`, `oracle.username`, `oracle.password` | `cdc.database.host`, `cdc.database.port`, `cdc.database.user`, `cdc.database.password` |
| `oracle.sid`, `oracle.service.name`, `oracle.pdb.name` | `cdc.database.sid`, `cdc.database.service`, `cdc.database.pdbs` |
| `table.inclusion.regex`, `table.exclusion.regex` | `cdc.tables.include`, `cdc.tables.exclude`, with `cdc.tables.case.sensitive=true` as in Confluent. Without a PDB the SID is removed from the start of each alternative, because the connector then matches `SCHEMA.TABLE` |
| `table.topic.name.template` | `cdc.topic.template`: `${databaseName}` becomes `${pdb}` with a PDB and `${database}` without, `${schemaName}` becomes `${schema}`, `${tableName}` becomes `${table}`, `${fullyQualifiedTableName}` becomes all three, `${connectorName}` the old connector's name. Other variables are manual |
| `start.from` | `cdc.snapshot.mode=none`, because the takeover starts at `cdc.start.scn` from the Confluent offset |
| `max.batch.size`, `poll.linger.ms`, `query.timeout.ms`, `max.retry.time.ms` | `cdc.poll.max.records`, `cdc.poll.linger.ms`, `cdc.mining.query.timeout.ms`, `cdc.retry.max.time.ms` |
| `snapshot.row.fetch.size`, `redo.log.row.fetch.size`, `snapshot.threads.per.task` | `cdc.snapshot.fetch.size`, `cdc.mining.fetch.size`, `cdc.snapshot.threads` |
| `heartbeat.interval.ms` | `cdc.heartbeat.interval.ms` when above zero; otherwise the connector's default heartbeat stays |
| `log.mining.transaction.age.threshold.ms` and `log.mining.transaction.threshold.breached.action` | With `discard`: `cdc.transaction.max.age.ms` and `cdc.transaction.max.age.action=discard`. With `warn`: nothing, as there is no warn-only limit |
| `behavior.on.dictionary.mismatch`, `behavior.on.unparsable.statement` | `cdc.on.decode.error`: `fail`, or `dlq` when either was `log` |
| `numeric.mapping` `none`, `best_fit` or `best_fit_or_decimal` | `cdc.decimal.mode=precise`; NUMBER(p,0) up to 18 digits becomes an integer rather than Decimal bytes |
| `emit.tombstone.on.delete` | `cdc.tombstones.on.delete`, pinned to `false`, Confluent's default |
| `log.mining.archive.destination.name`, `log.sensitive.data`, `redo.log.row.poll.username.exclude` | `cdc.archive.destination`, `cdc.log.sensitive.data`, `cdc.users.exclude` |
| `oracle.ssl.truststore.file`, `oracle.ssl.truststore.password`, `retry.error.codes`, `ldap.url` | `cdc.database.tls.truststore.location`, `cdc.database.tls.truststore.password`, `cdc.retry.extra.error.codes`, `cdc.database.url` |
| `tasks.max` | One. A single task captures every table with one LogMiner session |
| Kafka Connect properties | Copied unchanged |

Dropped: the redo log topic and its consumer settings, `confluent.license` and the `confluent.topic`
settings, `connection.pool.*`, `oracle.dictionary.mode`, the redo poll interval, the corruption
topic (corrupt redo stops the task), and buffering, validation and placement settings.

Manual: the `output.*` record format settings, `key.template`, `oracle.date.mapping`, other
`numeric.mapping` values, a non-default `numeric.default.scale`, `lob.topic.name.template`,
`redo.log.row.poll.username.include`, a time zone other than UTC, `oracle.fan.events.enable=true`
(RAC is not supported in this release), Kerberos and LDAP credentials, and any unknown property.

Every report also says that the redo log topic stops receiving data at the cutover, so its
consumers must move or retire. Exactly-once is checked against `--connect-url` as for
[Debezium](from-debezium.md#1-translate-the-configuration).

## 2. Take over

The procedure is the one for [Debezium](from-debezium.md): check the configuration with the
doctor, stop the Confluent connector with `PUT /connectors/{name}/stop`, run `takeover_scn.py`,
then create the new connector from its output. The Confluent offset has the partition `sidPdb`
and an `scn`; the tool takes the lowest SCN across the offset partitions, checks the archived logs
from it and writes `cdc.start.scn` and `cdc.snapshot.mode=none` into the configuration.

```bash
uv run python takeover_scn.py --connect-url http://connect:8083 \
    --connector confluent-oracle-cdc --target-config oso-connector.json \
    --db-password-env ORACLE_PASSWORD --output oso-connector-takeover.json \
    --report takeover.md
```

The new connector starts at `cdc.start.scn` because it has no stored offset; give it a name of
its own.

## 3. Verify

`verify_cutover.py` reads topics in the Debezium-compatible envelope, so it verifies the new
connector's topics. It cannot read Confluent's flat records, so it does not verify the old
pipeline. See [Cutover verification](cutover-verification.md).
