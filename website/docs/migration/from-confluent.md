---
title: Migrating from Confluent Oracle CDC Source
description: A tested cutover from Confluent's Oracle CDC Source connector, what changes for consumers, how the takeover position is chosen, and how to verify and roll back.
---

# Migrating from Confluent Oracle CDC Source

The migration tools translate a Confluent Oracle CDC Source connector, take over from its stored
position without a new snapshot, and verify the result against the database. The procedure on
this page was run end to end with Confluent's connector 2.15.2 on a Confluent Platform 7.9 Connect
worker, under a live workload, with a transaction left open across the switch: every committed row
reached Kafka, the open transaction was delivered by the new connector, and
`verify_cutover.py` matched the table to the topics by count and hash. It ran against Oracle
Database Free and against Amazon RDS for Oracle 19c (non-CDB) on 12 October 2026; the
qualification suite `ConfluentCutoverQualIT` repeats it.

## Before you start

- **Kafka Connect.** The takeover reads the old connector's offset through the Connect REST API
  and needs `PUT /connectors/{name}/stop` and `GET /connectors/{name}/offsets` (KIP-875: Apache
  Kafka 3.5 and later, Confluent Platform 7.5 and later). Install this connector's plugin in the
  same Connect cluster; it runs on Confluent Platform workers as well as Apache Kafka ones.
- **Where Confluent's connector cannot run.** Confluent documents that its connector does not
  support Amazon RDS for Oracle with the CDB architecture, RDS read replicas or Autonomous
  Database. This connector captures RDS with the CDB architecture from inside the tenant
  database (see [Amazon RDS for Oracle](../database-setup/amazon-rds.md)), so such a database
  starts with this connector rather than migrating to it.
- **Consumers.** The record format changes (next section). Plan the consumer move with the teams
  that own them before the cutover.
- **Database user.** Run `oracle-cdc-doctor setup-sql` and apply its script; Confluent's user may
  lack grants this connector needs, and the doctor names them.

## What changes for consumers

This connector writes Debezium-compatible change events. A Confluent-compatible flat format is not
built yet, so the translation always writes to **new topics**: the topic prefix, a dot, then the
name Confluent used. Confluent's topics stop receiving changes at the cutover.

| | Confluent Oracle CDC Source (defaults) | This connector |
|---|---|---|
| Topic | `${fullyQualifiedTableName}`, for example `PDB1.APP.ORDERS` | `<prefix>.PDB1.APP.ORDERS` after the translation |
| Value | The row's columns plus `table`, `scn`, `op_type`, `op_ts`, `current_ts`, `row_id` and `username` | An envelope: `before`, `after`, `source` (with `scn`, `commit_scn` and the transaction id), `op`, `ts_ms` |
| Operation codes | `op_type`: `I`, `U`, `D`, `R` for snapshot rows, `T` for truncate | `op`: `c`, `u`, `d`, `r`, `t` |
| Key | The primary key value for a one-column key (`key.template` default), a struct otherwise | A struct of the key columns |
| NUMBER | With `numeric.mapping=none`, a Connect Decimal; the JSON converter writes it as base64 bytes unless its `decimal.format` is `NUMERIC` | With `cdc.decimal.mode=precise` (the translation of `none`), NUMBER(p,0) up to 18 digits is an integer and other NUMBER values are Connect Decimals |
| Deletes | `op_type=D` with the row; a tombstone only with `emit.tombstone.on.delete=true` | `op=d` with the row in `before`; a tombstone when `cdc.tombstones.on.delete=true` |
| Redo log topic | `${connectorName}-${databaseName}-redo-log`, one partition | None: mining happens inside the task |
| LOB columns | Separate LOB topics, with `lob.topic.name.template` | Inline, left out, or read again as of the commit SCN (`cdc.lob.mode`) |

## 1. Translate the configuration

```bash
uv run python migrate_from_confluent.py --input confluent-connector.json \
    --output oso-connector.json --report migration-report.md \
    --connect-url http://connect:8083 --topic-prefix orders --name orders-oso
```

`--input` takes the connector as `GET /connectors/{name}` returns it. `--topic-prefix` sets
`cdc.topic.prefix`, which starts every new topic name and names the connector's internal topics; it
defaults to the Confluent connector's name. Every source property is classified as mapped, mapped
with a change, dropped (with the reason) or manual (with an instruction), and the report lists the
follow-ups. The main translations:

| Confluent property | Translation |
|---|---|
| `oracle.server`, `oracle.port`, `oracle.username`, `oracle.password` | `cdc.database.host`, `cdc.database.port`, `cdc.database.user`, `cdc.database.password` |
| `oracle.sid`, `oracle.service.name`, `oracle.pdb.name` | `cdc.database.sid`, `cdc.database.service`, `cdc.database.pdbs` |
| `table.inclusion.regex`, `table.exclusion.regex` | `cdc.tables.include`, `cdc.tables.exclude`, with `cdc.tables.case.sensitive=true` as in Confluent. Without a PDB the SID is removed from the start of each alternative, because the connector then matches `SCHEMA.TABLE` |
| `table.topic.name.template` (or its default) | `cdc.topic.template`, starting with `${prefix}.`: `${databaseName}` becomes `${pdb}` with a PDB and `${database}` without, `${schemaName}` becomes `${schema}`, `${tableName}` becomes `${table}`, `${fullyQualifiedTableName}` becomes all three, `${connectorName}` the old connector's name. Other variables are manual |
| `start.from` | `cdc.snapshot.mode=none`, because the takeover starts at `cdc.start.scn` |
| `max.batch.size`, `poll.linger.ms`, `query.timeout.ms`, `max.retry.time.ms` | `cdc.poll.max.records`, `cdc.poll.linger.ms`, `cdc.mining.query.timeout.ms`, `cdc.retry.max.time.ms` |
| `snapshot.row.fetch.size`, `redo.log.row.fetch.size`, `snapshot.threads.per.task` | `cdc.snapshot.fetch.size`, `cdc.mining.fetch.size`, `cdc.snapshot.threads` |
| `heartbeat.interval.ms` | `cdc.heartbeat.interval.ms` when above zero; otherwise the connector's default heartbeat stays |
| `log.mining.transaction.age.threshold.ms` and `log.mining.transaction.threshold.breached.action` | With `discard`: `cdc.transaction.max.age.ms` and `cdc.transaction.max.age.action=discard`. With `warn`: nothing, as there is no warn-only limit |
| `behavior.on.dictionary.mismatch`, `behavior.on.unparsable.statement` | `cdc.on.decode.error`: `fail`, or `dlq` when either was `log` |
| `numeric.mapping` `none`, `best_fit` or `best_fit_or_decimal` | `cdc.decimal.mode=precise` |
| `emit.tombstone.on.delete` | `cdc.tombstones.on.delete` with the source value; `false`, Confluent's default, when the source leaves it out |
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
(RAC is not supported yet), Kerberos and LDAP credentials, and any unknown property. The
translation masks the database password; set the capture user and its password from
`setup-sql` before creating the connector.

## 2. Check, stop and take over

Check the translated configuration with `oracle-cdc-doctor check --config oso-connector.json`.
Then stop the Confluent connector. The takeover refuses a connector that is only paused, because
a paused connector is one resume away from moving its offset again.

```bash
curl -X PUT http://connect:8083/connectors/confluent-oracle-cdc/stop
uv run python takeover_scn.py --connect-url http://connect:8083 \
    --connector confluent-oracle-cdc --target-config oso-connector.json \
    --db-dsn db.example.com:1521/ORCLCDB --db-user CDC --db-password-env ORACLE_PASSWORD \
    --output oso-connector-takeover.json --report takeover.md
```

**How the start is chosen.** Confluent's connector stores two kinds of offset partition: one per
database, `{"sidPdb": "CDB.PDB1"}` (just the SID on a non-CDB, for example `{"sidPdb": "ORCL"}`),
with `scn` (where its mining resumes) and `tablePlacement`;
and one per captured table, `{"catalog", "schema", "table"}`, with `scn`, `cscn` (the commit SCN
of the last change it delivered for the table), `rows`, `rps` and `def` (the column definitions).
Because Confluent starts each mining session at the oldest open transaction
(`use.transaction.begin.for.mining.session`, on by default), the database `scn` is at or below
every commit it has not delivered. The takeover uses the lowest `scn` as `cdc.start.scn`, checks
that the archived logs from it still exist and that the database incarnation matches, and reports
the highest table `cscn` as the last commit Confluent delivered. Changes committed between the two
reach the new topics again: an overlap, never a gap. A transaction that is open when Confluent
stops and commits later is delivered by the new connector.

## 3. Start the new connector

Create the connector from `oso-connector-takeover.json` under its own name: it has no stored
offset, so it starts at `cdc.start.scn` with `cdc.snapshot.mode=none`. Keep the workload running;
nothing has to pause.

## 4. Verify

Once the new connector has passed the check SCN, compare the table with both connectors' topics:

```bash
uv run python verify_cutover.py --input oso-connector-registered.json \
    --table APP.ORDERS --confluent-topic PDB1.APP.ORDERS \
    --db-dsn db.example.com:1521/PDB1 --db-user CDC --db-password-env ORACLE_PASSWORD \
    --bootstrap-servers kafka:9092 --connect-url http://connect:8083 --connector orders-oso \
    --output cutover-evidence.json --report verification.md
```

Confluent's topic is read first and by key: a key it delivered and the new connector never touched
counts as present when the table has it, and its values are not compared, because their encoding
depends on `numeric.mapping` and the converter. Every key the new connector delivered is compared
value by value. A key in neither topic, or a key only Confluent's topic still holds after the
table lost it, fails the check. The tool reads topics written with the JSON converter; see
[Cutover verification](cutover-verification.md) for the evidence file and its limits.

## 5. Move consumers and retire the old pipeline

Point consumers at the new topics. Each new topic holds the changes from the takeover on; the
earlier history stays in Confluent's topic for as long as its retention keeps it. Consumers that
apply changes by key handle the overlap without duplicates in their state. When nothing reads them
any more, delete the Confluent connector, its table topics, its redo log topic and, if no other
Confluent connector uses it, the `_confluent-command` licence topic.

## Rolling back

Until the Confluent connector is deleted, its offset is unchanged. If verification fails, delete
the new connector, then resume Confluent's with `PUT /connectors/{name}/resume`: it continues from
where it stopped, and the new topics can be deleted.
