---
title: Migrating from Confluent Oracle CDC Source
description: A tested cutover from Confluent's Oracle CDC Source connector, what changes for consumers, how the takeover position is chosen, and how to verify and roll back.
---

# Migrating from Confluent Oracle CDC Source

The default migration keeps Confluent's table topics, flat records, key templates and Schema
Registry subjects. Stop the source connector, translate its settings and start OSO from its
stored position. Existing consumers continue reading the same topics. This path is implemented
for the next release, subject to the release qualification gates.

For configuration and record examples before planning a cutover, see
[Confluent-compatible records](../guides/confluent-compatible-records.md).

The earlier migration to new Debezium topics was qualified on Oracle Database Free and RDS for
Oracle 19c non-CDB. The same-topic qualification uses Oracle Database Free and Confluent 2.15.2
in disposable local containers. No RDS instance was available for that additional format run.

The format was independently implemented from public documentation and verified by comparing
external output on synthetic test data. Only the official, unmodified reference connector was
used under its evaluation licence. No reverse-engineered or decompiled versions were used,
and no Confluent source code was inspected or copied. Its proprietary connector and libraries
are not shipped with OSO or run in CI. OSO is not affiliated with or endorsed by Confluent.

## Before you start

- Install this connector's plugin in the same Connect cluster. The takeover needs
  `PUT /connectors/{name}/stop` and `GET /connectors/{name}/offsets`, available from Kafka 3.5
  and Confluent Platform 7.5.
- Preserve the converter, registry URL and subject strategy used by the existing pipeline.
  Matching key/value converter overrides also configure the internal journal reader. For a converter inherited from worker settings, configure `cdc.journal.converter` and its settings before enabling internal topic readback. Converter libraries belong on the worker; the build never downloads Confluent's connector.
- Apply `oracle-cdc-doctor setup-sql` and run its checks. Separate LOB topics need stable column
  keys, complete values and undo sufficient for AS OF commit reselect.
- Review the differences below. Confluent documents no support for RDS CDB, read replicas or
  Autonomous Database; such installations start with OSO rather than migrating from Confluent.

## What stays, and the documented differences

| Item | Default same-topic path |
|---|---|
| Table topics | Existing template, without an added prefix |
| Value and metadata | Flat row, configured `output.*` names, operation values, nesting and headers |
| Key | Existing scalar, struct or concatenated template; `cdc.key.missing=none` preserves null keys for keyless tables |
| NUMBER and DATE | Existing numeric mapping, default scale and DATE mapping |
| LOB topics | Existing template; CLOB UTF-8, NCLOB UTF-16BE, BLOB raw bytes |
| Deletes | Source tombstone setting, including tombstone-only deletes when enabled |
| Avro | Existing topic subjects and compatible schemas with the same converter |
| Redo log topic | Retired; OSO mines inside its task |

Differences are explicit:

- A NUMBER needing rounding to its configured Decimal scale stops with CDC-3001, even during
  snapshots. Confluent snapshots rounded HALF_EVEN. Raise the scale if needed before cutover.
- Out-of-row LOB inserts can carry a resolved real ROWID in OSO where Confluent publishes
  LogMiner's all-A placeholder.
- Flat output requires complete supported row images and declared key values. Incomplete
  images stop with CDC-3001, so ALL-column supplemental logging is required.
- OSO adds diagnostic `cdc.*` headers and suppresses redo/undo text where column exclusions
  may apply.
- Requested SQL retains nine fractional timestamp digits, where Confluent 2.15.2 emits three.
  Timestamp column values remain at the same millisecond precision.
- OSO removes old LOB keys and publishes values under a changed row key; Confluent 2.15.2 left
  old LOB state behind.
- Optional truncate emission is an OSO metadata-only `T` extension. Both defaults are off;
  no Confluent truncate output was observed.
- Topic mode refuses unsupported XMLTYPE bytes, keyless or ROWID-only LOB tables and incomplete
  or oversized values. XMLTYPE representation remains unsettled by the recorded comparison.

For an intentional consumer format change, pass `--format=debezium`. That path writes new
prefixed topics and the `before`, `after`, `source`, `op` envelope. Keep the earlier history in
the source topics until their retention and consumers allow retirement.

## 1. Translate the configuration

```bash
uv run python migrate_from_confluent.py --input confluent-connector.json \
    --output oso-connector.json --report migration-report.md \
    --connect-url http://connect:8083 --topic-prefix orders --name orders-oso
```

`--input` takes the connector as `GET /connectors/{name}` returns it. `--topic-prefix` sets
`cdc.topic.prefix`, which names internal topics and the offset partition. Table topics retain
their existing names in the default Confluent format; the prefix starts new table topics only
with `--format=debezium`. It
defaults to the Confluent connector's name. Every source property is classified as mapped, mapped
with a change, dropped (with the reason) or manual (with an instruction), and the report lists the
follow-ups. The main translations:

| Confluent property | Translation |
|---|---|
| `oracle.server`, `oracle.port`, `oracle.username`, `oracle.password` | `cdc.database.host`, `cdc.database.port`, `cdc.database.user`, `cdc.database.password` |
| `oracle.sid`, `oracle.service.name`, `oracle.pdb.name` | `cdc.database.sid`, `cdc.database.service`, `cdc.database.pdbs` |
| `table.inclusion.regex`, `table.exclusion.regex` | `cdc.tables.include`, `cdc.tables.exclude`, with `cdc.tables.case.sensitive=true` as in Confluent. Without a PDB the SID is removed from the start of each alternative, because the connector then matches `SCHEMA.TABLE` |
| `table.topic.name.template` (or its default) | `cdc.topic.template`, without an added prefix by default: `${databaseName}` becomes `${pdb}` with a PDB and `${database}` without, `${schemaName}` becomes `${schema}`, `${tableName}` becomes `${table}`, `${fullyQualifiedTableName}` becomes all three, `${connectorName}` the old connector's name. Other variables are manual |
| `start.from` | `cdc.snapshot.mode=none`, because the takeover starts at `cdc.start.scn` |
| `max.batch.size`, `poll.linger.ms`, `query.timeout.ms`, `max.retry.time.ms` | `cdc.poll.max.records`, `cdc.poll.linger.ms`, `cdc.mining.query.timeout.ms`, `cdc.retry.max.time.ms` |
| `snapshot.row.fetch.size`, `redo.log.row.fetch.size`, `snapshot.threads.per.task` | `cdc.snapshot.fetch.size`, `cdc.mining.fetch.size`, `cdc.snapshot.threads` |
| `heartbeat.interval.ms` | `cdc.heartbeat.interval.ms` when above zero; otherwise the connector's default heartbeat stays |
| `log.mining.transaction.age.threshold.ms` and `log.mining.transaction.threshold.breached.action` | With `discard`: `cdc.transaction.max.age.ms` and `cdc.transaction.max.age.action=discard`. With `warn`: nothing, as there is no warn-only limit |
| `behavior.on.dictionary.mismatch`, `behavior.on.unparsable.statement` | `cdc.on.decode.error`: `fail`, or `dlq` when either was `log` |
| `output.*`, `key.template` | `cdc.output.confluent.*`, preserving field layout and key shape |
| `numeric.mapping`, `numeric.default.scale` | `cdc.output.confluent.numeric.mapping`, `cdc.output.confluent.numeric.default.scale`; legacy `best_fit` becomes `best_fit_or_decimal` |
| `oracle.date.mapping` | `cdc.output.confluent.date.mapping` |
| `lob.topic.name.template` | `cdc.lob.topic.template` and `cdc.lob.mode=topic`; `${columnName}` becomes `${column}` |
| `emit.tombstone.on.delete` | `cdc.tombstones.on.delete` with the source value; `false`, Confluent's default, when the source leaves it out |
| `log.mining.archive.destination.name`, `log.sensitive.data`, `redo.log.row.poll.username.exclude` | `cdc.archive.destination`, `cdc.log.sensitive.data`, `cdc.users.exclude` |
| `oracle.ssl.truststore.file`, `oracle.ssl.truststore.password`, `retry.error.codes`, `ldap.url` | `cdc.database.tls.truststore.location`, `cdc.database.tls.truststore.password`, `cdc.retry.extra.error.codes`, `cdc.database.url` |
| `tasks.max` | One. A single task captures every table with one LogMiner session |
| Kafka Connect properties | Copied unchanged |

Dropped: the redo log topic and its consumer settings, `confluent.license` and the `confluent.topic`
settings, `connection.pool.*`, `oracle.dictionary.mode`, the redo poll interval, the corruption
topic (corrupt redo stops the task), and buffering, validation and placement settings.

Manual: unsupported template variables, `redo.log.row.poll.username.include`, a time zone other than UTC, `oracle.fan.events.enable=true`
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
reach the same topics again: an overlap, never a gap. A transaction that is open when Confluent
stops and commits later is delivered by the new connector.

## 3. Start the new connector

Create the connector from `oso-connector-takeover.json` under its own name: it has no stored
offset, so it starts at `cdc.start.scn` with `cdc.snapshot.mode=none`. Keep the workload running;
nothing has to pause.

## 4. Verify

Once the new connector has passed the check SCN, compare flat-topic values with the database.
The JSON verifier recognises the compatible format, Decimal schema scales, DATE mapping,
renamed metadata and per-column numeric mappings:

```bash
uv run python verify_cutover.py --input oso-connector-registered.json \
    --table APP.ORDERS --confluent-baseline-connector confluent-oracle-cdc \
    --db-dsn db.example.com:1521/PDB1 --db-user CDC --db-password-env ORACLE_PASSWORD \
    --bootstrap-servers kafka:9092 --connect-url http://connect:8083 --connector orders-oso \
    --output cutover-evidence.json --report verification.md
```

Streaming verification needs `cdc.commit_scn` or the configured commit-SCN field. For historical Confluent records without that metadata, `--confluent-baseline-connector`
checks that the source remains STOPPED with unchanged offsets throughout verification. Its final
table commit SCN fences those historical values, and the check SCN must be at or above that fence.
Without either proof, verification is INCONCLUSIVE. The tool reads JSON, requires stable keys and lists omitted types and LOB columns
as ignored. The Avro qualification separately verifies an unchanged reader and subject reuse.
See [Cutover verification](cutover-verification.md) for the evidence file and limits.

With `--format=debezium`, add `--confluent-topic` to read the old flat topic as a key-only
baseline before comparing new envelope values.

## 5. Retire the old pipeline

Keep table and LOB topics, their subjects and their consumers on the same-topic path. After the
agreed rollback period, delete the stopped Confluent connector and retire its redo log and
internal topics when no other connector uses them. The new connector uses its own internal
schema and transaction journal topics. Do not delete the shared table or LOB topics.

## Rolling back

Until the Confluent connector is deleted, its offset is unchanged. If verification fails, delete
the new connector, then resume Confluent's with `PUT /connectors/{name}/resume`: it continues from
where it stopped. On the same-topic path keep the table and LOB topics and handle overlap
idempotently by key. Only the explicit Debezium migration has separate new topics to retire.
