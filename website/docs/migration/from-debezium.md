---
title: Migrating from Debezium Oracle
description: Translate the Debezium configuration, take over at the Debezium offset without a gap, and verify the cutover.
---

# Migrating from Debezium Oracle

The connector writes the Debezium-compatible envelope by default and the translated configuration
keeps Debezium's topic names, so consumers of the Debezium Oracle connector keep working. The
migration uses three of the [migration tools](tools.md): `migrate_from_debezium.py` translates the
configuration, `takeover_scn.py` starts the new connector at the Debezium offset, and
`verify_cutover.py` checks the result.

The new connector starts at the Debezium resume position (`cdc.start.scn`), so nothing is
skipped. Changes committed between that position and Debezium's last commit are delivered a second
time.

## Before you start

- The Debezium connector uses the LogMiner adapter (`logminer` or `logminer_unbuffered`). The
  offsets of the XStream and OpenLogReplicator adapters carry no position the takeover can use.
- The database is a single instance. RAC is not supported in this release.
- The archived logs from the Debezium resume SCN still exist.
- The new connector gets a name of its own. It starts at `cdc.start.scn` only while it has no
  stored offset, and Kafka Connect keeps offsets per connector name.

## 1. Translate the configuration

```bash
uv run python migrate_from_debezium.py --input debezium-connector.json \
    --output oso-connector.json --report migration-report.md \
    --connect-url http://connect:8083
```

The output is a create request for the new connector. Its name defaults to the old name followed
by `-oso` (`--name` sets another). It sets `cdc.snapshot.mode=none`, because a takeover streams
from Debezium's position instead of taking a snapshot. `cdc.start.scn` comes from the takeover in
step 4; if you already know the SCN, `--start-scn` writes it directly.

The report lists every source property as mapped, mapped with a change, dropped (with the reason)
or manual (with an instruction), then the follow-ups. The main translations:

| Debezium property | Translation |
|---|---|
| `database.hostname`, `database.port`, `database.user`, `database.password`, `database.url` | `cdc.database.host`, `cdc.database.port`, `cdc.database.user`, `cdc.database.password`, `cdc.database.url` |
| `database.dbname`, `database.pdb.name` | `cdc.database.service`, `cdc.database.pdbs` |
| `topic.prefix` (or `database.server.name`) | `cdc.topic.prefix`, plus `cdc.topic.template` set to `${prefix}.${schema}.${table}` (with `topic.delimiter` when set), so topic names do not change |
| `table.include.list`, `table.exclude.list`, `schema.include.list`, `schema.exclude.list` | `cdc.tables.include` and `cdc.tables.exclude`, rewritten as patterns over `PDB.SCHEMA.TABLE` |
| `message.key.columns` | `cdc.key.columns`, when the tables are named without regular expressions |
| `log.mining.username.exclude.list` | `cdc.users.exclude` |
| `snapshot.mode` | `cdc.snapshot.mode=none` for the takeover; the report gives the equivalent for a fresh deployment |
| `snapshot.max.threads`, `snapshot.fetch.size`, `incremental.snapshot.chunk.size` | `cdc.snapshot.threads`, `cdc.snapshot.fetch.size`, `cdc.snapshot.chunk.rows` |
| `decimal.handling.mode`, `time.precision.mode` | `cdc.decimal.mode`; `cdc.temporal.mode` (`adaptive`, or `iso_string` for `isostring`) |
| `lob.enabled`, `unavailable.value.placeholder` | `cdc.lob.mode` (`inline` or `skip`), `cdc.unavailable.placeholder` |
| `tombstones.on.delete`, `heartbeat.interval.ms`, `max.batch.size` | `cdc.tombstones.on.delete`, `cdc.heartbeat.interval.ms`, `cdc.poll.max.records` |
| `log.mining.transaction.retention.ms` | `cdc.transaction.max.age.ms` with `cdc.transaction.max.age.action=discard`; discarded transactions are written to the ops topic and the DLQ |
| `log.mining.archive.log.only.mode`, `archive.destination.name` | `cdc.capture.mode`, `cdc.archive.destination` |
| `event.processing.failure.handling.mode` | `cdc.on.decode.error`: `fail`, or `dlq` for `warn` and `skip`, because the connector never skips a row silently |
| `driver.*` | Trust store and wallet settings to their `cdc.database.*` properties, other driver options to `cdc.database.connection.properties` |
| Kafka Connect properties (converters, transforms, predicates, `errors.*`, client overrides) | Copied unchanged |

Settings the translator pins to Debezium's behaviour: `cdc.output.format=debezium`,
`cdc.decimal.mode=precise`, `cdc.temporal.mode=adaptive`, `cdc.tombstones.on.delete=true`,
`cdc.lob.mode=skip`, `cdc.unavailable.placeholder=__debezium_unavailable_value` when LOBs are
captured, and `cdc.key.missing=none`, because Debezium published tables without a primary key
with a null key.

Dropped, with the reason in the report: the LogMiner tuning settings (`log.mining.strategy`, batch
and sleep sizes, buffer settings, the query filter mode), `heartbeat.action.query` (heartbeats
never write to the database), the schema history settings (the connector keeps schema versions in
its own topic), the signal table (signals come from a Kafka topic) and the flush table.

Manual, because this release has no equivalent:

- `column.exclude.list`. Write the same columns in `cdc.columns.exclude` as regular expressions
  over `PDB.SCHEMA.TABLE.COLUMN` (`SCHEMA.TABLE.COLUMN` without a PDB); the name forms differ, so
  the translator does not rewrite the patterns. A key column cannot be excluded. Until the property
  is set, every column is published, so set it before cutting over a table whose excluded columns
  must not reach Kafka.
- `column.include.list`. There is no include form: list the columns to leave out in
  `cdc.columns.exclude` instead.
- `log.mining.username.include.list`; only an exclude filter exists.
- `binary.handling.mode` other than `bytes`; `time.precision.mode` `connect`, `microseconds` or
  `nanoseconds`; `interval.handling.mode=string`.
- `provide.transaction.metadata=true`. Records carry the `transaction` block, but BEGIN and END
  records are not written to a transaction topic.
- `skipped.operations` that keeps truncates: truncate records are not published.
- `rac.nodes`, the XStream and OpenLogReplicator adapters, custom converters, post processors,
  column masking and truncation, snapshot select overrides and custom topic naming strategies.
- Any property the translator does not know.

With `--connect-url` the translator asks the target Connect cluster to validate
`exactly.once.support=required`. When the workers run with exactly-once source support, the
output sets `exactly.once.support=required` and `transaction.boundary=connector`; otherwise it
stays at least once and a follow-up says why.

## 2. Check the database and the configuration

Resolve the follow-ups, including the password: a literal password is never copied, so set
`cdc.database.password` to a config provider reference. Then run the
[doctor](../operations/doctor.md) against the new configuration:

```bash
java -jar oracle-cdc-doctor-cli.jar check --config oso-connector.json
```

## 3. Stop the Debezium connector

```bash
curl -X PUT http://connect:8083/connectors/inventory-connector/stop
```

Pausing is not enough: only a stopped connector has a final offset.

## 4. Compute the start SCN

```bash
export ORACLE_PASSWORD='...'
uv run python takeover_scn.py --connect-url http://connect:8083 \
    --connector inventory-connector --target-config oso-connector.json \
    --db-password-env ORACLE_PASSWORD \
    --output oso-connector-takeover.json --report takeover.md
```

The tool:

1. Refuses unless the Debezium connector is `STOPPED`.
2. Reads its offset with `GET /connectors/{name}/offsets`. The start SCN is the offset's `scn`,
   Debezium's resume position, which is never later than the start of a transaction still open
   when the offset was written. `commit_scn` is recorded in the report. An offset written during
   Debezium's initial snapshot is refused; transactions listed in `snapshot_pending_tx` move the
   start back to the earliest of them.
3. Connects to the database named in the translated configuration (or `--db-dsn` and
   `--db-user`) and checks that the SCN is neither ahead of the database nor older than its
   RESETLOGS SCN, that the database is in ARCHIVELOG mode, and that for every redo thread the log
   holding the start SCN and every later log are still available, online or archived and not
   deleted. If any is gone it exits 1 and writes nothing.
4. Checks, through the Connect REST API, that the new connector has no stored offset; with one it
   would ignore `cdc.start.scn`.
5. Writes the translated configuration with `cdc.start.scn` set to the start SCN and
   `cdc.snapshot.mode=none` to `--output`. Without `--target-config` it writes just those two
   settings.

When the offsets come from a file (`--input`) instead of the REST API, the tool cannot confirm
that the connector was stopped and exits 2 with that follow-up.

## 5. Create the new connector

```bash
curl -X POST -H 'Content-Type: application/json' --data @oso-connector-takeover.json \
    http://connect:8083/connectors
```

With no stored offset, the connector starts streaming at `cdc.start.scn`. Once it has written an
offset, the property is ignored, so later restarts resume from the offset as usual. If the redo
at `cdc.start.scn` is no longer available when the task starts, it stops with `CDC-2002`.

## 6. The overlap

Changes committed after the start SCN, up to Debezium's last commit, are delivered again:
consumers that upsert by key are unaffected, and the `cdc.xid` and `cdc.commit_scn` headers
identify the repeats. A transaction that began before the start SCN and committed during that
overlap can be delivered again in part; Debezium had already delivered it whole. Nothing is
skipped.

Avoid DDL on captured tables between stopping Debezium and the end of the overlap. Rows written
before a DDL that the connector has not yet mined are decoded with a dictionary from the archived
logs (see [Schema and DDL](../concepts/schema-and-ddl.md)); if none is available the task stops
with `CDC-6001` and [its runbook](../operations/runbooks/dictionary-unavailable.md) explains the
way forward.

## 7. Verify

After the overlap has passed, run `verify_cutover.py` against the new connector's topics and keep
the evidence file. See [Cutover verification](cutover-verification.md). The verifier also reads
Debezium's topics, so you can verify the existing pipeline before you start.

## 8. Clean up

- Keep the Debezium schema history topic until the cutover is verified, then delete it.
- Debezium published schema change events to a topic named after `topic.prefix`; this connector
  does not, so move or retire its consumers.
- Drop the Debezium signal table and the LogMiner flush table, if there were any.
- Point dashboards and alerts at the connector's [metrics](../reference/metrics.md); the names
  differ from Debezium's.
- Delete the old connector's offsets and internal topics after the retention agreed for the
  change.
