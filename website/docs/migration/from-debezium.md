---
title: Migrate from the Debezium Oracle Connector
sidebar_label: Migrating from Debezium Oracle
description: "Move from the Debezium Oracle (LogMiner) connector: translate the configuration, take over at the Debezium offset with no gap, and verify the cutover."
---

import Faq from '@site/src/components/Faq';

# Migrate from the Debezium Oracle Connector

The Debezium Oracle connector captures row-level changes in an Oracle database, including tables
added while the connector is running, and it can limit capture to chosen schemas and tables. By
default it reads those changes with the native LogMiner API. Set `database.connection.adapter` to
`logminer` (the default) or to `logminer_unbuffered` when only committed changes should be
streamed. OpenLogReplicator (`olr`) and XStream (`xstream`, a commercial component of GoldenGate)
are alternative adapters. Debezium documents the adapters, the class
`io.debezium.connector.oracle.OracleConnector`, and compatibility with a standalone instance and
with Oracle Real Application Clusters in its
[Oracle connector reference](https://debezium.io/documentation/reference/stable/connectors/oracle.html#oracle-adapter-modes),
as read on 2 November 2026.

**Quick answer.** What changes is the connector class and the configuration, which the migration tools translate. What stays is the existing Kafka topics: the translated configuration keeps Debezium's topic names and writes the Debezium-compatible envelope. The new connector continues from a takeover SCN computed from the Debezium offset, with a short overlap so nothing is skipped.

The connector that takes over is `sh.oso.connect.oracle.OracleCdcSourceConnector`. It writes the
Debezium-compatible envelope by default and the translated configuration keeps Debezium's topic
names, so consumers of the Debezium Oracle connector keep working. The migration uses three of the
[migration tools](tools.md): `migrate_from_debezium.py` translates the configuration,
`takeover_scn.py` starts the new connector at the Debezium offset, and `verify_cutover.py` checks
the result. See [cutover verification](cutover-verification.md) for the evidence file, and the
[open source comparison](../comparison/index.md#which-open-source-oracle-cdc-options-exist) for how
the two connectors differ on capture method.

The new connector starts at the Debezium resume position (`cdc.start.scn`), so nothing is
skipped. Changes committed between that position and Debezium's last commit are delivered a second
time.

## What changes and what stays the same?

| Item | On this migration |
|---|---|
| Connector class | Changes, from `io.debezium.connector.oracle.OracleConnector` to `sh.oso.connect.oracle.OracleCdcSourceConnector`. |
| Configuration keys | Change. `migrate_from_debezium.py` rewrites them as `cdc.*` properties and reports every source property as mapped, mapped with a change, dropped or manual. |
| Offsets and start position | The new connector has its own name and no stored offset, so it starts at `cdc.start.scn`. That value is the Debezium offset's `scn`, and `cdc.snapshot.mode` is `none`. |
| Topics | Stay. `cdc.topic.template` is `${prefix}.${schema}.${table}` (with `topic.delimiter` when set), so Debezium's topic names do not change. |
| Record format | Stays the Debezium-compatible envelope (`cdc.output.format=debezium`). The translator pins the name-adjustment modes so record names, and the schemas registered under the existing subjects, stay the same. |
| Schema change topic | Does not stay. Debezium publishes schema change events to a topic named from `topic.prefix`. This connector keeps schema versions in its own topic and does not publish that schema change topic. |
| Real Application Clusters | Debezium documents compatibility with a standalone instance and with Real Application Clusters. This connector captures a single instance. Real Application Clusters are not supported yet. |

Debezium's LogMiner adapter already requires the database to run in ARCHIVELOG mode, with minimal
supplemental logging and supplemental logging on captured tables. This connector requires the
same, and it reads changes through LogMiner only. It does not read redo or archive files
directly. The mining user still needs the grants in
[database setup](../database-setup/index.md), including
[supplemental logging](../database-setup/supplemental-logging.md). Run the
[doctor](../operations/doctor.md) against the translated configuration before the cutover and
apply any grant it reports as missing. Debezium's preparation steps are in
[Preparing the database](https://debezium.io/documentation/reference/stable/connectors/oracle.html#_preparing_the_database).

## How does the takeover SCN avoid a gap?

Debezium stores two values in the connector offset. `scn` is the low-watermark position it
resumes from, and `commit_scn` records commits it has seen. Debezium documents both in its
[Oracle connector FAQ](https://debezium.io/documentation/reference/stable/connectors/oracle.html#oracle-frequently-asked-questions).
After the Debezium connector is stopped, `takeover_scn.py` reads `scn` and writes it as
`cdc.start.scn`. The new connector starts at that SCN, so no committed change is skipped. Changes
committed after it, up to Debezium's last commit, are delivered again. The checks the tool runs
are [step 4](#4-compute-the-start-scn). The overlap, and what it means for consumers, is
[step 6](#6-the-overlap).

## Before you start

- The Debezium connector uses the LogMiner adapter (`logminer` or `logminer_unbuffered`). The
  offsets of the XStream and OpenLogReplicator adapters carry no position the takeover can use.
- The database is a single instance. RAC is not supported yet.
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
| `schema.name.adjustment.mode`, `field.name.adjustment.mode`, `sanitize.field.names` | `cdc.schema.name.adjustment.mode` and `cdc.field.name.adjustment.mode` with the same value (`none`, `avro` or `avro_unicode`); `sanitize.field.names=true` becomes `cdc.field.name.adjustment.mode=avro` |
| `driver.*` | Trust store and wallet settings to their `cdc.database.*` properties, other driver options to `cdc.database.connection.properties` |
| Kafka Connect properties (converters, transforms, predicates, `errors.*`, client overrides) | Copied unchanged |

Settings the translator pins to Debezium's behaviour: `cdc.output.format=debezium`,
`cdc.decimal.mode=precise`, `cdc.temporal.mode=adaptive`, `cdc.tombstones.on.delete=true`,
`cdc.lob.mode=skip`, `cdc.unavailable.placeholder=__debezium_unavailable_value` when LOBs are
captured, and `cdc.key.missing=none`, because Debezium published tables without a primary key
with a null key.

A Debezium 1.x configuration (one with `database.server.name` and no `topic.prefix`, or with
`database.history.*` settings) also gets `cdc.schema.name.adjustment.mode=avro` unless it sets the
mode itself, because Debezium 1.x adjusted schema names for Avro by default; with an Avro
`key.converter` or `value.converter` it gets `cdc.field.name.adjustment.mode=avro` as well, because
1.x sanitised field names by default then. The record names, and the schemas registered under the
existing subjects, stay the same. Debezium 2.0 and later default to `none`, as this connector does.
See [record formats](../reference/record-formats.md#avro-and-other-strict-naming-rules).

Dropped, with the reason in the report: the LogMiner tuning settings (`log.mining.strategy`, batch
and sleep sizes, buffer settings, the query filter mode), `heartbeat.action.query` (heartbeats
never write to the database), the schema history settings (the connector keeps schema versions in
its own topic), the signal table (signals come from a Kafka topic) and the flush table.

Manual, because the connector has no equivalent or the translator cannot carry the value over:

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
   deleted. Each archived log is also added to a LogMiner session, which is never started, so a
   file removed outside RMAN that the catalog still lists is found before the new connector
   would stop on it. If any is gone it exits 1 and writes nothing. The database user needs the
   same LogMiner privileges as the connector's user; with the connector's own user that holds.
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

<Faq
  items={[
    {
      question: "Can I keep my existing Kafka topics when migrating from Debezium?",
      answer:
        "Yes. The translated configuration keeps Debezium's topic names, with cdc.topic.template set to ${prefix}.${schema}.${table}, and this connector writes the Debezium-compatible envelope by default. Consumers of those topics keep working. Record names, and the schemas registered under the existing subjects, stay the same.",
    },
    {
      question: "Will I lose or duplicate changes during the cutover?",
      answer:
        "Nothing is skipped. The new connector starts at the Debezium offset's scn, which is Debezium's resume position. Changes committed after that SCN, up to Debezium's last commit, are delivered a second time. Consumers that upsert by key are unaffected, and the cdc.xid and cdc.commit_scn headers identify the repeats. A transaction that began before the start SCN and committed during that overlap can be delivered again in part, because Debezium had already delivered it whole.",
    },
    {
      question: "Does the migration tool translate Debezium configuration automatically?",
      answer:
        "Yes. migrate_from_debezium.py translates a Debezium Oracle connector configuration into cdc properties and reports every source property as mapped, mapped with a change, dropped, or manual. Properties left manual, because the connector has no equivalent or the names cannot be rewritten, include column.exclude.list, column.include.list, log.mining.username.include.list, and the XStream and OpenLogReplicator adapters. A literal password is never copied.",
    },
    {
      question: "Do I need to change Oracle database setup (supplemental logging, privileges) when moving from Debezium's LogMiner adapter?",
      answer:
        "A LogMiner deployment already uses ARCHIVELOG mode and supplemental logging, which this connector also requires, and the connector reads changes through LogMiner only. You still run oracle-cdc-doctor against the translated configuration. The mining user needs the LogMiner grants in the database setup guide, including supplemental logging on captured tables, and the archived logs from the Debezium resume SCN must still exist. Real Application Clusters are not supported yet.",
    },
    {
      question: "How do I verify the cutover from Debezium?",
      answer:
        "After the overlap has passed, run verify_cutover.py against the new connector's topics and keep the evidence file. The verifier also reads topics written by the Debezium Oracle connector, so you can check the existing pipeline before the migration starts. The cutover verification guide describes the evidence file and its limits.",
    },
    {
      question: "Which Debezium adapters can this takeover use?",
      answer:
        "Only the LogMiner adapters, logminer and logminer_unbuffered. Debezium uses LogMiner by default. The offsets of the XStream and OpenLogReplicator adapters carry no position the takeover can use, and the database must be a single instance.",
    },
  ]}
/>
