---
title: oracle-cdc-doctor
description: The preflight checker, its rules, the redo profiler, sizing and lag explanation, output formats and exit codes.
---

# oracle-cdc-doctor

`oracle-cdc-doctor` runs against the database named in a connector configuration and reports
what would stop capture, with the SQL that fixes it. It also profiles the redo the connector has
to mine, sizes online logs and archive retention, and explains why a running connector lags. The
connector's own `validate`, which Kafka Connect runs when a connector is created or updated, runs
the fast subset of the rules and refuses the configuration on a blocking finding; warnings do not
refuse it. Validation also refuses a `cdc.columns.exclude` pattern that matches a column of a
table's record key, which `check` does not test. The operator commands for offsets, resnapshots,
transactions and the journal are described in [oracle-cdc-admin](admin.md).

```bash
java -jar oracle-cdc-doctor-cli.jar check --config connector.json [--format markdown|json|junit] [--rules all|fast]
    [--max-downtime 24h] [--bootstrap-servers host:9092] [--command-config client.properties] [--connect-url http://connect:8083]
java -jar oracle-cdc-doctor-cli.jar setup-sql [--config connector.json] [--profile production|lab] [--platform onprem|rds|autonomous]
    [--user c##cdc] [--password ...] [--non-cdb] [--pdbs A,B]
java -jar oracle-cdc-doctor-cli.jar redo-profile --config connector.json [--window 2h] [--sample-logs 1] [--top 20] [--query-timeout 10m]
java -jar oracle-cdc-doctor-cli.jar sizing --config connector.json [--days 7] [--max-downtime 24h]
java -jar oracle-cdc-doctor-cli.jar explain-lag (--jmx-url URL | --metrics-url URL) [--connect-url URL --name NAME | --config connector.json | --server PREFIX] [--interval 10s]
```

`--config` takes either the bare configuration map or the REST envelope
(`{"name": ..., "config": {...}}`). Durations are written as `2h`, `90m`, `7d` or in ISO-8601
form such as `PT2H`.

| Exit code | Meaning |
|---|---|
| 0 | No blocking findings (or the command completed) |
| 1 | Blocking findings, or the database could not be reached, or the command failed |
| 2 | Warnings only |
| 3 | Not implemented yet (Autonomous Database setup) |
| 64 | Usage or configuration error |

## check

`check` runs every rule below by default. `--rules fast` runs only the rules the connector's
`validate` runs, which need nothing but the database and the configuration. Output is Markdown (a findings table and the
SQL to run), JSON, or JUnit XML for CI: one test case per rule, failed when the rule has a blocking
finding, with warnings and information in the test case output.

The Kafka rules use the brokers in `cdc.kafka.bootstrap.servers` with the other `cdc.kafka.*`
client settings, or `--bootstrap-servers` and a Kafka client properties file in
`--command-config`. Without broker access they report that they did not check. With
`exactly.once.support=required` in the configuration and `--connect-url`, DOC-18 asks a Connect
worker to validate the configuration.

| Rule | Checks | Severity | In fast mode |
|---|---|---|---|
| DOC-1 | Database in ARCHIVELOG mode | blocking | yes |
| DOC-2 | Minimal supplemental logging at database level | blocking | yes |
| DOC-3 | Table-level supplemental logging on every captured table | blocking when absent, warning for primary-key-only | yes |
| DOC-4 | Grant profile, readable fixed views, common user and `CONTAINER_DATA=ALL` in a CDB | blocking | yes |
| DOC-5 | Identity columns and BOOLEAN, JSON, VECTOR, BFILE or nested-table columns | blocking | yes |
| DOC-6 | Table or column names over 30 characters | blocking | yes |
| DOC-7 | Primary key or NOT NULL unique index, honouring `cdc.key.missing`; ROW MOVEMENT on a keyless table | blocking, warning or info | yes |
| DOC-8 | CLOB, NCLOB, BLOB, XMLTYPE, LONG and LONG RAW columns and what `cdc.lob.mode` does with them | info, warning for XMLTYPE outside `skip` | no |
| DOC-9 | More than six log switches in an hour on any thread over the last seven days, with the online log size that keeps the peak hour at about four | warning | no |
| DOC-10 | Archived redo reaches back at least `cdc.txjournal.threshold.ms` plus the planned maximum downtime (`--max-downtime`, default 24 hours) | warning; info until a log has been deleted | no |
| DOC-11 | `UNDO_RETENTION` of at least 120 seconds, the longest a snapshot chunk read is expected to take | warning | no |
| DOC-12 | A valid local archive destination (or the configured one) | blocking | yes |
| DOC-13 | RAC: more than one enabled redo thread, which the task refuses at start; `cdc.database.fan.enabled` | blocking | yes |
| DOC-14 | Role and open mode fit `cdc.capture.mode`: online needs the primary open read-write; archive-only also takes a physical standby open read-only; a mounted, logical or snapshot standby is refused | blocking; warning for a standby open read-only without redo apply | yes |
| DOC-15 | Oracle Database 19c or later | blocking | yes |
| DOC-16 | Fixed-object statistics present | warning | no |
| DOC-17 | Internal topics exist with the right cleanup policy, or the connector may create them | blocking, warning or info | no |
| DOC-18 | With `exactly.once.support=required` or `transaction.boundary=connector`: `cdc.eos.batch.max.ms` below the producer's `transaction.timeout.ms`, that timeout within the brokers' `transaction.max.timeout.ms`, and the worker accepting exactly-once support | blocking | no |
| DOC-19 | TCP keepalive idle time (`oracle.net.TCP_KEEPIDLE` in `cdc.database.connection.properties`) below the default idle timeouts of common load balancers | info | no |
| DOC-20 | Whether the [lag case](../concepts/schema-and-ddl.md) can be recovered: `EXECUTE ON DBMS_LOGMNR_D`, the newest dictionary build flagged `DICTIONARY_BEGIN` and `DICTIONARY_END` in V$ARCHIVED_LOG, and every log from it on still present | info; warning when recovery is not possible | no |
| DOC-21 | Every pluggable database of the CDB is open (`V$PDBS`) and has a saved state (`DBA_PDB_SAVED_STATES`) | warning or info | no |
| DOC-22 | With an Avro converter set on the connector (`key.converter` or `value.converter`): captured names Avro refuses while `cdc.field.name.adjustment.mode` or `cdc.schema.name.adjustment.mode` is `none` | blocking (column names) or warning (owner, table or prefix) | yes |
| DOC-23 | Amazon RDS only: `archivelog retention hours` is at least `cdc.txjournal.threshold.ms` plus the planned maximum downtime (RDS deletes archived redo after it, and the default is 0) | blocking at 0, warning below the need, info when the capture user cannot read it | yes |

Notes on individual rules:

- DOC-7: snapshots read a keyless heap table in ROWID ranges, so a row that moves while a
  snapshot runs (ROW MOVEMENT) can be read twice or missed. With `cdc.key.missing=rowid` a moved
  row also changes its key.
- DOC-8: XMLTYPE values are neither assembled from redo nor reselected yet, so in `inline` and
  `reselect` mode the records carry `cdc.unavailable.placeholder` for them.
- DOC-9 and DOC-10 read the archive destination the connector mines from.
- DOC-10 measures how far back the oldest archived log still present reaches on every enabled
  thread. Until the catalog lists a deleted log, retention has not been exercised and the rule
  only reports what it needs.
- DOC-13: the connector is qualified for a single redo thread only. A database with more than one
  enabled redo thread is refused at start with CDC-5001 (see the
  [topology runbook](runbooks/topology.md)); a thread that `V$THREAD` shows DISABLED, left behind
  by an instance removed from the cluster, is fine. RAC capture is not available yet, and the
  connector does not subscribe to FAN events.
- DOC-19: the connector's connections use TCP keepalive. Without `oracle.net.TCP_KEEPIDLE` the
  operating system's idle time applies (two hours by default on Linux), which is longer than the
  idle timeout of an AWS Network Load Balancer (350 seconds) or an Azure Load Balancer (four
  minutes by default). Set it below the limit of any load balancer or firewall between the worker
  and the database, or set `SQLNET.EXPIRE_TIME` on the database server.
- DOC-20: replaying a lagging step needs a dictionary build in the redo before the rows and every
  log from that build on ([dictionary unavailable](runbooks/dictionary-unavailable.md)). With the
  privilege the connector writes a build at start when none exists and then on the
  `cdc.dictionary.build.*` schedule.
- DOC-21: LogMiner reads every container whose redo lies in the range it mines, captured or not,
  so a closed pluggable database makes mining wait with ORA-16331 until it opens (the connector
  retries, see [transient database errors](runbooks/transient-database.md)). A PDB without a
  saved state stays closed after a restart until someone opens it. The finding gives the
  `ALTER PLUGGABLE DATABASE ... OPEN` and `SAVE STATE` statements.
- Fix text follows the platform the doctor detects. On Amazon RDS (the `RDSADMIN` schema exists)
  it names the `rdsadmin` procedure, the DB parameter group or the AWS CLI call, because the master
  user has no SYSDBA, `ALTER SYSTEM` or `ALTER DATABASE`. See [Amazon RDS](../database-setup/amazon-rds.md).
- DOC-22: Avro names must match `[A-Za-z_][A-Za-z0-9_]*` in every part. A column name Avro
  refuses fails the task in the converter at its table's first record, so it blocks. An owner,
  table or topic prefix part Avro refuses passes the converter and a registry without a validity
  rule, and a consumer on Avro for Java 1.12 or later then refuses the record, so it warns. Set
  the [name adjustment modes](../reference/record-formats.md#avro-and-other-strict-naming-rules).
  A converter set only in the worker's configuration is not visible to the doctor; the rule then
  says nothing.

The `setup-sql --profile lab` output is the exact script that builds the test database image,
and a test asserts they stay identical.

## redo-profile

`redo-profile` reports archive generation per hour and thread over `--window` from
V$ARCHIVED_LOG, then mines the SCN range of the newest `--sample-logs` archived logs (every
thread's logs for that range, checked for continuity as the engine checks them) with LogMiner and
no table filter. It counts the rows by table and operation, flags tables that look like a
truncate-and-reload job (a TRUNCATE and at least 1,000 inserts in the sample) or a delete-and-reload
job (at least 1,000 deletes and inserts in similar numbers), and reports the share of rows naming a
table that belong to tables the connector does not capture. The connector's mining query filters
those rows out, but LogMiner still reads their redo, so a large share of uncaptured redo makes
every mining step slower. When the top redo source is not captured, the report says so.

The sample runs one LogMiner session on the doctor's connection with the online catalog; it reads
archived logs only and writes nothing to the database.

## sizing

`sizing` reads `--days` of V$ARCHIVED_LOG history (default seven) and reports per redo thread the
logs switched, average and peak switches per hour, the peak hour, archive generation per day, the
current online log size and the size that would keep the peak hour at about four switches. For
all threads together it reports the average and busiest day and the busiest hour, the retention a
maximum downtime of `--max-downtime` needs (the downtime plus `cdc.txjournal.threshold.ms`,
rounded up to whole hours) and the archive space that retention takes at the observed peak rate,
and how far back archived redo reaches now.

## explain-lag

`explain-lag` reads a running task's metrics twice, `--interval` apart, from the worker that runs
the task: over JMX (`--jmx-url`, which also gives the largest open transactions) or from the
Prometheus endpoint of the JMX exporter (`--metrics-url`, with the rules in `ops/jmx-exporter`).
The server name is the connector's `cdc.topic.prefix`, read from the configuration given with
`--config`, or through the Connect REST API with `--connect-url` and `--name` (which also prints
the connector's state), or given with `--server`.

The task's metrics give the mining step time as one figure (query, fetch and decode together),
the buffer, the record queue between the engine and Kafka Connect, and the delay from commit to
queue. The command names the most likely cause in plain English, with the readings behind it:

| Cause | Signature |
|---|---|
| Kafka side | The record queue is at least 80 per cent full in both readings, so mining waits for Kafka Connect to take records |
| Mining | Steps timed out at `cdc.mining.query.timeout.ms`, or the last step took more than twice `cdc.mining.target.latency.ms` (and at least five seconds) |
| Large transaction | One open transaction with at least 100,000 changes, a quarter of the buffer budget, or spilled changes; its records wait for its commit |
| Dictionary replays | Steps mined a second time with a dictionary from the redo, because rows predate a DDL the connector has not mined yet (the lag case) |

A breakdown of mining time into query, fetch and decode, and of delivery into emit and produce
time, needs metrics the task does not publish yet.

## Container image

Each release also publishes the CLI as a container image, `ghcr.io/osodevops/oracle-cdc-doctor`,
tagged with the version and `latest`, for amd64 and arm64, with an SBOM and build provenance in the
image. The image runs as an unprivileged user with `/work` as its working directory and contains no
Oracle Database software, so mount the directory that holds the connector configuration:

```bash
docker run --rm -v "$PWD:/work" ghcr.io/osodevops/oracle-cdc-doctor:0.1.1 check --config connector.json
```

Add `--network host` when the database listens only on the host's loopback address. Exit codes
are the same as for the jar.
