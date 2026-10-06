---
title: oracle-cdc-admin
description: Offsets, resnapshot, transaction and journal inspection commands.
---

# oracle-cdc-admin

The operator commands ship in the same CLI as [oracle-cdc-doctor](doctor.md). Run them as
`admin` subcommands of the doctor, or under their own name through the `AdminMain` entry point:

```bash
java -jar oracle-cdc-doctor-cli.jar admin offsets show --connect-url http://connect:8083 --name orders
java -cp oracle-cdc-doctor-cli.jar sh.oso.connect.oracle.doctor.cli.AdminMain offsets show --connect-url http://connect:8083 --name orders
```

Every command works through the Kafka Connect REST API of a worker in the connector's cluster and
takes the same options:

| Option | Meaning |
|---|---|
| `--connect-url` | Kafka Connect REST URL (required) |
| `--name` | Connector name (required) |
| `--config` | Connector configuration file to use instead of the worker's copy, for example when the worker holds the password as a config provider reference |
| `--bootstrap-servers` | Brokers for the ops, signals and journal topics; default `cdc.kafka.bootstrap.servers` |
| `--command-config` | Kafka client properties file with the security settings |

The database is reached with the connector's own `cdc.database.*` settings, and Kafka with its
`cdc.kafka.*` settings. Passwords are passed to the clients and never printed.

| Exit code | Meaning |
|---|---|
| 0 | Done |
| 1 | Refused, or a call failed; nothing was changed unless the message says otherwise |
| 64 | Usage or configuration error |

## offsets show

Reads the stored offset with `GET /connectors/{name}/offsets` and decodes it: resume SCN and redo
address, last acknowledged commit (SCN, transaction, thread, redo address, event index), journal
generation, schema epoch, database identity, the orphan release ledger and the snapshot progress.
`--format json` prints the raw offset next to the decoded fields. `--check-redo` also checks in
V$ARCHIVED_LOG, with the continuity checks the connector runs, that every log from the resume SCN
on is still present, and adds each archived log to a LogMiner session that is never started, as
`offsets set` does, so a file removed outside RMAN is reported as missing.

## offsets set

```bash
java -jar oracle-cdc-doctor-cli.jar admin offsets set --connect-url http://connect:8083 --name orders \
    --scn 48151623 --reason "re-mine transaction 3:5.12.900 after CDC-7001"
```

Sets the SCN the connector resumes mining from, through Kafka Connect's offsets API (KIP-875):

1. It refuses an SCN above the database's current SCN, and an SCN whose redo is not all present:
   a log the catalog marks deleted (CDC-2002), a missing sequence (CDC-2001), or a listed archived
   log that LogMiner cannot open, such as a file removed outside RMAN (CDC-2002), between the SCN
   and now. Every log is added to a LogMiner session as the task would add it; the session is
   never started. For tables whose redo is gone, use `resnapshot`.
2. Moving the position forward skips every change committed in between, so it is refused unless
   `--allow-skip` is given. Moving it back is always allowed: transactions already delivered are
   recognised by the stored last commit and not delivered again.
3. It stops the connector (`PUT /connectors/{name}/stop`) and waits until it is STOPPED, then
   checks again against the offset as it stands, since the task may have committed once more
   while stopping.
4. It writes an `offsets-set` event to the connector's [ops topic](../reference/ops-topic.md) with
   `outcome` `applying`, patches the offset (`PATCH /connectors/{name}/offsets`), and writes a
   second event with `outcome` `applied`, or `failed` with the error. A change is never made
   without a record of it.
5. The connector stays stopped unless `--resume` is given.

`--reason` is required. `--forget-released` removes transaction ids from the offset's orphan
release ledger, so that a transaction mined again from an earlier SCN is delivered instead of
stopping the task with [CDC-7001](runbooks/orphan-release-violation.md). Broker access is
required, since the change has to be recorded.

The `offsets-set` event details are `connector`, `command`, `reason`, `operator` (the user that
ran the command), `direction` (`backward`, `forward`, `same` or `initial`),
`previous_resume_scn`, `new_resume_scn`, `outcome`, and `forgotten_released` or `error` when they
apply. The record has the connector's ops record shape. Its JSON form follows the connector's
`value.converter` when that is the JSON converter (with or without the schema envelope as its
`schemas.enable` says) and is plain JSON otherwise; `--ops-format json` or `json_schemas` chooses
explicitly, and a non-JSON converter needs one of them.

## resnapshot

```bash
java -jar oracle-cdc-doctor-cli.jar admin resnapshot --connect-url http://connect:8083 --name orders \
    --tables FREEPDB1.APP.ORDERS,FREEPDB1.APP.ORDER_LINES --reason "archived logs purged during the outage"
```

Asks for a new snapshot of the named tables (PDB.OWNER.TABLE in a CDB, OWNER.TABLE otherwise;
each must be a table the connector captures). It writes a `snapshot` signal to the connector's
signals topic (`cdc.signals.topic`, default `${prefix}.cdc.signals`), keyed by connector name:

```json
{"id": "oracle-cdc-admin-...", "type": "snapshot", "data": {"tables": ["FREEPDB1.APP.ORDERS"]}}
```

The snapshot itself is taken by the connector's signal handling.

When the redo from the stored position is present, that is all: the offset is not touched and the
connector need not be stopped. When the stored position points into redo that is gone (the
connector stopped with [CDC-2002](runbooks/log-purged.md) or [CDC-2001](runbooks/log-gap.md)),
the command also moves the offset past the gap:

1. It finds the first SCN after the gap from which every log to now is present, and refuses when
   there is none.
2. Moving past the gap skips its changes for every captured table, not only the named ones. The
   snapshot restores the named tables; for any other captured table the command refuses and lists
   them, unless `--skip-gap-for-unlisted-tables` says they had no changes in the gap (the ops event
   records them as `unlisted_tables`).
3. It stops the connector, checks again, writes the signal, writes an `offsets-set` event with
   `command` `resnapshot`, the gap, the tables and the signal id, patches the offset, and writes the
   outcome. The signal goes out before the offset moves, so a failed offset change only leads to a
   repeated snapshot.
4. The connector stays stopped unless `--resume` is given.

## transactions

```bash
java -jar oracle-cdc-doctor-cli.jar admin transactions --connect-url http://connect:8083 --name orders \
    --jmx-url service:jmx:rmi:///jndi/rmi://worker-1:9999/jmxrmi
```

Lists open transactions with age, number of changes, size, user, client identifier, first SCN, and
whether GV$TRANSACTION still lists them:

- buffered transactions, from the task's `LargestTransactions` over JMX (the 20 largest by bytes;
  needs `--jmx-url` pointing at the worker that runs the task);
- journaled transactions, from the transaction journal topic (needs broker access); their age comes
  from `SCN_TO_TIMESTAMP` of their first SCN.

A transaction missing from GV$TRANSACTION has ended without its COMMIT or ROLLBACK being mined
yet, or is an [orphan](runbooks/orphan-transaction.md).

## journal inspect

Reads the transaction journal topic (`${prefix}.cdc.txjournal` by default) the way the task does at
start, with `cdc.journal.converter`, and prints the record counts (including tombstones and records
of other connectors), each journaled transaction with its chunks, generations, changes, undo
records, SCN range, user and client identifier, and what the next start does with it measured
against the stored offset: restored, or dropped because it is stale or will be mined again. It runs
the task's own journal loader, so a missing chunk is reported here as it would stop the task with
[CDC-4002](runbooks/journal-corruption.md), and the command exits with 1.

`explain-lag` is a doctor command: see [oracle-cdc-doctor](doctor.md#explain-lag).
