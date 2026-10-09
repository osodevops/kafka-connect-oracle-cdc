---
title: Multi-PDB capture
description: One mining session at CDB$ROOT routing changes from several PDBs.
---

# Multi-PDB capture

One connector can capture tables from several pluggable databases of one container database. List
them in `cdc.database.pdbs`; the connector user connects to `CDB$ROOT`, mines the redo once, and
routes every change by the container it came from.

- **Routing.** LogMiner reports the source container of each row (`SRC_CON_NAME`), so records go to
  the topic of their PDB, by default `<prefix>.<PDB>.<OWNER>.<TABLE>`, and carry the PDB in
  `source.pdb`. Tables with the same owner and name in two PDBs stay apart.
- **Transactions.** A transaction is identified by its container and transaction id together:
  undo is local to a PDB, so the same transaction id can be open in two PDBs at once.
- **Selection.** `cdc.tables.include` and `cdc.tables.exclude` match `PDB.OWNER.TABLE`, so one
  pattern can cover several PDBs (`FREEPDB[12]\.APP\..*`) or one PDB only.
- **Snapshots.** An initial snapshot reads the matching tables of every listed PDB, each through a
  connection switched to its container.
- **Every PDB must be open.** LogMiner reads every container whose redo lies in the range it mines,
  including PDBs the connector does not capture. While one is closed, for example just after a
  restart, mining waits and retries (ORA-16331, see
  [transient database errors](../operations/runbooks/transient-database.md)). Give each PDB a saved
  state so it opens with the CDB; `oracle-cdc-doctor check` reports PDBs that are closed or have
  none (DOC-21).

## Tables that appear later

When a `CREATE TABLE`, a rename or a `refresh-tables` signal adds a table that matches the include
patterns, the connector starts capturing it without a restart: the next mining step already
includes it. It writes a `table-added` event to the ops topic and, under
`cdc.snapshot.mode=initial`, snapshots the table while streaming continues when it may already hold
rows: after `CREATE TABLE ... AS SELECT`, a rename into the include patterns or a `refresh-tables`
signal. A table made by a plain `CREATE TABLE` starts empty, and streaming captures every row
written to it, so it is not snapshotted. Until that snapshot
has started, the offsets record the table as pending, so a restart does not lose it. A table that
is dropped or renamed away produces a `table-removed` event.

## Mining from inside a PDB

Mining at `CDB$ROOT` as a common user is the normal form, and the only one that captures several
PDBs from one connector. Where the root is out of reach, as on Amazon RDS with the CDB architecture
or on Autonomous Database, the connector can mine one PDB from inside it, connected to the PDB as
a local user. That is **range mode** (`cdc.mining.mode=range`, or `auto`, the default, which
chooses it whenever the connection is to a PDB):

- LogMiner refuses to add log files inside a PDB, so the connector starts each step with the SCN
  range only and Oracle chooses the logs. The connector still lists those logs from
  `V$ARCHIVED_LOG` and `V$LOG` and checks them for gaps, so a missing or purged log stops it as in
  the normal form.
- LogMiner cannot use a dictionary from the redo inside a PDB. Rows written before a DDL the
  connector has not mined yet (the lag case in [schema and DDL](schema-and-ddl.md)) therefore stop
  the task with CDC-6001 instead of being replayed, and dictionary builds are switched off. Keep the
  connector's lag short where DDL is frequent.
- The user needs `CREATE SESSION`, `LOGMINING`, `SELECT ANY TRANSACTION` and `EXECUTE ON
  DBMS_LOGMNR` in that PDB, and the fixed views the doctor checks (DOC-4). It is a local user, with
  no `C##` prefix and no `CONTAINER_DATA`.

Range mode is available from release 0.1.3. It passed the qualification suites on Amazon RDS for
Oracle 19c with the CDB architecture on 9 October 2026; Autonomous Database has not been qualified
yet.

