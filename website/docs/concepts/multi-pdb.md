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
