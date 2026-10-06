---
title: "CDC-3001 Decode failure"
description: "Runbook for CDC-3001 DECODE: a change to a captured table could not be turned into a record, or a captured table cannot be keyed or read."
slug: /runbooks/decode
---

# CDC-3001 Decode failure

**Code:** `CDC-3001` (`DECODE`). Not retried: the task stops at once, unless
`cdc.on.decode.error=dlq` applies (see below).

## What the connector observed

The message names the table, the redo position (SCN and redo address) and what went wrong. There
are two families.

Row-level failures, where one mined change could not be decoded:

- LogMiner returned the row with STATUS 2 or 3, meaning it could not rebuild the SQL. Rows written
  before a later DDL on their table, which LogMiner returns with generic `COL n` names, are not in
  this family: the connector handles them by mining the range again with a dictionary from the redo,
  and stops with [CDC-6001](dictionary-unavailable.md) only when that is not possible.
- LogMiner marked the row `UNSUPPORTED` for a captured table, typically because of a column type it
  cannot reconstruct: "LogMiner marked a row of ... UNSUPPORTED".
- The SQL_REDO text did not parse, named a column the table does not have, or disagreed with
  LogMiner's operation code; or a LOB row had a shape the connector does not know.

Table-level failures, raised when the connector first reads a captured table:

- The table has neither a primary key nor a NOT NULL unique index and `cdc.key.missing` is `fail`.
- A key override in `cdc.key.columns` names a column the table does not have.
- The table is not in the data dictionary of its PDB.
- A snapshot met a column type the snapshot reader does not support.
- `cdc.columns.exclude` matches a column of the record key (the primary key, the unique index the
  connector chose or a column in `cdc.key.columns`): "cdc.columns.exclude matches column ... which
  is part of the record key". The task checks this at start, and again when a DDL changes a
  table's key.

The message never quotes row data. Where the cause is a value (a literal the connector could not
convert, or the SQL_REDO text around a parse error), it says the value is withheld. To see it,
read the row's SQL_REDO from LogMiner at the SCN in the message, or set
`cdc.log.sensitive.data=true` and restart the task: the failure then repeats with the value in the
message and in the worker log. Set it back once the cause is known.

With `cdc.on.decode.error=dlq`, row-level failures do not stop the task: the row goes to the DLQ
topic with its raw SQL_REDO and a `decode-error-dlq` or `unsupported-row` event goes to the ops
topic. For a table that `cdc.columns.exclude` may match, the DLQ record and the message withhold
the statement text, because it may hold an excluded value. Table-level failures always stop the
task.

## Why it stopped rather than continued

Publishing a row the connector could not decode would mean guessing which value belongs to which
column. Skipping it would lose the change without a trace. A table without a usable key cannot be
keyed in Kafka, so compacted topics and consumers that keep the latest record per key would go
wrong.

## Confirm the cause

Run the doctor against the connector configuration. Rules DOC-5 (identity columns and BOOLEAN,
JSON, VECTOR, BFILE or nested-table columns), DOC-6 (table or column names longer than 30
characters) and DOC-7 (keys) name the tables that LogMiner or the connector cannot handle:

```bash
java -jar oracle-cdc-doctor-cli.jar check --config connector.json
```

Then look at the table named in the message:

```sql
SELECT column_name, data_type, data_length, data_precision, data_scale, identity_column
FROM dba_tab_columns WHERE owner = :owner AND table_name = :table ORDER BY column_id;
SELECT owner, object_name, last_ddl_time FROM dba_objects
WHERE owner = :owner AND object_name = :table AND object_type = 'TABLE';
SELECT constraint_name, constraint_type, status FROM dba_constraints
WHERE owner = :owner AND table_name = :table AND constraint_type IN ('P', 'U');
```

## Recover

- **Unsupported column type or name length.** Exclude the table with `cdc.tables.exclude`, or change
  the table so that LogMiner supports it. When the message says the decoder does not support a
  column's type and the column is not needed downstream (and is not a key column), excluding only
  that column with `cdc.columns.exclude` is enough: excluded values are never converted. Rows
  LogMiner itself marks `UNSUPPORTED` are not helped by a column filter. Then restart the task.
- **Excluded key column.** Change `cdc.columns.exclude` so that it no longer matches the key
  column, or key the table by other columns with `cdc.key.columns`. Then restart the task.
- **No key.** Add a primary key or a NOT NULL unique index, name key columns in `cdc.key.columns`,
  or set `cdc.key.missing` to `rowid` (records keyed by ROWID; a moved row changes its key) or
  `none` (records without a key). Then restart the task.
- **A row the connector does not understand.** Report it in a GitHub issue with the message, the
  Oracle version and release update, and the table's DDL; do not include row values. To keep the
  other tables flowing in the meantime, set `cdc.on.decode.error=dlq` and restart the task: it
  resumes from its last acknowledged position, the failing row goes to the DLQ topic when it is
  mined again, and the `OracleCdcDlqRecords` alert fires. Rows in the DLQ are not on the change
  topic; once a fixed release is running, reload the table with a
  [`snapshot` signal](../signals.md).

Restart the task with:

```bash
curl -s -X POST "$CONNECT/connectors/$NAME/restart?includeTasks=true&onlyFailed=true"
```
