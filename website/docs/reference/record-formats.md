---
title: Record formats
description: The Debezium-compatible envelope shipped first and the Confluent-compatible format that follows.
---

# Record formats

**Status:** the Debezium-compatible envelope (`op` of `c`, `u`, `d` and `r`, `before`, `after`,
`source`, `ts_ms`) is the Phase 1a format. The Confluent Oracle CDC Source compatible format,
selected with `cdc.output.format=confluent`, is Phase 2.

Every record carries headers with the Oracle transaction id, commit SCN, container name and
event index, so consumers can detect duplicates after an at-least-once restart.

## LOB columns

`cdc.lob.mode` decides how CLOB, NCLOB and BLOB columns appear in records.

| Mode | What the records carry |
|---|---|
| `skip` (default) | No LOB fields. A statement that only changes a LOB still produces an update record, with the other columns unchanged. |
| `inline` | The value assembled from redo. CLOB and NCLOB values are strings, BLOB values bytes. A value the redo does not carry is published as `cdc.unavailable.placeholder` (default `__cdc_unavailable_value`; its UTF-8 bytes for a BLOB). |
| `reselect` | As `inline`, then every value still unavailable in an insert or update is read from the table as of the commit SCN, one query per row. When the query cannot return it, the record carries the placeholder and `source.reselect` is `failed`. |

A value is unavailable when the redo does not hold all of it:

- the before image of an update or delete (LogMiner never logs LOB columns in a WHERE clause);
- a LOB the update did not change, including the new row of a primary key change;
- a partial change to an existing large value through `DBMS_LOB.WRITE`, `WRITEAPPEND`, `ERASE` or
  `TRIM`;
- a value larger than `cdc.lob.max.bytes` when `cdc.lob.oversize.action` is `placeholder`;
- XMLTYPE columns, in this release.

A consumer keeps the previous value of a field that carries the placeholder. Values written by an
INSERT or by a statement that replaces the whole value are always available in `inline` mode.

Savepoint rollbacks are handled. When a rollback undoes `DBMS_LOB` calls on an existing row, the
redo does not show where one call ends and the next begins. The connector then publishes an update
of that row with the LOB value unavailable rather than risk dropping a change that was committed. In
`reselect` mode that update carries the committed value.

`reselect` needs `FLASHBACK ANY TABLE` (or `FLASHBACK` on the captured tables) for the connector
user. A query that reaches past a DDL on the table (ORA-01466) or past the undo retention (ORA-01555)
leaves the value unavailable.
