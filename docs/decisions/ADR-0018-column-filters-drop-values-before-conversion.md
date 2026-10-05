# ADR-0018: Column filters drop values before conversion and keep the full layout

**Status:** Accepted
**Context:** PRD-01 SRC-SEL-2 (`cdc.columns.exclude`), increment P1-26, 5 October 2026.

## Context

SRC-SEL-2 asks for excluded columns to be "dropped after decoding and never logged". Building it
raised five questions the PRD does not answer: where exactly a value is dropped (a decoded value
already sits in the decoder's output, and a literal the decoder cannot convert fails the row with
the literal in its message), what the stored schema versions hold, how a LOB statement on an
excluded column takes part in savepoint undo, what the dead letter topic and error messages may
quote, and what happens when a key column matches a pattern.

## Decision

1. **Patterns.** Regular expressions over `PDB.SCHEMA.TABLE.COLUMN` in a container database and
   `SCHEMA.TABLE.COLUMN` otherwise, matched whole and case-insensitively unless
   `cdc.tables.case.sensitive=true`: the same rules as `cdc.tables.include` (SRC-SEL-1).
2. **Dropped before conversion.** The decoder skips an excluded column as soon as the parser has
   named it, before its literal is converted. Its value never reaches a `RowChange`, so the
   transaction buffer, the spill files and the transaction journal never hold it, and a literal
   the decoder cannot convert does not fail the row. A column the schema does not know still stops
   the row, excluded or not: that check guards against a stale layout.
3. **Full layout stored.** The schema registry and the schema topic keep the dictionary's layout
   (names and types, no values). The layout is projected where it is rendered or selected: the
   Debezium envelope builds Connect schemas without excluded columns, snapshots leave them out of
   the select list, and reselect never queries them. Changing the patterns therefore needs no
   schema rebuild and cannot trigger `CDC-6003`.
4. **LOB statements keep their shape.** A LOB row of an excluded column is decoded into a fragment
   with its positions and no data; the assembler keeps no content for it. The statement's change
   still enters the buffer, so an undo of the statement (ADR-0015) finds it instead of an older
   change of the same table.
5. **Raw redo withheld.** A table whose name a pattern matches, or could match with more
   characters, may have excluded columns. For such a table the DLQ record carries a fixed
   placeholder instead of SQL_REDO and SQL_UNDO, and a statement that fails to parse is reported
   without the parser's quotation of it (and without the parser exception as cause).
6. **Key columns cannot be excluded.** A pattern that matches a `cdc.key.columns` entry is a
   configuration error; validation reports a primary key or unique index column against
   `cdc.columns.exclude`; the task refuses to start, and the engine stops with `CDC-3001` at a
   table's first change or at a DDL that moves the key onto an excluded column.

## Consequences

- Rows whose excluded columns hold values the decoder cannot convert are now published (without
  those columns) instead of stopping the task.
- Journal chunks written before a column was excluded keep its values until their transaction
  commits and the chunks are tombstoned; the records built from them do not carry the column.
- The may-match test for raw redo is judged from the table name only, so a pattern starting with
  `.*` withholds the raw redo of every table.

## Evidence

`ColumnFilterTest`, `ColumnFilterDecodeTest`, `ColumnFilterEngineTest` (spill files and journal
chunks free of an excluded value, undo of a LOB statement on an excluded column, reselect), the
connector's configuration, envelope, DLQ and task tests, and the regression suites for dbz#1599:
`ExcludedColumnsStayOutAcrossDdlConnectorIT` (snapshot, streaming across ALTER TABLE, validation
of a key column) and `ExcludesColumnsOfRowsReplayedAfterDdlEngineIT` (rows replayed after DDL).

## PRD edits

PRD-01 SRC-SEL-2: patterns cover `PDB.SCHEMA.TABLE.COLUMN` in a CDB; columns are dropped while
decoding, before conversion; key columns cannot be excluded (ADR-0018).
