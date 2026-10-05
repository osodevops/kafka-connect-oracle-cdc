# ADR-0015: LOBs are assembled per statement, and undo targets the newest change

**Status:** Accepted (evidence in `oracle-cdc-core/src/main/resources/reference/lob-redo-shapes.md`)
**Context:** P1-18 (PRD-00 CORE-DEC-6, CORE-DEC-7, CORE-TX-2; PRD-01 SRC-LOB), 5 October 2026.

## Context

PRD-00 CORE-DEC-6 assumed LogMiner reports LOB changes as `SEL_LOB_LOCATOR` rows followed by
`LOB_WRITE`, `LOB_TRIM` and `LOB_ERASE` rows that can be assembled per row and column. The spike on
Oracle Database Free 23ai (SecureFile and BasicFile storage) found:

- `LOB_WRITE` (10), `LOB_TRIM` (11) and `LOB_ERASE` (29, not the documented 28) rows have STATUS 2
  with INFO "LOB sql_redo not re-executable", yet each SQL_REDO is a complete PL/SQL block: it
  selects the locator by the row's non-LOB columns and makes one `dbms_lob` call. Text amounts and
  offsets count characters (code points). Rows with code 9 are labelled INTERNAL and carry no
  SQL_REDO; the undo block address of LOB rows is zero.
- A statement that writes an out-of-row LOB gives its row piece (the INSERT with `EMPTY_CLOB()`, or
  the UPDATE of other columns) the placeholder ROWID `AAAAAAAAAAAAAAAAAA`, and its LOB rows the
  placeholder too. `DBMS_LOB` calls on an existing row may carry the real ROWID.
- Oracle undoes one statement's change to one row with one undo row, newest first, and that undo
  row names the real ROWID. A rollback of an UPDATE that set a column and a large LOB is a single
  `UPDATE ... set "NAME" = ..., "C" = EMPTY_CLOB()`. Two `DBMS_LOB` calls on one row give two undo
  rows, but nothing in LogMiner shows where one call's rows end and the next begin.

The existing undo rule (remove the latest earlier change with the same ROWID) therefore missed
every undo of a placeholder change, published rolled-back changes, and could remove an earlier
change of the same row instead. The query also filtered on code 28, so erase rows were never mined.

## Decision

1. LOB rows are parsed (`SqlRedoParser.parseLob`, no regular expressions) and folded into the change
   of their statement: a row piece with the placeholder ROWID keeps taking the LOB rows that name its
   row image, and an INSERT also takes its locator UPDATE (with the real ROWID). LOB rows without a
   row piece form their own change, closed by another row, another ROWID kind, or a write from the
   first character into a column the change already wrote.
2. A value is published only when known completely: an empty or literal base from the row piece, or
   writes covering it from the first character with a trim fixing the length. Otherwise the column is
   left out of the image, which the connector renders per `cdc.lob.mode`. Memory per value is bounded
   by `cdc.lob.max.bytes`; above it the value is unavailable and, with `cdc.lob.oversize.action=fail`,
   the commit stops with `CDC-3003`.
3. A change that LogMiner gave only the placeholder ROWID gets a synthetic ROWID (`RowIds`), never
   published. The buffer keeps, per transaction, the order of its synthetic changes. An undo of the
   same table whose operation reverses the newest surviving change, when that change is synthetic,
   targets it; otherwise the ROWID rule applies. The resolved target is what the spill file and the
   journal record, so replay after a restart gives the same result.
4. An undone LOB group (LOB rows without a row piece) is downgraded, not removed: its before image
   stays as an update whose LOB values are unavailable. Removing it could drop a committed earlier
   `DBMS_LOB` call merged with the undone one.
5. `cdc.lob.mode=reselect` queries the values still unavailable in insert and update after images AS
   OF the commit SCN, by key or by real ROWID, on a connection of its own that switches container. It
   runs synchronously as the sink reads the committed transaction (one query per row); ORA-01555,
   ORA-08181 and ORA-01466 leave the value unavailable and set `source.reselect` to `failed`.

## Consequences

- `skip` (the PRD default) still groups statements, so savepoint rollbacks resolve the same way and
  a LOB-only change still produces an update record.
- Inline values are exact for inserts and whole-value updates (proven against the table by
  `LobModesEngineIT`); partial `DBMS_LOB` edits of existing values and unchanged LOBs, including the
  new row of a primary key change, need `reselect`.
- A rollback that undoes `DBMS_LOB` calls emits an update with the LOB unavailable even when every
  call of the group was undone.
- Reselect adds latency per row instead of the bounded asynchronous queue PRD-00 CORE-DEC-7 asked
  for; asynchronous reselect is left for a later increment.
- XMLTYPE values (XML DOC rows) are not assembled in this release and are unavailable.
- The LOB row shapes are from 23ai; 19c and 21c qualification (P1-30) must rerun the spike, and a
  release that splits the locator select from the writes stops with `CDC-3001` until handled.

## Evidence

`LobRedoShapesRefEngineIT` (reference document), `LobModesEngineIT` (inline, reselect and skip
against the table, savepoints and a failed insert included), `CorrectnessOracleConnectorIT` with LOB
writes in `reselect` mode, `LobAssemblerTest`, `LobUndoBufferTest` (heap, spill and journal replay),
`LobEngineTest`, `LobRedoParserTest`.

## PRD edits

PRD-00 CORE-DEC-6, CORE-DEC-7 and CORE-TX-2 carry a one-line reference to this ADR.
