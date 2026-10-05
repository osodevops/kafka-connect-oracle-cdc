# ADR-0016: The lag case is replayed per step, and rows keep the version they were decoded with

**Status:** Accepted (evidence in `oracle-cdc-core/src/main/resources/reference/dictionary-replay.md`
and `operation-codes.md`, section "Online catalog after DDL")
**Context:** P1-17 (PRD-03 section 3 steps 4 and 5, ADR-0008), 5 October 2026.

## Context

ADR-0008 established that the online catalog returns rows written before a later DDL on their table
as STATUS 2 with generic `COL n` names, and that a dictionary stored in the redo plus
`DDL_DICT_TRACKING` decodes them with real names. PRD-03 section 3 step 4 detects the lag case from
`LAST_DDL_TIME` or object versions and a DDL ahead in the window. Building it raised four questions
the PRD does not answer: how to detect the case cheaply and exactly, which schema version decodes a
replayed row, how a record renders when the registry has moved on, and what happens when several
DDLs fall inside the replayed range.

## Decision

1. **Detection is by the rows themselves.** After a step's online pass, any DML row of a captured
   table with STATUS 2 and generic `"COL n"` names (LOB rows are STATUS 2 for another reason and
   keep their names) marks its table as lagging. The step is discarded and mined again with the
   redo dictionary; nothing of the online pass is applied (ADR-0005). The next step returns to the
   online catalog. This replaces the `LAST_DDL_TIME` comparison in PRD-03 step 4 as the trigger:
   LogMiner's own verdict is exact, where the time comparison is not.
2. **The build.** The newest build complete in an archived log that ends at or before the step's
   LogMiner start SCN, from `V$ARCHIVED_LOG.DICTIONARY_BEGIN` and `DICTIONARY_END`. Every log from
   the build to the step's end is added. No such build, a missing log in between, or a row still in
   generic names after the replay, is `DictionaryUnavailableException` (`CDC-6001`).
3. **Version per replayed row.** Rows of lagging tables decode with the version valid at their SCN
   (`SchemaRegistry.at`): the newest stored version whose `validFromScn` is at or below the row's
   SCN. A version first read from the dictionary is valid from
   `TIMESTAMP_TO_SCN(LAST_DDL_TIME + 10 s)`, or from any SCN when that time is older than the
   SCN-to-time mapping. Rows mined with the online catalog keep the current version: STATUS 0
   means LogMiner matched them to today's dictionary.
4. **Inexact versions.** When a DDL is applied and the table's `LAST_DDL_TIME` is more than ten
   seconds after the DDL's own time, the dictionary already reflects a later DDL. The new version
   gets today's layout but `exact = false`. Online rows may use it (LogMiner vouches for them).
   Replayed rows that would need it stop with `CDC-6001`, because the layout between the two DDLs
   is not known and DDL text is never parsed for layouts (PRD-03 section 2). When the later DDL is
   mined and confirms the layout, an exact version follows.
5. **Rows keep their version.** `RowChange.schemaVersion` records the version a row was decoded
   with, and the sink renders the record with that version. It is carried through the spill store
   and the journal as a trailing field; older payloads read as 0, the current version. Otherwise a
   row decoded with an older version could lose the values of a dropped column at render time.
6. **Builds.** `cdc.dictionary.build.interval.ms` (default one day, 0 off) and
   `cdc.dictionary.build.time` (default 02:00 database time) schedule builds on their own thread
   and connection. One extra build runs at start when the archived logs hold none. A privilege
   error switches builds off with a `dictionary-build` ops event; other failures are reported and
   the schedule continues.

## Consequences

- A lagging step reads all redo since the last build. A busy database with a day-old build pays
  for that on every lagging step until the window passes the DDL. More frequent builds bound the
  cost. Keeping one redo-dictionary session open across consecutive lagging steps, or decoding the
  `HEXTORAW` values of STATUS 2 rows directly from a stored version, would avoid it. Either needs
  a new ADR.
- Two DDLs on one table within ten seconds of each other are treated as one when applied out of
  date. Rows between them decode with the later layout: values for later-dropped columns raise a
  decode error, and later-added columns read as null.
- Without broker access, versions do not persist, so after a restart a table first read after its
  DDL cannot decode its earlier rows: `CDC-6001`.

## Evidence

`LagCaseReplayEngineIT` (the PRD-03 acceptance case: 50 DML, a drop and an add, 50 DML across a
restart) and `LagCaseNoBuildEngineIT` against Oracle Database Free; `LagCaseEngineTest`,
`SchemaTest` and `LogInventoryTest` for the rules above.

## PRD edits

PRD-03 section 3 step 4: detection is by STATUS 2 rows with generic names, and the replay covers
the mining step rather than "from the current position to that DDL".
