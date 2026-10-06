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

## Amendment, 6 October 2026: layouts are read at start

**Context.** `SchemaTopicLossNightlyIT` wrote rows and then ran `ALTER TABLE ... ADD` within a
second of the connector starting. The registry read a table's layout only when the first row of
the table was decoded, which came after the DDL, so version 1 had the new layout and was valid from
ten seconds after the DDL (point 3). The rows written before the DDL came back with generic names
and the replay stopped with `CDC-6001`: no version was known to be valid for them. A schema
migration soon after a deployment does the same, and so, above all, does a migration during a long
initial snapshot, which holds streaming back so that a table's first row may be decoded hours after
its DDL.

**Decision.**

1. **Read at start.** When the task starts it stores version 1 of every captured table that has no
   stored version (`OracleCdcSourceTask.start`, `CaptureEngine.readStartLayouts`,
   `SchemaRegistry.readLayouts`). The hook is the task's start: only there are the captured tables
   known (the session resolves them when it builds the engine), the schema topic already loaded and
   checked (SCH-6), and no snapshot or column filter check has yet read a layout with the
   decode-time rule. The rule lives in the engine and the registry, so engine tests prove it; the
   engine-tier `EngineDriver` reads at start as the task does.
2. **Validity.** A version read at start is valid from the start position's resume SCN when the
   table's last DDL is more than ten seconds (the slack of point 3) before the time of the SCN
   checked; otherwise the decode-time rule of point 3 applies. The slack leans the other way from
   point 4's. There the reference SCN is a DDL, whose own `LAST_DDL_TIME` always falls within the
   slack of it. Here a DDL that ran just after the start, or raced the read, must never pass for
   one before it, or version 1 would claim the later layout for rows written before that DDL. It is
   the converse of SCH-6: a stored layout that differs from the dictionary passes SCH-6 exactly when
   the DDL may be after the resume point, and a layout read at start is valid from the resume point
   exactly when it may not.
3. **First start (ADR-0019).** The resume SCN is then the mining start, which is below the start
   SCN when transactions were open. The version is valid from the mining start, and the check of
   point 2 is made at `start_open_scn`, the SCN read just before the open transactions were listed
   (or at the resume SCN if that is later). Below the start SCN the engine decodes only transactions that were listed
   or that began at or after `start_open_scn`; the others are dropped before they are decoded. A
   listed transaction that changed a table holds the table's DML lock from that change until it
   ends, which is after `start_open_scn`, and no DDL on the table completes while the lock is held
   (a DDL waits for it or fails with ORA-00054; an online DDL waits for the transactions in flight).
   The check rules out a DDL after `start_open_scn`, so the table has had one layout from that
   change on. A transaction that began at or after `start_open_scn` writes only after the table's
   last DDL. Every row decoded below the start SCN was therefore written with the layout read at
   start. Beginning at the start SCN instead would leave the early rows of an open transaction
   without a version whenever a DDL followed its commit before the engine reached them; checking at
   the mining start would pass over tables whose last DDL fell between the two, a window a long
   open transaction makes long, although the lock covers them. With `cdc.start.scn`,
   `start_open_scn` is the start SCN. A restart before the first acknowledged commit keeps these
   fields and applies the same rule.
4. **Tables that join (SRC-SEL-4).** A table the object id refresh adds is read at once, before its
   snapshot or its rows, valid from the cursor under the check of point 2. A table that joins
   through its own CREATE or RENAME has that DDL as its last, within the slack of the cursor, so it
   keeps the decode-time rule; reading it at the join still protects its rows from ten seconds
   after that DDL against a later one. The ten-second merge of point 4 is not extended to created
   tables.
5. **Cost.** The read is one pass per container: the container id, then for every 500 tables five
   queries (columns, supplemental log groups, primary keys, unique indexes, and last DDL times last,
   so a DDL in between shows as a later time, never as an older layout paired with an older time),
   plus one `SCN_TO_TIMESTAMP` for the checked SCN and one `TIMESTAMP_TO_SCN` for each distinct last
   DDL time among tables that fail the check. The per-table path takes about a dozen round trips
   for each table. Only tables without a stored version are read, so a restart with the schema topic
   reads none. This is designed for thousands of tables and has not been measured against a
   database.
6. **Schema topic.** Versions read in `start()` are kept and written once the engine thread, or the
   snapshot thread in `snapshot_only`, runs: `start()` cannot wait for `poll()` to drain the record
   queue, which holds four times `cdc.poll.max.records` and at least 1,000 records; more tables
   than that would fill it. This also ends a hang
   in which an initial snapshot of that many tables read every layout in `start()`. SCH-6 is
   unchanged: it checks the stored versions before the read, and the read leaves them alone.
7. **Inexact versions in the decoder.** A row that names a column missing from the version it
   decodes with stops with `CDC-6001` (`DictionaryUnavailableException`) rather than `CDC-3001`
   when that version is inexact: it was read after a further DDL, so the column may have existed
   when the row was written. Like every `CDC-6001`, the row is not sent to the DLQ.

**Consequences.**

- Every captured table gets a schema topic record at the first start, not only tables with changes.
- A table that cannot be keyed under `cdc.key.missing=fail`, or that vanished after it was
  resolved, is passed over at start and reported at its first change, as before; the start does not
  fail on it.
- `CDC-6001` for a missing version remains when a DDL ran while the connector was stopped and no
  stored version survived (no broker access, or a lost schema topic), when a table's last DDL ran
  within ten seconds of the start position, and when several DDLs on one table ran before the
  connector reached the first.
- When two DDLs cancel out, such as an ADD and a DROP of the same column, the layout read at the
  first equals the one before it. Nothing marks the gap, and rows between them that name the column
  still stop with `CDC-3001`.

**Future work.** Decoding rows under an inexact version, and marking the cancelled ADD and DROP,
need layouts derived from the redo dictionary's DDL tracking (the layout LogMiner itself holds at
each DDL) rather than from the online catalog. Not built; it needs its own ADR.

**Evidence.** `LayoutsAtStartEngineTest` (a DDL soon after the start; the first start of ADR-0019;
a DDL that may have run after the start; a table that joins), `LagCaseEngineTest` (the inexact
stop, with the DLQ configured), `RowDecoderTest`, `SchemaTest`, `JdbcDictionaryReaderTest`,
`SchemaTopicStoreTest` and `OracleCdcSourceTaskTest` (1,200 tables against a queue of 1,000
records; SCH-6 with the read at start; a table that joins by signal). To be confirmed against
Oracle Database Free by `SchemaTopicLossNightlyIT`, the lag case suites, `DdlUnderLoadEngineIT`,
the snapshot suites and `MultiPdbConnectorIT`.

**PRD edits.** PRD-03 section 2 gains the "Layouts at start" bullet.
