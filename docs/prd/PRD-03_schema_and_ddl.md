# PRD-03: Schema Registry and DDL Handling

**Status:** Draft for implementation
**Module:** `oracle-cdc-core`, package `sh.oso.connect.oracle.core.schema`
**Replaces:** Debezium schema history topic; Confluent dictionary modes (`oracle.dictionary.mode`)
**Depends on:** PRD-00
**Clean-room note:** Based on Oracle's documented dictionary options and our own design.

---

## 1. Objective

Know the exact column layout of every captured table at any SCN the engine may decode, with startup time and storage bounded by the number of captured tables, and support the DDL that Confluent's parser does not (PP-10).

## 2. Design

- **Per-table versioned schema.** Each captured table has a list of schema versions `(object_id, version, effective_scn, columns, key, partitions)`. Versions are stored in the compacted topic `cdc.schema.topic` (default `${prefix}.cdc.schema`), keyed by `PDB.SCHEMA.TABLE`, value holding the current version plus versions newer than the oldest needed SCN (the position's `resume_scn` or oldest journaled transaction). Older versions are pruned on write, so the topic stays small.
- **Source of truth is the data dictionary.** Column metadata is read from `DBA_TAB_COLS`, `DBA_CONSTRAINTS`, `DBA_CONS_COLUMNS`, `DBA_LOG_GROUPS`, `DBA_PART_TABLES`. We never parse DDL text to build schemas; we parse DDL only to classify it.
- **Rebuild.** If the schema topic is lost, the connector rebuilds current versions from the dictionary and, if `resume_scn` is older than the last DDL on a table (`DBA_OBJECTS.LAST_DDL_TIME`), requires `oracle-cdc-admin resnapshot` for that table instead of guessing.

## 3. DDL flow (`cdc.dictionary.mode=auto`, the only mode)

1. Mining returns a DDL row for a captured owner (operation DDL, with `SQL_REDO` holding the statement).
2. Classify: CREATE TABLE, ALTER TABLE (add, drop, modify, rename column; rename table; add or drop constraint; partition maintenance; supplemental log changes), DROP TABLE, TRUNCATE, RENAME, COMMENT, GRANT and others.
3. For structural changes to a captured table, pause decoding at the DDL commit SCN, drain earlier rows using the existing version, query the dictionary for the new layout, store a new version with `effective_scn` equal to the DDL commit SCN, emit an optional schema change event, and continue.
4. **Lag case.** If the engine is decoding redo older than the current dictionary state for a table (DDL happened after the rows being decoded but before the current time), the online catalog would decode with the wrong layout. Detection (ADR-0016): DML rows of the table come back from the online catalog with STATUS 2 and generic `COL n` names. Action: mine that step again using `DICT_FROM_REDO_LOGS` with `DDL_DICT_TRACKING`, starting from the most recent dictionary build in redo before the position, then return to the online catalog. If no dictionary build exists early enough, stop with `DictionaryUnavailableException` and the runbook action (run `DBMS_LOGMNR_D.BUILD`, then resnapshot the table).
5. **Dictionary builds.** When `cdc.dictionary.build.interval.ms` is set (default 86400000) and the user has `EXECUTE` on `DBMS_LOGMNR_D`, the connector runs `DBMS_LOGMNR_D.BUILD(OPTIONS => DBMS_LOGMNR_D.STORE_IN_REDO_LOGS)` off-peak at `cdc.dictionary.build.time` (default `02:00` database time). This is the only optional write-like action and is off when the privilege is absent; the doctor reports whether lag recovery is possible.

## 4. Functional requirements

| ID | Requirement |
|---|---|
| SCH-1 | Supported DDL on captured tables: add column (including with DEFAULT and virtual columns), drop column (single or multiple), modify type or nullability, rename column, rename table (topic follows template; ops event `table-renamed`), add or drop constraint (key changes handled per PRD-02 SNAP-9), add, drop, split, merge, truncate partition, supplemental logging changes, DROP TABLE (table removed; optional tombstone of schema), TRUNCATE (truncate event). |
| SCH-2 | Rename table keeps the same object ID; the topic for the new name is used from the DDL SCN onward. `cdc.rename.topic.policy` = `follow` (default) or `keep` (continue using the original topic). |
| SCH-3 | Schema change events (optional, `cdc.schema.changes.topic.enabled`, default false) to `${prefix}.cdc.schema-changes` in a Debezium-compatible shape (`ddl`, `tableChanges`) for consumers that rely on it. |
| SCH-4 | Connect schemas are derived deterministically from the version (schema name `${prefix}.${schema}.${table}.Value`, adjusted per `cdc.schema.name.adjustment.mode` and `cdc.field.name.adjustment.mode`, ADR-0020), so Schema Registry compatibility follows from DDL semantics: add nullable column is BACKWARD compatible; documented guidance for others. |
| SCH-5 | Unsupported or unknown DDL classifications affecting a captured table are a stop (`UnsupportedDdlException`) naming the statement, never ignored. Non-captured objects' DDL is ignored without parsing. |
| SCH-6 | Startup loads the schema topic only (bounded by captured tables) and validates each version against the dictionary when no DDL is pending; mismatch without a recorded DDL is a stop. |
| SCH-7 | Startup time for 1,000 tables with ten versions each is under 30 seconds. |

## 5. Configuration

| Property | Type | Default | Description |
|---|---|---|---|
| `cdc.schema.topic` | string | `${prefix}.cdc.schema` | Compacted schema topic |
| `cdc.schema.changes.topic.enabled` | boolean | false | Emit schema change events |
| `cdc.dictionary.build.interval.ms` | long | 86400000 | Interval between dictionary builds when permitted; 0 disables |
| `cdc.dictionary.build.time` | string | `02:00` | Preferred build time |
| `cdc.rename.topic.policy` | enum | `follow` | Topic after table rename |

## 6. Acceptance criteria

- [ ] Each DDL in SCH-1, executed mid-stream under load, results in correct decoding of rows before and after the DDL.
- [ ] Lag case: stop the connector, run 50 DML, a column drop and a column add, then 50 more DML; restart; all 100 rows decode with the right layout via redo dictionary replay.
- [ ] Without a dictionary build available, the lag case stops with `DictionaryUnavailableException` and correct guidance.
- [ ] Deleting the schema topic and restarting rebuilds it; a table with DDL after `resume_scn` is flagged for resnapshot.
- [ ] Schema topic size stays under 10 MB for 1,000 tables after 10,000 DDLs (pruning works).
- [ ] Rename table and rename column are captured (both unsupported in Confluent Oracle CDC per its documentation).
