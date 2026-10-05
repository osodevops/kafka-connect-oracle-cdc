---
title: Schema and DDL
description: The schema topic, DDL classification and recovery when redo predates a schema change.
---

# Schema and DDL

Each captured table has numbered schema versions. The layout always comes from the data dictionary;
DDL text is read only to decide what a statement did (PRD-03).

## DDL while the connector follows the redo

When the connector mines a DDL statement on a captured table, it classifies it:

| Statement | Effect |
|---|---|
| Add, drop, modify, rename or set unused columns; add, drop, enable or disable constraints; supplemental logging; CREATE TABLE | The layout is read from the dictionary; if it changed, it becomes the next version, effective from the statement's SCN, and a `ddl-applied` event goes to the ops topic |
| Rename table, DROP TABLE | The table is forgotten under its old name; a renamed table is captured under its new name when it matches `cdc.tables.include`, and its records go to the topic for the new name |
| TRUNCATE, partition maintenance | No new version; partition maintenance refreshes the captured object ids |
| Indexes, comments, grants, storage clauses | Nothing |
| Anything else | The task stops with `CDC-6002` (see the runbook) |

Rows are applied in redo order, so every row decodes with the version of its moment: rows written
before a column was added carry no value for it, rows written after carry it, and a renamed column
appears under its new name from the rename onward.

## Not yet in this release

- The schema topic (`cdc.schema.topic`), which keeps versions across restarts, and optional schema
  change events.
- Redo older than the dictionary: after a restart, rows written before a DDL that the connector has
  not yet mined decode with a dictionary stored in the redo by `DBMS_LOGMNR_D.BUILD`.
