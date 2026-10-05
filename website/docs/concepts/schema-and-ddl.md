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

## The schema topic

When the task has broker access (`cdc.kafka.bootstrap.servers`), every version change is written to
the compacted schema topic (`cdc.schema.topic`, by default the topic prefix followed by
`.cdc.schema`). Each record holds one table's versions, oldest first: the version valid at the
position the task started from, and every later one. Older versions are dropped as the committed
position moves on, so a restart always finds the version its first rows need. A dropped or renamed
table is removed with a tombstone. Each record carries the offset a quiet heartbeat would, so
committing it never moves the position past a change that has not been delivered.

The record key names the server (the topic prefix), the PDB, the owner and the table. The value
holds a format number and the list of versions; each version gives its number, the SCN it is valid
from, the key columns and where they came from, the supplemental logging state and the columns with
their types.

At start the task reads the topic back with the converter set by `cdc.journal.converter` and checks
each table against the data dictionary. When the two differ, the difference must be explained by a
DDL that the connector has not mined yet: the table's last DDL time must be later than the time of
the resume SCN. If it is not, a DDL was missed (for example, the offset was moved past it, or the
topic belongs to another connector), and the task stops with `CDC-6003`. The time of an SCN is known
only to within a few seconds, so a DDL within ten seconds of the resume point counts as ahead of it.

Without broker access the topic is neither read nor written, and versions start from the dictionary
at every start.

## Not yet in this release

- Optional schema change events on a topic of their own.
- Redo older than the dictionary: after a restart, rows written before a DDL that the connector has
  not yet mined decode with a dictionary stored in the redo by `DBMS_LOGMNR_D.BUILD`.
