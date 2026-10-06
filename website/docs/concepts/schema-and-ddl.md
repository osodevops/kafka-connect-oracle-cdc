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

After that check the task reads, in one pass over the dictionary, the layout of every captured
table that has no stored version, and stores it as version 1. A table that joins the captured set
later is read when it joins. The version counts as valid from the position the task started from
when the table's last DDL is more than ten seconds before that position, and otherwise from ten
seconds after that DDL. A DDL after the start, even one during a long initial snapshot, therefore
finds the earlier layout already stored.

Without broker access the topic is neither read nor written, and versions start from the dictionary
at every start.

## Redo written before a DDL (the lag case)

LogMiner's online catalog describes tables as they are now. When the connector mines rows that were
written before a later DDL on their table (because it was stopped, or behind when the DDL ran),
LogMiner can no longer map them and returns them with generic `COL n` column names. The connector
then mines that range again with a data dictionary that `DBMS_LOGMNR_D.BUILD` stored in the redo,
reading every archived log from the newest build up to the range, with DDL tracking so the column
names are those of each row's moment. The next range returns to the online catalog. Each such range
writes a `dictionary-replay` event to the ops topic and counts in the `LagReplays` metric.

Rows mined this way decode with the schema version valid at their SCN, and every record renders
with the version its row was decoded with. When the DDL ran while the connector was stopped and no
stored version survived, or within ten seconds of the position it started from, no version is known
for the earlier rows. When several DDLs on one table ran before the connector reached the first,
the dictionary is already past the earlier ones when the connector applies them, so the layout
between them is not known, and a row that names a column missing from such a version is not
decoded either. In these cases, and when no usable build exists, the task stops with `CDC-6001`
rather than decode with a guessed layout.

The connector writes builds itself when its user has `EXECUTE ON DBMS_LOGMNR_D`: at start if the
archived logs hold none, then at `cdc.dictionary.build.time` (02:00 database time by default) and
every `cdc.dictionary.build.interval.ms` (a day by default) after that. Without the privilege, builds are switched off with a
`dictionary-build` event. Mining a range again reads all redo since the last build, so more frequent
builds make the lag case cheaper.

## Not yet in this release

- Optional schema change events on a topic of their own.
