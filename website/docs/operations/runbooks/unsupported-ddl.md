---
title: "CDC-6002 Unsupported DDL"
description: "Runbook for CDC-6002 UNSUPPORTED_DDL: a DDL statement on a captured table could not be classified."
slug: /runbooks/unsupported-ddl
---

# CDC-6002 Unsupported DDL

**Code:** `CDC-6002` (`UNSUPPORTED_DDL`). Raised while mining. Not retried.

## What the connector observed

A DDL statement on a captured table that the connector could not classify. The message names the
table, the SCN and the statement. Recognised statements are CREATE TABLE, ALTER TABLE (add, drop,
modify, rename or set unused columns; add, drop, enable or disable constraints; supplemental logging;
partition maintenance; rename; storage clauses such as MOVE, SHRINK or row movement), RENAME,
TRUNCATE, DROP TABLE, and statements with no effect on the layout (indexes, comments, grants).
DDL on tables outside `cdc.tables.include` is never classified.

## Why it stopped rather than continued

The connector reads every new layout from the data dictionary, but it has to know whether a
statement changed the layout before it decodes the rows that follow. Guessing wrong would publish
rows under the wrong columns, so an unknown statement stops the task.

## Confirm the cause

```sql
SELECT owner, object_name, last_ddl_time FROM dba_objects
WHERE owner = :owner AND object_name = :table AND object_type = 'TABLE';
SELECT column_name, data_type, data_length, data_precision, data_scale
FROM dba_tab_columns WHERE owner = :owner AND table_name = :table ORDER BY column_id;
```

Compare the columns with the latest version on the schema topic or the latest `ddl-applied` event
on the ops topic for the table, to see whether the statement changed the layout.

## Recover

- Report the statement shape, with the Oracle version and release update, in a GitHub issue so it
  can be classified in a release.
- If the statement did not change the table's columns, key or supplemental logging, move the offset
  just past it: stop the connector, set `resume_scn` to the statement's SCN plus one and remove
  `resume_rs_id` and `resume_ssn`
  ([moving the offset by hand](../../concepts/offsets-and-recovery.md#reading-and-moving-the-offset-by-hand)),
  and resume. Changes of transactions on captured tables that were open across that SCN are
  delivered without their earlier part, so do this when no such transaction was open, or reload the
  affected tables afterwards with a [`snapshot` signal](../signals.md).
- If the statement did change the layout, do not move past it: the connector would go on decoding
  with the old layout and stop with [CDC-6003](schema-mismatch.md) at its next start. Exclude the
  table with `cdc.tables.exclude` until a release classifies the statement. When the table is
  included again, reload it with a `snapshot` signal; if the schema topic still holds its old
  layout, the task stops with CDC-6003 at start, and that runbook explains how to clear it.
