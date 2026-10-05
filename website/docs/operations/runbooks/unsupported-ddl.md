---
title: "CDC-6002 Unsupported DDL"
description: "Runbook for CDC-6002 UNSUPPORTED_DDL."
slug: /runbooks/unsupported-ddl
---

# CDC-6002 Unsupported DDL

**Code:** `CDC-6002` (`UNSUPPORTED_DDL`).

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
```

## Recover

- Report the statement shape, with the Oracle version, so it can be classified in a release.
- If the statement did not change the table's columns, key or supplemental logging, stop the
  connector, set its offset to just after the statement's SCN with Kafka Connect's
  `PATCH /connectors/{name}/offsets` (see the offsets page), and resume it.
- Otherwise exclude the table, or reset past the statement and resnapshot the table.
