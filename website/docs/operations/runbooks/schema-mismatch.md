---
title: "CDC-6003 Schema mismatch"
description: "Runbook for CDC-6003 SCHEMA_MISMATCH."
slug: /runbooks/schema-mismatch
---

# CDC-6003 Schema mismatch

**Code:** `CDC-6003` (`SCHEMA_MISMATCH`). Raised at task start. Not retried.

## What the connector observed

The schema topic holds a version of a captured table whose columns, key or supplemental logging
differ from the data dictionary, and the table's last DDL is not later than the position the task
resumes from. A DDL within ten seconds of the resume point counts as later, because the time of an
SCN is known only to within a few seconds. The message names the table, the stored version and both
times.

## Why it stopped rather than continued

The connector decodes rows with the version the schema topic recorded. A difference that no DDL
ahead in the redo explains means a DDL was not captured: for example, the offset was moved forward
past it, the table was outside the include pattern when it changed, or the schema topic belongs to
another database or connector. Decoding with either
layout could publish values under the wrong columns.

## Confirm the cause

```sql
SELECT owner, object_name, last_ddl_time FROM dba_objects
WHERE owner = :owner AND object_name = :table AND object_type = 'TABLE';
SELECT SCN_TO_TIMESTAMP(:resume_scn) FROM dual;
```

## Recover

- If the redo from before the DDL is still available, stop the connector and move its offset back
  before the DDL
  ([moving the offset by hand](../../concepts/offsets-and-recovery.md#reading-and-moving-the-offset-by-hand)),
  then resume. The DDL is mined again and becomes a version. Commits already delivered are skipped
  as long as the `last_commit_*` fields are kept.
- If the schema topic belongs to another connector or database, point `cdc.schema.topic` at the
  right topic, then restart.
- If the change is understood and the records published since the DDL are acceptable, clear the
  table's entry: with the connector stopped, produce a tombstone (a record with a null value) for
  the table's key on the schema topic, then resume. The table then starts again from the
  dictionary's current layout. Copy the key exactly as it is stored, for example with kcat:

  ```bash
  kcat -C -b "$BROKERS" -t cdc.cdc.schema -e -f '%k\n' | sort -u     # find the table's key
  printf '%s|\n' "$KEY" | kcat -P -b "$BROKERS" -t cdc.cdc.schema -K '|' -Z
  ```

  The key holds the server (the topic prefix), the PDB, the owner and the table. Reload the table
  with a [`snapshot` signal](../signals.md) if consumers need its rows in the new layout.

Restart a failed task with:

```bash
curl -s -X POST "$CONNECT/connectors/$NAME/restart?includeTasks=true&onlyFailed=true"
```
