---
title: "CDC-6001 Dictionary unavailable"
description: "Runbook for CDC-6001 DICTIONARY_UNAVAILABLE."
slug: /runbooks/dictionary-unavailable
---

# CDC-6001 Dictionary unavailable

**Code:** `CDC-6001` (`DICTIONARY_UNAVAILABLE`). Raised while mining. Not retried.

## What the connector observed

Rows of a captured table were written before a later DDL on that table, so LogMiner's online
catalog could no longer map them (it returns them with generic `COL n` column names). The connector
mines such a range again with a data dictionary stored in the redo by `DBMS_LOGMNR_D.BUILD`. One of
these was true:

- No dictionary build in the archived logs ends before the range, or a log between the build and
  the range is missing.
- The rows still came back with generic names when mined with the redo dictionary.
- No exact schema version of the table is known for the rows' SCN. Either the connector first read
  the table after the DDL, or several DDLs on the table fell inside the window being mined again, so
  the layout between them is not known.

The message says which, and names the table and the SCN.

## Why it stopped rather than continued

Decoding these rows with today's layout could publish values under the wrong columns or drop them.
Skipping them would lose them silently.

## Confirm the cause

Dictionary builds the connector can use, newest last:

```sql
SELECT thread#, sequence#, first_change#, next_change#, dictionary_begin, dictionary_end, deleted
FROM v$archived_log
WHERE dest_id = 1 AND (dictionary_begin = 'YES' OR dictionary_end = 'YES')
ORDER BY thread#, sequence#;
```

Whether the connector user can run a build:

```sql
SELECT privilege FROM dba_tab_privs
WHERE grantee = :connector_user AND table_name = 'DBMS_LOGMNR_D';
```

The `dictionary-build` events on the ops topic show whether scheduled builds ran, failed or were
switched off.

## Recover

- Grant `EXECUTE ON DBMS_LOGMNR_D` to the connector user. The connector then writes a build at
  start when the archived logs hold none, and on the schedule in `cdc.dictionary.build.interval.ms`
  and `cdc.dictionary.build.time`. A build helps only with rows written after it, so it does not
  recover the range that stopped the task.
- If archived logs between the last build and the range were deleted, restore them and restart.
- Otherwise the rows cannot be decoded. To go on, stop the connector and move its offset past the
  DDL as a deliberate skip
  ([moving the offset by hand](../../concepts/offsets-and-recovery.md#reading-and-moving-the-offset-by-hand)).
  The table's changes between the old offset and the DDL are not delivered. With broker access, the
  schema topic still holds the layout from before the DDL, so the task stops with
  [CDC-6003](schema-mismatch.md) at its next start; clear the table's entry as that runbook
  describes. Then reload the table with a [`snapshot` signal](../signals.md).

To avoid this, keep `cdc.kafka.bootstrap.servers` set so schema versions persist across restarts,
and avoid several DDLs on one captured table while the connector is stopped or far behind.
