---
title: Supplemental logging
description: Database-level minimal logging plus ALL COLUMNS per captured table, and what primary-key-only logging costs.
---

# Supplemental logging

LogMiner needs minimal supplemental logging at database level to reconstruct rows at all, and
table-level logging to know which columns an update or delete touched.

```sql
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;                        -- once, as SYSDBA
ALTER TABLE app.orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;  -- per captured table
```

| Table-level logging | Doctor rule DOC-3 | Effect on records |
|---|---|---|
| ALL COLUMNS | passes | Full before images on updates and deletes |
| PRIMARY KEY only | warning | Updates carry the key and the changed columns; the before image holds only those, and the other fields of `before` are null |
| none | blocking | Updates and deletes cannot be keyed; the doctor and the connector's validation refuse the configuration |

With primary-key-only logging, a null field in a `before` image can mean either that the column was
null or that it was not logged; the records do not mark a before image as partial.
Use ALL COLUMNS when consumers need complete before images.

Enabling table-level logging on a busy table invalidates the cursors of statements that use it,
so schedule it like any other DDL on that table. The doctor's `setup-sql` script lists the
statement per captured table as a reminder; it does not run it.

## Columns and names LogMiner cannot handle

Identity columns, BOOLEAN, JSON, VECTOR, BFILE and nested-table columns, and table or column names
longer than 30 characters, make LogMiner skip the table or mark its changes unsupported on the
releases tested. The doctor reports them before the connector starts (rules DOC-5 and DOC-6); see
the [decode runbook](../operations/runbooks/decode.md) for what happens if one gets through.
