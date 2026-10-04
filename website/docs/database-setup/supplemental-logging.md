---
title: Supplemental logging
description: Database-level minimal logging plus ALL COLUMNS per captured table, and what primary-key-only logging costs.
---

# Supplemental logging

LogMiner needs minimal supplemental logging at database level to reconstruct rows at all, and
table-level logging to know which columns a change touched.

```sql
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;                        -- once
ALTER TABLE app.orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;  -- per captured table
```

| Table-level logging | Doctor rule DOC-3 | Effect on records |
|---|---|---|
| ALL COLUMNS | passes | Full before and after images on updates and deletes |
| PRIMARY KEY only | warning | Updates carry the key and the changed columns; before images are partial and records say so |
| none | blocking | Updates and deletes cannot be keyed; capture refuses to start |

Identity columns, BOOLEAN, JSON and VECTOR columns and table or column names longer than 30
characters make LogMiner skip or mark the whole table unsupported on Oracle Database 23ai
(rules DOC-5 and DOC-6); the doctor reports them before the connector starts.
