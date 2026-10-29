---
title: Enable Oracle Supplemental Logging for CDC
sidebar_label: Supplemental logging
description: How to enable database-level and table-level supplemental logging for Oracle CDC, and how to verify it afterwards on V$DATABASE and DBA_LOG_GROUPS.
---

import Faq from '@site/src/components/Faq';

# Enable Oracle Supplemental Logging for CDC

LogMiner needs minimal supplemental logging at database level to reconstruct rows at all, and
table-level logging to know which columns an update or delete touched. The steps below enable
both, then show how to check them. Capture also needs the rest of
[database setup](./index.md), including ARCHIVELOG mode. The connector reads changes through
LogMiner only.

**Quick answer.** As SYSDBA, run `ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;` once at database level. Then, for each captured table, run `ALTER TABLE <schema>.<table> ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;`. Verify `SUPPLEMENTAL_LOG_DATA_MIN` on `V$DATABASE`, and the log group type of each table in `DBA_LOG_GROUPS` before capture starts.

```sql
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;                        -- once, as SYSDBA
ALTER TABLE app.orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;  -- per captured table
```

Run the database statement once, as SYSDBA, and the table statement for every captured table.
Replace `app.orders` with that table. The doctor's `setup-sql` script lists the table statement
as a reminder; it does not run it. On [Amazon RDS for Oracle](./amazon-rds.md) the generated
script includes the database-level call for you to run, and the per-table statement is still a
comment.

In a container database, Oracle requires this `ALTER DATABASE` statement to be issued from the
root, and it then applies to the whole database. The [database setup](./index.md) script is run
as SYSDBA at `CDB$ROOT`. An `ALTER TABLE` supplemental logging statement can be run from the root
or from a pluggable database and affects only that table. On Amazon RDS for Oracle with the CDB
architecture, connect to the tenant database and use
`rdsadmin.rdsadmin_util.alter_supplemental_logging` for the database-level change.

## Database-level vs table-level supplemental logging

Minimal supplemental logging is database-level only. Oracle describes it as the least information
LogMiner needs to identify, group and merge the redo for DML changes, including chained rows and
storage arrangements such as cluster tables. Enable it with:

```sql
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
```

Doctor rule DOC-2 is blocking when this is off. The doctor's fix is that statement, or, on Amazon
RDS for Oracle, `rdsadmin.rdsadmin_util.alter_supplemental_logging(p_action => 'ADD')`.

Table-level logging decides which columns of one table are written to the redo. For capture, use
`ALL` on each captured table:

```sql
ALTER TABLE app.orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

Oracle's `ALL` option places the columns of the row in the redo when a column changes, except
LOBs, `LONG` values and abstract data types. `PRIMARY KEY` places the primary key columns in the
redo whenever a row that has a primary key is updated.

| Table-level logging | Doctor rule DOC-3 | Effect on records |
|---|---|---|
| ALL COLUMNS | passes | Full before images on updates and deletes |
| PRIMARY KEY only | warning | Updates carry the key and the changed columns; the before image holds only those, and the other fields of `before` are null |
| none | blocking | Updates and deletes cannot be keyed; the doctor and the connector's validation refuse the configuration |

With primary-key-only logging, a null field in a `before` image can mean either that the column was
null or that it was not logged; the records do not mark a before image as partial.
Use ALL COLUMNS when consumers need complete before images.

Enabling table-level logging on a busy table invalidates the cursors of statements that use it,
so schedule it like any other DDL on that table. Oracle states the same for database-level
identification key logging: if the database is open, DML cursors in the cursor cache are
invalidated until the cache is filled again.

Oracle also accepts a database-wide form,
`ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS`. That statement turns on minimal
supplemental logging implicitly, and `SUPPLEMENTAL_LOG_DATA_MIN` then reads `IMPLICIT`. This
connector's doctor treats minimal logging as present only when that column is `YES`, which is the
value after `ALTER DATABASE ADD SUPPLEMENTAL LOG DATA`. Rule DOC-3 does not read
`SUPPLEMENTAL_LOG_DATA_PK`. It looks up each captured table's log group and passes
`ALL COLUMN LOGGING`, or warns for `PRIMARY KEY LOGGING`. The setup script comments the per-table
`ALL` statement. It does not emit the database-wide primary key statement.

Minimal supplemental logging does not, in Oracle's words, impose significant overhead.
Database-wide identification key logging can. Table-level `ALL` logging adds the logged columns
to the redo, so the volume of redo can grow. How that affects online log size and how long
archived logs must stay is covered in [redo sizing and archive retention](./redo-sizing.md).

## How do I check supplemental logging is enabled?

Query the database-level flags:

```sql
SELECT SUPPLEMENTAL_LOG_DATA_MIN, SUPPLEMENTAL_LOG_DATA_PK, SUPPLEMENTAL_LOG_DATA_ALL
FROM V$DATABASE;
```

Oracle documents these values on `V$DATABASE`:

| Column | Values | Meaning |
|---|---|---|
| `SUPPLEMENTAL_LOG_DATA_MIN` | `NO`, `IMPLICIT`, `YES` | `YES` after `ALTER DATABASE ADD SUPPLEMENTAL LOG DATA`. `IMPLICIT` when minimal logging is on only because primary key, unique key or foreign key supplemental logging is enabled. `NO` when none of the database-wide supplemental logging directives are enabled. In a container database, `YES` means minimal logging is enabled in all of the PDBs, and `NO` means it is not enabled in all of them. LogMiner treats `YES` and `IMPLICIT` as minimal logging enabled. The doctor accepts only `YES`. |
| `SUPPLEMENTAL_LOG_DATA_PK` | `YES`, `NO` | Whether database-wide primary key supplemental logging places the primary key columns in the redo on update. |
| `SUPPLEMENTAL_LOG_DATA_ALL` | `YES`, `NO` | Whether database-wide all-column supplemental logging places the row's fixed-length columns in the redo. |

Then list the log group on each captured table. `DBA_LOG_GROUPS` describes log groups on all
tables in the database. `ALL_LOG_GROUPS` has the same columns for tables the current user can
see:

```sql
SELECT OWNER, TABLE_NAME, LOG_GROUP_TYPE
FROM DBA_LOG_GROUPS
ORDER BY OWNER, TABLE_NAME;
```

`LOG_GROUP_TYPE` is one of `ALL COLUMN LOGGING`, `PRIMARY KEY LOGGING`, `UNIQUE KEY LOGGING`,
`FOREIGN KEY LOGGING` or `USER LOG GROUP`. For a captured table, doctor rule DOC-3 passes
`ALL COLUMN LOGGING` and warns for `PRIMARY KEY LOGGING`. Any other result, including no row, is
blocking. In a container database the doctor reads `CDB_LOG_GROUPS` and matches those same two
`LOG_GROUP_TYPE` values. See [oracle-cdc-doctor](../operations/doctor.md).

## How do I disable supplemental logging?

To stop minimal supplemental logging, after any database-level identification key logging has
been dropped:

```sql
ALTER DATABASE DROP SUPPLEMENTAL LOG DATA;
```

Oracle allows that drop only when no other database-level supplemental logging remains.
Otherwise the statement returns `ORA-32589`. In a container database, issue it from the root.
With minimal supplemental logging off, LogMiner cannot reconstruct rows, and doctor rule DOC-2
blocks the configuration.

## Columns and names LogMiner cannot handle

Identity columns, BOOLEAN, JSON, VECTOR, BFILE and nested-table columns, and table or column names
longer than 30 characters, make LogMiner skip the table or mark its changes unsupported on the
releases tested. The doctor reports them before the connector starts (rules DOC-5 and DOC-6); see
the [decode runbook](../operations/runbooks/decode.md) for what happens if one gets through.

Sources, checked on 29 October 2026: Oracle Database 19c
[LogMiner supplemental logging](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html),
[`V$DATABASE`](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/V-DATABASE.html),
[`DBA_LOG_GROUPS`](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/DBA_LOG_GROUPS.html),
[`ALL_LOG_GROUPS`](https://docs.oracle.com/en/database/oracle/oracle-database/19/refrn/ALL_LOG_GROUPS.html)
and [`ALTER DATABASE`](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/ALTER-DATABASE.html).
Statements about the connector and the doctor come from this site.

<Faq
  items={[
    {
      question: 'Does enabling supplemental logging require a database restart?',
      answer:
        'No. Oracle allows you to add supplemental logging while the database is open, and the statement does not require a restart. It does invalidate DML cursors in the cursor cache until that cache is filled again. ARCHIVELOG mode, which capture also requires, does need a restart, and that step is separate. Source: https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/ALTER-DATABASE.html',
    },
    {
      question: 'Is minimal supplemental logging enough for CDC?',
      answer:
        'No. Minimal supplemental logging at database level lets LogMiner reconstruct rows, and each captured table still needs table-level supplemental logging. With none, updates and deletes cannot be keyed, and doctor rule DOC-3 and the connector\'s validation refuse the configuration. Source: https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html',
    },
    {
      question: 'Should I use ALL COLUMNS or PRIMARY KEY supplemental logging?',
      answer:
        'Use ALL COLUMNS when consumers need the full before image of the columns Oracle logs. Primary-key-only logging is accepted with a warning: an update carries the key and the changed columns, the other fields of before are null, and a null can mean that the column was null or that it was not logged. The records do not mark a before image as partial. Oracle\'s ALL option still omits LOBs, LONG values and abstract data types.',
    },
    {
      question: 'Does supplemental logging increase redo volume?',
      answer:
        'It can, once logging goes beyond the minimum. Oracle states that minimal supplemental logging does not impose significant overhead, and that database-wide identification key logging can. Table-level ALL COLUMNS logging places the row columns, except LOBs, LONG values and abstract data types, in the redo when a column changes. The redo sizing and archive retention page explains how that extra redo affects online log size and how long archived logs must stay. Source: https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html',
    },
    {
      question: 'Does the connector enable supplemental logging for me?',
      answer:
        'No. The doctor setup-sql command writes a script for you to review and run, and it does not execute the statements. On premises the supplemental logging statements are comments. On Amazon RDS for Oracle the script includes the database-level call, still for you to run, and the per-table statement remains a comment.',
    },
  ]}
/>
