---
title: Amazon RDS for Oracle
description: What changes on Amazon RDS for Oracle, the setup script for the master user, archived redo retention and what the doctor checks.
---

# Amazon RDS for Oracle

:::note Preview
Amazon RDS for Oracle 19c, non-CDB, passed the connector's qualification suites on 9 October 2026:
the setup script below, the doctor's checks, capture in online and archive-only mode, and
dictionary builds, against RDS SE2 License Included. A real Kafka Connect worker against RDS and a
restart there are not yet part of that run, so treat RDS support as a preview. The CDB
architecture is not supported.
:::

The connector mines an RDS for Oracle database with LogMiner exactly as it mines one on premises:
LogMiner reads the online and archived redo logs on the database server, so nothing has to reach
the files. What differs is how the database is prepared, because RDS gives the master user no
SYSDBA, no `ALTER SYSTEM` and no `ALTER DATABASE`.

| Topic | On premises | Amazon RDS for Oracle |
|---|---|---|
| Grants on SYS objects | `GRANT SELECT ON V_$... TO user` as SYSDBA | `rdsadmin.rdsadmin_util.grant_sys_object` as the master user |
| Minimal supplemental logging | `ALTER DATABASE ADD SUPPLEMENTAL LOG DATA` | `rdsadmin.rdsadmin_util.alter_supplemental_logging(p_action => 'ADD')` |
| ARCHIVELOG mode | `ALTER DATABASE ARCHIVELOG` | On while automated backups are on (backup retention of 1 day or more) |
| How long archived redo stays | Your deletion job (RMAN) | `archivelog retention hours`, 0 by default |
| Initialisation parameters | `ALTER SYSTEM` | The instance's DB parameter group |
| Online redo log size | `ALTER DATABASE ADD LOGFILE` | `rdsadmin.rdsadmin_util.add_logfile` and `drop_logfile` |

The doctor detects RDS (the `RDSADMIN` schema exists) and gives every fix in the form above.

## Architecture

Only the non-CDB architecture is covered: Oracle Database 19c created as a non-CDB. With the CDB
architecture, which every RDS release from 21c uses and 19c offers, the client always connects to
the tenant database as a local user and cannot reach `CDB$ROOT`. Capturing there needs per-PDB
mining, which the connector does not have yet; doctor rule DOC-4 reports the local user.

## The setup script

```bash
java -jar oracle-cdc-doctor-cli.jar setup-sql --platform rds --user cdc > setup-rds.sql
```

The script carries a `<change-me>` placeholder password: put the real one in before you run it,
and keep the edited file out of version control. Review it, then run it as the master user. It:

1. creates the capture user (`cdc` by default; a local user, no `C##` prefix);
2. grants `CREATE SESSION`, `LOGMINING`, `EXECUTE_CATALOG_ROLE`, `SELECT ANY TABLE`,
   `FLASHBACK ANY TABLE`, `SELECT ANY DICTIONARY` and `SELECT ANY TRANSACTION`;
3. grants `EXECUTE` on `DBMS_LOGMNR` and `DBMS_LOGMNR_D`, and `SELECT` on every fixed view the
   connector reads, through `grant_sys_object` (names as in `DBA_OBJECTS`, grantee in upper case);
4. turns on minimal supplemental logging;
5. sets `archivelog retention hours` and commits.

Supplemental logging for each captured table is still a statement per table, run by its owner or
the master user:

```sql
ALTER TABLE <owner>.<table> ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

## Archived redo retention

RDS deletes archived redo from the instance once it is older than `archivelog retention hours`.
The default is 0, so logs disappear soon after they are archived and a connector that stops for
longer than the online logs last finds its redo gone (CDC-2002). The script sets the retention to
the journal threshold plus a day of downtime; raise it if the connector may be stopped for longer,
and size the instance's storage for that much redo:

```sql
BEGIN
  rdsadmin.rdsadmin_util.set_configuration(
    name  => 'archivelog retention hours',
    value => '48');
END;
/
COMMIT;
```

The commit is required. To read the setting back, as the master user:

```sql
SET SERVEROUTPUT ON
EXEC rdsadmin.rdsadmin_util.show_configuration;
```

Doctor rule DOC-23 reads the same setting. It is blocking at 0 and a warning below
`cdc.txjournal.threshold.ms` plus the planned maximum downtime (`--max-downtime`, default 24
hours). The capture user usually cannot read `rdsadmin.rds_configuration`; the finding is then
information, and running `oracle-cdc-doctor check` with the master user's credentials gives the
full answer.

## Connecting

Point the connector at the instance endpoint, port 1521 by default, and the database name you
gave RDS as the service:

```json
{
  "cdc.database.host": "<instance>.<id>.<region>.rds.amazonaws.com",
  "cdc.database.port": "1521",
  "cdc.database.service": "<database name>",
  "cdc.database.user": "cdc"
}
```

Leave `cdc.database.pdbs` empty: a non-CDB has no pluggable databases.
