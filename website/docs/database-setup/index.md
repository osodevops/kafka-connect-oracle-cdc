---
title: Database setup
description: Prepare an Oracle database for capture with the script oracle-cdc-doctor generates.
---

# Database setup

Three things must be true before capture: the database runs in ARCHIVELOG mode, minimal
supplemental logging is enabled at database level, and a mining user exists with the LogMiner
grant profile. The doctor generates the DBA script and checks the result.

```bash
java -jar oracle-cdc-doctor-cli.jar setup-sql --profile production --user c##cdc > setup.sql
# review, then as SYSDBA at CDB$ROOT:
sqlplus / as sysdba @setup.sql
java -jar oracle-cdc-doctor-cli.jar check --config connector.json
```

The production profile creates a common user (`C##` prefix) with `CONTAINER=ALL` grants, sets
`CONTAINER_DATA=ALL` so the user sees every PDB's rows in the container data views, grants
`LOGMINING`, `EXECUTE_CATALOG_ROLE`, `SELECT ANY TABLE`, `FLASHBACK ANY TABLE`, `SELECT ANY
DICTIONARY`, `SELECT ANY TRANSACTION`, execute on `DBMS_LOGMNR` and `DBMS_LOGMNR_D`, and select
on the fixed views the engine reads. Pass `--non-cdb` for a non-container database.

Platforms:

| Platform | Status |
|---|---|
| On-premises and self-managed cloud VMs (single instance, CDB or non-CDB) | Supported by `setup-sql --platform onprem` |
| Oracle RAC | Phase 2 qualification |
| Amazon RDS for Oracle | Phase 2 (`rdsadmin` procedures) |
| Oracle Autonomous Database | Phase 3 (per-PDB mining) |

ARCHIVELOG itself is enabled by the DBA:

```sql
SHUTDOWN IMMEDIATE;
STARTUP MOUNT;
ALTER DATABASE ARCHIVELOG;
ALTER DATABASE OPEN;
ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;
```
