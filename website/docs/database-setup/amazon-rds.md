---
title: Oracle CDC on Amazon RDS for Oracle
sidebar_label: Amazon RDS for Oracle
description: "Set up Oracle CDC on Amazon RDS for Oracle as the master user: rdsadmin procedures, supplemental logging, archived redo retention and the doctor checks."
---

import Faq from '@site/src/components/Faq';

# Oracle CDC on Amazon RDS for Oracle

:::note Preview
Amazon RDS for Oracle 19c passed the connector's qualification suites on 9 October 2026 in both
architectures, on RDS SE2 License Included. As a non-CDB: the setup script below, the doctor's
checks, capture in online and archive-only mode, and dictionary builds. With the CDB architecture,
from release 0.1.3: the same setup script, checks and capture, mined from inside the tenant
database in range mode. A real Kafka Connect worker against each, killed and restarted
mid-stream, then delivered every committed row. A log purged by the retention setting, Multi-AZ
failover and releases from 21c have not been run on RDS yet, so treat RDS support as a preview.
:::

**Quick answer.** As the RDS master user, run the doctor's `setup-sql --platform rds` script. It creates the capture user, then calls `rdsadmin.rdsadmin_util.grant_sys_object`, `alter_supplemental_logging` for minimal supplemental logging, and `set_configuration` for `archivelog retention hours`, which the script commits. Table supplemental logging stays a separate statement. The doctor reports fixes in that form.

The connector mines an RDS for Oracle database with [LogMiner](../concepts/how-capture-works.md)
exactly as it mines one on premises:
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

The [doctor](../operations/doctor.md) detects RDS (the `RDSADMIN` schema exists) and gives every fix in the form above.

Moving from Confluent's Oracle CDC Source connector on an RDS non-CDB? The cutover in
[Migrating from Confluent Oracle CDC Source](../migration/from-confluent.md) was run on RDS for
Oracle 19c on 12 October 2026.

## Architecture

The preview covers Oracle Database 19c in both architectures. With the CDB architecture, which every
RDS release from 21c uses and 19c offers, the client always connects to the tenant database as a
local user and cannot reach `CDB$ROOT`. From release 0.1.3 the connector mines that tenant database
in [range mode](../concepts/multi-pdb.md#mining-from-inside-a-pdb) (`cdc.mining.mode=auto` chooses
it): LogMiner chooses the logs, and the connector still lists them and checks them for gaps. Two
things differ from a non-CDB:

- Rows written before a DDL that the connector has not mined yet stop the task with CDC-6001
  instead of being replayed, because LogMiner cannot use a dictionary from the redo inside a PDB.
  Keep the connector's lag short where DDL is frequent.
- Dictionary builds are switched off.

Run the setup script below connected to the tenant database. Releases from 21c have not been
through the qualification suites yet.

## How do I run the RDS setup script? {#the-setup-script}

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

[Supplemental logging](./supplemental-logging.md) for each captured table is still a statement per table, run by its owner or
the master user:

```sql
ALTER TABLE <owner>.<table> ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

## How long does RDS keep archived redo? {#archived-redo-retention}

RDS deletes archived redo from the instance once it is older than `archivelog retention hours`.
The default is 0, so logs disappear soon after they are archived and a connector that stops for
longer than the online logs last finds its redo gone ([CDC-2002](../operations/runbooks/log-purged.md)). The script sets the retention to
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

<Faq
  items={[
    {
      question: 'Does the connector support Amazon RDS for Oracle?',
      answer:
        'Yes, as a preview. Amazon RDS for Oracle 19c passed the connector\'s qualification suites on 9 October 2026 in both architectures, on RDS SE2 License Included. A real Kafka Connect worker against each, killed and restarted mid-stream, then delivered every committed row. A log purged by the retention setting, Multi-AZ failover and releases from 21c have not been run on RDS yet.',
    },
    {
      question: 'How do I enable supplemental logging on RDS for Oracle?',
      answer:
        'The setup script calls rdsadmin.rdsadmin_util.alter_supplemental_logging with the ADD action, and you run that script as the master user. That enables minimal supplemental logging at database level. Each captured table still needs ALTER TABLE owner.table ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS, run by its owner or the master user.',
    },
    {
      question: 'How long does RDS keep archived redo logs for LogMiner?',
      answer:
        'RDS keeps archived redo for archivelog retention hours, and the default is 0, so logs disappear soon after they are archived. The setup script sets the retention to the journal threshold plus a day of downtime, and the commit is required. Doctor rule DOC-23 is blocking at 0 and a warning below cdc.txjournal.threshold.ms plus the planned maximum downtime (--max-downtime, 24 hours by default). A log purged by the retention setting has not been run on RDS yet.',
    },
    {
      question: 'Why does the master user not need SYSDBA?',
      answer:
        'RDS does not grant the master user SYSDBA, ALTER SYSTEM or ALTER DATABASE, so the setup script does not use them. It calls rdsadmin procedures the master user can run: grant_sys_object for SYS objects, alter_supplemental_logging for minimal supplemental logging, and set_configuration for archivelog retention hours.',
    },
    {
      question: 'Does it work with RDS Multi-AZ or Oracle 21c?',
      answer:
        'Not yet. Multi-AZ failover and releases from 21c have not been run on RDS, so treat both as untested. The preview covers Oracle Database 19c in both architectures. From release 0.1.3 the connector mines a 19c tenant database in range mode, and RDS releases from 21c use that CDB architecture, but those releases have not been through the qualification suites.',
    },
    {
      question: 'Do I set cdc.database.pdbs on a non-CDB?',
      answer:
        'No. Leave cdc.database.pdbs empty, because a non-CDB has no pluggable databases. Point the connector at the instance endpoint, port 1521 by default, and the database name you gave RDS as the service.',
    },
  ]}
/>
