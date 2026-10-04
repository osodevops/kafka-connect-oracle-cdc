---
title: oracle-cdc-doctor
description: The preflight checker, its rules, output formats and exit codes.
---

# oracle-cdc-doctor

`oracle-cdc-doctor` runs against the database named in a connector configuration and reports
what would stop capture, with the SQL that fixes it. The connector's own `validate` runs the
same fast rules, so a configuration that passes the doctor is accepted by Connect.

```bash
java -jar oracle-cdc-doctor-cli.jar check --config connector.json [--format markdown|json|junit]
java -jar oracle-cdc-doctor-cli.jar setup-sql [--profile production|lab] [--user c##cdc] [--non-cdb] [--pdbs A,B]
```

| Exit code | Meaning |
|---|---|
| 0 | No blocking findings |
| 1 | Blocking findings, or the database could not be reached |
| 2 | Warnings only |
| 3 | Not implemented yet (RDS and Autonomous setup) |
| 64 | Usage or configuration error |

## Rules in the current build

| Rule | Checks | Severity |
|---|---|---|
| DOC-1 | Database in ARCHIVELOG mode | blocking |
| DOC-2 | Minimal supplemental logging at database level | blocking |
| DOC-3 | Table-level supplemental logging on every captured table | blocking when absent, warning for primary-key-only |
| DOC-4 | Grant profile, readable fixed views, common user and `CONTAINER_DATA=ALL` in a CDB | blocking |
| DOC-5 | Identity columns and BOOLEAN, JSON, VECTOR, BFILE or nested-table columns | blocking |
| DOC-6 | Table or column names over 30 characters | blocking |
| DOC-7 | Primary key or NOT NULL unique index, honouring `cdc.key.missing`; ROW MOVEMENT with ROWID keys | blocking, warning or info |
| DOC-12 | A valid local archive destination (or the configured one) | blocking |
| DOC-14 | PRIMARY and READ WRITE for online capture mode | blocking |
| DOC-15 | Oracle Database 19c or later | blocking |

Rules DOC-8 to 11, 13 and 16 to 20 (redo rate, retention, Kafka transaction timeout, broker
reachability and more) and the `redo-profile`, `sizing` and `explain-lag` commands arrive with
the full doctor in Phase 1d.

The `setup-sql --profile lab` output is the exact script that builds the test database image,
and a test asserts they stay identical.
