# ADR-0024: The platform is detected, not configured; RDS setup runs through rdsadmin

**Status:** Accepted
**Context:** PRD-01 Phase 2 scope (RDS non-CDB), PRD-05 setup-sql and fix text; research on
Amazon RDS for Oracle, 8 and 9 October 2026.

## Context

On Amazon RDS for Oracle the master user holds the DBA role without `ALTER DATABASE`,
`ALTER SYSTEM`, `GRANT ANY PRIVILEGE` or `GRANT ANY ROLE`, and never SYSDBA. SYS objects are
granted with `rdsadmin.rdsadmin_util.grant_sys_object`, supplemental logging is changed with
`rdsadmin.rdsadmin_util.alter_supplemental_logging`, parameters live in DB parameter groups, and
ARCHIVELOG mode exists only while automated backups are on. RDS deletes archived redo after
`archivelog retention hours`, which is 0 by default, so a stopped connector finds nothing to
resume from beyond the online logs.

LogMiner itself behaves as on premises: `DBMS_LOGMNR.ADD_LOGFILE` reads the online and archived
logs server-side by the paths in `V$LOGFILE` and `V$ARCHIVED_LOG`. What does not work on RDS is
the advice: `setup-sql --platform rds` printed a notice and exited with code 3, and every doctor
fix that changes the database (DOC-1, DOC-2, DOC-4, DOC-9, DOC-10, DOC-11, DOC-12, DOC-20) gave
SQL the master user cannot run.

With the CDB architecture (every RDS release from 21c, and 19c by choice) the client always
connects to the tenant database as a local user and CDB$ROOT is unreachable, which needs per-PDB
mining. That is a separate decision.

## Decision

1. `Platform { ONPREM, RDS, AUTONOMOUS }` lives in `core/topology` and is **detected**: RDS when the
   `RDSADMIN` schema exists in `ALL_USERS` (readable by every user), Autonomous Database when
   `SYS_CONTEXT('USERENV', 'CLOUD_SERVICE')` is set, otherwise on premises. There is no setting:
   the engine mines identically on RDS and on premises, so a setting could only mislabel a
   database.
2. `setup-sql --platform rds` prints a script for an RDS non-CDB, run by the master user: a local
   user (default `cdc`), plain `GRANT`s for the system privileges and roles, `grant_sys_object`
   for `DBMS_LOGMNR`, `DBMS_LOGMNR_D` and every fixed view the engine reads, minimal supplemental
   logging through `alter_supplemental_logging`, and `set_configuration('archivelog retention
   hours', ...)` followed by `COMMIT`. The retention it sets is DOC-10's need with the defaults,
   so DOC-23 stays quiet on a database prepared by the script. The exit code is 0.
3. Doctor fix text comes from one place, `PlatformSql`, keyed by the detected platform. On RDS it
   names the `rdsadmin` procedure, the parameter group or the AWS CLI call instead of SQL that RDS
   refuses.
4. New rule DOC-23 (fast mode and full): on RDS, `archivelog retention hours` read from
   `rdsadmin.rds_configuration` is blocking at 0 and a warning below DOC-10's need. When the
   setting cannot be read (the capture user usually cannot), the finding is information and
   points at `show_configuration` for the master user.

## Consequences

- An operator on RDS gets a script and fixes that run; nothing in the connector's runtime path
  changes. RDS stays "not qualified" until the qualification run on the dev account lab (T4).
- Detection costs two cheap queries per doctor run.
- The CDB form of RDS is still not supported; DOC-4 reports the local user in a CDB.

## Evidence

AWS documentation: master user privileges, `grant_sys_object`, `alter_supplemental_logging`,
`add_logfile`, `set_configuration` and `show_configuration`, and the CDB architecture page; the
AWS DMS RDS LogMiner grant list; an AWS re:Post answer that queries
`rdsadmin.rds_configuration`. Unit tests: `PlatformTest`, `RulesTest`
(`onAmazonRdsEveryFixIsOneTheMasterUserCanRun`,
`rdsArchiveRetentionMustCoverTheJournalThresholdAndTheDowntime`, `setupSqlProfiles`) and
`DoctorMainTest.rdsScriptUsesRdsadminGrantsAndALocalUser`. To be confirmed on the lab: which of
the plain grants the master user may make on the target engine version, and that a non-master
user can run `DBMS_LOGMNR_D.BUILD`.

## PRD edits

PRD-05: setup-sql platforms (RDS non-CDB available, Autonomous later), the DOC-23 row, and the
fast-mode list.

## Amendments

None.
