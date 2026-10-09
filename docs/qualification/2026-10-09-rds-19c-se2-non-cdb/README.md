# Qualification: Amazon RDS for Oracle 19c SE2, non-CDB (9 October 2026)

| Item | Value |
|---|---|
| Target | Amazon RDS for Oracle, engine `oracle-se2` `19.0.0.0.ru-2026-07.mrp-2026-07.r1`, License Included, non-CDB, `db.t3.medium`, eu-west-2, temporary dev-account lab (`terraform/oracle-cdc-lab` in `oso_aws_infrastructure_resources`) |
| Access | AWS Systems Manager port forward through the lab bastion to `localhost:15210` |
| Connector | `main` at `261fdbb` (after release 0.1.1) |
| Setup | `oracle-cdc-doctor setup-sql --platform rds` applied by the master user (`-De2e.external.apply.setup=true`) |
| Command | `./mvnw -pl e2e-tests verify -De2e.groups=qual -De2e.external.url=jdbc:oracle:thin:@//localhost:15210/CDCLAB -De2e.external.user=CDC -De2e.external.admin.user=cdcadmin -De2e.external.platform=rds -De2e.external.apply.setup=true` |
| Result | 4 of 4 passed |

| Suite | Checks | Verdict |
|---|---|---|
| `PreconditionQualIT` | Platform detected as RDS; the doctor's fast rules, which `validate()` runs, report nothing at all (DOC-23 read 25 hours of archived redo retention); `EXECUTE ON DBMS_LOGMNR_D` | PASS |
| `CaptureQualIT` (online) | Insert, update, delete and the row after an `ALTER TABLE ADD`, mined through `rdsadmin`-granted views | PASS |
| `CaptureQualIT` (archive-only) | The same, mined only after `rdsadmin.rdsadmin_util.switch_logfile` archived the log | PASS |
| `DictionaryBuildQualIT` | The capture user runs `DBMS_LOGMNR_D.BUILD` (granted through `grant_sys_object`) and the inventory finds the build | PASS |

Not covered by this run: a real Kafka Connect worker against RDS, restart without loss on RDS, a
log purged by the retention setting (CDC-2002), and the CDB architecture (per-PDB mining is not
implemented). The evidence files beside this one are the runs' own records; they hold no
credential and no performance figure.
