# Qualification: Amazon RDS for Oracle 19c SE2, CDB architecture (9 October 2026)

| Item | Value |
|---|---|
| Target | Amazon RDS for Oracle, engine `oracle-se2-cdb` `19.0.0.0.ru-2026-07.mrp-2026-07.r1`, License Included, CDB architecture with tenant database `CDCPDB`, `db.t3.medium`, eu-west-2, temporary dev-account lab (`terraform/oracle-cdc-lab` in `oso_aws_infrastructure_resources`) |
| Access | AWS Systems Manager port forward through the lab bastion to `localhost:15210`, service `CDCPDB` (the tenant database; `CDB$ROOT` is not reachable on RDS) |
| Connector | `main` at `6364a0b` (range mode, ADR-0027), plus the range-mode skip in `DictionaryBuildQualIT` committed with this evidence |
| Mining mode | `cdc.mining.mode=auto`, which chose range mode because the session is in a PDB |
| Setup | `oracle-cdc-doctor setup-sql --platform rds` applied by the master user in the tenant database (`-De2e.external.apply.setup=true`) |
| Command | `./mvnw -pl e2e-tests verify -De2e.groups=qual -De2e.external.url=jdbc:oracle:thin:@//localhost:15210/CDCPDB -De2e.external.user=CDC -De2e.external.admin.user=cdcadmin -De2e.external.platform=rds-cdb -De2e.external.apply.setup=true` |
| Result | 3 passed, 1 skipped by design |

| Suite | Checks | Verdict |
|---|---|---|
| `PreconditionQualIT` | Platform detected as RDS from inside the tenant database; the doctor's fast rules report nothing (DOC-4 accepts the local user in range mode); `EXECUTE ON DBMS_LOGMNR_D` | PASS |
| `CaptureQualIT` (online) | Insert, update and delete, mined before the DDL, then the row after an `ALTER TABLE ADD`, with no `ADD_LOGFILE`: Oracle chose the logs from the SCN range | PASS |
| `CaptureQualIT` (archive-only) | The same, mined only after `rdsadmin.rdsadmin_util.switch_logfile` archived the log | PASS |
| `DictionaryBuildQualIT` | Skipped in range mode: LogMiner cannot use a dictionary from the redo inside a PDB (ORA-01371), so range mode switches builds off | SKIPPED |

Facts from the run:

- The RDS setup script runs unchanged in the tenant database: `grant_sys_object`,
  `alter_supplemental_logging` and `set_configuration('archivelog retention hours')` all succeed
  there, and the capture user reads `rdsadmin.rds_configuration` (DOC-23 found the retention).
- A first run, before the skip, had `DictionaryBuildQualIT` fail: `DBMS_LOGMNR_D.BUILD` ran from the
  tenant database without error, but the log inventory found no archived log marked as a
  dictionary build. Range mode does not use builds, so this does not affect capture.
- Rows written before a DDL that the connector has not mined yet cannot be replayed in range mode;
  the task stops with CDC-6001 (`PdbLocalCaptureEngineIT` on Oracle Database Free), which is why
  the capture suite mines the earlier rows before running the DDL.

Not covered by this run: a real Kafka Connect worker against RDS, restart without loss on RDS, a
log purged by the retention setting (CDC-2002), and Oracle Database 21c on RDS. The evidence files
beside this one are the runs' own records; they hold no credential and no performance figure.
