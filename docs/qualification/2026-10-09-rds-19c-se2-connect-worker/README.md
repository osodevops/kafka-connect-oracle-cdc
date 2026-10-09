# Qualification: a Kafka Connect worker against Amazon RDS for Oracle 19c SE2 (9 October 2026)

| Item | Value |
|---|---|
| Targets | Amazon RDS for Oracle, `19.0.0.0.ru-2026-07.mrp-2026-07.r1`, License Included, `db.t3.medium`, eu-west-2, temporary dev-account lab (`terraform/oracle-cdc-lab` in `oso_aws_infrastructure_resources`): engine `oracle-se2` as a non-CDB (service `CDCLAB`) and engine `oracle-se2-cdb` with the tenant database `CDCPDB` |
| Access | AWS Systems Manager port forward through the lab bastion to `localhost:15210`; the Connect worker in Docker on the workstation reaches it as `host.docker.internal:15210` |
| Connector | `main` at `6fa6eb7` (release 0.1.4 plus `ConnectorQualIT`), the plugin ZIP built from it |
| Kafka | `apache/kafka:3.9.1`, one broker and one Connect worker in Docker |
| Command | `./mvnw -pl e2e-tests verify -De2e.groups=qual -De2e.external.url=jdbc:oracle:thin:@//localhost:15210/<service> -De2e.external.user=CDC -De2e.external.admin.user=cdcadmin -De2e.external.platform=<rds or rds-cdb> -De2e.external.apply.setup=true` |
| Result | Non-CDB: 5 of 5 passed. CDB: 4 passed, 1 skipped by design (dictionary builds are off in range mode) |

| Suite | Non-CDB | CDB (range mode) |
|---|---|---|
| `PreconditionQualIT` | PASS | PASS |
| `CaptureQualIT` (online) | PASS | PASS |
| `CaptureQualIT` (archive-only) | PASS | PASS |
| `DictionaryBuildQualIT` | PASS | SKIPPED (ADR-0027) |
| `ConnectorQualIT` | PASS: 150 rows committed in single-row transactions, the worker SIGKILLed and restarted between batches, 150 rows delivered | PASS: the same, 150 of 150 |

`ConnectorQualIT` registers the connector through the Connect REST API with the database
properties the doctor and the task read (`cdc.database.host`, `port`, `service`, `user`,
`password`, `pdbs`), streams without an initial snapshot, kills the worker after the first 50
transactions, commits 50 more while it is down or restarting and 50 after it is running again, and
requires every committed row in the topic at least once.

Not covered by this run: a log purged by the retention setting on RDS (CDC-2002; covered on Oracle
Database Free, including in range mode), Multi-AZ failover, and Oracle Database 21c on RDS. The
evidence files beside this one are the runs' own records (prefixed `non-cdb-` and `cdb-`); they
hold no credential and no performance figure.
