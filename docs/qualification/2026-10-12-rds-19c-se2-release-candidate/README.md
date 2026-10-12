# Qualification: Amazon RDS for Oracle 19c SE2, release candidate (12 October 2026)

| Item | Value |
|---|---|
| Targets | Amazon RDS for Oracle `19.0.0.0.ru-2026-07.mrp-2026-07.r1`, License Included, `db.t3.medium`, eu-west-2, temporary lab in OSO's development AWS account, provisioned from OSO's private infrastructure repository and destroyed after the run: `oracle-se2` as a non-CDB (service `CDCLAB`) and `oracle-se2-cdb` with the tenant database `CDCPDB` |
| Access | AWS Systems Manager port forward through the lab bastion to `localhost:15210`; Connect workers in Docker on the workstation reach it as `host.docker.internal:15210` |
| Connector | `main` at `2fb492d`: the release candidate after 0.1.4 (RAC position and thread states behind the refusal, the Confluent migration tooling) |
| Confluent | Confluent's Oracle CDC Source connector 2.15.2 on a Confluent Platform 7.9.2 worker (`confluentinc/cp-server-connect`), used under Confluent's evaluation licence in this test environment only and not distributed |
| Command | `./mvnw -pl e2e-tests verify -De2e.groups=qual -De2e.external.url=jdbc:oracle:thin:@//localhost:15210/<service> -De2e.external.user=CDC -De2e.external.admin.user=cdcadmin -De2e.external.platform=<rds or rds-cdb> -De2e.external.apply.setup=true -De2e.confluent.plugin=<local copy>` |
| Result | Non-CDB: 6 of 6 passed. CDB: 4 passed, 2 skipped by design |

| Suite | Non-CDB | CDB (range mode) |
|---|---|---|
| `PreconditionQualIT` | PASS | PASS |
| `CaptureQualIT` (online and archive-only) | PASS | PASS |
| `DictionaryBuildQualIT` | PASS | SKIPPED: builds are off in range mode (ADR-0027) |
| `ConnectorQualIT` | PASS: 150 single-row transactions across a worker SIGKILL and restart, 150 delivered | PASS: 150 of 150 |
| `ConfluentCutoverQualIT` | PASS: see below | SKIPPED: Confluent documents no support for RDS with the CDB architecture |

## The cutover from Confluent's connector (non-CDB)

Confluent's connector snapshotted and streamed a keyed table under a live workload of single-row
transactions; a transaction was opened and left uncommitted; the connector was stopped; the
translation, the takeover and the new connector followed with the tools in `tools/migration`; the
open transaction then committed. 218 rows were committed in all and none was missing: 81 reached
Confluent's topic, 159 the new topic and 22 both (the overlap), and the transaction that spanned
the switch was delivered by this connector. `verify_cutover.py --confluent-topic` then found the
table equal to the two topics by count and hash: 218 rows, 159 compared value by value from the
new topic and 59 verified by key from Confluent's.

What Confluent stored and wrote on this non-CDB database:

- Offset partitions: `{"sidPdb": "CDCLAB"}` with `scn` and `tablePlacement`, and
  `{"catalog": "CDCLAB", "schema": ..., "table": "ORDERS"}` with `scn`, `cscn`, `rows`, `rps` and
  `def`. The takeover started at the database `scn` (548379), below the table's `cscn` (548446),
  the last commit Confluent delivered.
- Topic `CDCLAB.<schema>.ORDERS`; the translation chose `${prefix}.${database}.${schema}.${table}`,
  which gave `osocut.CDCLAB.<schema>.ORDERS`.
- Records: the row's columns plus `table`, `scn`, `op_type` (`R` for snapshot rows), `op_ts`,
  `current_ts`, `row_id` and `username`, with the one-column primary key as the record key.

Not covered: a log purged by the retention setting on RDS, Multi-AZ failover and releases from 21c.
The evidence files beside this one are the runs' own records (prefixed `non-cdb-` and `cdb-`); they
hold no credential and no performance figure.
