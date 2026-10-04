# Testing Strategy: Proving "No Silent Loss"

**Status:** Draft for implementation
**Modules:** unit tests in each module, `e2e-tests`, `bench/`
**Principle:** The product claim is correctness. Every release must prove, with recorded evidence, that committed changes are neither lost nor (in exactly-once mode) duplicated under failure.

---

## 1. Tiers

| Tier | Where | Runs | Content |
|---|---|---|---|
| T0 Unit | Every PR | Seconds | Parser fuzzing, type conversion, buffer and rollback logic against `FakeLogMiner`, position maths, config validation, migration golden files |
| T1 Integration | Every PR | Under 20 minutes | Oracle Database Free 23ai (`gvenzl/oci-oracle-free:23.26.3-faststart` or the current tag) plus Kafka via Testcontainers; DML, DDL, LOBs, snapshots, signals, multi-PDB, `oracle-cdc-doctor` fixtures, `FaultyJdbc` injections |
| T2 Fault and correctness | Nightly | About 3 hours | Correctness oracle under random fault schedules, worker kills, Kafka broker restarts, log switch storms, archive deletion, long transactions; 26ai image as second version |
| T3 Soak and performance | Weekly and before release | 72 hours | Sustained workload, memory and latency tracking, comparison with Debezium on the same workload |
| T4 Extended lab | Before release | Days | 19c and 21c EE, two-node RAC on Podman and Active Data Guard on the workstation or the AWS dev RAC host; RDS SE2 non-CDB (Phase 2) and CDB (Phase 3) in the AWS dev account; Autonomous Database on OCI Always Free (Phase 3). See `07_test_lab_and_budget.md` |

## 2. Test database image

- Base: `gvenzl/oci-oracle-free` faststart images, which are designed for tests and include `FREEPDB1` ([gvenzl/oci-oracle-free](https://github.com/gvenzl/oci-oracle-free)).
- Our derived image `ghcr.io/osodevops/oracle-cdc-test-db` adds an init script that: switches the database to ARCHIVELOG (shutdown, mount, `ALTER DATABASE ARCHIVELOG`, open); enables minimal supplemental logging; creates a second PDB `FREEPDB2`; creates the common capture user with grants from `oracle-cdc-doctor setup-sql`; sets small online logs (for example three 50 MB groups) to force frequent switches.
- A precondition test asserts ARCHIVELOG and supplemental logging before any suite runs.
- The image is built in CI and never published outside our registry, so Oracle's own image terms stay with the user pulling the base image.

## 3. Correctness oracle

1. **Workload generator** (`bench/workload`): deterministic, seeded; mixes inserts, updates (including key changes), deletes, savepoint rollbacks, full rollbacks, multi-table transactions, large transactions (up to 5 million rows), LOB writes, truncates and DDL; concurrent sessions; writes a ledger of committed transaction IDs and per-table expected effects to a side table in a non-captured schema.
2. **Materialiser**: consumes all topics with `read_committed`, applies by key, tracks the highest commit SCN seen.
3. **Check**: at a check SCN behind the connector position, compare materialised state with `SELECT ... AS OF SCN` for every table (same normalisation as `verify_cutover.py`), and compare the set of committed XIDs in the ledger with XIDs seen in record headers.
4. **Assertions**: no missing XIDs, no missing keys, no value mismatch; in exactly-once mode no duplicate `(xid, event_index)`; in at-least-once mode duplicates only for the transaction in flight at each kill.
5. Each run publishes a JSON evidence file (fault schedule, seed, versions, results, SHA-256), uploaded as a workflow artefact. Release notes link the evidence for the release commit.

## 4. Fault injection

| Fault | Mechanism |
|---|---|
| ORA errors at precise points | `FaultyJdbc` (PRD-00 CORE-TEST-2): ORA-00310, ORA-00334, ORA-01291, ORA-01013, ORA-03113, ORA-01555, ORA-04036 at connect, execute, Nth `next`, commit |
| Online log overwritten during mining | Force rapid `ALTER SYSTEM SWITCH LOGFILE` with small logs while a slow consumer holds a step |
| Archive purge | `RMAN DELETE ARCHIVELOG` or file removal during lag; expect `OracleCdcPurgedException`, never skip |
| Worker kill | `SIGKILL` the Connect worker at random times (1,000 iterations in nightly) |
| Kafka faults | Broker restart, transaction coordinator failover, produce timeouts |
| Network | Toxiproxy between Connect and Oracle: latency, resets, idle drop at 350 seconds |
| Abandoned transaction | Session killed mid-transaction; XID gone from `GV$TRANSACTION`; expect orphan release |
| Database restart | `SHUTDOWN ABORT` and startup during streaming |
| Disk full | Spill directory at capacity; expect `BufferExhaustedException` |
| Schema topic loss | Delete topic; expect rebuild path |

## 5. Debezium regression corpus

Every Oracle issue in [debezium/dbz](https://github.com/debezium/dbz/issues) that describes data loss, duplication, pinned offsets or wrong values becomes a named test in `e2e-tests/src/test/java/sh/oso/connect/oracle/e2e/regression/`, named after the public issue (for example `Dbz2504OnlineLogOverwrittenMidIterationTest`). The test reproduces the scenario from the issue description against our connector, not Debezium's code. Initial set:

| Issue | Scenario |
|---|---|
| [dbz#2504](https://github.com/debezium/dbz/issues/2504) | ORA-00310 during iteration |
| [dbz#2544](https://github.com/debezium/dbz/issues/2544) | Position ahead of delivered events on restart |
| [dbz#2779](https://github.com/debezium/dbz/issues/2779) | Restart during snapshot with transactions in flight |
| [dbz#2683](https://github.com/debezium/dbz/issues/2683) | All-zero XID START row |
| [dbz#2184](https://github.com/debezium/dbz/issues/2184) | Missing events under mixed DDL and DML |
| [dbz#2713](https://github.com/debezium/dbz/issues/2713) | Missing redo for open transaction must stop |
| [dbz#24](https://github.com/debezium/dbz/issues/24) | Excluded user transaction markers |
| [dbz#1599](https://github.com/debezium/dbz/issues/1599) | Column filters with DDL |
| [dbz#2781](https://github.com/debezium/dbz/issues/2781), [dbz#2475](https://github.com/debezium/dbz/issues/2475) | Offsets advance on quiet database |
| Savepoint bugs fixed in [Debezium 3.7](https://debezium.io/blog/2026/09/29/debezium-3-7-final-released/) | Partial rollback of insert, update, delete, LOB |
| RAC PRIVATE thread ([Debezium 3.6 CR1](https://debezium.io/blog/2026/06/24/debezium-3-6-cr1-released/)) | Thread state change before consumption (T4) |

A scheduled job lists new Oracle issues weekly and opens a triage ticket in our repo.

## 6. Version and platform matrix

| Platform | Tier | Phase |
|---|---|---|
| Oracle Database Free 23ai, 26ai (CDB with two PDBs) | T1, T2 | 1 |
| 19c EE and 21c EE, non-CDB and CDB | T4 | 1 |
| 19c two-node RAC | T4 | 2 |
| Active Data Guard physical standby (archive-only) | T4 | 2 |
| Amazon RDS for Oracle 19c non-CDB | T4 | 2 |
| Autonomous Database, RDS CDB (per-PDB mining) | T4 | 3 |
| Kafka Connect 3.6, 3.9, 4.x; Confluent Platform 7.6 and later; Strimzi; MSK Connect | T1 (Apache), T4 (others) | 1 and 2 |
| Java 17 and 21 | T0, T1 | 1 |

Until Oracle publishes a 26ai Free container image, the second version in the CI matrix is the oldest supported 23ai tag (currently `gvenzl/oracle-free:23.9-faststart`); every "23ai and 26ai" above reads accordingly.

## 7. Performance and comparison

- `bench/` runs the same workload against OSO CDC Connector and Debezium (latest stable) on identical infrastructure; reports throughput, p50, p95 and p99 latency, heap, mining session CPU and PGA, and catch-up time after one hour of downtime.
- Results stay internal. Oracle's development licence terms forbid disclosing benchmark results without Oracle's consent ([OTN License](https://www.oracle.com/downloads/licenses/standard-license.html)), so the docs publish the harness and method, and customers run it on their own licensed systems.

## 8. Upgrade tests

- Upgrade and downgrade between consecutive minor versions with open transactions, journaled transactions and a snapshot in progress; no manual steps (PP-16).

## 9. Release gate

A release is blocked unless: T0 and T1 green; last nightly T2 green with evidence; T3 soak within five per cent of previous release on latency and memory; T4 checklist complete for every platform listed as supported in that release.
