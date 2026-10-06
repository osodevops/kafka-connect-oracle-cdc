# Phase 1 exit checklist (P1-30)

Each acceptance criterion of PRD-00 to PRD-03 with the suite that proves it on Oracle Database Free
(23.26.3) and what is still open. Status: **proven** (a suite asserts it in the T1 tiers),
**partly** (the behaviour is proven, the stated scale, duration or platform is not), **pending**
(no suite yet), **phase 2** (not a Phase 1 criterion). Run commands: `mvn -pl e2e-tests verify
-De2e.groups=engine|connector|nightly -Dit.test=<Suite>`.

Outside this list, Phase 1 exit also needs the 72-hour soak with `bench check` (T3) and the 19c and
21c single-instance qualification from `lab/local` (T4). Neither has run.

## PRD-00 capture core

| Criterion | Status | Proof |
|---|---|---|
| CORE-REF-1 and CORE-REF-2 documents for 23ai and 26ai, and for 19c and 21c | partly | The `*RefEngineIT` suites regenerate and check `oracle-cdc-core/src/main/resources/reference/` on Oracle Database Free 23.26.3 on every run; 19c and 21c need the T4 lab |
| ORA-00310 at the first, middle and last `next` of a step: no missing rows, no duplicates | pending | The P1-26 regression corpus (dbz-2504) |
| A transaction open for 48 hours across archive deletion older than 24 hours is journaled and commits correctly | partly | `JournaledTransactionConnectorIT` (journal across a restart past the transaction's start); the 48-hour run belongs to the soak |
| A 5 million row transaction completes with the heap capped | partly | `SpillingBufferTest`, `SpillStoreTest` (spill under the budget); the 5 million row run belongs to the nightly tier |
| A killed session's transaction is released by orphan detection within two intervals, with an ops event | proven | `OrphanReleaseEngineIT`, `OrphanDetectorTest`; the abandoned transaction nightly suite |
| Excluded user transactions never reach the buffer | proven | `EngineCorrectnessEngineIT` (dbz-24) |
| Include patterns pushed down as object ids | proven | `ObjectIdResolverTest`, `LogMinerQueryTest`, `ObjectIdStabilityRefEngineIT` (query text, not a query plan) |
| A deleted archive log gives `OracleCdcPurgedException` naming thread and sequence; no later start | proven | `StopsWhenRedoForOpenTransactionIsMissingEngineIT` (dbz-2713), `LogInventoryEngineIT` |
| Savepoint rollback for insert, update, delete and LOB | proven | `HeapTransactionBufferTest`, `LobUndoBufferTest`, `LobModesEngineIT`, `CorrectnessOracleConnectorIT` (savepoint rollbacks in the workload) |
| Position v1 readable by v1.1 and the other way | partly | `PositionCodecTest` keeps unknown keys as extras; no v1.1 exists yet |
| Every data-loss issue in the regression corpus has a passing test | pending | The P1-26 corpus and its index test |

## PRD-01 source connector

| Criterion | Status | Proof |
|---|---|---|
| A Debezium consumer reads our output without change for every type, Avro and JSON | partly | `DebeziumEnvelopeTest`, `TypeRoundTripEngineIT`, `CorrectnessOracleConnectorIT` (JSON); `AvroNamesRoundTripTest` converts every round-trip type to Avro and back with an Avro converter's `AvroData` once `cdc.*.name.adjustment.mode` is set (ADR-0020); `AvroConverterConnectorIT` (connector tier) runs a worker whose connector writes through Apicurio Registry's `AvroConverter` to an in-memory registry, reads every round-trip type back with the Avro deserializer under names that need adjustment and compares it with the database, and shows that with the modes at `none` a column name Avro refuses fails the task, while a table name passes the converter and the registry and is refused by a consumer on Avro for Java 1.12 or later. Written, not yet run green in the connector tier, so the status stays partly until it has |
| 1,000 worker kills under exactly-once: no duplicates, no losses | partly | `ExactlyOnceConnectorIT` (two kills); the kill loop nightly suite with `-Dnightly.kills=1000` |
| A `read_committed` consumer never sees a partial Oracle transaction (except flagged splits) | proven | `ExactlyOnceConnectorIT`, `EosBoundariesTest` |
| One connector, three PDBs, one LogMiner session, per-PDB topics | partly | `MultiPdbConnectorIT` (two PDBs, the test image has two) |
| A new table matching the include patterns is captured without restart or gap | proven | `MultiPdbConnectorIT`, `OpsTopicConnectorIT`, the new-table task tests |
| Quiet database for 48 hours: offsets advance, no ORA-01291 on restart, no writes to source | partly | `AdvancesOffsetsOnQuietDatabaseConnectorIT` (dbz-2781, dbz-2475) over minutes; 48 hours belongs to the soak |
| Validation rejects missing ARCHIVELOG, supplemental logging, identity columns, long names, privileges | partly | `DoctorEngineIT` (rule fixtures), `RulesTest`; connector validation runs the same rules. ARCHIVELOG and privileges cannot be withdrawn on the shared test database |
| A snapshot by signal works in archive-only mode with no source writes | proven | `SignalConnectorIT` |
| Confluent format byte-identical to recorded fixtures | phase 2 | |

## PRD-02 snapshots

| Criterion | Status | Proof |
|---|---|---|
| 200 GB, 160 tables with `UNDO_RETENTION` at 900 seconds under OLTP load | pending | AWS lab |
| Killing the worker at 50 per cent of 100 million rows resumes at the first incomplete chunk | partly | `SnapshotConnectorIT` (a stop part way through 60,000 rows resumes at the frontier) |
| Materialised state after snapshot plus concurrent changes equals the database | proven | `SnapshotConnectorIT` (correctness oracle under concurrent updates, deletes and inserts; key changes not in that workload) |
| A three-column composite key snapshot within 20 per cent of a single-column key | pending | Performance harness (internal figures only) |
| A keyless table is snapshotted by ROWID ranges | proven | `SnapshotChunksEngineIT` |
| Resnapshot after a simulated purge restores only the affected tables | pending | `oracle-cdc-admin resnapshot`; `ResnapshotConnectorIT` |

## PRD-03 schema and DDL

| Criterion | Status | Proof |
|---|---|---|
| Each SCH-1 DDL mid-stream decodes rows before and after it | proven | `DdlUnderLoadEngineIT`, `DdlEngineTest` |
| Lag case: 50 DML, a drop and an add, 50 DML across a restart decode with the right layout | proven | `LagCaseReplayEngineIT`, `LagCaseConnectorIT` |
| Without a dictionary build the lag case stops with `DictionaryUnavailableException` and guidance | proven | `LagCaseNoBuildEngineIT` |
| Deleting the schema topic rebuilds it; a table with DDL after the resume SCN is flagged | partly | Without stored versions the task reads the dictionary (every task test without broker access); the schema topic loss nightly suite; the flag is the lag case (CDC-6001) rather than a resnapshot request |
| Schema topic under 10 MB for 1,000 tables after 10,000 DDLs | partly | `SchemaTopicStoreTest` (pruning); the size at scale is not measured |
| Rename table and rename column are captured | proven | `DdlUnderLoadEngineIT` |
