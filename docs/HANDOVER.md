# Implementation handover: OSO CDC Connector for Oracle Database

**Written:** 5 October 2026, for whoever (person or model) continues the build.
**Read first:** `CLAUDE.md` (rules), this file, then `docs/decisions/` and the PRDs under `docs/prd/`.
**Working rules that override everything else:** no silent data loss; LogMiner only; offsets only encode
what Kafka Connect acknowledged; every data-loss bug gets a regression test before its fix; commit
locally only, never push; conventional commits with `git commit -s` and the trailer
`Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` (keep the trailer form the repository has
used so far); never weaken a failing regression test.

This document is the operational plan. The product requirements stay in `docs/prd/`, the design
decisions in `docs/decisions/` (ADR-0001 to ADR-0014), and the test tiers in `docs/testing_strategy.md`.

---

## 1. Where the build stands

### Committed and verified (local `main`, nothing pushed)

| Commit | Content | Verified by |
|---|---|---|
| `ec66f77` and earlier | Phase 0 (P0-01 to P0-13, P0-15, P0-16) and Phase 1a (P1-01 to P1-05, P1-07 to P1-10) | full gate on JDK 17 and 21, engine and connector tiers, Strimzi edge cases kill_worker, rolling_update, oracle_restart, rebalance |
| `ae3c34e` | P1-13 correctness oracle (`bench check`, ADR-0012) | `CorrectnessOracleConnectorIT` PASS |
| `cafbccc` | P1-11 ops topic and internal topics, P1-06 spill store, P1-14 transaction journal, P1-15 orphan detection, ADR-0014 redo-address cursor | full gate, connector tier 7 suites, engine tier 31 tests, all green |
| `ddb4c4d` | P1-21 decode DLQ (`cdc.dlq.topic`), long-transaction policy (`cdc.transaction.max.age.ms`, `CDC-4004`), archive-only coverage, `dbz-2713` regression | full gate, connector tier 7 suites plus the two surefire tests, engine tier 33 tests, all green |
| `5d152a5` | P1-18 LOBs: `cdc.lob.mode` skip, inline, reselect; `cdc.lob.max.bytes`, `cdc.lob.oversize.action` (`CDC-3003`), `cdc.unavailable.placeholder`; per-statement assembly and newest-change undo (ADR-0015); LOB_ERASE code 29 | full gate, connector tier 7 suites plus the two surefire tests (oracle suite with LOBs in reselect mode), engine tier 37 tests, all green |
| next after `5d152a5` (hash recorded at the following commit) | P1-23 task MXBean (41 attributes, top 20 transactions), generated metrics page and JMX exporter rules, Grafana dashboard, Prometheus alert rules, parallel decoding (`cdc.mining.decode.threads`) | full gate, connector tier 7 suites plus the two surefire tests, engine tier 38 tests, all green |

"Full gate" means `mvn clean verify -DskipE2E` on JDK 17: Spotless, SpotBugs, JaCoCo 80 per cent on
`oracle-cdc-core`, licence allowlist, every `*Test`, and the generated-docs drift check.

### In flight, not committed

Nothing. The next increment is P1-12 (exactly-once), section 5.

### How the dbz-2713 regression was finished (worth knowing for later suites)

`StopsWhenRedoForOpenTransactionIsMissingEngineIT` failed three times on the test, never on the
product. The first two causes are in the commit history (a shell-mangled view name; the deleted
archived log still had a copy in one of the online redo groups, so the suite now switches once more
than `SELECT COUNT(*) FROM v$log` before deleting the file). The last one: both engine runs shared one
mining connection, and `JdbcLogMinerSession.close()` closes its connection, so the second run mined on
a closed connection. ORA-17008 is transient, the engine reconnected, and the suite's reconnector handed
back the same dead sources, so it looped until the two-minute deadline. Each run now opens its own
mining connection, and the suite's reconnector throws, so an unexpected reconnect fails with its
cause instead of looping. Any engine-level suite that builds `CaptureEngine` by hand should do the same.

The first full engine tier with the suite then failed `MiningBehavioursRefEngineIT`: it mines the last
5,000 SCNs, which reached the log the regression had removed, and the catalog still listed it. Suites
now move a log aside with `OracleSql.hideArchivedLog` and put it back with
`OracleSql.restoreHiddenLogs` in a finally block (regression, `MiningBehavioursRefEngineIT`,
`LogInventoryEngineIT`, `JournaledTransactionConnectorIT`), so no suite leaves a hole for the next.

---

## 2. How the system is built now (what new code must respect)

### Mining and the cursor (ADR-0014)

- The cursor between steps is a redo byte address: `StepCursor(scn, lastApplied, inclusive)` where
  `lastApplied` is a `RedoRecordId(scn, rsId, ssn)`. `RedoRecordId.compareTo` orders by `rsId`
  (RBA) then `ssn`, falling back to `scn` only when a side has no RBA. Never reintroduce an SCN lower
  bound on rows: redo of an open transaction reaches the log late (private strands) with its original
  SCNs. Evidence: `oracle-cdc-core/src/main/resources/reference/redo-flush-lag.md`.
- `LogMinerQuery.sql(filter, rbaCursor, inclusive)` binds `RS_ID, RS_ID, SSN, endScn` with an RBA
  cursor, `startScn, endScn` without. `LogMinerEventSource.open(StepCursor, endScn)` sets LogMiner's
  `STARTSCN` to the first SCN of the log holding the cursor.
- The step runner's next cursor is the maximum RBA seen in the step; a DDL cut produces
  `StepCursor(ddl.scn, ddl.id, exclusive)`.
- `FakeLogMiner` honours the same rules and has `late(scn)` to model a late-bound strand. Add events
  to the fake before starting or restarting an engine or task; events added after a run started
  with a higher safe end are never mined (the cursor passed them).

### Position and offsets

- `Position` v1 fields: `resume_scn`, `resume_rs_id`, `resume_ssn`, `last_commit_scn`,
  `last_commit_xid`, `last_commit_thread`, `last_commit_rs_id`, `last_commit_ssn`, `event_index`,
  `journal_generation`, `schema_epoch`, `dbid`, `resetlogs_scn`, `released_xids` (CSV string),
  `snapshot` (JSON string). Offsets may only contain primitives (Connect rejects lists and maps).
- `ResumeCalculator.resume(cursor, oldestOpen)` returns a `RedoRecordId` resume point; the SCN is the
  lower of the two SCNs (for log selection), the RBA the earlier in redo order. `Position.withResume`
  stores it; `Position.withCommit(RedoRecordId, thread, key, acked)` records the commit's RBA.
- `CommitOrder.compare` is redo order within a thread when both sides carry RBAs, else the old
  (scn, thread, key) rule. `SkipRule.eventsToSkip` uses `position.lastCommitId()`.
- `RecordQueueSink` offset rules: a change record carries
  `base.withCommit(tx.commitId(), ...).withResume(min(candidate, tx.firstCaptured()))` for all but
  the last record of a transaction; heartbeats, ops events, journal chunks, tombstones and DLQ
  records carry `lastEmittedCommit.withResume(lastResumeCandidate)` (the "quiet heartbeat" rule:
  every record queued before them allows that position). Keep that rule for any new record type.
- `EventSink` methods (core): `committed(tx, skipped, RedoRecordId resume)`,
  `stepApplied(minedTo, RedoRecordId resume)`, `idle(...)`, `ddl`, `decodeFailed`, `unsupported`,
  `idsRefreshed(owners)`, `reconnected(cause)`, `orphanReleased(release, ledger)`,
  `transactionDiscarded(tx, age, ledger)`. Implementations: `RecordQueueSink`, `CaptureEngineTest.Sink`,
  `EngineCorrectnessEngineIT.Collector`, `ReconnectEngineIT`, `ArchiveOnlyEngineIT`,
  `OrphanReleaseEngineIT`, the dbz-2713 suite.

### Buffer, spill, journal, orphans, age

- `HeapTransactionBuffer(budget, SpillStore, JournalPolicy, JournalSink, generation, clock)`;
  the no-argument constructor is heap-only and never journals. `TransactionEntry` tracks heap
  changes, a spill file, journal state (`journaled`, `chunkRefs`, `lastJournaled`, `pendingJournal`),
  `firstSeenAt`, `lastSeen`, session number and serial.
- Spill (`SpillStore`): one CRC32-framed file per transaction under `cdc.buffer.spill.dir`
  (default worker tmp dir + `oracle-cdc-spill` + connector name), largest-first above
  `cdc.buffer.memory.max.bytes`, lazy read-back at commit (`SpilledChanges`), `CDC-4001` on the cap
  or a full disk, `CDC-3002` on a damaged file. Undo frames carry the undo record id.
- Journal (`JournalPolicy`, `JournalSink`, `JournalChunk`, `JournalFrames`): first chunk holds the
  whole history, later chunks the frames since the previous flush (`flushJournal` runs every step
  and when idle); resume bound for a journaled entry is `lastJournaled`; `restore(chunks)` rebuilds
  an entry; tombstones are written per `JournalChunk.Ref(chunk, generation)` under the generation that
  wrote each chunk. Connector side: `journal/JournalRecords`, `JournalTopicLoader` (drops frames at or
  after the resume point in redo order, tombstones newer-generation chunks, `CDC-4002` on a gap),
  `KafkaJournalReader` (read_committed, bounded wait, logs progress), `TolerantJsonConverter`
  (default `cdc.journal.converter`; reads JSON with or without the schema envelope), `BufferSetup`
  passed through `EngineFactory.Session.engine(start, sink, setup)`. Journaling needs
  `cdc.kafka.bootstrap.servers`; without it the task warns and journals nothing.
- Orphans (`orphan/OrphanDetector`, `JdbcTransactionProbe`): release after absent twice from
  `GV$TRANSACTION`, cursor past the first absence, owning session gone (when the START row named one);
  bounded ledger (256) carried in `Position.released`; `CDC-7001` on a later COMMIT; `CDC-7002` with
  `cdc.transaction.orphan.action=fail`. Engine: `withOrphanDetector(...)`, `checkOrphans()`.
- Age (P1-21): `enforceTransactionAge()` runs after `flushJournal()` in both engine paths.

### LOBs (P1-18, ADR-0015, `reference/lob-redo-shapes.md`)

- LOB rows (`LOB_WRITE` 10, `LOB_TRIM` 11, `LOB_ERASE` 29) have STATUS 2 but a complete PL/SQL block;
  `SqlRedoParser.parseLob` and `RowDecoder.decodeLob` turn them into a `LobFragment`. Code 9 rows
  (labelled INTERNAL, no SQL_REDO) are skipped in `CaptureEngine.apply`. Text offsets and amounts are
  code points, not UTF-16 units.
- `engine/LobAssembler` replaced `LobInsertCoalescer`: one open change per transaction (a row piece
  with the placeholder ROWID, or LOB rows alone), folded until another row arrives. Unavailable LOB
  values are absent from the image; the envelope renders them per `cdc.lob.mode` (placeholder in
  inline and reselect, field omitted in skip). Never put a null for "unavailable": null is a real
  NULL.
- `model/RowIds`: synthetic ROWIDs `#R` (row piece), `#L` (LOB group), `#X` (downgraded group).
  `TransactionEntry` keeps the order of synthetic changes (`resolve`, `track`, `untrack`) and an undo
  of the same table that reverses the newest one targets it; the resolved target goes into spill and
  journal frames. Use `TransactionBuffer.undo(key, id, rowId, table, op)` for new code.
- Reselect: `ReselectingEvents` wraps the committed transaction (a `CommittedTransaction.Lazy` list,
  never copied) and `JdbcLobReselector` queries AS OF the commit SCN on its own connection
  (`JdbcEngineFactory` opens it on first use and drops it on reconnect). It runs while the sink reads
  the events, inside the engine's step, so its exceptions stop the task like any other.

### Metrics and parallel decoding (P1-23, ADR-0013 amendment)

- `core/metrics/TaskMetricsMXBean`: every getter needs a `@Description` (kind COUNTER or GAUGE);
  after adding one run `mvn -pl oracle-cdc-core test -Dtest=MetricsReferenceTest
  -Dmetricsdocs.update=true` and commit website/docs/reference/metrics.md and
  ops/jmx-exporter/oracle-cdc.yml. The dashboard and alert rules may only use exported names.
- `CaptureEngine.publishBuffer()` copies `buffer.metrics()` and `buffer.largest(20)` into
  `EngineMetrics` after every step and idle poll; never read the buffer from another thread.
- `RecordQueueSink` implements `SinkMetrics`; the task registers `TaskMetrics` after the engine starts
  and unregisters it in `closeQuietly`.
- `CaptureEngine.withDecodeThreads(n)`: rows up to the first DDL of a step of 256 rows or more are
  decoded on a fork-join pool; results (and decode exceptions) are consumed in order by `apply`.
  Any new per-row decoding must stay a pure function of the row and its `TableSchema`.

### Ops topic, internal topics, DLQ

- `ops/OpsEvent` wire names (PRD-01 section 4.9): `startup`, `stop`, `ddl-seen`, `ids-refreshed`,
  `reconnected`, `unsupported-row`, `decode-error-dlq`, `transaction-orphan-released`,
  `transaction-discarded` are emitted today; the rest of the enum is reserved. Record schema
  `io.oso.cdc.ops.Event` v1, key by server. `EngineLifecycle` runs the failure callback before the
  failure becomes visible so the `stop` event is drained before `poll()` rethrows.
- `topics/InternalTopics` + `InternalTopicManager` over `TopicAdmin` (`KafkaTopicAdmin`, test fake):
  ops, heartbeat (24 h retention), signals, schema (compact), txjournal (compact), transactions
  (optional), dlq (when `cdc.on.decode.error=dlq` or the age action is `discard`). Needs
  `cdc.kafka.bootstrap.servers`; `cdc.kafka.*` are passed to the admin client and the journal reader.
- `dlq/DecodeDlqWriter`: `io.oso.cdc.dlq.Record` v1 with `kind` in `decode-error`,
  `unsupported-row`, `transaction-discarded`.

### Configuration and docs

- Core keys in `CoreConfig` (43 keys; `CoreConfigTest` asserts the count), connector keys in
  `OracleCdcSourceConnectorConfig`. Every key change needs
  `mvn -pl e2e-tests test -Dtest=ConfigDocsGeneratorTest -Dconfigdocs.update=true` (after an
  install of the connector) and the result committed; the gate fails on drift.
- Reference documents under `oracle-cdc-core/src/main/resources/reference/` are generated by
  `*RefEngineIT` spikes and compared on every run (`ReferenceDoc.assertUpToDate`). They may contain
  only facts that are stable run after run: no counts, no SCNs, no timings. Rewrite with
  `-Dreference.update=true` and review the diff.
- Runbooks: one page per `ErrorCode` slug under `website/docs/operations/runbooks/`; the ops topic,
  offsets, buffer and journal pages are current as of `cafbccc` plus the P1-21 edits.

---

## 3. Verification protocol for every increment

1. Unit tests while developing: `mvn -pl oracle-cdc-core,kafka-connect-oracle-cdc test -Djacoco.skip -Dspotbugs.skip -Dlicense.skip`
   (always both modules together: the connector compiles against the reactor's core, not `~/.m2`).
2. `mvn spotless:apply` before every build; the gate checks formatting.
3. `mvn clean verify -DskipE2E` (the gate). Expect JaCoCo to require tests for new core code; JDBC-bound
   classes are excluded in `config/jacoco` or by the existing `**/Jdbc*` style exclusions, check
   `oracle-cdc-core/pom.xml` before adding exclusions.
4. `mvn -q install -DskipTests -DskipE2E` then the tiers: `mvn -pl e2e-tests verify -De2e.groups=connector`
   and `-De2e.groups=engine` (one at a time, 15 seconds apart). A single suite: add
   `-Dit.test=<ClassName>`; the two surefire tests (`ConfigDocsGeneratorTest`, `PluginZipLayoutTest`)
   always run first and must pass.
5. Commit only when 1 to 4 are green. One commit per increment, conventional type and scope
   (`feat(core,connector): ...`), body in UK English, trailer as above, `-s`.
6. Update `docs/HANDOVER.md` section 1 and the plan status in your own notes; the PRDs are amended
   only through ADRs plus one-line edits.

Expected tier contents after P1-23: connector tier 8 suites (`FirstRecord`, `RestartNoLoss`,
`CorrectnessOracle`, `OpsTopic`, `JournaledTransaction`, `DecodeDlq`, `AdvancesOffsetsOnQuietDatabase`,
plus the two surefire tests), engine tier 38 tests. `RestartNoLossConnectorIT` prints LogMiner rows,
ops and heartbeat records on a miss; a miss is a product bug until proven otherwise.

---

## 4. Pitfalls met on this workstation (read before debugging a red run)

- A flashback query within about three seconds of a DDL on the table raises ORA-01466; reselect
  treats it as unavailable. Suites that reselect sleep 3.5 s after creating their tables.
- Spikes that filter LogMiner rows by `SEG_OWNER` miss the code 9 rows and anything else without an
  owner; filter by `DATA_OBJ#` and attribute rows to scenarios by XID
  (`DBMS_TRANSACTION.LOCAL_TRANSACTION_ID` before the commit).
- `sed` on a Java source holding a literal non-ASCII character (Spotless keeps the emoji as typed)
  silently does nothing; patch such files with Python and assert the match.
- Never `rm` an archived log in a suite: the Oracle container is shared by every suite in the JVM
  and the catalog keeps listing the file. Use `OracleSql.hideArchivedLog` and
  `OracleSql.restoreHiddenLogs` (finally block).
- `mvn install -DskipTests` does not skip failsafe in `e2e-tests`: add `-DskipE2E` or it starts Oracle.
- Never run Maven on `oracle-cdc-core` or `kafka-connect-oracle-cdc` while a tier runs: the tier
  resolves jars from `~/.m2` and copies the plugin from `kafka-connect-oracle-cdc/target/...-kafka-connect-plugin/`.
  Edits to Java sources are safe during a tier run only if nothing compiles or installs them.
- Two e2e JVMs must not overlap: the Docker network is pinned to `10.214.0.0/24` and
  `OracleTestDatabase` waits up to 60 s for a stale network to be released by Ryuk.
- A fresh Oracle container takes one to two minutes; if `Container startup failed` with exit code 241
  appears, rerun once before investigating (seen once under load).
- Writing Java through a shell heredoc without quoting the delimiter expands `$`: `v$archived_log`
  became `v`. Use `cat <<'EOF'` (quoted) or escape as `\$`. Check generated files with `grep '\$'`.
- Spotless rewraps code, so exact-string Python patches can silently miss; assert that every `old`
  string was found (`assert old in s`) and re-check with `grep` after formatting.
- Failsafe selects tiers by class-name suffix (`*EngineIT`, `*ConnectorIT`, `*NightlyIT`) in the
  matching package; `@Tag` alone does nothing. Regression suites live in `e2e/regression` and must
  still end in `EngineIT` or `ConnectorIT`.
- The connector worker in `ConnectCluster` uses the JSON converter with `schemas.enable=false`;
  anything that reads a topic back (journal loader, tests) must cope with both envelope modes.
- `cdc.event_index` header is zero-based; `transaction.total_order` is one-based.
- A capture-user connection in a PDB cannot read `v$session`; use the SYSDBA connection for
  session serial numbers in tests.
- Connections killed by a test (`ALTER SYSTEM KILL SESSION`) throw on `close()`; do not put them in
  try-with-resources.
- `TaskHarness` filters heartbeat, ops, journal and DLQ records out of `pollUntil` unless
  `keepInternal` is true; `TaskHarness.sql(r)` labels them `<heartbeat>`, `<ops:type>`,
  `<chunk:n>`, `<tombstone:n>`, `<dlq:kind>`.
- `OrphanDetector.check` and the journal flush run only when the engine steps; in tests advance
  `FixedClock` and call `runUntilIdle` again.
- The pinned subnet, the reused Oracle container and the lab stacks (`oracle-cdc-lab-*` compose,
  minikube `cdc-lab`) are all running; `docker network prune` cleared leftovers once before.

---

## 5. Remaining increments, in order, with concrete designs

Each item lists what to build, where it plugs in, and the tests that prove it. Keep the proven
patterns: core logic with a fake for T0, a `*EngineIT` against Oracle, a `*ConnectorIT` through a
real worker where Kafka matters, docs regenerated, runbook per new error code.

### P1-18 follow-ups (not blocking; record decisions in ADR-0015 amendments)

- XMLTYPE: XML DOC BEGIN, WRITE and END rows are ignored today (`Operation.UNKNOWN`), so XMLTYPE
  values are unavailable; assemble them or reselect them.
- Asynchronous reselect with a bounded queue (PRD-00 CORE-DEC-7) instead of one synchronous query
  per row at commit.
- 19c and 21c (P1-30): rerun `LobRedoShapesRefEngineIT`; older releases may split the locator select
  from the writes.
- P1-26: the LOB savepoint invariants (corpus row "Savepoint bugs fixed in Debezium 3.7") are proven
  by `LobModesEngineIT` and `LobUndoBufferTest`; decide with Sion how rows without an issue number
  are tagged before moving them into `e2e/regression`.

### P1-23 follow-ups

- Overlap fetch and decode (CORE-MINE-6 in full): decode while the step is still being read.
- An alert on resume position age against archive retention needs the retention (RMAN policy or
  `DB_RECOVERY_FILE_DEST` usage) as a metric; the doctor (P1-24) is the natural source.
- The connector tier cannot read the worker's JMX (the worker runs in a container); `MetricsEngineIT`
  proves the MXBean at the engine level.

### P1-12 Exactly-once (ADR-0007)

- `OracleCdcSourceConnector.exactlyOnceSupport()` returns SUPPORTED and
  `canDefineTransactionBoundaries()` SUPPORTED; `OracleCdcSourceTask` uses
  `context.transactionContext()` to commit a Kafka transaction after the last record of each Oracle
  commit (`RecordQueueSink.committed` marks the last record: add a `TransactionBoundary` marker list
  the task consults in `poll()`), batching several Oracle commits per Kafka transaction within
  `cdc.eos.batch.max.records`, `cdc.eos.batch.max.ms`, `cdc.eos.batch.max.bytes` (16 MiB), and
  splitting a single huge transaction at `cdc.eos.split.max.bytes` (256 MiB) with a
  `transaction-split` ops event (SRC-EOS-4). Heartbeats, ops, journal and DLQ records ride inside
  whichever transaction is open; a quiet heartbeat alone commits a small transaction.
- DOC-18 in the doctor: read `transaction.max.timeout.ms` from the broker through the admin client
  and validate `cdc.eos.*`.
- Tests: `ExactlyOnceConnectorIT` (read_committed consumer never sees a partial Oracle
  transaction), `WorkerKillEosConnectorIT` (ten kills, zero duplicates with read_committed),
  `SplitTransactionConnectorIT`. The worker in `ConnectCluster` already has
  `exactly.once.source.support=enabled`.

### P1-16 Schema topic and DDL flow (PRD-03)

- Core `schema/DdlClassifier` (parse the DDL text into kinds: add column, drop column, rename,
  modify type, truncate, add partition and so on, from the DDL rows already mined) and `DdlApplier`
  (apply to `TableSchema` producing a new version); `SchemaRegistry` versions by SCN
  (`TableSchema` gains `version` and `validFromScn`); `RowDecoder` decodes with the version valid at
  the row's SCN. Unknown or unsupported DDL is `UnsupportedDdlException` (`CDC-6002`).
- Connector `schema/SchemaTopicStore` (compacted `${prefix}.cdc.schema`, key table id plus version,
  value the schema JSON, written from `poll()` like journal chunks) and `SchemaTopicLoader` at start
  (same reader pattern as the journal: `cdc.kafka.bootstrap.servers`, tolerant converter); pruning
  of versions older than the position's resume SCN; optional `${prefix}.cdc.schema-changes` topic
  with Debezium-shaped schema change events. Emit `ddl-applied` on the ops topic.
- Tests: T0 classifier corpus (every SCH-1 form), T1 `DdlUnderLoadEngineIT` (every form while a
  workload runs; records before and after decode with the right version), `SchemaTopicRebuildConnectorIT`,
  `SchemaTopicPruningConnectorIT`, regressions `dbz-2184`, `dbz-1599` in `e2e/regression`.

### P1-17 Lag case and dictionary builds (ADR-0008)

- `LagCaseDetector`: STATUS 2 rows with generic `COL n` names after a DDL on the table; `RedoDictionaryReplay`:
  reopen the range with `DICT_FROM_REDO_LOGS + DDL_DICT_TRACKING` adding every log from the last
  `DBMS_LOGMNR_D.BUILD` (`V$ARCHIVED_LOG.DICTIONARY_BEGIN/END`); `DictionaryBuildScheduler`
  (`cdc.dictionary.build.interval.ms`, needs `EXECUTE ON DBMS_LOGMNR_D`, the lab user has it).
  Without a usable build: `DictionaryUnavailableException` (`CDC-6001`).
- `LogMinerSource.start(startScn, endScn, DictionaryMode)` already takes the mode.
- Tests: `LagCaseReplayEngineIT` (50 DML, drop and add column, 50 DML across a restart) and
  `LagCaseNoBuildEngineIT`; `reference/dictionary-replay.md` records the current facts.

### P1-19 Snapshots (PRD-02, ADR-0004)

- Core `snapshot/`: `ChunkPlanner` (`KeyRangePlanner` with NTILE over the key, composite keys,
  partitions; `RowidRangePlanner` from `DBA_EXTENTS` for keyless heap tables; IOT by key always),
  `ChunkReader` (`SELECT ... AS OF SCN :s` with `cdc.snapshot.fetch.size`; ORA-01555 and ORA-08181
  retry with a new SCN and half the chunk down to 1,000 rows), `SnapshotPosition` (frontier plus
  exceptions, serialised into `Position.snapshot` as a JSON string), `ChunkHold` (changes for rows
  inside a chunk not yet read are held until the chunk lands), `Interleaver` (snapshot chunks and
  streamed commits in one record stream, snapshot records marked `op=r`, `snapshot=true`),
  `SnapshotCoordinator` (`cdc.snapshot.mode` initial, none, when_needed, schema_only;
  `cdc.snapshot.threads`).
- Connector: snapshot records carry offsets whose `snapshot` block names the frontier; a restart
  resumes the frontier (`SnapshotResumeConnectorIT` kills the worker at 50 per cent).
- Tests: `SnapshotCorrectnessConnectorIT` with `bench check` (`workloads/snapshot.json`),
  `IotAndKeylessEngineIT`, `CompositeKeyRateEngineIT`, regressions `dbz-2779`, `dbz-2297`.
- Doctor DOC-7 extension: keyless table with row movement is a warning.

### P1-20 Signals (SRC-SIG)

- `signals/SignalConsumer` reading `${prefix}.cdc.signals` (JSON commands keyed by connector name):
  `snapshot` (tables, predicate), `snapshot-stop`, `snapshot-pause`, `snapshot-resume`,
  `refresh-tables`, `log-state` (dump buffer `largest(20)` and position to the ops topic). Every
  signal ends in a `signal-ack` ops event with outcome. Reader pattern as the journal (consumer on
  `cdc.kafka.*`), polled from `poll()` between batches.
- Tests: `SignalSnapshotConnectorIT` (archive-only, no source writes), `SignalAckConnectorIT`.

### P1-22 Multi-PDB and new-table detection (ADR-0002)

- `cdc.database.pdbs` with several names already resolves objects per container (`ObjectKey`);
  verify routing with `MultiPdbConnectorIT` that creates and drops `FREEPDB3` (Oracle Free allows
  three user PDBs). `TopicRouter` uses `SRC_CON_NAME` for the topic.
- New table detection (SRC-SEL-4): a `CREATE TABLE` matching the include pattern already refreshes
  ids; add the internal snapshot signal for the new table (needs P1-19), `table-added` and
  `table-removed` ops events, and `NewTableDetectionConnectorIT`.

### P1-24 Doctor full and admin CLI (PRD-05)

- Rules DOC-8 to DOC-11, DOC-13, DOC-16 to DOC-20 (DOC-18 per ADR-0007), row-movement warning,
  JUnit XML output, `redo-profile` (V$ARCHIVED_LOG rates, redo per table from a short LogMiner
  sample), `sizing`, `explain-lag` (three scripted bottlenecks), `oracle-cdc-admin offsets show|set --reason`
  (PATCH `/connectors/{name}/offsets`, refuses a purged SCN, writes `offsets-set` to the ops topic),
  `resnapshot`, `transactions`, `journal inspect`.
- Tests: `RedoProfileEngineIT`, `ExplainLagEngineIT`, `AdminOffsetsConnectorIT`, `ResnapshotConnectorIT`.

### P1-25 Migration tooling (PRD-04)

- `tools/migration/pyproject.toml` (uv, Python 3.11, python-oracledb, confluent-kafka, pytest,
  ruff), shared translator framework, `migrate_from_debezium.py` (PRD-01 Appendix A mapping),
  `takeover_scn.py`, `verify_cutover.py` (VER-1 to VER-5, same canonical normalisation as
  `bench/check/Normaliser`), golden files, a canary test that secrets are masked.
- `e2e-tests/.../migration/DebeziumCutoverConnectorIT` with Debezium 3.7 from Maven Central, evidence
  JSON uploaded as `migration-evidence`; `ci.yml` job `migration-tools`.

### P1-26 Regression corpus naming (ADR-0011)

- `RegressionCorpusIndexTest` keeps `docs/testing_strategy.md` section 5 and `e2e/regression/` one
  to one by `@Tag("dbz-nnnn")`. Present today: `AdvancesOffsetsOnQuietDatabaseConnectorIT`
  (`dbz-2781`, `dbz-2475`), `StopsWhenRedoForOpenTransactionIsMissingEngineIT` (`dbz-2713`),
  `NeverEncodesPositionAheadOfDelivery` as a task test (`dbz-2544`), the zero-XID and excluded-user
  cases inside `EngineCorrectnessEngineIT` (`dbz-2683`, `dbz-24`). Move or wrap them so each listed
  issue has a suite in the regression package.

### P1-27 Nightly and issue watch

- `.github/workflows/nightly.yml`: matrix `23.26.3-slim-faststart` and `23.9-faststart`; `t2-faults`
  (Toxiproxy latency and resets, switch storms, purge, database restart, disk-full spill, schema
  topic loss, broker restart), `t2-kills` (1,000 EOS kills), evidence JSON upload, issue on
  failure; fault schedules in `bench/src/main/resources/faults/*.json`; `dbz-issue-watch.yml`.
  CI only runs after a push, which is the user's decision.

### P1-28 Docs

- Generated: configuration (done), `metrics.md` (P1-23), `error-classes.md` from `ErrorCode`,
  `ops-topic.md` (hand-written today; generate the event table from `OpsEvent.Type`),
  `record-format-debezium.md`. `RunbookPresenceTest`: a page per `ErrorCode` slug. Concept, setup,
  migration, comparison (no benchmark figures), enterprise support pages per
  `docs/06_repo_scaffolding_spec.md` section 5. Site builds with `onBrokenLinks: throw`.

### P1-29 Release pipeline

- `release.yml` with Central dry run (`-Prelease -DskipPublishing`), Hub manifest test, ZIP attached,
  `oracle-cdc-doctor` image to GHCR with SBOM (syft) and provenance; `verify-release-secrets.yml`;
  release-please `extra-files` lists every module POM. Never tag by hand.

### P1-30 Phase 1 exit

- 72-hour soak on the workstation with `bench check` (comparison with a Debezium baseline stays
  internal), 19c and 21c single-instance qualification from `lab/local`, every PRD-00 to PRD-03
  acceptance box ticked with a link to the proving suite.

### P1-31 Strimzi edge cases (lab/local/k8s)

- Passing with ledger verification: kill_worker, rolling_update, oracle_restart, rebalance.
  Remaining: rollout_restart, broker_restart, partition, config_update,
  operator_restart_during_change, oom, offsets_list. Wire `bench check` into
  `lab/local/k8s/edge-cases.sh` (Kafka must be reachable from the host, or run the check in-cluster).
  Rebuild the Connect image with every plugin change (`make connect-image`, unique tag, patch
  `spec.image`).

---

## 6. Open points and decisions that belong to the user

- Pushing to GitHub and enabling CI, the DCO app and the domain registration.
- The 1b/1c scope cut flagged in the plan: EOS and signal snapshots could move to 1.1 if time is short.
- Oracle legal view on lab use and OPN pricing before 1.0 GA.
- Tearing down the running labs (compose stack and minikube `cdc-lab`) is theirs to decide.
- RAC (per-thread position vector, CORE-POS-4) stays Phase 2; the RBA cursor is per thread by design,
  `CommitOrder.compare` currently orders threads before addresses and must become a per-thread
  vector then.

## 7. Facts worth knowing that are not in the code

- Debezium's flush table was rejected on purpose (CORE-POS-5, no writes to the source); the RBA
  cursor replaces it. If anyone proposes an SCN lag or a flush table again, point them at ADR-0014
  and the spike.
- The one intermittent miss in `RestartNoLossConnectorIT` before `cafbccc` is explained by the
  private-strand race; four runs of about 20,000 transactions each passed after the fix, plus the
  full tier twice. Keep running that suite on every change to mining or offsets.
- The lab Oracle image is `oracle-cdc-test-db:23.26.3-slim-faststart` built from `docker/test-oracle`
  (ARCHIVELOG, supplemental logging, `FREEPDB2`, common user `c##cdc` with the grants in
  `04-capture-user.sql`, which `oracle-cdc-doctor setup-sql --profile lab` must reproduce byte for byte).
