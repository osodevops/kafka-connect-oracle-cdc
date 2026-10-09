# PRD-00: `oracle-cdc-core` Capture Engine

**Status:** Draft for implementation
**Module:** `oracle-cdc-core` (package `sh.oso.connect.oracle.core`)
**Replaces:** Nothing (new engine)
**Depends on:** `ojdbc11` 23.x, Java 17
**Clean-room note:** Built from Oracle's public documentation and our own design. No Confluent code or binaries. Debezium is Apache-2.0; we use its public issues as a failure catalogue and do not copy its code by default (see feasibility report section 3).

---

## 1. Objective

Provide a library, independent of Kafka Connect, that turns Oracle redo (through LogMiner) into an ordered stream of committed, fully decoded row changes with a resumable position, and that never skips data silently. PRD-01 wraps it as a connector; `oracle-cdc-doctor` reuses its database probes.

## 2. Scope

**In:** JDBC connection management; log inventory and continuity checks (single instance and RAC); mining session management with adaptive windows; object-ID filtering; `SQL_REDO` decoding and type conversion; transaction buffer with spill and journal; position model; orphan transaction detection; schema registry (PRD-03 owns DDL semantics); typed errors; fake LogMiner for unit tests; JDBC fault injection.

**Out:** Kafka Connect specifics (PRD-01), snapshots (PRD-02), CLI (PRD-05), XStream (Phase 3).

## 3. User stories

- As a platform engineer, I want the engine to stop with a clear error instead of skipping redo, so that I never discover missing rows weeks later (PP-01, PP-04).
- As a DBA, I want long-running or abandoned transactions not to force the connector to keep redo for days (PP-03).
- As an operator, I want memory use to stay inside a fixed budget whatever the transaction size (PP-07).
- As an operator, I want one latency target, not nine tuning properties (PP-05).
- As a DBA, I want the connector to read only redo for captured tables wherever Oracle allows it (PP-06, PP-13).

## 4. Functional requirements

### 4.1 Connections (CORE-CONN)

| ID | Requirement |
|---|---|
| CORE-CONN-1 | Connect by `host`/`port`/`service`, `sid`, or full JDBC URL (including TNS descriptors, LDAP naming and wallets). |
| CORE-CONN-2 | Maintain separate connections for mining, metadata and each snapshot thread. Mining never shares a connection. |
| CORE-CONN-3 | Enable TCP keepalive (`oracle.net.keepAlive=true`) and a configurable application-level probe at half `cdc.database.idle.timeout.ms` (default 300000), so load balancers with idle timeouts such as the AWS 350-second case do not hang the connector (PP-14). |
| CORE-CONN-4 | Set `NLS_DATE_FORMAT`, `NLS_TIMESTAMP_FORMAT`, `NLS_TIMESTAMP_TZ_FORMAT`, `NLS_NUMERIC_CHARACTERS` and the session `TIME_ZONE` (UTC) on every session to fixed values used by the decoder; never depend on database defaults. Evidence: `reference/sql-redo-shapes.md` shows the default `DD-MON-RR` mask dropping the time of day and `TIMESTAMP WITH LOCAL TIME ZONE` rendered in the mining session's zone. |
| CORE-CONN-5 | Detect topology at startup: CDB or non-CDB, open PDBs, RAC thread list (`V$THREAD`), database role (`V$DATABASE.DATABASE_ROLE`), open mode, log mode, version and RU. Store a topology fingerprint; changes (for example a new RAC thread) trigger a controlled re-plan, not a restart. Shapes the release is not qualified for stop the task with `TopologyException` before any offset is read (ADR-0023); until the per-thread position exists (CORE-POS-4) that is any database with more than one enabled redo thread. |
| CORE-CONN-6 | Reconnect with exponential backoff and jitter on retriable errors (CORE-ERR table), bounded by `cdc.retry.max.time.ms`. After a reconnect the database identity (DBID, RESETLOGS SCN) is read again before mining; a new incarnation stops the task with `TopologyException` (ADR-0023 amendment). |

### 4.2 Log inventory and continuity (CORE-LOG)

| ID | Requirement |
|---|---|
| CORE-LOG-1 | Build the log set for an SCN range from `V$LOG`, `V$LOGFILE` and `V$ARCHIVED_LOG` per thread, preferring archived copies from `cdc.archive.destination` (default: lowest `DEST_ID` that is valid and local). |
| CORE-LOG-2 | Before every mining step, verify for every thread that was open in the range that sequences are contiguous from the log containing the start SCN to the log containing the end SCN. A gap is `OracleCdcGapException` (stop). |
| CORE-LOG-3 | Treat a thread as required if `V$THREAD` or `V$ARCHIVED_LOG` shows it enabled or with redo in the range, regardless of PUBLIC or PRIVATE status. A thread state change is logged as an ops event. This removes the Debezium RAC PRIVATE skip case (PP-01). |
| CORE-LOG-4 | If a required log has been purged, raise `OracleCdcPurgedException` naming thread, sequence, SCN range and the captured tables affected. Never choose a later start. Recovery is an explicit operator action (PRD-05 `oracle-cdc-admin resnapshot`). |
| CORE-LOG-5 | Handle online log reuse: if an online log in the set is overwritten during mining (ORA-00310, ORA-00334, ORA-01289 style errors), discard the partial result for that step and re-mine the same range from the archived copy. Rows from a failed step are never emitted or buffered. This fixes the [dbz#2504](https://github.com/debezium/dbz/issues/2504) class. |
| CORE-LOG-6 | Archive-log-only mode (`cdc.capture.mode=archive_only`): never add online logs; the safe end SCN is the highest `NEXT_CHANGE#` with all threads archived to that point. Required for standby capture (Phase 2). |
| CORE-LOG-7 | Expose per thread: current sequence, oldest needed sequence, archived lag, switch rate per hour. |

### 4.3 Mining (CORE-MINE)

| ID | Requirement |
|---|---|
| CORE-MINE-1 | Use `DBMS_LOGMNR.START_LOGMNR` with `DICT_FROM_ONLINE_CATALOG`, `NO_ROWID_IN_STMT`, `NO_SQL_DELIMITER`, without `COMMITTED_DATA_ONLY` and without `SKIP_CORRUPTION`. Redo dictionary mode is used only by PRD-03 DDL replay windows. |
| CORE-MINE-2 | Query filter: `OPERATION_CODE IN (1,2,3,5,6,7,36,...)` restricted to captured `DATA_OBJ#` values for DML, plus DDL rows for captured owners, plus transaction control rows. The exact operation-code list is produced by CORE-REF-1. Transaction control rows for users in `cdc.users.exclude` are dropped in the query so excluded users never create transactions ([dbz#24](https://github.com/debezium/dbz/issues/24)). |
| CORE-MINE-3 | Object-ID resolution: resolve `cdc.tables.include` and `cdc.tables.exclude` (regular expressions allowed) against `DBA_OBJECTS` per container, including partitions and subpartitions and the top index of index-organized tables (`DATA_OBJ#` is the logical `OBJECT_ID` of the segment written, stable across TRUNCATE, MOVE and partition maintenance; `DATA_OBJD#` is the physical id and is not used for filtering; see `reference/object-id-stability.md`). Refresh on DDL for captured owners, on CREATE TABLE matching a pattern, and every `cdc.tables.refresh.interval.ms`. If the list exceeds `cdc.mining.inlist.max` (default 1000), use a global temporary table join instead of an IN list. Object ids are allocated per container, so the pushdown and the row naming pair `SRC_CON_ID` with `DATA_OBJ#` (a root AWR table shared an id with a PDB table in the Strimzi lab). |
| CORE-MINE-4 | Adaptive window. The scheduler chooses the end SCN of each step to meet `cdc.mining.target.latency.ms` (default 2000): when lag is below target, mine to the safe end SCN; when lag is above target, size the window by log count and observed rows per second, starting at one log and doubling to `cdc.mining.max.logs.per.step` (default 8). Every decision is exported (window SCN span, logs, rows, query time, fetch time). No other tuning properties exist. The cursor between steps is the redo byte address (`RS_ID`, `SSN`) of the last row applied, not an SCN: redo of an open transaction can reach the log after the step that covered its SCNs and still carry those SCNs (private redo strands, ADR-0014). LogMiner starts at the first SCN of the log holding the cursor. |
| CORE-MINE-5 | Timeouts shrink, never fail: if a mining query exceeds `cdc.mining.query.timeout.ms` (default 600000) the engine cancels, halves the window and retries; three consecutive halvings to a single log raises `MiningStalledException` (stop) with diagnostics. ORA-01013 from our own cancel is never surfaced as a task failure (PP-03). |
| CORE-MINE-6 | Pipelining: one thread fetches rows (`cdc.mining.fetch.size`, default 10000), N decode threads (default number of cores minus one, maximum 8) convert rows, one thread applies to the buffer in SCN order using a sequence number assigned at fetch. Amended by ADR-0013 (P1-23): decode runs in parallel after the step is read; fetch and decode do not overlap yet. |
| CORE-MINE-7 | Session reuse: keep one LogMiner session open across steps; re-add logs only when the set changes; restart the session every `cdc.mining.session.max.age.ms` (default 3600000) to release PGA. Report PGA use from `V$PROCESS` for the mining session. |
| CORE-MINE-8 | Parallel catch-up (Phase 2): when lag exceeds `cdc.mining.catchup.threshold.ms` (default 300000), run up to `cdc.mining.catchup.parallelism` (default 2) sessions over adjacent, non-overlapping SCN windows on separate connections; merge by SCN before the buffer. Off when lag is under threshold. |
| CORE-MINE-9 | Multi-PDB: in a CDB connect to `CDB$ROOT` with a common user, mine once, route by `SRC_CON_NAME` or `CON_ID` (per CORE-REF-2). One connector serves all PDBs in `cdc.database.pdbs`. |
| CORE-MINE-10 | `STATUS` handling: rows with `STATUS` 2 or 3, `OPERATION` `UNSUPPORTED`, `MISSING_SCN` or `CORRUPTED_BLOCKS` for a captured object are stop conditions (`DecodeException`, `OracleCdcCorruptionException`) unless `cdc.on.decode.error=dlq` and the row is a DML row, in which case the raw row goes to the DLQ with full context and an ops event. `MISSING_SCN` and corruption are always stops. |

### 4.4 Decoding and types (CORE-DEC)

| ID | Requirement |
|---|---|
| CORE-DEC-1 | Parse `SQL_REDO` with a hand-written scanner for the LogMiner-generated SQL subset (INSERT ... VALUES, UPDATE ... SET ... WHERE, DELETE ... WHERE), handling `CSF` continuation, quoted identifiers, `HEXTORAW`, `TO_DATE`, `TO_TIMESTAMP`, `TO_TIMESTAMP_TZ`, `TO_DSINTERVAL`, `TO_YMINTERVAL`, `EMPTY_CLOB()`, `EMPTY_BLOB()`, `NULL`, `IS NULL`, Unicode escapes (`UNISTR`). No regular-expression parsing of SQL. |
| CORE-DEC-2 | The parser is fuzz-tested (jqwik) with generated statements for every supported type and identifier form, and benchmarked at no less than 200,000 statements per second per core on reference hardware (target, validated in Phase 0). |
| CORE-DEC-3 | Map columns by name using the schema version effective at the row's SCN (PRD-03). Unknown column names are a `DecodeException`. |
| CORE-DEC-4 | Update before images: with `ALL` supplemental logging, rebuild the full before and after row from `WHERE` and `SET`. Without it, emit only key plus changed columns and set `source.partial=true`; `oracle-cdc-doctor` warns. |
| CORE-DEC-5 | Types supported in 1.0: CHAR, NCHAR, VARCHAR2 (including 32 KB extended), NVARCHAR2, NUMBER, FLOAT, BINARY_FLOAT, BINARY_DOUBLE, DATE, TIMESTAMP, TIMESTAMP WITH TIME ZONE, TIMESTAMP WITH LOCAL TIME ZONE, INTERVAL YEAR TO MONTH, INTERVAL DAY TO SECOND, RAW, CLOB, NCLOB, BLOB, XMLTYPE (CLOB storage), LONG and LONG RAW via reselect only. Phase 3: BOOLEAN, VECTOR, JSON where LogMiner provides them (CORE-REF-1 determines this). |
| CORE-DEC-6 | LOBs: LogMiner emits LOB writes as separate `SEL_LOB_LOCATOR`, `LOB_WRITE`, `LOB_TRIM`, `LOB_ERASE` rows. Assemble them within the transaction buffer keyed by row and column. If assembly is impossible (piecewise operations, unsupported storage), the column value is resolved by the configured LOB mode in PRD-01 (`skip`, `inline` with placeholder, or `reselect`). Amended by ADR-0015: LOB rows fold into the change of their statement; 23ai has no usable SEL_LOB_LOCATOR rows. |
| CORE-DEC-7 | Reselect: fetch unavailable columns with `SELECT ... AS OF SCN :commit_scn WHERE <key>`; on ORA-01555 or ORA-08181 fall back to the placeholder and mark `source.reselect=failed`. Reselect runs on the metadata connection pool with a bounded queue so it cannot stall mining. Amended by ADR-0015: reselect runs synchronously at commit in this release. |

### 4.5 Transaction buffer (CORE-TX)

| ID | Requirement |
|---|---|
| CORE-TX-1 | Buffer by XID (`XIDUSN.XIDSLT.XIDSQN`). A transaction entry is created only on its first captured DML row; START rows and all-zero XIDs never create entries ([dbz#2683](https://github.com/debezium/dbz/issues/2683)). Each entry records `firstCapturedScn`, `startScn` where known, user, client id and RAC thread. |
| CORE-TX-2 | Rollback handling: `ROLLBACK=1` rows undo the matching prior operation (by `RS_ID` and `SSN`), which implements partial rollback to savepoint; a full ROLLBACK discards the entry. Tests cover savepoint rollback of insert, update, delete and LOB writes, which is where Debezium found bugs until 3.7 ([Debezium 3.7 release](https://debezium.io/blog/2026/09/29/debezium-3-7-final-released/)). Amended by ADR-0015: an undo targets the newest surviving change when that change has no real ROWID. Amended by ADR-0022: a row with the partial XID sequence 0xFFFFFFFF belongs to the transaction open in its undo slot. |
| CORE-TX-3 | Memory budget `cdc.buffer.memory.max.bytes` (default 268435456). When exceeded, spill the largest transactions' events to append-only segment files in `cdc.buffer.spill.dir` (default Connect worker temp dir plus connector name), with a per-connector cap `cdc.buffer.spill.max.bytes` (default 10737418240). Exceeding the spill cap is `BufferExhaustedException` (stop) with the top ten transactions in the message. |
| CORE-TX-4 | Durable journal: transactions open longer than `cdc.txjournal.threshold.ms` (default 300000), or larger than `cdc.txjournal.threshold.events` (default 100000), are journaled to the compacted topic `cdc.txjournal.topic`. Records are keyed by XID plus chunk number; on commit or rollback a tombstone is written per chunk. A journaled transaction no longer constrains the restart position (CORE-POS-2). |
| CORE-TX-5 | On restart, journal state for open transactions is reloaded before mining resumes; chunks for a transaction are applied in order; a missing chunk is `JournalCorruptionException` (stop). |
| CORE-TX-6 | Long transaction policy: `cdc.transaction.max.age.ms` (default -1, unlimited) with `cdc.transaction.max.age.action` = `fail` or `discard`. `discard` writes the XID, user, first SCN and event count to the ops topic and DLQ before dropping. There is no silent discard. |
| CORE-TX-7 | Orphan detection: every `cdc.transaction.orphan.check.interval.ms` (default 300000), for buffered transactions older than that interval, query `GV$TRANSACTION` for the XID. If absent, mine the range from the entry's last seen SCN to the current safe SCN for the XID's COMMIT or ROLLBACK row only. If neither is found and the transaction is absent from `GV$TRANSACTION` in two consecutive checks, apply `cdc.transaction.orphan.action`: `release` (default; treat as rolled back, ops event with full details) or `fail`. |
| CORE-TX-8 | Metrics per transaction (top 20 by age and size): XID, user, client id, age, events, bytes in heap, bytes spilled, journaled flag (PP-07). |
| CORE-TX-9 | Commit emits the transaction's events in redo order with a per-transaction event index and total count. |

### 4.6 Position model (CORE-POS)

| ID | Requirement |
|---|---|
| CORE-POS-1 | Position fields: `v` (format version), `resume_scn`, `last_commit_scn`, `last_commit_xid`, `last_commit_thread`, `event_index` (events of that transaction acknowledged), `journal_generation`, `schema_epoch`, `snapshot` (PRD-02). The position also carries `resume_rs_id`, `resume_ssn`, `last_commit_rs_id` and `last_commit_ssn` (ADR-0014). |
| CORE-POS-2 | `resume_scn` is the minimum of the safe mined SCN and the `firstCapturedScn` of open, non-journaled transactions. Journaled transactions are excluded. |
| CORE-POS-3 | On restart, mine from `resume_scn`; skip commits with `(commit_scn, xid)` at or before `(last_commit_scn, last_commit_xid)` in commit order; for the transaction equal to `last_commit_*`, skip the first `event_index` events. `event_index` is computed only from records acknowledged by the framework (`commitRecord`), never from emitted or buffered counts ([dbz#2544](https://github.com/debezium/dbz/issues/2544)). Commit order is the redo order of the COMMIT rows within a thread when the position carries redo byte addresses (ADR-0014). |
| CORE-POS-4 | Commit ordering across RAC threads uses `(commit_scn, thread, xid)` as a total order. |
| CORE-POS-5 | Quiet databases: the position advances to the safe mined SCN even when no captured rows are seen, without writing to the source (PP-14). |
| CORE-POS-6 | Position format changes are versioned; each version has a reader for all previous versions; downgrade by one minor version is supported (PP-16). |

### 4.7 Errors (CORE-ERR)

All errors extend `OracleCdcException` with `code`, `retriable`, `operatorAction` and a link to the runbook page.

| Class | Typical cause | Behaviour |
|---|---|---|
| `TransientDatabaseException` | ORA-03113, ORA-03114, ORA-12170, ORA-12541, ORA-12514, ORA-01033, ORA-01089, ORA-00604 with network cause, IO exceptions | Retry with backoff |
| `MiningStepRetryException` | ORA-00310, ORA-00334, ORA-01289, ORA-01291 when log appears later, own cancel | Re-mine same range |
| `OracleCdcGapException` | Sequence gap | Stop |
| `OracleCdcPurgedException` | Needed log deleted | Stop with resnapshot guidance |
| `DecodeException` | Unparseable `SQL_REDO`, unknown column, STATUS 2 or 3 | Stop or DLQ per CORE-MINE-10 |
| `OracleCdcCorruptionException` | `CORRUPTED_BLOCKS`, `MISSING_SCN` | Stop |
| `BufferExhaustedException` | Spill cap | Stop |
| `JournalCorruptionException` | Missing journal chunk | Stop |
| `TopologyException` | Unsupported topology change | Stop |
| `PrivilegeException` | ORA-01031, ORA-00942 on required views | Stop at validation |

`cdc.retry.extra.error.codes` adds ORA codes to the transient list. No configuration makes a stop condition continue.

### 4.8 Test support (CORE-TEST)

| ID | Requirement |
|---|---|
| CORE-TEST-1 | `FakeLogMiner`: an in-memory implementation of the mining interface that replays scripted `V$LOGMNR_CONTENTS` rows, used by unit tests for buffer, rollback, position and journal logic. |
| CORE-TEST-2 | `FaultyJdbc`: a JDBC proxy (test-jar) that injects a given `SQLException` with a given ORA code at a chosen call (connect, execute, the Nth `ResultSet.next`, commit), used by integration tests against real Oracle. |
| CORE-TEST-3 | `logminer-columns.md` and `operation-codes.md` generated by CORE-REF tasks and checked by a CI test against each Oracle version in the matrix. |

## 5. Configuration owned by this module

These properties are surfaced by PRD-01 with the `cdc.` prefix.

| Property | Type | Default | Description |
|---|---|---|---|
| `cdc.database.host` | string | none | Host (alternative to URL) |
| `cdc.database.port` | int | 1521 | Port |
| `cdc.database.service` | string | none | Service name |
| `cdc.database.sid` | string | none | SID |
| `cdc.database.url` | string | none | Full JDBC URL; overrides host, port, service, sid |
| `cdc.database.user` | string | none | Mining user (common user in CDB) |
| `cdc.database.password` | password | none | Password |
| `cdc.database.wallet.location` | string | none | Wallet directory |
| `cdc.database.tls.truststore.location`, `.password`, `.type` | string | none | TLS trust |
| `cdc.database.kerberos.ccache` | string | none | Kerberos credential cache (Phase 2) |
| `cdc.database.connection.properties` | string | none | Extra driver properties, `k=v;k=v` |
| `cdc.database.idle.timeout.ms` | long | 300000 | Network idle guard |
| `cdc.database.pdbs` | list | empty | PDBs to capture; empty for non-CDB |
| `cdc.database.fan.enabled` | boolean | false | RAC Fast Application Notification |
| `cdc.capture.mode` | enum | `online` | `online` or `archive_only` |
| `cdc.archive.destination` | string | auto | Archive destination name |
| `cdc.mining.target.latency.ms` | long | 2000 | Latency goal for the adaptive window |
| `cdc.mining.max.logs.per.step` | int | 8 | Upper bound for catch-up windows |
| `cdc.mining.fetch.size` | int | 10000 | JDBC fetch size |
| `cdc.mining.query.timeout.ms` | long | 600000 | Per-step timeout before halving |
| `cdc.mining.session.max.age.ms` | long | 3600000 | Session recycle interval |
| `cdc.mining.decode.threads` | int | auto | Decode threads |
| `cdc.mining.inlist.max` | int | 1000 | Switch to temporary table join above this |
| `cdc.mining.catchup.threshold.ms` | long | 300000 | Lag that enables parallel catch-up (Phase 2) |
| `cdc.mining.catchup.parallelism` | int | 2 | Catch-up sessions (Phase 2) |
| `cdc.rac.safety.lag.ms` | long | 3000 on RAC, 0 otherwise | Hold-back from cluster SCN on RAC |
| `cdc.buffer.memory.max.bytes` | long | 268435456 | Heap budget |
| `cdc.buffer.spill.dir` | string | auto | Spill directory |
| `cdc.buffer.spill.max.bytes` | long | 10737418240 | Spill cap |
| `cdc.txjournal.topic` | string | `${topic.prefix}.cdc.txjournal` | Journal topic (compacted) |
| `cdc.txjournal.threshold.ms` | long | 300000 | Age before journaling |
| `cdc.txjournal.threshold.events` | long | 100000 | Size before journaling |
| `cdc.transaction.max.age.ms` | long | -1 | Long transaction limit |
| `cdc.transaction.max.age.action` | enum | `fail` | `fail` or `discard` |
| `cdc.transaction.orphan.check.interval.ms` | long | 300000 | Orphan check interval |
| `cdc.transaction.orphan.action` | enum | `release` | `release` or `fail` |
| `cdc.on.decode.error` | enum | `fail` | `fail` or `dlq` (DML only) |
| `cdc.retry.max.time.ms` | long | 86400000 | Retry budget |
| `cdc.retry.extra.error.codes` | list | empty | Extra transient ORA codes |
| `cdc.log.sensitive.data` | boolean | false | Allow row values in logs |

## 6. Non-functional requirements

- **Correctness:** zero lost or duplicated committed changes under the fault suite in `testing_strategy.md` (duplicates allowed only in at-least-once mode, and then only for the last partially acknowledged transaction).
- **Performance targets (to validate in Phase 0, then publish):** sustain 20,000 captured row changes per second on a single-instance reference database with p99 end-to-end latency under five seconds at steady state; catch up a one-hour backlog in under 20 minutes with parallel catch-up.
- **Memory:** heap use within `cdc.buffer.memory.max.bytes` plus 256 MB overhead under any transaction size.
- **Source impact:** mining session CPU and PGA reported; no writes to the source database by default.
- **Security:** no row values in logs unless `cdc.log.sensitive.data=true`; passwords only through Connect config providers or `password` type.

## 7. Acceptance criteria

- [ ] CORE-REF-1 and CORE-REF-2 documents produced and checked in CI for 23ai and 26ai, and for 19c and 21c in the extended lab.
- [ ] Injected ORA-00310 at the 1st, middle and last `ResultSet.next` of a step results in no missing rows and no duplicates.
- [ ] A transaction open for 48 hours with archive log deletion older than 24 hours does not stop the connector (journaled) and commits correctly.
- [ ] A 5 million row single transaction completes with heap capped at the configured budget.
- [ ] A transaction killed with `ALTER SYSTEM KILL SESSION` and an XID absent from `GV$TRANSACTION` is released by orphan detection within two intervals, with an ops event.
- [ ] Excluded user transactions do not appear in the buffer at all.
- [ ] Regex include patterns are pushed down as object IDs (verified by query plan logging).
- [ ] Deleted archive log yields `OracleCdcPurgedException` naming thread and sequence; no later start is chosen.
- [ ] Savepoint rollback tests for insert, update, delete and LOB pass.
- [ ] Position format v1 readable by v1.1; v1.1 positions readable by v1.0 (downgrade).
- [ ] Every Debezium Oracle issue labelled data loss in the regression corpus has a passing test (testing strategy section 5).
