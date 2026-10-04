# Debezium Oracle Connector: What Users Complain About and What We Do Differently

**Status:** Research, source-linked. Collected 4 October 2026.
**Purpose:** The evidence base for the product. Every pain point below carries an ID (`PP-nn`) that the PRDs reference, so each requirement can be traced back to a real user problem.

---

## 1. Context and fairness note

Debezium's Oracle connector is the de facto open-source Oracle CDC option, and it is improving quickly. The 3.7 release (29 September 2026) removed continuous mining, fixed savepoint rollback bugs that could emit rolled-back changes or silently drop valid inserts, and added XStream downstream mining ([Debezium 3.7 release](https://debezium.io/blog/2026/09/29/debezium-3-7-final-released/)). The 3.6 release replaced nine interdependent tuning properties with log-count-based mining, which the maintainers themselves described as a "combinatorial tuning problem" ([No More Tuning](https://debezium.io/blog/2026/07/06/oracle-logminer-no-more-tuning/)), and deprecated the `redo_log_catalog` strategy ([Debezium 3.6 release](https://debezium.io/blog/2026/07/01/debezium-3-6-final-release/)).

So the opportunity is not "Debezium is broken". It is that Debezium's Oracle connector is a general CDC framework adapted to Oracle, with architectural choices (single task, schema history topic, connector-side transaction buffering, many strategies and adapters) that create recurring operational and correctness failure modes. Users also cannot buy production support for it easily outside Red Hat's supported configurations ([Red Hat supported configurations](https://access.redhat.com/articles/4938181)). We build an Oracle-only engine designed around those failure modes, with support from OSO.

## 2. Pain point catalogue

### PP-01 Silent data loss bugs keep appearing

| Evidence | Detail |
|---|---|
| [dbz#2504](https://github.com/debezium/dbz/issues/2504) (open, 26 August 2026) | ORA-00310 raised during LogMiner ResultSet iteration is treated as end of batch; the skipped SCN range is never re-mined. Reporter verified the code path is identical in 3.5.1 to 3.7.0.Alpha2 and `main`. |
| [dbz#2544](https://github.com/debezium/dbz/issues/2544) (open, 2 September 2026) | Buffered LogMiner persists a transaction sequence (`txSeq`) that can exceed the events actually emitted; skip-ahead on restart then drops the difference. RAC, 3.6.1. |
| [dbz#2779](https://github.com/debezium/dbz/issues/2779) (open, 2 October 2026) | Streaming offsets omit `snapshot_pending_tx`, causing event loss after restart; a regression from an earlier offset refactor. |
| [dbz#2184](https://github.com/debezium/dbz/issues/2184) (open) | "Missing CDC events" on 3.4.0 with hybrid strategy. |
| [Debezium 3.7 release](https://debezium.io/blog/2026/09/29/debezium-3-7-final-released/) | Savepoint partial rollbacks could emit rolled-back changes or silently drop valid inserts until 3.7. |
| [Debezium 3.5 release](https://debezium.io/blog/2026/03/31/debezium-3-5-final-released/) | The new unbuffered adapter shipped with a data-loss regression (fixed in PR #7039). |
| [Debezium 3.6 CR1](https://debezium.io/blog/2026/06/24/debezium-3-6-cr1-released/) | If a DBA moves a RAC redo thread from PUBLIC to PRIVATE before Debezium consumes it, "those unconsumed changes will be skipped, which can lead to data loss"; the scenario is declared unsupported. |
| [r/dataengineering](https://www.reddit.com/r/dataengineering/comments/1f37dyp/realtime_cdc_opensource_tool/) | A team moving 6 to 7 TB a day reports the Oracle connector "has been missing some records". |

**Root cause pattern:** error paths that continue instead of stopping, offsets that encode positions which may not match what was actually delivered, and several adapters and strategies multiplying the code paths that must each be correct.

### PP-02 Configuration footguns cause loss on restart

A user lost 13 to 15 minutes of transactions on every connector restart; the cause was a snapshot mode configuration, and the restart itself took 15 minutes because the schema history refresh captured the entire database schema ([Debezium Google Group](https://groups.google.com/g/debezium/c/0RkmfQYzaAs/m/sorh6yUXBQAJ)).

### PP-03 Phantom and ghost transactions pin the offset

| Evidence | Detail |
|---|---|
| [Debezium Google Group, ORA-01013](https://groups.google.com/g/debezium/c/vuwnG1ON_7E) | The offset SCN stayed pinned for hours by transactions the team could not find in `V$TRANSACTION`; recovery required renaming the connector; it happened more than twice a week with real business impact. Default ten-minute query timeout produced ORA-01013. |
| [Debezium Google Group, performance](https://groups.google.com/g/debezium/c/ou1rZtv2XaU) | "Debezium registers an open transaction and holds it, but never receives the 'end of transaction' signal ... our DBA has confirmed there are no such transactions active". Lag grew until the required SCN was no longer in the redo logs. Problems began on 3.3.1 and appeared within days on 3.3.2 to 3.4.1. |
| [dbz#2683](https://github.com/debezium/dbz/issues/2683) (open) | A LogMiner START row with an all-zero XID creates a phantom transaction that holds back the offset; unchanged in `main`. |
| [Debezium Google Group, lag](https://groups.google.com/g/debezium/c/hQRAELw0Nrc/m/cbV4jb0xGAAJ) | `MilliSecondsBehindSource` ranged from under a minute to "hours or even day"; maintainer explains long transactions pin `OldestScnAgeInMilliseconds`, and with `lob.enabled` the connector "must always go back to the start of the transaction and re-read it". |
| [Debezium 3.5 release](https://debezium.io/blog/2026/03/31/debezium-3-5-final-released/) | Long transactions pin the low watermark; the `log.mining.window.max.ms` workaround is risky. |
| [Debezium 3.6 CR1](https://debezium.io/blog/2026/06/24/debezium-3-6-cr1-released/) | The new deferred transaction start mode avoids pinning but "there is a risk that parts of a transaction may not be fully re-mined if an event spans a window boundary". |

**Root cause pattern:** the restart position must stay at or before the start of the oldest open transaction because buffered events are held only in volatile memory. Any transaction that never resolves (phantom XID, missed commit, abandoned session) pins the position until archive logs are purged and the connector fails.

### PP-04 Missing archive logs end in a dead connector

The "requested SCN is no longer available because the redo logs have been deleted" failure follows from PP-03 ([Google Group](https://groups.google.com/g/debezium/c/ou1rZtv2XaU)). Users report "none of log files contains offset" instability ([r/apachekafka](https://www.reddit.com/r/apachekafka/comments/1jt75ui/cdc_debezium_oracle/)). ORA-01291 missing logfile errors recur across release notes ([Debezium 2.7 notes](https://debezium.io/releases/2.7/release-notes), [Debezium 3.0 notes](https://debezium.io/releases/3.0/release-notes)). Until 3.6 the error did not even say whether the log had been purged or was simply missing ([Debezium 3.6 CR1](https://debezium.io/blog/2026/06/24/debezium-3-6-cr1-released/)). Confluent's own docs note the same ORA-01291 risk on quiet databases whose offsets never advance ([Confluent best practices](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/best-practices.html)). An open feature request asks for an opt-in fail-fast policy because Debezium currently "warns that events will be lost, selects a later mining start, and continues" when redo for an open transaction is gone ([dbz#2713](https://github.com/debezium/dbz/issues/2713)).

### PP-05 Tuning is hard and latency is unpredictable

| Evidence | Detail |
|---|---|
| [Google Group, optimisation](https://groups.google.com/g/debezium/c/vUDlnwvnaXU) | Target three-minute latency; observed up to two hours. Maintainer analysis: about 20k rows per second read from LogMiner but about 1.5k per second consumed, 8.1 hours in a day waiting on `ResultSet#next`, 2 to 109 logs mined per step, 20 or more log switches an hour ("three to four times what Oracle recommends"). A user-chosen 15M batch maximum was called "not recommended under any circumstance". |
| [Google Group, large lag](https://groups.google.com/g/debezium/c/ddnKfqXwgVs) | 75 tables, 5M events a day, lag five to ten minutes and up to 200 minutes; splitting into three connectors did not help; periodic lag after log switches. |
| [No More Tuning](https://debezium.io/blog/2026/07/06/oracle-logminer-no-more-tuning/) | Maintainers acknowledge nine interacting tuning properties; replaced in 3.6. |
| [Debezium Oracle series part 3](https://debezium.io/blog/2023/06/29/debezium-oracle-series-part-3/) | Performance depends heavily on DBA-controlled redo sizing and dictionary load time. |

### PP-06 Redo from tables you do not capture still slows you down

Pepkor NexTech (12 Oracle instances, four years on Debezium) saw fetch queries peak at 95 minutes during an evening batch window. Two tables outside `table.include.list` generated about 476.8 million redo operations in two hours, from a truncate-and-reload job. Tuning batch and fetch sizes did not help; rewriting the job as a `MERGE` brought average query time down to about 2.26 minutes ([Debezium blog, Pepkor](https://debezium.io/blog/2026/04/20/oracle-cdc-replication-lag/)). The fix was organisational, and finding the offending tables required hand-written LogMiner queries.

### PP-07 Large transactions exhaust memory, and the off-heap answer is complex

A 1.7 million row update caused an out-of-memory failure; the guidance was to grow the heap gradually or configure Infinispan or Ehcache caches, each with sizing rules, a `CacheCapacityExceededException` failure mode for undersized Ehcache, and four separate caches to budget ([Google Group](https://groups.google.com/g/debezium/c/2ytNoK7uSDg)). Maintainers noted no JMX metric showed buffered events per transaction. The same pattern appears in Flink CDC, which embeds Debezium ([flink-cdc discussion](https://github.com/apache/flink-cdc/discussions/1961)). Confluent's connector also buffers in connector memory and recommends breaking up large transactions ([Confluent overview](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/overview.html)).

### PP-08 One task, no horizontal scale

Debezium's Oracle connector runs a single task and ignores higher `tasks.max`; running overlapping LogMiner sessions to scale creates duplicates ([changedatacapture.net](https://changedatacapture.net/debezium-oracle-concurrency-scaling/)). Confluent's XStream connector is also single task ([Confluent XStream overview](https://docs.confluent.io/kafka-connectors/oracle-xstream-cdc-source/current/overview.html)).

### PP-09 Snapshots fail late and restart from zero

A 160-table, roughly 200 GB initial snapshot failed with ORA-01555 (snapshot too old) and the user asked how to resume without reprocessing everything ([Google Group](https://groups.google.com/g/debezium/c/MvDxr2H-LWE)). A request to retry failed snapshot chunks instead of the whole snapshot is open ([dbz#2297](https://github.com/debezium/dbz/issues/2297)). Composite-key incremental snapshots are slow ([Debezium 3.5 release](https://debezium.io/blog/2026/03/31/debezium-3-5-final-released/)). Read-only incremental snapshots for Oracle are still a feature request ([dbz#2606](https://github.com/debezium/dbz/issues/2606)), and signalling is limited in archive-log-only mode ([dbz#2605](https://github.com/debezium/dbz/issues/2605)). Both Confluent connectors restart incomplete snapshots from the beginning ([Confluent overview](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/overview.html)).

### PP-10 Schema history and DDL handling are fragile

The schema history topic must have infinite retention and one partition; restarts can take 15 minutes while it replays ([Google Group](https://groups.google.com/g/debezium/c/0RkmfQYzaAs/m/sorh6yUXBQAJ)); file-based history can run out of memory ([dbz#91](https://github.com/debezium/dbz/issues/91)); schema history memory for many tables needed 3.6 and 3.7 fixes ([Debezium 3.6 CR1](https://debezium.io/blog/2026/06/24/debezium-3-6-cr1-released/)). With `online_catalog` the user must coordinate schema changes in lock-step, and the `hybrid` strategy cannot handle LOB, XML or JSON and fails if `lob.enabled` is set ([Red Hat Debezium Oracle FAQ](https://docs.redhat.com/ko/documentation/red_hat_build_of_debezium/3.2.7/html/debezium_user_guide/debezium-oracle-connector-frequently-asked-questions)). Hybrid also breaks with column filtering ([dbz#1599](https://github.com/debezium/dbz/issues/1599)). Unparseable DDL errors continue to be reported on 3.6.2 ([dbz#2674](https://github.com/debezium/dbz/issues/2674)).

### PP-11 Heavy DBA dependency and source impact

Enabling supplemental logging caused "a significant slowdown in application requests" and was rolled back ([Google Group](https://groups.google.com/g/debezium/c/R4YkBNgp7No)). Redo sizing, archive destinations and dictionary builds are DBA tasks the connector cannot check for you ([Debezium Oracle series part 3](https://debezium.io/blog/2023/06/29/debezium-oracle-series-part-3/)). Oracle itself notes that enabling identification-key logging on an open database invalidates DML cursors ([Oracle LogMiner 19c](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html)).

### PP-12 Platform coverage gaps

| Gap | Evidence |
|---|---|
| One connector per PDB | Capturing multiple PDBs with one connector has been open since DBZ-3666 ([dbz#478](https://github.com/debezium/dbz/issues/478)); each extra connector adds another LogMiner session. |
| Amazon RDS CDB | Not supported because RDS denies `CDB$ROOT` access ([dbz#1925](https://github.com/debezium/dbz/issues/1925)). |
| Autonomous Database | Confluent's LogMiner connector "does not work with Oracle Autonomous Databases" ([Confluent overview](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/overview.html)). |
| Standby capture | Confluent LogMiner requires the primary; Debezium LogMiner supports physical standby only in archive-log-only mode ([Debezium 3.7 release](https://debezium.io/blog/2026/09/29/debezium-3-7-final-released/)). |
| New types | Oracle 26ai BOOLEAN and VECTOR not supported in 3.5 ([Debezium 3.5 release](https://debezium.io/blog/2026/03/31/debezium-3-5-final-released/)); Red Hat's supported build excludes BOOLEAN, VECTOR, domain types and other 23ai features ([Red Hat supported configurations](https://access.redhat.com/articles/4938181)). |

### PP-13 Filters that do not filter

`log.mining.username.exclude.list` does not exclude the START, COMMIT and ROLLBACK rows of the excluded user ([dbz#24](https://github.com/debezium/dbz/issues/24)). Database-side filtering (`log.mining.query.filter.mode=in`) only works when `table.include.list` contains no regular expressions ([Google Group](https://groups.google.com/g/debezium/c/hQRAELw0Nrc/m/cbV4jb0xGAAJ)). Transaction markers cannot be filtered, and the experimental CTE query feature doubles redo scans ([Debezium CTE blog](https://debezium.io/blog/2025/08/14/oracle-new-feature-cte-query/)).

### PP-14 Day-two operations are manual

The official FAQ tells users to edit offsets with `kafkacat` by hand and warns that an AWS load balancer idle timeout of 350 seconds can hang the connector indefinitely ([Red Hat Debezium Oracle FAQ](https://docs.redhat.com/ko/documentation/red_hat_build_of_debezium/3.2.7/html/debezium_user_guide/debezium-oracle-connector-frequently-asked-questions)). Quiet tables need a heartbeat action query that writes to the source database, and offsets still fail to advance in some modes ([dbz#2781](https://github.com/debezium/dbz/issues/2781), [dbz#2475](https://github.com/debezium/dbz/issues/2475)). Self-managed Debezium generally needs Kafka, Connect and JVM expertise plus custom monitoring ([Estuary](https://estuary.dev/blog/debezium-cdc-pain-points/), [Streamkap](https://streamkap.com/resources-and-guides/self-managed-debezium-pain-points)).

### PP-15 Procurement wants a supported product

"Approvers insist that without a production license they will not allow it to run in production" ([r/bigdata](https://www.reddit.com/r/bigdata/comments/f1op2v/anyone_using_debezium_in_production/fhbu5j3/)). Red Hat supports specific Debezium builds and configurations only ([Red Hat supported configurations](https://access.redhat.com/articles/4938181)); Confluent's Oracle connectors are premium subscriptions ([Confluent Hub](https://www.confluent.io/hub/confluentinc/kafka-connect-oracle-cdc)).

### PP-16 Upgrades carry hazards

Upgrading to 3.7 requires clearing the XMLTYPE buffer cache first ([Debezium 3.7 release](https://debezium.io/blog/2026/09/29/debezium-3-7-final-released/)); strategies and modes are deprecated across releases ([Debezium 3.6 release](https://debezium.io/blog/2026/07/01/debezium-3-6-final-release/)).

## 3. Market signals worth knowing

- **Fivetran sunset LogMiner.** New Fivetran Oracle connections cannot use LogMiner; existing ones stop syncing at the end of 15 May 2026. Fivetran cites "redo-log availability constraints inherent to LogMiner" and says its Binary Log Reader supports types LogMiner does not ([Fivetran FAQ](https://fivetran.com/docs/connectors/databases/oracle/troubleshooting/logminer-migration-faq)).
- **Oracle pushes back on binary log readers.** Oracle says third-party tools that reverse-engineer redo are unsupported and may breach the Oracle Master Agreement and licence terms, and positions LogMiner as a free CDC API included with the database and used by Debezium ([Oracle blog](https://blogs.oracle.com/dataintegration/binary-log-readers)). That makes LogMiner the only legally safe free capture API for an Apache-2.0 project; see the feasibility report.
- **Confluent's XStream connector is built on Debezium** and Kafka Connect frameworks ([Confluent XStream overview](https://docs.confluent.io/kafka-connectors/oracle-xstream-cdc-source/current/overview.html)).

## 4. How we answer each pain point

| ID | Our answer | Requirements |
|---|---|---|
| PP-01 | No-silent-loss contract: every unexpected condition stops the task with a typed error; one capture path (LogMiner, uncommitted mode, our own buffer), not several strategies; every Debezium Oracle data-loss issue becomes a named regression test. | PRD-00 CORE-ERR, PRD-01 SRC-COR, testing strategy section 5 |
| PP-02 | No snapshot mode can skip data on restart; startup refuses configurations that would; startup time is bounded by captured tables, not database DDL history. | PRD-01 SRC-CFG, PRD-03 |
| PP-03 | Durable transaction journal in Kafka lets the restart position move past long-running transactions; orphan transaction detection cross-checks `GV$TRANSACTION`; zero XIDs and marker-only transactions never create buffer entries. | PRD-00 CORE-TX, PRD-01 SRC-TX |
| PP-04 | Log inventory checks continuity per RAC thread before every mining step; missing redo is always a stop with a runbook; recovery is a targeted per-table resnapshot, not a renamed connector. | PRD-00 CORE-LOG, PRD-05 |
| PP-05 | One latency target instead of tuning knobs; adaptive window with every decision exported as a metric; timeouts shrink the window instead of failing. | PRD-00 CORE-MINE, PRD-05 |
| PP-06 | Object-ID filtering in the mining query and a built-in redo profiler that names the tables and jobs generating redo. | PRD-00 CORE-MINE, PRD-05 doctor |
| PP-07 | Bounded memory with automatic spill to local disk; no cache products to size; per-transaction metrics. | PRD-00 CORE-TX |
| PP-08 | Pipelined decoding, parallel catch-up mining, multi-PDB from one session, parallel snapshot threads. Honest single-miner design, because extra LogMiner sessions multiply source load. | PRD-00, PRD-01, PRD-02 |
| PP-09 | SCN-anchored chunked snapshots: short flashback reads per chunk (no ORA-01555 on long snapshots), resumable per chunk, read-only, runs alongside streaming. | PRD-02 |
| PP-10 | Compacted per-table schema topic, rebuildable from the data dictionary; one DDL strategy (`auto`). | PRD-03 |
| PP-11 | `oracle-cdc-doctor` checks every prerequisite, estimates redo impact, and generates a reviewed SQL script for the DBA. | PRD-05 |
| PP-12 | Multi-PDB in one connector (1.0); standby archive-log mining and RDS (Phase 2); per-PDB mining for Autonomous and RDS CDB (Phase 3, qualification-gated). | PRD-01, roadmap |
| PP-13 | Filters resolve to object IDs and user IDs client-side, so regex filters still push down; excluded users' markers are dropped. | PRD-00 CORE-MINE |
| PP-14 | Offsets managed through the Connect offsets REST API with a CLI; quiet databases advance offsets without writing to the source; TCP keepalive and idle-timeout guards. | PRD-01, PRD-05 |
| PP-15 | Apache-2.0 plus OSO enterprise support subscription with published response targets. | Repo scaffolding |
| PP-16 | Versioned offset and journal formats with tested upgrade and downgrade paths; no manual cache clearing. | PRD-00, testing strategy |
