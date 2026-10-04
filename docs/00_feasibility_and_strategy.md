# OSO CDC Connector for Oracle Database: Feasibility, Clean-Room and Strategy Report

**Status:** Draft for approval, 4 October 2026
**Owner:** Sion Smith (OSO)
**Repository (proposed):** `osodevops/kafka-connect-oracle-cdc`
**Licence:** Apache-2.0
**Related:** [kafka-connect-salesforce-oss](https://github.com/osodevops/kafka-connect-salesforce-oss) (house style, release model and support model reused)

---

## 1. Executive summary

Oracle CDC is the highest-demand connector in our Semrush ranking (about 1,680 monthly Kafka-intent searches across the US, UK and DE, more than twice Salesforce). The two existing options both leave a gap:

- **Confluent Oracle CDC Source** is a premium, closed-source subscription with documented limits: at-least-once delivery, snapshots that restart from zero, no Autonomous Database, no standby capture, one PDB per connector, a long list of unsupported DDL, and a redo topic that writes every captured row to Kafka twice ([Confluent overview](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/overview.html)).
- **Debezium Oracle** is Apache-2.0 and improving fast, but users keep reporting silent data loss, ghost transactions that pin offsets until archive logs are purged, unpredictable lag, out-of-memory failures on large transactions, ORA-01555 on large snapshots, fragile schema history, and heavy DBA dependency. Four data-loss issues are open as of this week. Full evidence is in `research/debezium_oracle_pain_points.md`.

**Recommendation: build.** Build an Oracle-only, Apache-2.0 Kafka Connect source on LogMiner, with five design choices aimed at the complaints:

1. **No silent loss.** One capture path, typed stop conditions, offsets that only encode what was delivered, Kafka exactly-once with Oracle transaction boundaries, and every known Debezium Oracle loss bug as a regression test.
2. **Durable transaction journal.** Long-running transactions are journaled to a compacted Kafka topic, so the restart position no longer has to stay pinned to the oldest open transaction. This removes the main path to "SCN no longer available".
3. **Bounded memory without cache products.** In-heap buffer with automatic spill to local disk, sized by one property.
4. **SCN-anchored chunked snapshots.** Short flashback reads per chunk, resumable, read-only, running alongside streaming. No ORA-01555 on large tables, no restart from zero.
5. **Operability built in.** `oracle-cdc-doctor` preflight and redo profiler, one latency target instead of tuning knobs, offsets managed through the Connect REST API, a Grafana dashboard and a supported product from OSO.

Phase 1 (1.0 GA) is realistic in about four to five months for two senior engineers with agent assistance, because the LogMiner surface is well documented and Debezium's public issue history gives us a ready-made failure catalogue. Qualification on RAC, 19c and managed platforms is the long pole, not code.

## 2. Naming and trademark decision

Oracle's trademark guidelines say: "Do not use Oracle trademarks or potentially confusing variations as all or part of your company, product or service names", with the example "XYZ for Oracle database" not "OraXYZ or XYZ Oracle", and "Do not use Oracle trademarks or potentially confusing variations in your Internet domain name" ([Oracle trademarks](https://www.oracle.com/legal/trademarks/)). Oracle wins domain transfers even against sites with real Oracle training content when they do not disclose the lack of affiliation (WIPO D2022-3022, `oracleappstechnical.com`). Repository and package names that include "oracle" are the ecosystem norm (Confluent `kafka-connect-oracle-cdc`, Debezium `debezium-connector-oracle`, A2 `solutions.a2.oracle`), and a GitHub repository ranks on page one for "oracle cdc". Full evidence and Semrush data: `research/naming_and_seo.md`.

| Item | Decision |
|---|---|
| Product name | OSO CDC Connector for Oracle Database |
| Short name | OSO CDC Connector |
| Website | `kafkacdcconnector.com`, one domain for this connector, matching the `salesforcekafkaconnector.com` pattern |
| Repository | `osodevops/kafka-connect-oracle-cdc` (descriptive; if Oracle objects, rename to `kafka-connect-oso-cdc`, and GitHub redirects old URLs) |
| GitHub description and topics | "Oracle Database CDC source connector for Apache Kafka Connect"; topics `oracle`, `oracle-cdc`, `logminer`, `kafka-connect`, `debezium-alternative` |
| Maven group and artefacts | `sh.oso`, `oracle-cdc-core`, `kafka-connect-oracle-cdc` (plugin), `oracle-cdc-doctor` |
| Java package | `sh.oso.connect.oracle` |
| Connector class | `sh.oso.connect.oracle.OracleCdcSourceConnector` |
| Config prefix | `cdc.*` |
| Domain rules | Never a domain containing "oracle" or "logminer" (both Oracle trademarks). "kafka" is acceptable in a domain with extra words for a directly related product, per the [ASF domain policy](https://www.apache.org/foundation/marks/domains.html), but not in the product brand. Domain choice evidence: `research/naming_and_seo.md` section 6 |
| Disclaimer | "Oracle and Java are registered trademarks of Oracle and/or its affiliates. This project is not affiliated with or endorsed by Oracle." in README, site footer and Hub listing |
| Wording | Always "Oracle Database" as a product reference, with the trademark notice in README and site footer |

## 3. Legal and IP analysis

### 3.1 What we can use

| Component | Licence position | Decision |
|---|---|---|
| Oracle JDBC driver (`ojdbc11`) | Oracle Free Use Terms and Conditions; redistribution permitted unmodified ([Maven Central listing](https://central.sonatype.com/artifact/com.oracle.database.jdbc/ojdbc5)) | Declared as a Maven dependency and bundled unmodified in the plugin archive with its licence file; `NOTICE` explains the separate licence |
| LogMiner | Included in all editions ([Oracle licensing](https://docs.oracle.com/en/database/oracle/oracle-database/18/dblic/Licensing-Information.html)); Oracle describes it as a free CDC API ([Oracle blog](https://blogs.oracle.com/dataintegration/binary-log-readers)) | Our only capture API in 1.0 |
| Debezium | Apache-2.0 | Independent implementation. We read Debezium's public issues, blogs and docs freely. If we ever copy a source file, it keeps its header and is listed in `NOTICE`; the default is to write our own |
| Kafka Connect API | Apache-2.0 | Used |
| Oracle Database Free container images | Free to use for development and testing under Oracle's terms; build files by gvenzl are Apache-2.0 ([gvenzl/oci-oracle-free](https://github.com/gvenzl/oci-oracle-free)) | CI only, never redistributed by us |
| `python-oracledb` | Apache-2.0 or UPL | Used by migration and verification tools |

### 3.2 What we do not use

| Component | Reason |
|---|---|
| Confluent Oracle CDC code, binaries, docs prose | Proprietary. Clean-room: we use only the public property names and documented behaviour for parity and migration (`research/confluent_oracle_connectors_detail.md`). Engineers do not install or decompile the Confluent connector; the translator is built from documentation only |
| Binary redo parsing | Oracle states third-party binary log readers reverse-engineer an undocumented format that can change in any patch, are unsupported, and may breach the Oracle Master Agreement and licence terms ([Oracle blog](https://blogs.oracle.com/dataintegration/binary-log-readers)). We will not read redo files directly |
| OpenLogReplicator | GPL-3.0 ([OpenLogReplicator](https://github.com/bersler/OpenLogReplicator)) and a binary reader; both reasons above apply |
| XStream in 1.0 | Requires a GoldenGate licence ([XStream guide](https://docs.oracle.com/database/121/XSTRM/xstrm_intro.htm)); Confluent covers its customers through its own Oracle agreement ([Confluent XStream](https://docs.confluent.io/kafka-connectors/oracle-xstream-cdc-source/current/overview.html)) and we have none. Phase 3 adds an XStream adapter for customers who already own GoldenGate licences |

### 3.3 Residual legal and positioning risks

- **Oracle's 19c LogMiner wording.** The 19c guide says LogMiner "is not intended to be used for any third party replication of data in a production environment" ([19c LogMiner](https://docs.oracle.com/en/database/oracle/oracle-database/19/sutil/oracle-logminer-utility.html)). This is guidance about support, not a licence restriction, and Oracle's own 2025 blog endorses LogMiner for CDC. We document it plainly on the enterprise support page: OSO supports the connector, Oracle supports the database.
- **Trademark.** Mitigated by section 2.

## 4. Technical stack

| Item | Choice |
|---|---|
| Language and build | Java 17 baseline, tested on 17 and 21; Maven multi-module, as in the Salesforce repo |
| Kafka Connect | Built against Apache Kafka 3.9 API; supported runtimes Kafka 3.6 and later (offsets REST API from KIP-875) |
| JDBC | `ojdbc11` 23.x line (one driver for 19c to 26ai; Debezium moved to a single 23.26 driver in 3.6, see [Debezium 3.6 release](https://debezium.io/blog/2026/07/01/debezium-3-6-final-release/)) |
| Spill store | Own append-only segment files (no native libraries) |
| Metrics | JMX via Kafka Connect metrics plus Micrometer bridge for Prometheus |
| CLI | `oracle-cdc-doctor` as a self-contained Java CLI (picocli), also shipped as a container image |
| Tools | Python 3.11 for `migrate_from_confluent.py`, `migrate_from_debezium.py`, `verify_cutover.py` |
| Tests | JUnit 5, Testcontainers, Oracle Database Free 23ai and 26ai images, Kafka via Testcontainers; 19c, 21c, RAC and Data Guard on the local and AWS dev lab (`07_test_lab_and_budget.md`) |

## 5. Architecture

```
                         Oracle Database (primary, standby or PDB)
                                       |
                         JDBC sessions (one mining, N snapshot)
                                       |
  +------------------------------------------------------------------------+
  | Task 0 (OracleCdcSourceTask)                                                |
  |                                                                        |
  |  LogInventory -> MiningScheduler -> MiningSession(s) -> RowDecoder     |
  |        |               |                                   |           |
  |   continuity      adaptive window                  SQL_REDO parser     |
  |   per thread      target latency                   type mapping        |
  |                                                            |           |
  |                         TransactionBuffer (heap, spill, journal)       |
  |                                      |                                 |
  |  SnapshotCoordinator ---> Interleaver (commit SCN order) ---> Emitter  |
  |    chunk planner                     |                         |       |
  |    chunk readers (threads)    SchemaRegistry (per table)   Connect API |
  +------------------------------------------------------------------------+
        |                   |                    |                  |
   table topics    schema topic (compacted)  txjournal topic   ops topic
```

Key decisions:

- **One mining task per connector.** Extra LogMiner sessions over the same redo multiply source CPU and I/O; Debezium and Confluent both mine with one session. Parallelism comes from pipelining (fetch, decode and emit on separate threads), catch-up mining over adjacent SCN windows when lag is high (Phase 2), multi-PDB capture from one root session, and snapshot threads. Tasks above one are rejected at validation with a clear message.
- **Uncommitted mining with our own buffer.** `COMMITTED_DATA_ONLY` would move large-transaction memory into the database (Oracle warns it can run out of memory, see `research/logminer_reference.md`). We buffer by XID in the connector and emit in commit order.
- **Object-ID push-down.** Include and exclude patterns, including regular expressions, are resolved client-side to `DATA_OBJ#` and `OBJ#` lists and pushed into the mining query, together with DDL and transaction control rows. Re-resolved on DDL and on a timer.
- **Online catalog first, redo dictionary only for DDL windows** (`cdc.dictionary.mode=auto`), see PRD-03.
- **Exactly-once.** The connector supports KIP-618 exactly-once source and defines transaction boundaries at Oracle commit boundaries, so `read_committed` consumers see whole Oracle transactions ([KIP-618](https://cwiki.apache.org/confluence/display/KAFKA/KIP-618:%20Exactly-Once%20Support%20for%20Source%20Connectors)). Debezium also supports Connect exactly-once for Oracle with `transaction.boundary=poll` ([Debezium EOS](https://debezium.io/documentation/reference/stable/configuration/eos.html)); our addition is Oracle-transaction-aligned boundaries and offsets that cannot run ahead of delivery.

## 6. Parity matrix

| Capability | Confluent Oracle CDC | Confluent XStream | Debezium Oracle (3.7) | OSO CDC Connector |
|---|---|---|---|---|
| Licence | Proprietary premium | Proprietary premium | Apache-2.0 | Apache-2.0 |
| Commercial support | Confluent | Confluent | Red Hat (specific builds) | OSO |
| Capture API | LogMiner | XStream | LogMiner, XStream, OLR | LogMiner (XStream adapter Phase 3) |
| Delivery | At-least-once | At-least-once | At-least-once; exactly-once via Connect | Exactly-once via Connect with Oracle transaction boundaries; at-least-once otherwise |
| Transaction boundaries in Kafka | No | No | Transaction metadata topic | Kafka transaction per Oracle transaction plus optional metadata topic |
| Scaling | Redo topic then N table tasks | One task | One task | One task, pipelined; parallel catch-up; parallel snapshot threads |
| Multiple PDBs per connector | No | No | No ([dbz#478](https://github.com/debezium/dbz/issues/478)) | Yes (1.0) |
| Long transactions | Buffered in memory; warn or discard | Database side | Pin offsets; Infinispan or Ehcache optional | Heap, spill to disk, Kafka journal; offsets not pinned |
| Initial snapshot | Parallel; restarts from zero | Parallel threads; restarts from zero | Blocking or incremental (signal table) | SCN-anchored chunks; resumable; read-only; parallel threads |
| Ad-hoc table snapshot | New tables auto-detected | No | Signals | Kafka signal topic; no source writes |
| DDL | Many unsupported forms | Schema history topic | Schema history topic | Per-table schema topic; rename supported |
| LOBs | Separate LOB topics | Inline with placeholders | Inline, `lob.enabled` re-reads transactions | Inline, skip, reselect AS OF commit SCN, or Confluent-style LOB topics |
| RAC | Yes | Yes | Yes | Yes (qualified in Phase 2) |
| Standby capture | No | Downstream capture | Archive-log-only physical standby | Archive-log-only standby (Phase 2) |
| Autonomous Database | No | No | Not documented | Per-PDB mining (Phase 3, gated) |
| RDS non-CDB | Yes | 19c | Yes | Yes (Phase 2) |
| RDS CDB | No | No | No ([dbz#1925](https://github.com/debezium/dbz/issues/1925)) | Per-PDB mining (Phase 3, gated) |
| Preflight checker | Validation only | Validation only | No | `oracle-cdc-doctor` with DBA script and redo profiler |
| Offsets tooling | Connect REST | Connect REST | Manual `kafkacat` in docs | CLI over Connect REST API |
| Quiet database offsets | Heartbeat topic | Heartbeat | Heartbeat plus action query | Advance from mined SCN; no source writes |
| Migration from others | n/a | n/a | n/a | `migrate_from_confluent`, `migrate_from_debezium`, `verify_cutover` |

## 7. Module and document map

| Module | PRD |
|---|---|
| `oracle-cdc-core` | PRD-00 Core engine |
| `kafka-connect-oracle-cdc` | PRD-01 Source connector |
| `kafka-connect-oracle-cdc` (snapshot package) | PRD-02 Snapshots and backfill |
| `oracle-cdc-core` (schema package) | PRD-03 Schema and DDL |
| `tools/migration` | PRD-04 Migration tooling |
| `oracle-cdc-doctor`, `ops/` | PRD-05 Doctor, operations and observability |
| `e2e-tests`, `bench/` | `testing_strategy.md` |
| Repository, CI, release, site | `06_repo_scaffolding_spec.md` |
| `lab/` (local, AWS, OCI) | `07_test_lab_and_budget.md` |

## 8. Phases

| Phase | Scope | Exit criteria |
|---|---|---|
| 0 Foundations (weeks one to four) | Repo scaffolding, Oracle Free test harness with ARCHIVELOG, JDBC fault-injection layer, LogMiner column verification (CORE-REF-1, CORE-REF-2), `oracle-cdc-doctor check` v0, workload generator, baseline benchmark of Debezium on the same workload | Harness green on 23ai and 26ai; benchmark baseline published internally |
| 1 GA 1.0 (weeks five to twenty) | PRD-00 to PRD-03 for single instance non-CDB and CDB with multiple PDBs; DML, DDL auto mode, buffer with spill and journal, exactly-once, Debezium-compatible envelope, chunked snapshots, signal topic, metrics and dashboard, `oracle-cdc-doctor` full, `migrate_from_debezium`, `verify_cutover`, docs site | All acceptance criteria in PRD-00 to PRD-03; zero-loss suite green over 72-hour soak; 19c and 21c qualification in the lab |
| 2 Enterprise (weeks twenty-one to thirty-two) | RAC qualification, Confluent-compatible output and LOB topics, `migrate_from_confluent`, parallel catch-up, archive-log-only standby, LOB reselect, RDS non-CDB, Kerberos and LDAP | RAC two-node soak with node failover; Confluent migration rehearsal with evidence artefact |
| 3 Platforms (after 1.2) | Per-PDB mining for Autonomous Database and RDS CDB, XStream adapter for GoldenGate-licensed customers, 23ai and 26ai types (BOOLEAN, VECTOR, JSON) where LogMiner exposes them | Each platform qualified on a real tenancy before it is listed as supported |

## 9. Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| LogMiner throughput ceiling on very high redo rates | Medium | High | Object-ID push-down, redo profiler to remove noisy jobs (the Pepkor fix), parallel catch-up; publish honest throughput numbers from our benchmark |
| RAC SCN ordering and thread edge cases | Medium | High | Thread-aware log inventory, safety lag, two-node RAC soak with kill tests before listing RAC as supported |
| LogMiner column behaviour differs by version or RU | Medium | Medium | CORE-REF tasks, version matrix in CI, version-specific SQL in one class |
| Kafka transaction timeout on huge Oracle transactions | Medium | Medium | Documented split rule (PRD-01 SRC-EOS-4) with metric and ops event |
| Oracle wording that LogMiner is a debugging tool | Low | Medium | Clear support statement; Oracle's 2025 endorsement; capture adapter interface keeps XStream open |
| Debezium closes the gap | Medium | Medium | Our differentiation is support, migration tooling, doctor and journal design; we still win on procurement (PP-15) |
| Lab use under Oracle's development terms (no production data, no published benchmarks) | Medium | Medium | Synthetic data only; benchmark harness published instead of figures; legal view and Oracle PartnerNetwork pricing before 1.0 GA (`07_test_lab_and_budget.md` section 5) |

## 10. Verdict

Feasible and worth building now. The licence path is clean if we stay on LogMiner and avoid binary readers; demand is the highest in our portfolio; and the competitive weaknesses are well evidenced and architectural, so a new engine can address them rather than patch them. Proceed to Phase 0 once the naming in section 2 and the lab budget in `07_test_lab_and_budget.md` are approved.
