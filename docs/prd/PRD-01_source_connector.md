# PRD-01: Source Connector (Kafka Connect)

**Status:** Draft for implementation
**Module:** `kafka-connect-oracle-cdc` (package `sh.oso.connect.oracle`)
**Connector class:** `sh.oso.connect.oracle.OracleCdcSourceConnector`
**Replaces:** Confluent Oracle CDC Source (`io.confluent.connect.oracle.cdc.OracleCdcSourceConnector`) and Debezium Oracle (`io.debezium.connector.oracle.OracleConnector`) for LogMiner users
**Depends on:** PRD-00 `oracle-cdc-core`, PRD-02 snapshots, PRD-03 schema
**Clean-room note:** Confluent behaviour and property names are taken from public documentation only, for parity and migration. The Debezium-compatible envelope reproduces a documented, Apache-2.0 record format so existing consumers keep working.

---

## 1. Objective

A production Kafka Connect source that captures inserts, updates, deletes, truncates and DDL from Oracle Database 19c, 21c, 23ai and 26ai via LogMiner into Kafka, with exactly-once delivery aligned to Oracle transactions, and with record formats compatible with both Debezium and Confluent consumers.

## 2. Scope

**In (1.0):** single instance, non-CDB and CDB with one or many PDBs; DML and DDL; Debezium-compatible envelope; exactly-once and at-least-once; signals via Kafka topic; heartbeats; DLQ; transaction metadata topic; ops topic; validation.

**In (Phase 2):** RAC qualified; Confluent-compatible output and LOB topics; archive-log-only standby; RDS non-CDB; Kerberos, LDAP.

**In (Phase 3):** per-PDB mining (Autonomous Database, RDS CDB); XStream adapter.

**Out:** Sink connector (the Apache-2.0 JDBC sink space is already served); binary redo reading (prohibited, see feasibility report).

## 3. User stories

- As a data engineer moving from Confluent, I want the same topic names and record shape so downstream consumers do not change.
- As a data engineer moving from Debezium, I want the Debezium envelope and a start SCN taken from my existing offsets, with no gap.
- As a consumer team, I want to read whole Oracle transactions atomically.
- As an operator, I want one connector to capture all PDBs in a CDB with one LogMiner session (PP-12).
- As an operator, I want misconfigurations rejected at validation, not discovered at runtime (PP-02).

## 4. Functional requirements

### 4.1 Lifecycle (SRC-LC)

| ID | Requirement |
|---|---|
| SRC-LC-1 | `validate()` runs the `oracle-cdc-doctor` rule set (PRD-05) in fast mode and returns field-level errors for blocking findings (privileges, ARCHIVELOG, supplemental logging, unsupported tables, names over 30 characters, tables without key when `cdc.key.missing=fail`). |
| SRC-LC-2 | `taskConfigs()` always returns one task. `tasks.max` above one is accepted with a warning (for compatibility with migrated configs) and ignored. |
| SRC-LC-3 | Startup order: load position, load schema topic, load journal, start snapshot coordinator if required, start mining. Startup must not read DDL history beyond the schema topic (PP-02, PP-10). |
| SRC-LC-4 | `stop()` closes the LogMiner session with `END_LOGMNR`, flushes spill files and returns within `cdc.shutdown.timeout.ms` (default 30000). |

### 4.2 Capture selection (SRC-SEL)

| ID | Requirement |
|---|---|
| SRC-SEL-1 | `cdc.tables.include` and `cdc.tables.exclude`: comma-separated regular expressions over `PDB.SCHEMA.TABLE` (CDB) or `SCHEMA.TABLE` (non-CDB). Case-insensitive unless `cdc.tables.case.sensitive=true`. |
| SRC-SEL-2 | `cdc.columns.exclude`: regular expressions over `PDB.SCHEMA.TABLE.COLUMN` (CDB) or `SCHEMA.TABLE.COLUMN` (non-CDB), with the case rules of SRC-SEL-1; excluded columns are dropped while decoding, before conversion, and never logged; key columns cannot be excluded (ADR-0018). |
| SRC-SEL-3 | `cdc.users.include`, `cdc.users.exclude`: Oracle user filters pushed down into the mining query (PRD-00 CORE-MINE-2). |
| SRC-SEL-4 | New tables that match include patterns are detected on CREATE TABLE DDL and on refresh; they are snapshotted (PRD-02) and then streamed with no gap, without restart. |

### 4.3 Topics and keys (SRC-TOP)

| ID | Requirement |
|---|---|
| SRC-TOP-1 | `cdc.topic.prefix` (required) and `cdc.topic.template` (default `${prefix}.${pdb}.${schema}.${table}` for CDB, `${prefix}.${schema}.${table}` otherwise). Variables: `prefix`, `pdb`, `schema`, `table`, `database`. |
| SRC-TOP-2 | Keys: `cdc.key.mode` = `primary_key` (default; falls back to the first unique index with all NOT NULL columns), `rowid`, or `none`. Per-table override `cdc.key.columns` as `SCHEMA.TABLE:COL1,COL2;...`. |
| SRC-TOP-3 | Tables with no usable key: `cdc.key.missing` = `fail` (default, at validation) or `rowid` (warns that ROWID changes on row movement) or `none`. |
| SRC-TOP-4 | Primary key changes emit a delete and tombstone for the old key followed by an insert or create for the new key, both inside the same Kafka transaction. |
| SRC-TOP-5 | `cdc.tombstones.on.delete` (default true). |
| SRC-TOP-6 | Internal topics: schema topic (PRD-03), transaction journal (PRD-00), ops topic `${prefix}.cdc.ops`, optional transaction metadata topic `${prefix}.cdc.transactions`, heartbeat topic `${prefix}.cdc.heartbeat`, signal topic `${prefix}.cdc.signals`. The connector creates internal topics with correct settings if allowed (`topic.creation.enable`), otherwise validation checks they exist with compaction where required. |

### 4.4 Record format (SRC-FMT)

| ID | Requirement |
|---|---|
| SRC-FMT-1 | `cdc.output.format=debezium` (default): value envelope with `before`, `after`, `source`, `op` (`c`, `u`, `d`, `r`, `t`), `ts_ms`, `ts_us`, `ts_ns`, optional `transaction` block. Field names, op codes, schema names and logical types follow the [Debezium Oracle connector documentation](https://debezium.io/documentation/reference/stable/connectors/oracle.html) for the selected compatibility level `cdc.output.debezium.version` (default `3`). `source` includes `version`, `connector` (`oracle`), `name`, `ts_ms`, `snapshot`, `db`, `schema`, `table`, `txId`, `scn`, `commit_scn`, `rs_id`, `ssn`, `redo_thread`, `user_name`, `row_id`, plus our additions under `source.redo` (`event_index`, `event_count`, `partial`, `reselect`). |
| SRC-FMT-2 | `cdc.output.format=confluent` (Phase 2): flat row value matching Confluent Oracle CDC: one field per column plus `table`, `scn`, `op_type` (R, I, U, D, T), `op_ts`, `current_ts`, `row_id`, `username`, and optional before-state, commit SCN, redo and undo fields, with names and op values configurable through `cdc.output.confluent.*` exactly as listed in `research/confluent_oracle_connectors_detail.md` section 1.4. |
| SRC-FMT-3 | Truncate: `op=t` record per table (Debezium format) or `op_type=T` (Confluent format); `cdc.truncate.emit` (default true). |
| SRC-FMT-4 | Headers on every record: `cdc.scn`, `cdc.commit_scn`, `cdc.xid`, `cdc.event_index`, `cdc.event_count`, `cdc.schema_version`. |
| SRC-FMT-5 | Types: `cdc.decimal.mode` = `precise` (default, Connect Decimal), `string`, `double`, `best_fit` (integers to INT8 to INT64 by precision, others Decimal); `cdc.decimal.default.scale` (default 127 for unconstrained NUMBER is reported as variable scale in Debezium format); `cdc.temporal.mode` = `adaptive` (default), `connect`, `iso_string`; `cdc.temporal.timezone` (default UTC) for DATE and TIMESTAMP; `cdc.binary.mode` = `bytes`, `base64`, `hex`. |

### 4.5 LOBs (SRC-LOB)

| ID | Requirement |
|---|---|
| SRC-LOB-1 | `cdc.lob.mode` = `skip` (default; LOB columns omitted), `inline` (assembled from redo; unchanged LOBs in updates carry `cdc.unavailable.placeholder`, default `__cdc_unavailable_value`), `reselect` (unavailable or unassembled values fetched AS OF commit SCN, PRD-00 CORE-DEC-7), or `topic` (Phase 2; Confluent-compatible LOB topics keyed by table, column and primary key, template `cdc.lob.topic.template`). |
| SRC-LOB-2 | `cdc.lob.max.bytes` (default 1048576) with `cdc.lob.oversize.action` = `fail` or `placeholder`. |
| SRC-LOB-3 | LOB handling never forces re-reading transactions from their start (contrast with Debezium `lob.enabled`, PP-03). |

### 4.6 Delivery and exactly-once (SRC-EOS)

| ID | Requirement |
|---|---|
| SRC-EOS-1 | Implement `exactlyOnceSupport()` returning SUPPORTED and `canDefineTransactionBoundaries()` returning SUPPORTED. |
| SRC-EOS-2 | With `transaction.boundary=connector` (recommended, set by the migration tools), the task commits a Kafka transaction at Oracle commit boundaries, batching several small Oracle transactions into one Kafka transaction up to `cdc.eos.batch.max.records` (default 5000) or `cdc.eos.batch.max.ms` (default 500). Oracle transactions are never split across Kafka transactions except under SRC-EOS-4. |
| SRC-EOS-3 | Offsets for each Kafka transaction carry the position after the last Oracle transaction in it, so `event_index` is always zero in exactly-once mode. |
| SRC-EOS-4 | An Oracle transaction larger than `cdc.eos.split.max.records` (default 500000) is split into consecutive Kafka transactions, each with offsets at an exact event index; an ops event is emitted and `source.cdc.split=true` is set. This keeps within Kafka's `transaction.max.timeout.ms`. Amended by ADR-0007 (P1-12): the split mark is the header `cdc.split=true`. |
| SRC-EOS-5 | At-least-once mode (Connect exactly-once disabled): positions from PRD-00 CORE-POS-3; duplicates on restart are limited to the in-flight Oracle transaction. |
| SRC-EOS-6 | Transaction metadata topic (optional, `cdc.transactions.topic.enabled`, default false): BEGIN and END records with XID, commit SCN, event counts per table, matching Debezium's transaction metadata shape. |

### 4.7 Heartbeats and quiet databases (SRC-HB)

| ID | Requirement |
|---|---|
| SRC-HB-1 | `cdc.heartbeat.interval.ms` (default 10000). Heartbeat records carry the current position so Connect commits offsets on quiet databases, without writing to the source (PRD-00 CORE-POS-5). There is no heartbeat action query. |

### 4.8 Signals (SRC-SIG)

| ID | Requirement |
|---|---|
| SRC-SIG-1 | Signal topic accepts JSON commands keyed by connector name: `snapshot` (tables, optional predicate), `snapshot-stop`, `snapshot-pause`, `snapshot-resume`, `refresh-tables`, `log-state` (dump buffer and position to the ops topic). |
| SRC-SIG-2 | Signals are read-only for the source database and work in archive-only mode (PP-09, [dbz#2605](https://github.com/debezium/dbz/issues/2605)). |
| SRC-SIG-3 | Every signal produces an acknowledgement event on the ops topic with outcome. |

### 4.9 Ops topic (SRC-OPS)

Structured JSON events: `startup`, `position-committed` (sampled), `log-switch-detected`, `thread-state-changed`, `ddl-applied`, `table-added`, `table-removed`, `transaction-journaled`, `transaction-discarded`, `transaction-orphan-released`, `transaction-split`, `snapshot-chunk-done`, `snapshot-complete`, `decode-error-dlq`, `stop` (with exception class and runbook link). Schema documented and versioned.

### 4.10 Errors and DLQ (SRC-ERR)

| ID | Requirement |
|---|---|
| SRC-ERR-1 | Stop conditions from PRD-00 CORE-ERR fail the task with the typed message; the Connect status shows `operatorAction` text. |
| SRC-ERR-2 | `cdc.dlq.topic` used only when `cdc.on.decode.error=dlq`; records contain raw `SQL_REDO`, SCN, XID, table, schema version and exception. |
| SRC-ERR-3 | Converter or serialisation errors use the standard Connect `errors.tolerance` behaviour; the documentation recommends `none` for CDC. |

## 5. Configuration (connector-level, in addition to PRD-00 section 5)

| Property | Type | Default | Description |
|---|---|---|---|
| `cdc.topic.prefix` | string | required | Logical name and topic prefix |
| `cdc.topic.template` | string | see SRC-TOP-1 | Table topic template |
| `cdc.tables.include` | list | required | Include regexes |
| `cdc.tables.exclude` | list | empty | Exclude regexes |
| `cdc.tables.case.sensitive` | boolean | false | Regex case sensitivity |
| `cdc.tables.refresh.interval.ms` | long | 300000 | Object-ID refresh |
| `cdc.columns.exclude` | list | empty | Excluded columns |
| `cdc.users.include` | list | empty | Only these Oracle users |
| `cdc.users.exclude` | list | empty | Exclude these Oracle users |
| `cdc.key.mode` | enum | `primary_key` | `primary_key`, `rowid`, `none` |
| `cdc.key.columns` | string | empty | Per-table key overrides |
| `cdc.key.missing` | enum | `fail` | `fail`, `rowid`, `none` |
| `cdc.tombstones.on.delete` | boolean | true | Tombstones |
| `cdc.truncate.emit` | boolean | true | Truncate events |
| `cdc.output.format` | enum | `debezium` | `debezium` or `confluent` (Phase 2) |
| `cdc.output.debezium.version` | int | 3 | Envelope compatibility level |
| `cdc.output.confluent.*` | various | Confluent defaults | Field names and op values (Phase 2) |
| `cdc.decimal.mode` | enum | `precise` | Numeric mapping |
| `cdc.decimal.default.scale` | int | 127 | Scale for unconstrained NUMBER in Confluent format |
| `cdc.temporal.mode` | enum | `adaptive` | Temporal mapping |
| `cdc.temporal.timezone` | string | UTC | DATE and TIMESTAMP zone |
| `cdc.temporal.date.mode` | enum | `timestamp` | `date` or `timestamp` for DATE columns |
| `cdc.binary.mode` | enum | `bytes` | Binary mapping |
| `cdc.lob.mode` | enum | `skip` | LOB handling |
| `cdc.lob.max.bytes` | long | 1048576 | LOB size limit |
| `cdc.lob.oversize.action` | enum | `fail` | `fail` or `placeholder` |
| `cdc.lob.topic.template` | string | `${prefix}.${schema}.${table}.${column}` | LOB topic (Phase 2) |
| `cdc.unavailable.placeholder` | string | `__cdc_unavailable_value` | Placeholder |
| `cdc.eos.batch.max.records` | int | 5000 | Kafka transaction batching |
| `cdc.eos.batch.max.ms` | long | 500 | Kafka transaction batching |
| `cdc.eos.split.max.records` | int | 500000 | Split threshold |
| `cdc.transactions.topic.enabled` | boolean | false | Transaction metadata topic |
| `cdc.heartbeat.interval.ms` | long | 10000 | Heartbeats |
| `cdc.signal.topic` | string | `${prefix}.cdc.signals` | Signal topic |
| `cdc.ops.topic` | string | `${prefix}.cdc.ops` | Ops topic |
| `cdc.dlq.topic` | string | `${prefix}.cdc.dlq` | Decode DLQ |
| `cdc.poll.max.records` | int | 2000 | Records per poll |
| `cdc.poll.linger.ms` | long | 100 | Wait for more records |
| `cdc.shutdown.timeout.ms` | long | 30000 | Stop timeout |
| `cdc.start.scn` | long | none | Start position for migrations (PRD-04); only honoured when no offset exists |

## 6. Non-functional requirements

- End-to-end latency and throughput targets in PRD-00 section 6.
- Startup to first streamed record under 60 seconds for 1,000 captured tables (schema topic load, no DDL history replay).
- Compatible with Connect running on Kafka 3.6 and later, Strimzi, Confluent Platform 7.6 and later, and Amazon MSK Connect (exactly-once availability depends on the runtime; documented per platform).

## 7. Acceptance criteria

- [ ] A Debezium consumer test suite (Avro and JSON converters) reads our `debezium` format output without change for every supported type.
- [ ] With `exactly.once.support=required` and `transaction.boundary=connector`, killing the worker at random points over 1,000 runs yields zero duplicates and zero losses (testing strategy section 4).
- [ ] A `read_committed` consumer never observes a partial Oracle transaction (except split transactions, which are flagged).
- [ ] One connector captures three PDBs with one LogMiner session, routing to per-PDB topics.
- [ ] A new table matching include patterns is captured without restart or gap.
- [ ] Quiet database for 48 hours: offsets advance, no ORA-01291 on restart, no writes to source.
- [ ] Validation rejects: missing ARCHIVELOG, missing supplemental logging on a captured table, a captured table with an identity column, a captured name over 30 characters, missing privileges.
- [ ] Signal-driven snapshot of one table works in archive-only mode with no source writes.
- [ ] Phase 2: `confluent` format output is byte-identical (JSON converter) to recorded Confluent output fixtures derived from Confluent's documented examples.

## Appendix A: Debezium property mapping (for `migrate_from_debezium`)

| Debezium property | OSO CDC property or action |
|---|---|
| `database.hostname`, `database.port`, `database.user`, `database.password`, `database.url` | `cdc.database.*` |
| `database.dbname` | `cdc.database.service` (or sid) |
| `database.pdb.name` | `cdc.database.pdbs` |
| `topic.prefix` | `cdc.topic.prefix` |
| `table.include.list`, `table.exclude.list`, `schema.include.list`, `schema.exclude.list` | `cdc.tables.include`, `cdc.tables.exclude` (rewritten to FQN regex) |
| `column.exclude.list` | `cdc.columns.exclude` |
| `log.mining.username.exclude.list`, `log.mining.username.include.list` | `cdc.users.exclude`, `cdc.users.include` |
| `snapshot.mode` | `cdc.snapshot.mode` (mapping table in PRD-04) |
| `snapshot.max.threads` | `cdc.snapshot.threads` |
| `log.mining.strategy`, `log.mining.batch.size.*`, `log.mining.sleep.time.*`, `log.mining.query.filter.mode`, `log.mining.buffer.type`, `log.mining.buffer.*` | Dropped; replaced by adaptive window and spill |
| `log.mining.transaction.retention.ms` | `cdc.transaction.max.age.ms` with action `discard` |
| `log.mining.archive.log.only.mode` | `cdc.capture.mode=archive_only` |
| `log.mining.archive.destination.name`, `archive.destination.name` | `cdc.archive.destination` |
| `lob.enabled` | `cdc.lob.mode=inline` |
| `unavailable.value.placeholder` | `cdc.unavailable.placeholder` |
| `decimal.handling.mode`, `time.precision.mode`, `binary.handling.mode` | `cdc.decimal.mode`, `cdc.temporal.mode`, `cdc.binary.mode` |
| `tombstones.on.delete` | `cdc.tombstones.on.delete` |
| `heartbeat.interval.ms` | `cdc.heartbeat.interval.ms` |
| `heartbeat.action.query` | Dropped (not needed) |
| `schema.history.internal.*` | Dropped; schema topic created (PRD-03) |
| `signal.data.collection` | Dropped; signal topic used |
| `rac.nodes` | Dropped; threads detected |
| `database.connection.adapter` | Must be `logminer` or `logminer_unbuffered`; other adapters are reported as manual follow-ups |
| `event.processing.failure.handling.mode` | `cdc.on.decode.error` (`fail` or `dlq`; `skip` and `warn` map to `dlq` with a manual follow-up note) |

## Appendix B: Confluent property mapping

See `research/confluent_oracle_connectors_detail.md` section 1.4; PRD-04 owns the final translator table.
