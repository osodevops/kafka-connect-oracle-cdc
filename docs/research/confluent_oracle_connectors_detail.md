# Confluent Oracle Connectors: Parity Reference

**Status:** Research, source-linked. Collected 4 October 2026.
**Clean-room note:** This file records Confluent's publicly documented behaviour and property names so that we can (a) define feature parity and (b) build the `migrate_from_confluent` config translator. We never read, decompile or copy Confluent code. Property names appear here only as migration inputs; our connector uses its own `cdc.*` namespace.

Sources: [Oracle CDC Source overview](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/overview.html), [Oracle CDC configuration reference](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/configuration-properties.html), [Oracle CDC best practices](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/best-practices.html), [Oracle CDC horizontal scaling](https://docs.confluent.io/cloud/current/connectors/cc-oracle-cdc-source/oracle-cdc-setup-includes/high-level-architecture.html), [Oracle XStream CDC overview](https://docs.confluent.io/kafka-connectors/oracle-xstream-cdc-source/current/overview.html).

---

## 1. Oracle CDC Source (LogMiner based)

### 1.1 Architecture

- Task zero mines LogMiner (`V$LOGMNR_CONTENTS`) and writes raw redo rows to a single-partition **redo log topic** (`redo.log.topic.name`, default `${connectorName}-${databaseName}-redo-log`), at-least-once.
- All tasks (including task zero) consume the redo log topic and produce **table topics** (`table.topic.name.template`, default `${fullyQualifiedTableName}`). Events are emitted after the commit record arrives.
- Optional **LOB topics** (`lob.topic.name.template`); keys are table name, column name and primary key. Large LOBs over 1 KB need `enable.large.lob.object.support=true`.
- Optional redo log **corruption topic** and **heartbeat topic**.
- Snapshot work is spread across tasks; with `snapshot.by.table.partitions=true` a table with P partitions can use up to P plus one tasks. Confluent recommends `tasks.max` of one plus the number of tables for maximum parallelism ([best practices](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/best-practices.html)).
- Tables are balanced across tasks at startup and periodically rebalanced by throughput; new and dropped tables are detected automatically ([overview](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/overview.html)).

**Implication for us:** Confluent achieves table-level parallelism by writing every captured redo row to Kafka twice (redo topic, then table topic). Mining is still single-session. We keep one mining session but avoid the double write; see PRD-01.

### 1.2 Supported platforms and limits (as documented)

| Topic | Confluent statement |
|---|---|
| Oracle versions | 19c EE, 21c EE, 23ai; support for 11g, 12c and 18c ended 30 June 2025 |
| RAC | Supported |
| CDB/PDB | One PDB per connector (`oracle.pdb.name`); all PDBs must be open READ WRITE for the dictionary |
| Autonomous Database | Does not work |
| Exadata | Not tested |
| Standby / read replicas | Not supported; must point to the primary. RDS read replica not supported (online catalog needs write access). RDS CDB not supported |
| Delivery | At-least-once; no exactly-once claim; no transaction summary records |
| Snapshot restart | Incomplete snapshots restart from the beginning |
| Unsupported types | JSON (21c), VECTOR and BOOLEAN (23ai), some PL/SQL-only types; tables with BFILE, nested tables, identity columns, temporal validity, PKREF, PKOID are ignored entirely |
| Names | Table and column names over 30 characters cannot be mined |
| DDL not supported | Add or drop constraints; drop multiple columns in one statement; add TIMESTAMP column with DEFAULT; user-defined types; rename table or column; truncate partition; modify LOB; add or drop partition; MODIFY NULL without type; GRANT; DROP TABLE CASCADE CONSTRAINTS |
| Other | DATE to epoch ignores time portion; `numeric.mapping` cannot change on a running connector; LOB tables need primary keys; ROWID-only UPDATE or DELETE cannot be rebuilt; mTLS with DB auth fails for PDBs (ORA-65053) |

### 1.3 Licensing

Premium connector under the Confluent enterprise licence with an additional subscription; 30-day trial; licence stored in `_confluent-command` ([overview](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/overview.html), [Confluent Hub](https://www.confluent.io/hub/confluentinc/kafka-connect-oracle-cdc)).

### 1.4 Complete configuration reference (migration input)

From the [configuration reference](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/configuration-properties.html). "Map" gives our target property or the translator action (PRD-04 owns the final table).

| Confluent property | Type | Default | Meaning | Map |
|---|---|---|---|---|
| `oracle.server` | string | none | Host | `cdc.database.host` |
| `oracle.port` | int | 1521 | Port | `cdc.database.port` |
| `oracle.username` | string | none | User | `cdc.database.user` |
| `oracle.password` | string | none | Password | `cdc.database.password` |
| `oracle.sid` | string | none | SID (CDB or non-CDB) | `cdc.database.sid` |
| `oracle.pdb.name` | string | none | PDB | `cdc.database.pdbs` (single value) |
| `oracle.service.name` | string | "" | Service name, preferred over SID | `cdc.database.service` |
| `key.converter`, `value.converter` | class | none | Converters | copied unchanged |
| `table.inclusion.regex` | string | none | Case-sensitive FQN regex | `cdc.tables.include` (FQN form rewritten, case sensitivity set) |
| `table.exclusion.regex` | string | blank | Exclusion regex | `cdc.tables.exclude` |
| `table.topic.name.template` | string | `${fullyQualifiedTableName}` | Table topic template | `cdc.topic.template` (variables translated) |
| `redo.log.topic.name` | string | `${connectorName}-${databaseName}-redo-log` | Redo topic | dropped; report notes redo topic consumers must move |
| `redo.log.consumer.bootstrap.servers` and `redo.log.consumer.*` (fetch.min.bytes 1, max.partition.fetch.bytes 1048576, fetch.max.bytes 52428800, max.poll.records 500, request.timeout.ms 30000, receive.buffer.bytes 65536, send.buffer.bytes 131072) | various | as listed | Redo topic consumer | dropped |
| `start.from` | string | `snapshot` | `snapshot`, `current`, `force_current`, SCN or timestamp | `cdc.snapshot.mode` plus `cdc.start.scn` |
| `key.template` | string | `${primaryKeyStructOrValue}` | Record key | `cdc.key.mode` |
| `max.batch.size` | int | 1000 | Records per poll | `cdc.poll.max.records` |
| `query.timeout.ms` | long | 300000 | Query timeout | `cdc.mining.query.timeout.ms` |
| `max.retry.time.ms` | long | 86400000 | Retry budget | `cdc.retry.max.time.ms` |
| `redo.log.poll.interval.ms` | int | 1000 | Poll interval (19c and later) | `cdc.mining.target.latency.ms` (advisory) |
| `snapshot.row.fetch.size` | int | 2000 | Snapshot fetch hint | `cdc.snapshot.fetch.size` |
| `redo.log.row.fetch.size` | int | 10 pre-19c, 1000 otherwise | Redo fetch hint | `cdc.mining.fetch.size` |
| `poll.linger.ms` | long | 5000 | Empty batch wait | `cdc.poll.linger.ms` |
| `snapshot.threads.per.task` | int | 4 | Snapshot threads | `cdc.snapshot.threads` |
| `heartbeat.interval.ms` | int | 0 | Heartbeats | `cdc.heartbeat.interval.ms` |
| `heartbeat.topic.name` | string | `${connectorName}-${databaseName}-heartbeat-topic` | Heartbeat topic | `cdc.heartbeat.topic` |
| `use.transaction.begin.for.mining.session` | boolean | true | Start mining at oldest open transaction | dropped; always on in our design |
| `log.mining.transaction.age.threshold.ms` | long | -1 | Long transaction threshold | `cdc.transaction.max.age.ms` |
| `log.mining.transaction.threshold.breached.action` | string | warn | `discard` or `warn` | `cdc.transaction.max.age.action` |
| `redo.log.corruption.topic` | string | blank | Corruption events | `cdc.ops.topic` (corruption is a stop condition; see PRD-01) |
| `behavior.on.dictionary.mismatch` | string | fail | `fail` or `log` | `cdc.on.decode.error` |
| `behavior.on.unparsable.statement` | string | fail | `fail` or `log` | `cdc.on.decode.error` |
| `oracle.dictionary.mode` | string | auto | `auto`, `online`, `redo_log` | `cdc.dictionary.mode` |
| `log.mining.archive.destination.name` | string | "" | Archive destination | `cdc.archive.destination` |
| `record.buffer.mode` | string | connector | Only `connector` supported | dropped |
| `max.batch.timeout.ms` | long | 60000 | Deprecated | dropped |
| `max.buffer.size` | int | 0 | Batch buffer | dropped |
| `lob.topic.name.template` | string | "" | LOB topic | `cdc.lob.mode=topic` plus `cdc.lob.topic.template` |
| `enable.large.lob.object.support` | boolean | false | Large LOBs | implied by `cdc.lob.mode` |
| `log.sensitive.data` | boolean | false | Log data values | `cdc.log.sensitive.data` |
| `numeric.mapping` | string | none | `none`, `best_fit_or_decimal`, `best_fit_or_double`, `best_fit_or_string`, `precision_only`, `best_fit` | `cdc.decimal.mode` |
| `numeric.default.scale` | int | 127 | Default scale | `cdc.decimal.default.scale` |
| `oracle.date.mapping` | string | date | `date` or `timestamp` | `cdc.temporal.date.mode` |
| `emit.tombstone.on.delete` | boolean | false | Tombstones | `cdc.tombstones.on.delete` |
| `oracle.fan.events.enable` | boolean | false | RAC FAN events | `cdc.database.fan.enabled` |
| `table.task.reconfig.checking.interval.ms` | long | 300000 | Table placement monitor | `cdc.tables.refresh.interval.ms` |
| `table.rps.logging.interval.ms` | long | 60000 | Per-table rate logging | dropped (metrics instead) |
| `log.mining.end.scn.deviation.ms` | long | 0 single node, 3000 RAC | Mining end SCN lag | `cdc.rac.safety.lag.ms` |
| `output.before.state.field` | string | "" | Before image field | `cdc.output.confluent.before.field` |
| `output.table.name.field` | string | table | Table field | `cdc.output.confluent.*` family |
| `output.scn.field` | string | scn | SCN field | as above |
| `output.commit.scn.field` | string | "" | Commit SCN field | as above |
| `output.op.type.field` | string | op_type | Operation field | as above |
| `output.op.ts.field` | string | op_ts | Operation timestamp | as above |
| `output.current.ts.field` | string | current_ts | Processing timestamp | as above |
| `output.row.id.field` | string | row_id | ROWID | as above |
| `output.username.field` | string | username | Oracle user | as above |
| `output.redo.field`, `output.undo.field` | string | "" | Raw redo and undo SQL | as above |
| `output.op.type.read.value`, `.insert.value`, `.update.value`, `.delete.value`, `.truncate.value` | string | R, I, U, D, T | Operation codes | as above |
| `redo.log.startup.polling.limit.ms` | long | 300000 | Startup wait | dropped |
| `snapshot.by.table.partitions` | boolean | false | Partition-level snapshot | dropped; our chunking covers it (PRD-02) |
| `oracle.validation.result.fetch.size` | int | 5000 | Validation fetch size | dropped |
| `redo.log.row.poll.fields.include`, `.exclude` | list | none | Redo topic columns | dropped |
| `redo.log.row.poll.username.include`, `.exclude` | list | none | User filters | `cdc.users.include`, `cdc.users.exclude` |
| `db.timezone` | string | UTC | Timezone for DATE and TIMESTAMP | `cdc.temporal.timezone` |
| `db.timezone.date` | string | none | Timezone for DATE | `cdc.temporal.date.timezone` |
| `oracle.supplemental.log.level` | string | full | `msl` or `full` | dropped; doctor validates per table |
| `ldap.url`, `ldap.security.principal`, `ldap.security.credentials` | string | none | LDAP naming | `cdc.database.url` with LDAP descriptor plus credentials |
| `oracle.ssl.truststore.file`, `.password` | string | "" | TLS trust store | `cdc.database.tls.truststore.*` |
| `oracle.kerberos.cache.file` | string | "" | Kerberos | `cdc.database.kerberos.ccache` |
| `retry.error.codes` | list | none | Extra retriable ORA codes | `cdc.retry.extra.error.codes` |
| `enable.metrics.collection` | boolean | false | JMX metrics | dropped; always on |
| `confluent.license`, `confluent.topic*` | various | | Licensing | dropped |
| `connection.pool.*` (initial.size 0, min.size 0, max.size 1, timeouts, validation, harvest, fast.failover) | various | | UCP pool | dropped; our pool is internal, `cdc.database.connection.properties` passes driver options |
| `redo.log.initial.delay.interval.ms` | long | 0 | Delay after snapshot | dropped |

### 1.5 Record format (needed for consumer compatibility)

Flat row values with one field per column, plus metadata fields `table`, `scn`, `op_type`, `op_ts`, `current_ts`, `row_id`, `username` by default, configurable to nested fields or headers. Operation codes R, I, U, D, T. Primary key updates emit delete, tombstone and insert ([overview](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/overview.html)). Our `cdc.output.format=confluent` reproduces this shape so consumers do not change (PRD-01 section 6).

## 2. Oracle XStream CDC Source

| Topic | Confluent statement |
|---|---|
| Technology | XStream Out: capture process, LCRs, outbound server; built on Debezium and Kafka Connect frameworks |
| Licensing | Premium connector. Confluent has an agreement with Oracle so customers do not buy extra Oracle licences; Oracle flow-down terms apply |
| Tasks | Single task; parallel snapshot threads; one outbound server per connector |
| Delivery | At-least-once, commit order |
| Schema history | Dedicated topic, one partition, infinite retention |
| LOB and XMLType | Inline in table events; unchanged LOBs use a placeholder; oversize handling `fail` or `skip`; values over 2 GiB cannot be skipped; no piecewise `DBMS_LOB` operations |
| Platforms | 19c and 21c EE and SE, Exadata, RDS non-CDB 19c; not Autonomous; capture cannot run on a standby (downstream capture instead) |
| Latency | About three seconds extra under low activity due to XStream batching, not configurable |
| Recovery | Fixed list of retriable errors, three retries, then failed |

Source: [XStream overview](https://docs.confluent.io/kafka-connectors/oracle-xstream-cdc-source/current/overview.html). XStream itself requires an Oracle GoldenGate licence for anyone without such an agreement ([Oracle XStream guide](https://docs.oracle.com/database/121/XSTRM/xstrm_intro.htm)).

## 3. Parity targets we adopt

We match every Confluent Oracle CDC capability that a user can observe, except the redo log topic itself (an internal artefact of their design). Where Confluent documents a limitation we aim to remove it: resumable snapshots, rename DDL, transaction boundaries, Autonomous and standby support (phased), and exactly-once delivery. See the parity matrix in `00_feasibility_and_strategy.md`.
