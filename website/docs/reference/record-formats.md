---
title: Record formats
description: The Debezium-compatible change records the connector writes, field by field, with headers, snapshot markers and type mapping.
---

# Record formats

The connector writes Debezium-compatible change records: the envelope with `before`, `after`,
`source` and `op`, the same source schema name and the same semantic type names as Debezium's
Oracle connector, so consumers and sink connectors written for that format read these records.
This is the only format in this release (`cdc.output.format=debezium`). A format compatible with
Confluent's Oracle CDC Source connector is not built yet.

How records are serialised (JSON, Avro, Protobuf) is up to the worker's `key.converter` and
`value.converter`, as for any source connector.

## Topics

Each captured table has its own topic, named by `cdc.topic.template`. The default is
`${prefix}.${pdb}.${schema}.${table}` in a container database and `${prefix}.${schema}.${table}`
otherwise, where the prefix is `cdc.topic.prefix`. Characters Kafka does not allow in topic names
become underscores. For example, table `APP.ORDERS` in `FREEPDB1` with prefix `cdc` goes to
`cdc.FREEPDB1.APP.ORDERS`.

## Key

The record key is a struct named `<prefix>.<pdb>.<schema>.<table>.Key` (without the PDB part in a
non-container database) holding the key columns:

- the columns named for the table in `cdc.key.columns`, when set;
- otherwise the primary key;
- otherwise the columns of a unique index whose columns are all NOT NULL;
- otherwise, depending on `cdc.key.missing`: the task refuses the table (`fail`, the default), a
  struct with a single `ROWID` string field (`rowid`; a row that moves gets a new key), or no key
  at all (`none`).

## Value

The value schema is `<prefix>.<pdb>.<schema>.<table>.Envelope`, version 1:

| Field | Type | Content |
|---|---|---|
| `before` | struct `<prefix>.<pdb>.<schema>.<table>.Value`, optional | The row before the change; null for `c` and `r` |
| `after` | the same struct, optional | The row after the change; null for `d` |
| `source` | struct `io.debezium.connector.oracle.Source` | Where the change came from, below |
| `op` | string | `c`, `u`, `d` or `r`, below |
| `ts_ms` | int64, optional | The Oracle transaction's commit time in milliseconds since the epoch, or 0 when the redo carries none; for a snapshot record, the time its chunk was read |
| `ts_us`, `ts_ns` | int64, optional | The same time in microseconds and nanoseconds |
| `transaction` | struct `event.block`, optional | `id` (the transaction id), `total_order` (the change's position in the transaction, counting from one) and `data_collection_order` (its position among the transaction's changes to this table, counting from one); null for snapshot records |

Every field of the row struct is optional, because a before image can be partial (see
[supplemental logging](../database-setup/supplemental-logging.md)).

### Operations

| `op` | Meaning |
|---|---|
| `c` | An INSERT |
| `u` | An UPDATE that kept the key |
| `d` | A DELETE |
| `r` | A row read by a [snapshot](../concepts/snapshots.md) |

An UPDATE that changes the key is written as a `d` record for the old key, a tombstone for the old
key (when tombstones are on), and a `c` record for the new key, so compacted topics and consumers
that keep the latest record per key stay correct.

After every `d` record that has a key, a tombstone follows: a record with the same key and a null
value, so that log compaction removes the key. Set `cdc.tombstones.on.delete=false` to leave them
out.

### Source block

| Field | Type | Content |
|---|---|---|
| `version` | string | The connector version |
| `connector` | string | `oracle-cdc` |
| `name` | string | The topic prefix |
| `ts_ms` | int64 | The time of the change in the redo; for a snapshot record, the time its chunk was read |
| `snapshot` | string | `false` for a streamed change. For a snapshot record: `first` for the first record of the snapshot, `last` for the last, `true` otherwise; `incremental` for every record of a snapshot started by a signal |
| `db` | string | The PDB name in a container database, the database name otherwise |
| `sequence` | string, optional | Not used; always null |
| `schema` | string | The table owner |
| `table` | string | The table name |
| `txId` | string, optional | The Oracle transaction id, as `usn.slot.sqn`; null for snapshot records |
| `scn` | string, optional | The SCN of the change; for a snapshot record, the SCN its chunk was read at |
| `commit_scn` | string, optional | The commit SCN of the transaction; null for snapshot records |
| `rs_id`, `ssn` | string and int64, optional | The redo record address of the change; null for snapshot records |
| `redo_thread` | int32, optional | The redo thread of the commit; null for snapshot records |
| `user_name` | string, optional | The Oracle user that ran the transaction; null for snapshot records |
| `row_id` | string, optional | The ROWID of the row, when known |
| `pdb` | string, optional | The PDB name; null in a non-container database |
| `reselect` | string, optional | `failed` when `cdc.lob.mode=reselect` could not read a LOB value that the redo did not carry; null otherwise |

## Headers

Streamed change records, and the tombstones that follow deletes, carry:

| Header | Type | Content |
|---|---|---|
| `cdc.scn` | int64 | The SCN of the change |
| `cdc.commit_scn` | int64 | The commit SCN of the transaction |
| `cdc.xid` | string | The transaction id |
| `cdc.event_index` | int32 | The change's position in its transaction, counting from zero |
| `cdc.event_count` | int32 | The number of changes in the transaction |
| `cdc.schema_version` | int32 | The [schema version](../concepts/schema-and-ddl.md) the row was decoded with |
| `cdc.split` | boolean | `true` on every record of an Oracle transaction delivered in several Kafka transactions (exactly-once mode only); absent otherwise |

Snapshot records carry `cdc.scn` (the chunk's SCN), `cdc.snapshot` (`true`) and
`cdc.schema_version`.

In at-least-once mode a restart can deliver the records of the transaction that was in flight
again. `cdc.xid` and `cdc.event_index` together identify a change, so a consumer can drop the
repeats. The records of one key change (delete, tombstone and create) share an event index.

## Column types

With the defaults (`cdc.decimal.mode=precise`, `cdc.temporal.mode=adaptive`):

| Oracle type | Connect schema |
|---|---|
| VARCHAR2, NVARCHAR2, CHAR, NCHAR, LONG, ROWID, UROWID | string |
| CLOB, NCLOB, XMLTYPE | string (see [LOB columns](#lob-columns)) |
| RAW, LONG RAW | bytes |
| BLOB | bytes (see [LOB columns](#lob-columns)) |
| BINARY_FLOAT, BINARY_DOUBLE | float32, float64 |
| NUMBER(p) or NUMBER(p,0) | int8 below 3 digits, int16 below 5, int32 below 10, int64 below 19, otherwise Connect `Decimal` with scale 0 |
| NUMBER(p,s) with a positive scale | Connect `Decimal` (`org.apache.kafka.connect.data.Decimal`) with scale s |
| NUMBER without precision, FLOAT | `io.debezium.data.VariableScaleDecimal`: a struct of `scale` (int32) and `value` (bytes, the unscaled value) |
| DATE | `io.debezium.time.Timestamp`: int64 milliseconds |
| TIMESTAMP(p) | `io.debezium.time.Timestamp` (milliseconds) up to precision 3, `io.debezium.time.MicroTimestamp` (microseconds) up to 6, `io.debezium.time.NanoTimestamp` (nanoseconds) above |
| TIMESTAMP WITH TIME ZONE | `io.debezium.time.ZonedTimestamp`: ISO 8601 text with the stored offset |
| TIMESTAMP WITH LOCAL TIME ZONE | `io.debezium.time.ZonedTimestamp`: ISO 8601 text in UTC |
| INTERVAL YEAR TO MONTH, INTERVAL DAY TO SECOND | `io.debezium.time.MicroDuration`: float64 microseconds, with a month counted as 365.25 / 12 days |

DATE and TIMESTAMP values have no time zone in Oracle; they are published as if they were UTC, so
the wall-clock value is preserved.

`cdc.decimal.mode=string` publishes every NUMBER and FLOAT as plain text and `double` as float64.
`cdc.temporal.mode=iso_string` publishes DATE and TIMESTAMP as ISO 8601 local date-time text,
intervals as ISO 8601 durations, and the time zone types as plain ISO 8601 text.

Column types not in the table, such as user-defined object types, are not supported. Identity
columns and BOOLEAN, JSON, VECTOR, BFILE and nested-table columns are not supported by LogMiner on
the tested releases; `oracle-cdc-doctor` (rule DOC-5) reports them before the connector starts.

## LOB columns

`cdc.lob.mode` decides how CLOB, NCLOB and BLOB columns appear in records.

| Mode | What the records carry |
|---|---|
| `skip` (default) | No LOB fields. A statement that only changes a LOB still produces an update record, with the other columns unchanged. |
| `inline` | The value assembled from redo. CLOB and NCLOB values are strings, BLOB values bytes. A value the redo does not carry is published as `cdc.unavailable.placeholder` (default `__cdc_unavailable_value`; its UTF-8 bytes for a BLOB). |
| `reselect` | As `inline`, then every value still unavailable in an insert or update is read from the table as of the commit SCN, one query per row. When the query cannot return it, the record carries the placeholder and `source.reselect` is `failed`. |

A value is unavailable when the redo does not hold all of it:

- the before image of an update or delete (LogMiner never logs LOB columns in a WHERE clause);
- a LOB the update did not change, including the new row of a primary key change;
- a partial change to an existing large value through `DBMS_LOB.WRITE`, `WRITEAPPEND`, `ERASE` or
  `TRIM`;
- a value larger than `cdc.lob.max.bytes` when `cdc.lob.oversize.action` is `placeholder`;
- XMLTYPE columns, in this release.

A consumer keeps the previous value of a field that carries the placeholder. Values written by an
INSERT or by a statement that replaces the whole value are always available in `inline` mode.

Savepoint rollbacks are handled. When a rollback undoes `DBMS_LOB` calls on an existing row, the
redo does not show where one call ends and the next begins. The connector then publishes an update
of that row with the LOB value unavailable rather than risk dropping a change that was committed. In
`reselect` mode that update carries the committed value.

`reselect` needs `FLASHBACK ANY TABLE` (or `FLASHBACK` on the captured tables) for the connector
user. A query that reaches past a DDL on the table (ORA-01466) or past the undo retention (ORA-01555)
leaves the value unavailable.

## Not available yet

- A format compatible with Confluent's Oracle CDC Source connector, and separate LOB topics.
- Transaction metadata records (BEGIN and END per transaction): `cdc.transactions.topic.enabled`
  creates the topic but nothing is written to it yet.
- Schema change events on a topic of their own. Schema versions are kept on the internal
  [schema topic](../concepts/schema-and-ddl.md#the-schema-topic), which is not meant for consumers.
