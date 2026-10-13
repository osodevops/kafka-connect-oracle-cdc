---
title: Confluent-compatible records
description: Configure flat records, understand snapshot and change events, customise metadata, publish separate LOB topics and preserve consumers during a Confluent cutover.
---

# Confluent-compatible records

The Confluent-compatible format lets OSO publish flat records shaped like Confluent's Oracle CDC
Source connector. It is intended for pipelines that already consume that format, including a
cutover that keeps table topics, LOB topics and Schema Registry subjects. Debezium-compatible
output remains the default. The new format is implemented for the next release; publication
depends on the release qualification gates.

This guide shows the format on a small orders table. The examples use synthetic data, JSON
without the converter's schema wrapper and the default operation names. SCNs, timestamps and
ROWIDs are illustrative; your database supplies the actual values.

## What was added

| Capability | What it does |
|---|---|
| Flat row format | Table columns followed by configurable metadata, without a Debezium envelope |
| Compatible keys and topics | Scalar, struct or concatenated keys; table topic names without an added prefix |
| Metadata layout | Renamed fields, nested metadata, string headers, optional commit SCN, SQL and update before-state |
| Numeric and date mappings | Confluent-style NUMBER mappings and DATE as a date or timestamp |
| Delete and key-change sequences | The observed tombstone behaviour, including the tombstone-only delete option |
| Separate LOB topics | One topic per CLOB, NCLOB or BLOB column, with complete byte values and stable keys |
| Optional truncate records | An OSO metadata-only `T` event; off by default |
| Same-topic migration | Configuration translation, stopped-source takeover and cutover verification |

The format changes how captured changes are represented. OSO still mines through LogMiner,
buffers uncommitted transactions and publishes changes only after commit. Rolled-back changes
produce no records. Snapshot rows use `R`; later inserts, updates and deletes use `I`, `U` and
`D`. Delivery offsets advance only for records Kafka Connect acknowledges. A row that produces
several records, including LOB records, counts as complete only after its final record.

See [how capture works](../concepts/how-capture-works.md),
[snapshots](../concepts/snapshots.md) and [exactly-once delivery](../concepts/exactly-once.md) for
the capture and recovery behaviour shared by both formats.

## 1. Prepare the database and configure capture

Follow [database setup](../database-setup/index.md) for ARCHIVELOG, the capture user and grants.
Flat records require complete supported row images, so enable ALL-column supplemental logging
on each captured table. Primary-key-only logging is insufficient for this format.

For this example, create the table as `APP` in `FREEPDB1`:

```sql
CREATE TABLE app.orders (
    id       NUMBER(10) PRIMARY KEY,
    status   VARCHAR2(20) NOT NULL,
    quantity NUMBER(5),
    notes    CLOB
);

ALTER TABLE app.orders ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

INSERT INTO app.orders (id, status, quantity, notes)
VALUES (1001, 'NEW', 2, 'hello');
COMMIT;
```

Save the following as `orders-flat.json`. The mining connection goes to the CDB root service;
`cdc.database.pdbs` selects the PDB. Replace the example services and capture user with your own.
Enable the worker's file config provider, then mount the secrets file at the path used below:

```properties
config.providers=file
config.providers.file.class=org.apache.kafka.common.config.provider.FileConfigProvider
```

The file contains the `password` property and stays outside version control, with restricted
access. See [the credentials guide](../operations/security.md#credentials).

```json
{
  "name": "orders-flat",
  "config": {
    "connector.class": "sh.oso.connect.oracle.OracleCdcSourceConnector",
    "tasks.max": "1",
    "cdc.database.host": "db.example.com",
    "cdc.database.port": "1521",
    "cdc.database.service": "FREE",
    "cdc.database.user": "C##CDC",
    "cdc.database.password": "${file:/opt/kafka/secrets/oracle.properties:password}",
    "cdc.database.pdbs": "FREEPDB1",
    "cdc.tables.include": "FREEPDB1\\.APP\\.ORDERS",
    "cdc.topic.prefix": "orders",
    "cdc.kafka.bootstrap.servers": "kafka:9092",
    "cdc.snapshot.mode": "initial",
    "cdc.output.format": "confluent",
    "cdc.output.confluent.numeric.mapping": "precision_only",
    "cdc.output.confluent.commit.scn.field": "commit_scn",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false"
  }
}
```

This example chooses `precision_only` so its scale-zero NUMBER columns appear as integers:
`ID` is int64 and `QUANTITY` is int32. The default mapping is `none`, which uses Connect Decimal.
For a migration, preserve the source mapping so the key and value schemas remain compatible.
The named `commit_scn` field is also an explicit option, not a default field.

Before registration, run the doctor against a private, locally resolved copy of the configuration.
The standalone CLI does not expand the worker's config-provider references; the copy used by the
doctor must have credentials available locally. Keep that copy outside version control with
restricted file permissions. Send the provider-based configuration to Kafka Connect:

```bash
java -jar oracle-cdc-doctor-cli.jar check --config /secure/orders-flat-doctor.json
curl --fail -X POST -H 'Content-Type: application/json' \
    --data @orders-flat.json http://localhost:8083/connectors
curl --fail http://localhost:8083/connectors/orders-flat/status
```

The table topic is `FREEPDB1.APP.ORDERS`. With Confluent output, the default table template is
`${pdb}.${schema}.${table}` in a CDB and `${database}.${schema}.${table}` in a non-CDB.
The required `cdc.topic.prefix=orders` still names internal topics, such as `orders.cdc.schema`
and `orders.cdc.heartbeat`, and the connector's offset partition.

Read keys and values with a Kafka console consumer:

```bash
kafka-console-consumer.sh --bootstrap-server kafka:9092 \
    --topic FREEPDB1.APP.ORDERS --from-beginning \
    --property print.key=true --property key.separator=' | '
```

## 2. Understand the records

The snapshot of the example row has key `1001` and this value:

```json
{
  "ID": 1001,
  "STATUS": "NEW",
  "QUANTITY": 2,
  "table": "FREEPDB1.APP.ORDERS",
  "scn": "104200",
  "commit_scn": "104200",
  "op_type": "R",
  "op_ts": null,
  "current_ts": "1791892800000",
  "row_id": null,
  "username": null
}
```

`NOTES` is absent because LOBs never appear in the flat table value. Separate LOB topics are
enabled later in this guide. Supported columns retain table order; key and NOT NULL columns
are required in the Connect schema, and other columns are optional.

After an insert commits, its record uses the same flat shape with `op_type=I`. For example:

```sql
INSERT INTO app.orders (id, status, quantity, notes)
VALUES (1002, 'NEW', 1, 'second order');
COMMIT;
```

Key `1002` then accompanies a value such as:

```json
{
  "ID": 1002,
  "STATUS": "NEW",
  "QUANTITY": 1,
  "table": "FREEPDB1.APP.ORDERS",
  "scn": "104210",
  "commit_scn": "104212",
  "op_type": "I",
  "op_ts": "1791892801000",
  "current_ts": "1791892801500",
  "row_id": "ROWID_2",
  "username": "APP"
}
```

| Metadata | Meaning |
|---|---|
| `table` | Fully qualified table name |
| `scn` | Individual row-change SCN, or the snapshot chunk's SCN |
| `commit_scn` | Transaction commit SCN when enabled; snapshot SCN on `R` |
| `op_type` | Operation name, configurable per operation |
| `op_ts` | Redo time as an epoch-millisecond string; null on snapshots |
| `current_ts` | Publication time as an epoch-millisecond string |
| `row_id`, `username` | Database ROWID and the user who made the change; null on snapshots |

Metadata values are strings, even SCNs and timestamps. Changes in one transaction can have
different row SCNs and the same commit SCN. The observed redo time has second resolution; the
Kafka record's CreateTime is supplied by the producer. OSO also includes `cdc.*` diagnostic
headers, such as `cdc.commit_scn`, `cdc.xid` and `cdc.event_index`.

### Updates and before-state

Ordinary updates carry the complete supported row after the change with `op_type=U`.
To include the previous row, add this setting before the first start:

```json
{
  "cdc.output.confluent.before.state.field": "before"
}
```

An update of order `1001` from `NEW` to `PROCESSING` can then produce:

```sql
UPDATE app.orders SET status = 'PROCESSING' WHERE id = 1001;
COMMIT;
```

```json
{
  "ID": 1001,
  "STATUS": "PROCESSING",
  "QUANTITY": 2,
  "table": "FREEPDB1.APP.ORDERS",
  "scn": "104221",
  "commit_scn": "104229",
  "op_type": "U",
  "op_ts": "1791892802000",
  "current_ts": "1791892802500",
  "row_id": "ROWID_1",
  "username": "APP",
  "before": {
    "ID": 1001,
    "STATUS": "NEW",
    "QUANTITY": 2
  }
}
```

The before-state field exists only on ordinary update records, including their schemas. It is
absent from snapshot, insert, delete and key-change schemas. A converter must therefore handle
both schema shapes. Before-state must have a plain field name; it cannot be nested or put in a
header.

### Deletes, tombstones and key changes

A tombstone is a Kafka record with the old key and a null value. It is distinct from a `D`
record, whose non-null value contains the row before deletion.

| Change | Default, `cdc.tombstones.on.delete=false` | With `cdc.tombstones.on.delete=true` |
|---|---|---|
| Delete order `1001` | Key `1001`, `D` value | Key `1001`, tombstone only |
| Change key from `1001` to `2001` | Old-key `D`, old-key tombstone, new-key `I` | Two old-key tombstones, new-key `I` |
| Roll back a change | No record | No record |

These are the observed Confluent-compatible sequences. The two old-key tombstones in the
enabled key-change case are intentional. The delete default differs from OSO's Debezium
default. Preserve the source setting during migration. For a new compacted table topic, decide
whether consumers process `D` themselves or need tombstones to remove keys through compaction.

## 3. Choose keys and customise metadata

For the orders table, the three key templates give:

| `cdc.output.confluent.key.template` | JSON key |
|---|---|
| `${primaryKeyStructOrValue}` (default) | `1001` |
| `${primaryKeyStruct}` | `{"ID":1001}` |
| `${primaryKey}` | `"1001"` |

For a composite key `(TENANT_ID, ID)`, the default becomes a struct such as
`{"TENANT_ID":7,"ID":1001}`. The concatenated template gives `"7_1001"`.
Numeric key schemas follow the selected numeric mapping. Keyless tables can use
`cdc.key.missing=none` and carry null keys; they cannot use separate LOB topics.

Each metadata property can name a field, a dotted nested path or `header:name`. An empty string
omits that metadata item. For example, add these properties to the connector configuration:

```json
{
  "cdc.output.confluent.scn.field": "header:row_scn",
  "cdc.output.confluent.commit.scn.field": "commit_scn",
  "cdc.output.confluent.op.type.field": "operation",
  "cdc.output.confluent.op.ts.field": "meta.changed_at",
  "cdc.output.confluent.current.ts.field": "meta.published_at",
  "cdc.output.confluent.row.id.field": "",
  "cdc.output.confluent.username.field": ""
}
```

The value then has `operation`, `commit_scn` and a nested `meta` struct. `scn` is replaced by a
string header named `row_scn`; `row_id` and `username` are omitted. A null header value is
omitted rather than written as a null header. Duplicate or conflicting field paths are refused,
including collisions with captured column names.

For the earlier insert, the value becomes:

```json
{
  "ID": 1002,
  "STATUS": "NEW",
  "QUANTITY": 1,
  "table": "FREEPDB1.APP.ORDERS",
  "commit_scn": "104212",
  "operation": "I",
  "meta": {
    "changed_at": "1791892801000",
    "published_at": "1791892801500"
  }
}
```

Its `row_scn` header has string value `"104210"`, alongside OSO's diagnostic headers.

Redo and undo are opt-in through `cdc.output.confluent.redo.field` and
`cdc.output.confluent.undo.field`. They contain LogMiner SQL text, including values. Where column
exclusions may apply, OSO suppresses that text to protect excluded data. Operation names can be
changed with the `cdc.output.confluent.op.type.*.value` properties, but must remain distinct.
The [generated configuration reference](../reference/configuration/confluent-compatible-format.md)
lists every property and its default.

## 4. Choose numeric and temporal representations

`cdc.output.confluent.numeric.mapping` controls both NUMBER columns and numeric keys:

| Mapping | Use and behaviour |
|---|---|
| `none` (default) | Exact Connect Decimal with the column's declared precision and scale |
| `precision_only` | Scale-zero NUMBER with precision below 19 becomes an integer; other NUMBER columns remain Decimal |
| `best_fit_or_decimal` | Integers for zero or negative scale when precision permits, double for positive scale or unconstrained NUMBER, Decimal for large integer fallback |
| `best_fit_or_double` | Same integer widths and positive-scale behaviour; large integer fallback is double |
| `best_fit_or_string` | Same integer widths and positive-scale behaviour; large integer fallback is a plain string |

Integer widths follow declared precision: int8 for 1 or 2, int16 for 3 or 4, int32 for 5 to 9,
int64 for 10 to 18. Negative scale does not widen them. An out-of-range integer stops with
CDC-3001. Floating-point mappings can lose precision; choose `none` when exact decimals matter.
With JSON's default decimal encoding, Decimal values appear as base64 bytes. Use the schema's
scale to interpret them; a JSON number is not interchangeable with a Connect Decimal schema.

An unconstrained `NUMBER` uses precision 38 and
`cdc.output.confluent.numeric.default.scale`, which defaults to 127. For example:

```json
{
  "cdc.output.confluent.numeric.mapping": "none",
  "cdc.output.confluent.numeric.default.scale": "2"
}
```

For an unconstrained NUMBER column, `0.12` fits this scale exactly. `0.125` needs rounding and
stops the task with typed CDC-3001 during either a snapshot or streaming. OSO never silently
rounds a Decimal to fit. This intentionally differs from Confluent's observed snapshot
HALF_EVEN rounding. Choose a scale that represents the actual data before capture or cutover.

`cdc.output.confluent.date.mapping=date` is the default: Oracle DATE becomes a Connect Date,
dropping the time of day. Set it to `timestamp` to retain the time of day in UTC milliseconds.
TIMESTAMP types also become Connect Timestamp at millisecond precision; sub-millisecond
fractions are truncated. RAW is bytes, FLOAT is double, and CHAR/NCHAR retain padding.
BINARY_FLOAT, BINARY_DOUBLE and INTERVAL columns are omitted from the flat row, as in the
recorded reference. Review omitted types before treating a topic as a complete table copy.
Live comparison used a UTC database time zone; other time zone combinations are not claimed
as verified parity.

## 5. Publish LOBs in separate topics

Add these settings before capturing the example table:

```json
{
  "cdc.lob.mode": "topic",
  "cdc.lob.topic.template": "${pdb}.${schema}.${table}.${column}",
  "cdc.lob.max.bytes": "1048576",
  "cdc.lob.oversize.action": "fail"
}
```

`NOTES` now uses topic `FREEPDB1.APP.ORDERS.NOTES`. Its snapshot record for order `1001` has:

```json
{
  "table": "FREEPDB1.APP.ORDERS",
  "column": "NOTES",
  "primary_key": 1001
}
```

That is the key. The value is bytes, not a row object. CLOB bytes use UTF-8, NCLOB uses
UTF-16BE and BLOB keeps its original bytes. With the example JSON converter, the CLOB text
`hello` is serialised as the JSON string `"aGVsbG8="`. A composite row key becomes a nested
`primary_key` struct. The LOB key remains a required struct regardless of the table key template.

| Database action | LOB-topic result |
|---|---|
| Snapshot or insert | One complete value per LOB column |
| Update a LOB | A value for each touched LOB column; a LOB-only update emits no table record |
| Update only ordinary columns | No LOB record |
| Assign NULL to a LOB | Required zero-length bytes, JSON `""` in this example |
| Delete a row | Tombstone on every LOB column topic |
| Change a row key | Tombstones under old LOB keys, complete values under the new keys |

NULL assignment and deletion therefore differ: empty bytes versus a null Kafka value. Compacted
LOB topics can retain the latest state per column key. Consumers reading table and LOB topics
must account for delivery across separate topics; use the transaction and commit metadata where
they need to correlate records.

For example, this change publishes the bytes for `updated note` only on the `NOTES` topic:

```sql
UPDATE app.orders SET notes = 'updated note' WHERE id = 1001;
COMMIT;
```

Topic mode requires a stable column key, complete supported values and the grants and undo
retention needed for AS OF commit reselect. It refuses keyless and ROWID-only LOB tables,
unavailable content, invalid text, placeholder oversize handling and unsupported XMLTYPE bytes.
Values above `cdc.lob.max.bytes` stop with CDC-3003. The template must contain `${column}`;
use your existing LOB topic names for migration. See [LOB configuration](../reference/configuration/lobs.md)
and the [LOB-too-large runbook](../operations/runbooks/lob-too-large.md).

## 6. Enable truncate records when consumers need them

Truncate emits no record by default in either format. To opt in:

```json
{
  "cdc.truncate.emit": "true"
}
```

The table topic receives a null-key, metadata-only value with `op_type=T`, for example:

```sql
TRUNCATE TABLE app.orders;
```

```json
{
  "table": "FREEPDB1.APP.ORDERS",
  "scn": "104300",
  "commit_scn": "104302",
  "op_type": "T",
  "op_ts": "1791892803000",
  "current_ts": "1791892803500",
  "row_id": null,
  "username": "APP"
}
```

There are no row columns or before-state. This is an OSO extension: no Confluent truncate
record was observed. Consumers must explicitly interpret `T`; a null-key event does not itself
remove every row key from a compacted topic. It is also not a set of per-row LOB tombstones.

## 7. Keep Avro consumers and migrate existing topics

For an existing Avro pipeline, preserve both converters, their registry settings and subject
strategies. Install converter libraries on the worker separately; the OSO plugin does not bundle
them. The flat value's Connect struct is unnamed, which Confluent's Avro converter represents as
`io.confluent.connect.avro.ConnectDefault`. An enabled update before-state adds a nested record
and a distinct value schema; registered schemas are reused when that shape recurs.

For a new Avro pipeline, a connector-level converter fragment is:

```json
{
  "key.converter": "io.confluent.connect.avro.AvroConverter",
  "key.converter.schema.registry.url": "http://schema-registry:8081",
  "value.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter.schema.registry.url": "http://schema-registry:8081"
}
```

Replace the JSON converter properties from the earlier example with these settings. Registry
authentication belongs in config providers. Matching connector-level key/value converters also
configure internal journal and schema-topic readback. If the converter is inherited from worker
settings, configure `cdc.journal.converter` and `cdc.journal.converter.*` to match before using
internal readback. See [using an Avro converter](../reference/record-formats.md#using-an-avro-converter)
for naming validation and converter setup. Preserve existing field names during a same-topic
migration; changing them changes the schema seen by consumers.

For a Confluent cutover, use the [migration guide](../migration/from-confluent.md):

1. Translate the source configuration with `migrate_from_confluent.py`. Its default
   `--format=confluent` preserves topic, key, metadata, numeric, DATE, LOB and converter settings.
   Review the generated report and resolve manual settings.
2. Check the target with `oracle-cdc-doctor`, then stop the source connector. Pausing it is
   insufficient for the takeover.
3. Use `takeover_scn.py` to validate retained redo and create the target start position.
4. Start OSO under its own connector name with `cdc.snapshot.mode=none` and the generated
   `cdc.start.scn`. The takeover permits overlap, so consumers must handle repeated changes
   idempotently. It does not promise a duplicate-free handover.
5. Run `verify_cutover.py` after catch-up. Its JSON verification needs commit metadata or the
   stopped-source baseline fence; without proof it reports INCONCLUSIVE. The separate Avro
   qualification tested unchanged readers and subject reuse.

Keep the stopped source during the rollback period and retain the shared table topics, LOB
topics and subjects. Choosing `--format=debezium` is an explicit change to new prefixed topics
and the Debezium envelope, with its own consumer migration.

## Compatibility differences and qualification

Compatibility is qualified for the recorded configurations, rather than every possible source
setting. The documented differences and input requirements are:

- A NUMBER needing Decimal rounding stops with CDC-3001 in snapshots and streaming; the source
  snapshots rounded HALF_EVEN. Integer overflow also stops typed.
- OSO retains additional `cdc.*` diagnostic headers.
- Redo and undo SQL are suppressed where column exclusions may apply.
- Requested SQL retains nine fractional timestamp digits, where Confluent 2.15.2 emitted three.
  Timestamp column values still agree at milliseconds.
- OSO removes old LOB keys and republishes values under changed row keys; Confluent 2.15.2 left
  the old composite LOB key behind.
- Out-of-row LOB inserts can carry a resolved real ROWID, or null when unresolved, rather than
  LogMiner's all-A placeholder.
- Optional `T` truncate emission is an OSO extension; the default remains off.
- Flat output requires complete supported images and declared key values, with ALL-column
  supplemental logging. Incomplete images, missing keys, required-column failures and unsupported
  key types stop with CDC-3001.
- LOB topic mode requires stable keys and complete valid bytes. Unsupported XMLTYPE bytes,
  unavailable values and oversize values are refused rather than replaced by placeholders.

The reference used Confluent 2.15.2 and Oracle Database Free 23ai. Fifteen reference profiles,
fourteen live comparison profiles across the recorded runs and reruns, and a same-topic Avro
handover establish the published specification. The Avro handover verified 24 committed rows,
zero missing rows, unchanged reader schemas, subject versions and restart readback. Live non-CDB
format parity and unsettled XMLTYPE LOB bytes are not claimed. See the
[qualification evidence](https://github.com/osodevops/kafka-connect-oracle-cdc/tree/main/docs/qualification/2026-10-13-confluent-format)
and [format reference](../reference/record-formats.md#confluent-compatible-format).

The format was independently implemented from public documentation and verified by comparing
external output on synthetic test data. Only the official, unmodified reference connector was
used in disposable lab containers under its evaluation licence. No reverse-engineered or
decompiled versions were used, and no Confluent source code was inspected or copied. The
proprietary reference connector and its bundled libraries are not shipped with OSO or run in CI.
OSO is not affiliated with or endorsed by Confluent. The release `NOTICE` records this provenance.
