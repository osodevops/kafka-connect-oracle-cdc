# Confluent-compatible record format

Specification: Confluent's public [configuration reference](https://docs.confluent.io/kafka-connectors/oracle-cdc/current/configuration-properties.html), checked against the external output of its Oracle CDC Source connector 2.15.2 on Oracle Database Free 23ai, 13 October 2026. `ConfluentFormatRefQualIT` runs a hand-placed plugin only in disposable lab containers, refuses CI and records only this suite's test data. No proprietary code or files form part of the specification. The evaluation licence ends on 11 November 2026; runs requiring it must finish by about 4 November 2026.

Fixtures: `kafka-connect-oracle-cdc/src/test/resources/confluent-reference/`. Fifteen profiles record keys, values, headers and registered schemas. The follow-up adds numeric boundaries, NOT NULL columns, statements with distinct SCNs in one transaction, every metadata field in the body, composite LOB keys and the Avro subject version used by each record. Placeholders preserve equality between volatile values, not their numeric ordering. Diagnostics remain local and are not fixtures. This reference records facts and decisions, never timings.

## Rows, keys and schemas

| Question | Observed fact | Decision |
|---|---|---|
| Value shape | An unnamed required struct, columns in table order, metadata afterwards | Preserve that shape, including column order |
| Required columns | Primary key columns and NOT NULL non-key columns are required; nullable columns are optional | Use both key membership and dictionary nullability |
| Default metadata | Optional strings, in order: `table`, `scn`, `op_type`, `op_ts`, `current_ts`, `row_id`, `username` | Preserve defaults and order |
| All metadata | Order: table, SCN, commit SCN, operation, operation time, current time, ROWID, username, redo, undo; before-state last when present | Commit SCN immediately follows SCN |
| Qualified table | `PDB.OWNER.TABLE`, or the database name in place of the PDB on a non-CDB | Use the same namespace; table topics will use it without a prefix by default |
| Placement | A dotted path creates nested optional structs in first-member order. `header:` gives string headers; null headers are absent | Preserve nesting, header order and omission; reject name collisions |
| Single primary key | `${primaryKeyStructOrValue}` uses the column's own schema and value | Preserve the scalar key |
| Composite primary key | An unnamed required struct, key columns in key order | Preserve the struct |
| Key templates | `${primaryKeyStruct}` always gives a struct. `${primaryKey}` gives an optional string with values joined by underscores | Expose these three templates in `cdc.output.confluent.key.template` |
| Keyless table | Null key; `${primaryKey}` still supplies an optional string schema with a null payload | Preserve both the null and its schema when applicable |
| Numeric keys | Follow `numeric.mapping`, for example an `int64` scalar | Apply the column mapping to keys too |
| Avro names | Unnamed values become `io.confluent.connect.avro.ConnectDefault`; nested unnamed records become `ConnectDefault2` onwards | Keep Connect schemas unnamed; verify real converter output in the parity suite |
| Avro optionality | Optional fields have `["null", type]` and `default: null`; ADD COLUMN registers a new version | Preserve optionality and schema evolution |
| Before-state | Present only on ordinary `U` records, as an optional struct of the columns; absent from the schema on `R`, `I`, `D` and both halves of a key change | Build separate schemas with and without before-state; match the observed subject versions |

## Metadata and operations

| Question | Observed fact | Decision |
|---|---|---|
| Streaming SCN | Two inserts separated by another connection's commit have different SCNs but the same commit SCN | Use the row's `RedoRecordId.scn()`, not the transaction start SCN |
| Snapshot metadata | `R`, table snapshot SCN in both SCN fields; operation time, ROWID, username, redo and undo are null | Use the actual snapshot position and null streaming metadata |
| Operation time | Epoch milliseconds as a string, with second resolution in these LogMiner rows | Use the redo timestamp |
| Current time | Write time, as a millisecond string | Use the clock; exclude this volatile field from live parity value comparisons |
| ROWID and username | The real ROWID and database user | Never expose synthetic LOB grouping ROWIDs |
| SQL text | SQL_REDO and SQL_UNDO verbatim, with ROWID predicates and delimiters. A key change carries its UPDATE text on both records; LOB INSERT text uses EMPTY_CLOB/EMPTY_BLOB | Preserve LogMiner text when requested; suppress it whenever a column filter may touch the table |
| Insert, update, delete | `I` after image, `U` after image, `D` before image | Preserve these operation values, with configurable replacements |
| Default delete | `D` and no tombstone | Confluent tombstones off by default; Debezium default unchanged |
| Enabled tombstones | A delete emits only a tombstone, even without a key | Replace the delete record rather than append a tombstone |
| Key change | Default: old-key `D`, old-key tombstone, new-key `I`. With tombstones enabled: two old-key tombstones, then new-key `I` | Preserve both sequences |
| Rollback | No records | Publish committed changes only |
| LOB-only change | No table record, with or without separate LOB topics | Suppress its table record; a filtered transaction still needs an acknowledged position |
| Truncate | No `T` observed. The public property list names its operation value but documents no enabling property | Keep emission off by default in both formats. Any explicit OSO truncate emission is an extension whose shape must be documented |
| Kafka headers and timestamps | No default headers; Kafka CreateTime corresponds to publication | Preserve a null OSO SourceRecord timestamp so the producer supplies it. Retain OSO's additional `cdc.*` diagnostic headers and document them |

## Column mappings

| Oracle type | Observed Connect representation | Decision |
|---|---|---|
| NUMBER(p,s), mapping `none` | Decimal with `scale: s` and `connect.decimal.precision: p` | Preserve precision and scale, including negative scales |
| Unconstrained NUMBER, mapping `none` | Decimal(38,127) by default | Use `numeric.default.scale` and precision 38 |
| FLOAT(n) | `float64` | Preserve the double mapping |
| CHAR, VARCHAR2, NCHAR, NVARCHAR2 | String; CHAR and NCHAR remain blank-padded | Preserve the stored text |
| DATE, mapping `date` | Connect Date, days since epoch, time of day removed | Preserve the date mapping |
| DATE, mapping `timestamp` | Connect Timestamp, UTC milliseconds, time of day retained | Preserve this alternative |
| TIMESTAMP(0,3,6,9) | Connect Timestamp, UTC milliseconds; pre-epoch `1969-12-31 23:59:59.123456789` becomes -877 | Truncate sub-millisecond precision towards negative infinity |
| TIMESTAMP WITH TIME ZONE | Correct instant in milliseconds | Preserve the instant |
| TIMESTAMP WITH LOCAL TIME ZONE | Milliseconds in the database time zone, UTC in the fixture lab | Use the engine's decoded instant |
| RAW | Bytes | Preserve bytes |
| BINARY_FLOAT, BINARY_DOUBLE, both INTERVAL types | Absent; Confluent reports them unsupported | Leave them out of the compatible row schema |
| CLOB, NCLOB, BLOB | Absent from the table row | Publish in separate topics only when requested |
| JSON decimals | JsonConverter's default BASE64 | Fixture comparisons use the same converter defaults |
| Avro decimals | Bytes with precision, scale, `connect.parameters`, `connect.name`, version and `logicalType: decimal` | Preserve all attributes, including the unusual unconstrained and negative-scale schemas accepted by Confluent's registry |

### Numeric mappings and overflow

For each `best_fit_*` mapping, zero or negative scale selects integers by the **declared precision**, without widening for negative scale:

| Precision | Connect type |
|---|---|
| 1 or 2 | int8 |
| 3 or 4 | int16 |
| 5 to 9 | int32 |
| 10 to 18 | int64 |
| 19 or above | Decimal, double or string according to the mapping suffix |

Positive-scale and unconstrained NUMBER use double in all three `best_fit_*` modes. This includes NUMBER(15,2), NUMBER(16,2) and NUMBER(38,10). Floating point can change precision: the NUMBER(16,2) fixture's stored 99999999999999.99 is represented as 99999999999999.98. These are explicit floating-point mappings; choose `none` for exact decimals.

`precision_only` selects the same integer widths only for scale zero. Positive or negative scale and unconstrained NUMBER stay Decimal. NUMBER(19,0) stays Decimal.

A snapshot with NUMBER(3,-2) value 99900 fails Confluent with JDBC `Numeric Overflow` in its short conversion. The regular reference input uses 12300 in that column and 1234567890123456700 in NUMBER(17,-2), so all mappings and streaming can be recorded. OSO must use exact integer conversions and fail typed instead of wrapping an out-of-range value.

### Scale-two rounding

Confluent's snapshot uses HALF_EVEN: 0.125 becomes 0.12, 0.135 becomes 0.14, and -0.125 becomes -0.12. The earlier high-precision snapshot value becomes 12345.68. Its streaming path successfully publishes a NUMS insert with 0.12, then fails on the same insert shape with 0.125: the external diagnostic names conversion of `N_ROUND` to BYTES. `numeric-none-scale2` therefore contains all snapshots and the successful insert; the expected failed task is part of the completed reference, not an incomplete run.

**Decision and documented difference:** OSO never rounds a NUMBER to fit the configured Decimal scale. A value that would need rounding stops the task with typed `DecodeException` (CDC-3001) in snapshots as well as streaming. Increase the scale for unconstrained NUMBER before restarting. No value is silently changed or skipped.

## Separate LOB topics

The reference enables large LOB support and uses `${connectorName}.lob.${fullyQualifiedTableName}.${columnName}`.

| Question | Observed fact | Decision |
|---|---|---|
| Topic | One per LOB column | Route with `cdc.lob.topic.template` and `${column}` |
| Key | Required unnamed struct: required string `table`, required string `column`, required `primary_key` | Preserve that shape |
| Primary key inside LOB key | Native scalar for one column; unnamed required struct for composite key | Keep it independent of the outer LOB key struct |
| Value | Required bytes: UTF-8 for CLOB, UTF-16BE for NCLOB, raw BLOB bytes | Preserve encoding; publish complete values only |
| Snapshot and insert | One record per LOB column containing its current or final value, not the empty LOB marker | Assemble or reselect AS OF commit; keep offsets behind unacknowledged column records |
| LOB-only update | One LOB record for the touched column and no row record | Preserve selective publication |
| Non-LOB update | No LOB record | Do not publish unchanged LOBs |
| Delete | `D` on the table topic and a tombstone per LOB column, independent of the table tombstone setting | Preserve deletion, including null values on LOB topics |
| Composite key change | Table `D`, tombstone, `I`; **no LOB record moves or removes the old key**. The later delete tombstones the new key | OSO must tombstone the old LOB key and publish the complete value under the new key. This is a documented difference required to keep LOB state consistent |
| Missing row key | Not part of the supported topic-mode input | OSO stops typed at schema load and reports the same condition through the doctor |

## Documented differences and qualification limits

- OSO refuses Decimal rounding in snapshots, where Confluent rounds HALF_EVEN.
- OSO retains additional `cdc.*` diagnostic headers; Confluent has none by default.
- OSO suppresses redo and undo text for tables a column exclusion may touch, to avoid exposing excluded data.
- OSO will maintain LOB state across key changes with old-key tombstones and new-key values; Confluent 2.15.2 did not emit those records.
- Explicit truncate emission is an OSO extension until a Confluent `T` shape can be observed; default emission remains off.

The subsequent parity and in-place cutover suites must record any further differences here and in ADR-0028 before acceptance. The fixtures specify the recorded profiles; unobserved combinations must not be presented as verified parity. No RDS target is currently running. The optional non-CDB rerun is therefore not planned, and the remaining cloud lab has been destroyed.
