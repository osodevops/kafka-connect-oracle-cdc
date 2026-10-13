# Handover: the Confluent-compatible record format (ADR-0028)

**Written:** 13 October 2026, for whoever (person or model) continues this piece of work in a fresh
session. **Read first:** `CLAUDE.md`, then
`docs/decisions/ADR-0028-a-confluent-compatible-record-format-specified-from-documentation-and-verified-black-box.md`,
then this file. The older `docs/HANDOVER.md` covers the Phase 1 build and is background only.

## 0. Goal and the decisions behind it

`cdc.output.format=confluent` makes this connector write records shaped like those of Confluent's
Oracle CDC Source connector, so someone leaving Confluent keeps their topics, consumers and Schema
Registry subjects: stop Confluent's connector, start this one from its position, same topics.
PRD-01 SRC-FMT-2 and SRC-FMT-3, SRC-TOP-4, SRC-LOB-1; PRD-04 MIG-2 and MIG-5.

Sion's decisions (12 October 2026):

- **Specification from Confluent's public documentation, verified black-box.** Where the
  documentation is silent, run Confluent's connector (version 2.15.2, under its evaluation licence)
  and this connector on the same database changes in a lab and compare external output only.
- **Full parity:** the flat row with metadata fields, renames and operation codes, key templates,
  `numeric.mapping`, the DATE mapping, tombstones and the key-change sequence, the before-state
  field, the redo and undo fields, `header:` placement and nested paths, truncate records, separate
  LOB topics, and Avro schemas that Schema Registry accepts as the same subject's.

Hard rules for this work (also in `CLAUDE.md` and ADR-0028):

- Confluent's connector runs only inside a disposable lab container started by
  `ConfluentFormatRefQualIT` (later also `ConfluentFormatParityQualIT`). It never runs in CI,
  and it is never decompiled. Its code is never read and its files are never committed or
  downloaded by the build. The suite runs only when `-De2e.confluent.plugin` names a local copy
  that someone placed there by hand.
- **The evaluation licence ends on 11 November 2026.** Everything that needs Confluent's connector
  (the follow-up reference run, the parity suite and the in-place cutover suite) must be finished by
  about 4 November 2026. After that, the committed fixtures are the specification, and anything
  unsettled becomes a documented difference.
- The fixtures hold only this suite's own test data, with volatile values replaced by placeholders.
- The Debezium-compatible output stays byte-identical throughout. Every change must keep the
  existing Debezium tests green.
- Before the format is released, OSO should confirm the black-box approach legally
  (ADR-0028, Consequences).

## 1. Where everything is right now

### Main checkout (`kafka-connect-oracle-cdc`, branch `main`)

Committed in `d321d02` (test(qual): ConfluentFormatRefQualIT and the recorded Confluent reference):

| Path | What it is |
|---|---|
| `e2e-tests/src/test/java/sh/oso/connect/oracle/e2e/qual/ConfluentFormatRefQualIT.java` | The reference spike. Starts a Confluent Platform worker (`confluentinc/cp-server-connect:7.9.2`) with Confluent Schema Registry (`confluentinc/cp-schema-registry:7.9.2`), runs Confluent's connector once per profile against Oracle Database Free, and writes normalised fixtures |
| `e2e-tests/src/test/java/sh/oso/connect/oracle/e2e/support/ConnectCluster.java` | Adds `withConfluentSchemaRegistry()`, `confluentSchemaRegistryUrl()` and `CONFLUENT_SCHEMA_REGISTRY_URL` (earlier commits added `confluentPlatform(image)`, `workerReaches` and `topics()`) |
| `kafka-connect-oracle-cdc/src/test/resources/confluent-reference/<profile>/` | The fixtures: one `<PDB>.SCHEMA.<TABLE>.jsonl` per topic (one JSON line per record: timestamp type, normalised timestamp, key, value, headers); Avro profiles also have `subjects/<subject>.v<n>.avsc` and record bodies replaced by `{"avro":true,"magic":0}` |

The full run of all 14 profiles finished on 13 October 2026 (build success, about 33 minutes).
`confluent-reference/` holds these 14 directories: `default`,
`default-avro`, `before-state-avro`, `renamed`, `key-struct`, `key-concat`,
`numeric-best-fit-or-decimal`, `numeric-best-fit-or-double`, `numeric-best-fit-or-string`,
`numeric-precision-only`, `numeric-none-scale2`, `date-timestamp`, `tombstones` and `lob-topics`.
The step 1 follow-up completed on 13 October 2026, with all 11 selected profiles passing the
reference suite. There are now 15 profile directories, including `all-fields`, and NUMS and LOBS2
fixtures in the rerun profiles. Avro records also identify their subject and schema version.
**`numeric-none-scale2` is complete:** it records the snapshots and an exact-scale streaming insert,
then asserts the expected task failure on a value needing rounding. Its diagnostics remain local.
A first numeric follow-up hit Confluent's JDBC short overflow on NUMBER(3,-2) value 99900;
regular input now fits its mapped integer widths. The independent reference is
`oracle-cdc-core/src/main/resources/reference/confluent-record-format.md`. The run deletes and rewrites the
whole `confluent-reference` directory only when it is started with the `rm -rf` shown in
section 4. A subset run rewrites only its own profiles.

### Feature worktree (`../kafka-connect-oracle-cdc-fmt`, branch `feat/confluent-format`)

Rebased on `main` after `d321d02`, so the fixtures are on the branch too. It has three commits of
its own, listed here with their IDs as of 13 October 2026. The IDs change with every rebase, so
find the commits by subject (`git log --oneline main..feat/confluent-format`). The branch is
pushed to `origin` as a backup, with no pull request yet; open one only at step 7:

| Commit | Content | State |
|---|---|---|
| `d7e62a7` | fix(envelope): each record of a change owns its headers (`DebeziumEnvelope` passed one `ConnectHeaders` to a delete and its tombstone, so `RecordQueueSink` added `cdc.split` twice). Test `DebeziumEnvelopeTest.eachRecordOfAChangeOwnsItsHeaders` | Full gate green |
| `de8c43f` | refactor(envelope): `envelope/RecordFormat` interface (`partition()`, `records(...)`, `snapshotRecords(...)` returning a list, `create(config, router, databaseName)`); `DebeziumEnvelope implements RecordFormat`; `RecordQueueSink` and `OracleCdcSourceTask` use it. Only the last record of the last snapshot row carries the completed position | Full gate green |
| `f592941` | feat(envelope): Confluent-compatible format, first cut (work in progress). See the list below | **Compiles only. No tests yet. The generated configuration reference is not regenerated, so the full gate fails on the docs check** |

What the `feat(envelope)` commit contains:

- `kafka-connect-oracle-cdc/src/main/java/sh/oso/connect/oracle/envelope/confluent/`:
  - `ConfluentFormat`: implements `RecordFormat`.
    - Values and keys:
      - The value is an unnamed struct: the table's columns in table order, then the metadata
        fields.
      - Keys follow the three templates. A table without a key, or keyed by ROWID, has a null key.
    - Operations:
      - INSERT gives `I` with the after image. DELETE gives `D` with the before image, and a
        tombstone only when tombstones are on.
      - An UPDATE that changes the key gives `D` (old key, before image), a tombstone (always),
        then `I` (new key, after image). Any other UPDATE gives `U` with the after image, plus the
        before-state struct when that field is named.
      - A snapshot row gives `R`: `scn` and the commit SCN are both the snapshot SCN; `op_ts`,
        `row_id` and `username` are null.
    - Metadata values:
      - `scn` is the change's own SCN (`change.id().scn()`), the commit SCN is `tx.commitScn()`.
      - `op_ts` is the redo timestamp in milliseconds, `current_ts` comes from the clock.
      - `row_id` is `RowIds.real(rowId)`, `username` is `tx.username()`.
      - The redo and undo fields come from `RowChange.sql()`.
    - Tombstones carry the header-placed metadata of the delete before them.
    - Errors: a column that has the name of a metadata field stops the task with
      `NameCollisionException` (CDC-6004).
    - Kept from the Debezium-compatible format: the `cdc.*` headers (after any Confluent
      metadata headers) and a null record timestamp.
  - `FieldLayout`: places each metadata field as a top-level field, a field of a nested struct (a
    dotted name; the struct takes the place of its first member), a header (`header:name`, string
    value, left out when null), or nowhere (empty name). The order is table, scn, commit scn,
    op type, op ts, current ts, row id, username, redo, undo.
  - `ConfluentTypes`: the column schemas and values.
    - Done: `numeric.mapping=none` and both DATE mappings.
    - **Not done:** the `best_fit_*` and `precision_only` mappings. The enum exists but
      `schema()` and `value()` ignore it.
  - `ConfluentSettings` (record) and `ConfluentFormatConfig`: the `cdc.output.confluent.*` keys,
    with validation.
- `OracleCdcSourceConnectorConfig`:
  - `cdc.output.format` accepts `confluent`.
  - New `OutputFormat` enum, plus `outputFormat()`, `confluentSettings()` and `keepsSqlText()`.
  - When `cdc.tombstones.on.delete` is unset, the confluent format uses `false`.
  - `ConfluentFormatConfig.define(def)` is called at the end of `configDef()`.
- `RecordFormat.create` switches on the format.
- Core:
  - New `core/model/RedoText(redo, undo)`, and `RowChange` gains an 11th component `sql`, with
    10-argument and 9-argument constructors kept and a `withSql(...)` method.
  - `RowChangeCodec` writes it as a trailing optional field after the schema version, so older
    payloads decode and older readers stop before it.
  - `SizeEstimate` counts it. `LobAssembler` and `ReselectingEvents` pass it through.
    `ColumnFilter.project` drops it when columns are removed.
  - `CaptureEngine.withSqlText(boolean)` attaches `d.sqlRedo()` and `d.sqlUndo()` in
    `decodeAndBuffer` unless `excluded.mayExclude(table)`. `JdbcEngineFactory` calls it with
    `config.keepsSqlText()`.

C2.2 is in progress in the feature worktree, at the C2.2 checkpoint (find the `feat(envelope): match Confluent fixtures` commit): fixture-backed JSON and
Avro schema comparisons; field/config/codec/SQL-retention tests; numeric mappings and typed refusal
of rounding and integer overflow; required non-key fields; corrected tombstone and keyless string
key semantics; LOB-only row suppression and an acknowledged heartbeat for a filtered transaction
tail. The full `clean verify -DskipE2E` gate, fixture tests and regenerated configuration/error docs pass.
Verbatim SQL has its own parser mode, preserving the legacy delimiter regression; mining retains
ROWID predicates and delimiters only when SQL output is requested. The engine tier and connector
tier are still required before step 2 is ticked off. The feature branch is rebased on `38e5f71`. Long suites use the temporary
`../kafka-connect-oracle-cdc-fmt-lab` worktree; do not edit it while Maven runs. No PR or format release has been created.

## 2. Facts established so far (Confluent 2.15.2, Oracle Database Free 23ai, 13 October 2026)

These are recorded in `oracle-cdc-core/src/main/resources/reference/confluent-record-format.md`
(facts and decisions, never timings). The step 1 follow-up settled the open points below.

### Value and key

- The value schema is an unnamed, required struct. Its columns come in table order: key columns
  are required and the other columns optional. A NOT NULL non-key column is required too (NUMS.NN).
- The default metadata fields follow the columns, all optional strings, in this order: `table`,
  `scn`, `op_type`, `op_ts`, `current_ts`, `row_id`, `username`. `table` is `PDB.OWNER.TABLE`.
  `scn`, `op_ts` and `current_ts` are decimal strings.
- With renames (profile `renamed`):
  - The order is table, (scn), commit scn, op type, nested `x_meta {op_ts, current_ts}`,
    (row id), username, redo, undo, then before-state last.
  - `header:x_scn` and `header:x_rowid` become string headers in that order. The row id header is
    absent when null, for example on snapshot rows.
  - Tombstones carry the same headers.
  - The `all-fields` follow-up places commit SCN immediately after `scn` when both are in the body.
- `scn` is **not** the commit SCN: `x_scn` (header) differs from `x_cscn` on every streaming row.
  The follow-up commits on a second connection between two statements: their row SCNs differ,
  while their commit SCNs are equal. Use the row's SCN, not the transaction start SCN.
- Snapshot rows: `op_type=R`, `scn` and the commit SCN are both the snapshot SCN (one per table),
  and `op_ts`, `row_id`, `username`, redo and undo are null.
- `op_ts` has second resolution (milliseconds ending in 000). `current_ts` is the write time.
- `username` is the database user. `row_id` is the ROWID.
- Redo and undo fields hold LogMiner's SQL_REDO and SQL_UNDO verbatim, including `ROWID = '...'`.
  A key change's `D` and `I` both carry the update's text. For a LOB insert the redo has
  `EMPTY_CLOB()` and `EMPTY_BLOB()`.
- The before-state field appears **only on `U` records**, as a struct of the columns, and the field
  is then present in the schema. Insert, delete, snapshot and both halves of a key change have no
  before-state field at all, so update records use a different value schema. In Avro this alternates
  between two subject versions (`ConnectDefault` with or without a `before` field of record type
  `ConnectDefault2`).
- Keys:
  - The default `${primaryKeyStructOrValue}` gives the key column's own schema and value for a
    one-column key, and an unnamed required struct otherwise.
  - `${primaryKeyStruct}` always gives a struct.
  - `${primaryKey}` gives an **optional** string of the values joined with `_` (for example
    `1_a`, `0_s`).
  - A table without a key gets a null key.
  - Under `best_fit_*` and `precision_only` the key column follows the column mapping (for
    example `int64`).

### Operations

- A key change produces `D` (old key, before image), a tombstone (old key), then `I` (new key),
  whatever `emit.tombstone.on.delete` is.
- A plain delete with the default setting produces `D` with no tombstone.
- With `emit.tombstone.on.delete=true` (profile `tombstones`) a delete produces **only a
  tombstone, no `D` record**. A key change produces two tombstones for the old key, then `I` for
  the new one. On a table without a key, the tombstone has a null key and a null value.
- Default truncate produces no record. No `T` was observed in the follow-up either. The public property list documents the operation
  value but no enabling property. Explicit OSO emission is an extension to document; default stays off.
- A rolled-back transaction produces nothing.
- A change touching only LOB columns produces no record without LOB topics: the CLOB-only update
  produced nothing.
- Records have no headers by default. The Kafka record timestamp is CreateTime, set by the
  producer (the SourceRecord timestamp is null).

### Types (`numeric.mapping=none`, `oracle.date.mapping=date`)

- NUMBER(p,s) becomes Connect Decimal with parameters `{scale: s, connect.decimal.precision: p}`.
  An unconstrained NUMBER gets scale 127 (`numeric.default.scale`) and precision 38. NUMBER(5,-2)
  gets scale -2.
- With `numeric.default.scale=2`, snapshot rounding is **HALF_EVEN**: 0.125 becomes 0.12,
  0.135 becomes 0.14, and -0.125 becomes -0.12. The streaming follow-up first publishes 0.12,
  then fails on 0.125, naming conversion of N_ROUND to BYTES in the external diagnostic.
  **Decision authorised by Sion:** OSO refuses any Decimal value needing rounding, with typed
  CDC-3001 in snapshots and streaming. This is a documented difference from Confluent.
- FLOAT(n) becomes a double.
- CHAR, VARCHAR2, NCHAR and NVARCHAR2 become strings, with CHAR and NCHAR blank-padded.
- DATE becomes a Connect Date: days since the epoch, with the time of day dropped. With
  `oracle.date.mapping=timestamp` (profile `date-timestamp`), DATE becomes a Connect Timestamp
  keeping the time of day, in UTC milliseconds.
- TIMESTAMP(0, 3, 6, 9) becomes a Connect Timestamp in UTC milliseconds, truncated towards
  negative infinity (`1969-12-31 23:59:59.123456789` gives -877).
- TIMESTAMP WITH TIME ZONE becomes the correct instant in milliseconds. WITH LOCAL TIME ZONE
  becomes milliseconds in the database time zone (UTC in the container).
- RAW becomes bytes.
- BINARY_FLOAT, BINARY_DOUBLE, INTERVAL YEAR TO MONTH and INTERVAL DAY TO SECOND are **left out of
  the schema** (Confluent logs "not currently supported").
- CLOB, NCLOB and BLOB columns are absent from the table's value.
- The JSON converter's default `decimal.format=BASE64` applies, so fixtures hold base64 decimals.
- Avro: the value record is `io.confluent.connect.avro.ConnectDefault`. Optional fields are
  `["null", type]` with `default: null`. Decimals carry `precision`, `scale`,
  `connect.parameters` and `logicalType: decimal`. ADD COLUMN registers a new subject version.

### LOB topics (profile `lob-topics`)

The profile set `lob.topic.name.template=${connectorName}.lob.${fullyQualifiedTableName}.${columnName}`
and `enable.large.lob.object.support=true`.

- There is one topic per LOB column, for example `<connector>.lob.FREEPDB1.SCHEMA.LOBS.C`.
- The key is a required unnamed struct `{table: string, column: string, primary_key: <the key
  column's schema>}`, where `table` is the fully qualified name and `column` the column name.
  For a composite key, `primary_key` is a required unnamed struct of the key columns.
- The value is required `bytes`, for every LOB type. CLOB is UTF-8, NCLOB is **UTF-16BE**, and
  BLOB is the raw bytes. A 3000-character CLOB arrived whole.
- Snapshot rows get one LOB record per LOB column with the current value. Inserts get one per LOB
  column with the final value (not the empty LOB).
- An update that touches only a LOB column gives one LOB record and **no** row record. An update
  of only non-LOB columns gives no LOB record.
- A delete gives `D` on the table topic and a tombstone on every LOB topic of the row, whatever
  the tombstone setting.
- The table topic's value never contains the LOB columns.
- A composite row key change emits no LOB records: the old LOB key remains, and the later delete
  tombstones only the new key. The planned OSO old-key tombstones and new-key LOB values are a
  documented difference, required to keep LOB state consistent.

### `numeric.mapping` (column types of table TYPES and EXOTIC)

| Column | none | best_fit_or_decimal | best_fit_or_double | best_fit_or_string | precision_only |
|---|---|---|---|---|---|
| NUMBER(10) | Decimal(10,0) | int64 | int64 | int64 | int64 |
| NUMBER(5) | Decimal(5,0) | int32 | int32 | int32 | int32 |
| NUMBER(20,0) | Decimal(20,0) | Decimal(20,0) | double | string (plain digits) | Decimal(20,0) |
| NUMBER(10,2) | Decimal(10,2) | double | double | double | Decimal(10,2) |
| NUMBER (unconstrained) | Decimal(38,127) | double | double | double | Decimal(38,127) |
| NUMBER(5,-2) | Decimal(5,-2) | int32 | int32 | int32 | Decimal(5,-2) |
| FLOAT(10) | double | double | double | double | double |

The follow-up settles the widths: precision 1 or 2 gives int8, 3 or 4 int16, 5 to 9 int32,
10 to 18 int64, 19 or above the suffix's fallback. Negative scale does **not** widen the integer:
NUMBER(3,-2) is int16 and NUMBER(17,-2) int64. Out-of-range values must fail typed.
Every tested positive-scale NUMBER is double under all `best_fit_*` mappings, including
NUMBER(15,2), NUMBER(16,2) and NUMBER(38,10). `precision_only` uses integers only for scale zero.

### Confluent's own quirks met while recording (worth knowing, not to copy)

- Its snapshot failed with a NullPointerException without table statistics, so the suite gathers
  them.
- `oracle.dictionary.mode=auto` stopped with ORA-01371 at the script's DDL even after a
  dictionary build, so the suite uses `online`.
- In online mode, rows written before a DDL but mined after it fail as unparsable statements
  ("COL n"). The script therefore waits 40 seconds before its DDL part.
- Its tasks log "SQL exception" and "Graceful stop failed" when the suite deletes them, which is
  harmless.

## 3. What is left, in order (exact steps)

Tick these off in this file as they land. Commit messages: conventional, `git commit -s`, trailer
`Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Build with JDK 21:
`export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home`.

### Step 1: finish and record the reference

- [x] Follow-up run completed, all selected profiles recorded, facts and decisions written.
- [x] Full `clean verify -DskipE2E`, fixture inspection and secret scan; follow-up committed with the suite, fixtures and reference file.

1. Done: the 14-profile run ended and every profile is read (section 2).
2. Extend `ConfluentFormatRefQualIT` for the open points and run only the new or affected
   profiles before about 4 November:
   - Table `NUMS`, added to `TABLES`:
     - Columns: `id NUMBER(10) PRIMARY KEY`, `n2 NUMBER(2)`, `n3 NUMBER(3)`, `n4 NUMBER(4)`,
       `n9 NUMBER(9)`, `n18 NUMBER(18)`, `n19 NUMBER(19)`, `n3m2 NUMBER(3,-2)`,
       `n17m2 NUMBER(17,-2)`, `n15_2 NUMBER(15,2)`, `n16_2 NUMBER(16,2)`,
       `n38_10 NUMBER(38,10)`, `n_round NUMBER` and `nn VARCHAR2(10) NOT NULL`.
     - Seed rows with `n_round` values 0.125, 0.135 and -0.125, and boundary values that fit
       each column.
   - In `script`, one transaction whose rows have different SCNs. Between its statements,
     commit on a second connection into a table outside `TABLES` (for example `SCRATCH`) so the
     SCN advances.
   - A profile `all-fields` that names every metadata field in the body without `header:` or
     dots: `output.commit.scn.field=cscn`, `output.redo.field=redo`,
     `output.undo.field=undo` and `output.before.state.field=before`. It settles the commit SCN
     position.
   - Rerun `numeric-none-scale2` with `log.sensitive.data=true` added to its properties (the
     log is local and holds only test data, but do not copy it into the repository), and give
     `NUMS.n_round` one row whose values fit two decimals so streaming can be seen to work.
   - A composite-key LOB table (for example `LOBS2 (a NUMBER(5), b VARCHAR2(5), c CLOB, PRIMARY
     KEY (a, b))`) for the LOB topic key shape.
   - Run profiles `default`, `renamed`, `all-fields`, all four `numeric-*` profiles,
     `numeric-none-scale2`, `lob-topics` and `default-avro` with
     `-De2e.confluent.reference.profiles=a,b,...` (section 4).
   - If time allows, a truncate profile: check Confluent's documentation for a property that
     makes truncate emit `T`, and run it.
   - Consider recording the subject version per Avro record. Today every Avro body becomes
     `{"avro":true,"magic":0}`, so which schema each record used is lost. Map the schema id from
     bytes 1 to 4 to `subject` and `version` through the registry, and write them instead.
3. Write `oracle-cdc-core/src/main/resources/reference/confluent-record-format.md` from section
   2 plus the new facts: one fact and one decision per question, no timings.
4. Done for the first run (`d321d02`). After the follow-up run, commit the changed suite and
   fixtures together with the reference file the same way, and run
   `gitleaks dir . --config .gitleaks.toml --redact` first. The fixtures must contain no host
   names, passwords or real schema names. The normaliser writes `SCHEMA`, but check anyway.

### Step 2 (C2.2): fixture test and corrections

1. Done: `feat/confluent-format` is rebased on `main` with the fixtures. Rebase again after the
   follow-up run.
2. Write `kafka-connect-oracle-cdc/src/test/java/sh/oso/connect/oracle/envelope/confluent/ConfluentFormatFixtureTest.java`.
   It builds the same tables as `TableSchema`s, feeds `ConfluentFormat` the same changes as the
   suite's script (`CommittedTransaction`s and `SnapshotRow`s, with SCNs, ROWIDs and timestamps
   chosen to match the placeholders' relations) and serialises with `JsonConverter`
   (`schemas.enable=true`, default decimal format). It then normalises like the suite and
   compares each record's key, value and non-`cdc.*` headers to the fixture lines.
   - Profiles to cover: `default`, `renamed`, `key-struct`, `key-concat`, every `numeric-*`,
     `date-timestamp` and `tombstones`.
   - For `default-avro` and `before-state-avro`, compare against the `.avsc` files.
     Confluent's Avro converter is not Apache-2.0 licensed, so **do not add it as a
     dependency** without checking the build's licence allowlist. Instead, map the Connect
     schemas to the Avro shape in the test (an unnamed struct gives
     `io.confluent.connect.avro.ConnectDefault`, nested structs `ConnectDefault2` onwards,
     optional fields `["null", type]` with default null, and Decimal with `connect.parameters`).
     Leave the real conversion to the parity suite, which uses the Schema Registry container.
3. Fix what the test finds. Known gaps:
   - `ConfluentTypes` `best_fit_*` and `precision_only` (table in section 2 plus the thresholds
     from step 1).
   - The NOT NULL non-key column optionality.
   - The `scn` value if it turns out to be the start SCN. `CommittedTransaction.startId()` holds
     the start.
   - Commit SCN field position.
   - Rounding mode.
4. Unit tests:
   - `FieldLayoutTest`: nesting, headers, duplicates refused.
   - `ConfluentFormatConfigTest`: defaults equal `ConfluentSettings.defaults`; bad names, before
     field with `header:` or dots, duplicate op values all refused.
   - `RowChangeCodecTest`: a payload with `sql` round-trips; an old payload without it decodes.
   - `SizeEstimate`.
   - `CaptureEngineTest`: SQL text kept only with `withSqlText(true)` and not for a table the
     column filter may touch.
   - `OracleCdcSourceConnectorConfigTest`: the tombstone default per format.
5. Regenerate the configuration reference in `e2e-tests` with `-Dconfigdocs.update=true` (see
   `ConfigDocsGeneratorTest`), and the error docs with `-Derrordocs.update=true` if the CDC-6004
   description is extended to mention metadata field names (it should be).
6. Full gate `./mvnw clean verify -DskipE2E`, then `-De2e.groups=engine` and
   `-De2e.groups=connector`. Debezium output must be unchanged.

### Step 3 (C3): truncate

1. Engine spike `TruncateShapesRefEngineIT` (T1, `-Dreference.update=true`): how LogMiner shows
   TRUNCATE (OPERATION_CODE, DDL text) and in which transaction.
2. `Operation.TRUNCATE` appended after `UNKNOWN` (the codec stores ordinals). `CaptureEngine.applyDdl`
   buffers a TRUNCATE change for a captured table in its transaction, so commit order and
   offsets apply.
3. `cdc.truncate.emit` resolved per format: off by default for Debezium (keeps output
   unchanged; shape `op=t` when on), and for Confluent as observed (default none; `T` record
   shape from step 1 if a setting enables it).
4. Regression test first if it touches delivery order.

### Step 4 (C4): LOB topics

1. `cdc.lob.mode=topic` (Confluent format only, refused with Debezium) and
   `cdc.lob.topic.template` with `${column}` in `envelope/TopicRouter`. Values come from inline
   assembly or reselect as of the commit SCN.
2. One record per LOB column, with the key and value shape from the `lob-topics` fixtures, and
   tombstones on delete and key change. Only the last record of a row carries its offset (as
   snapshot records already do in `RecordQueueSink`).
3. A LOB table without a key stops typed at schema load. Add a doctor rule for the same.

### Step 5 (C5): topics, migration tool, enabling

1. Confluent-format topic default `${pdb}.${schema}.${table}` (CDB) or
   `${database}.${schema}.${table}` (non-CDB), with no prefix.
2. `tools/migration` `migrate_from_confluent.py --format {confluent,debezium}`, default
   confluent:
   - It maps `output.*`, `key.template`, `numeric.mapping`, `numeric.default.scale`,
     `oracle.date.mapping`, `lob.topic.name.template` and `emit.tombstone.on.delete` to the
     `cdc.output.confluent.*` and other keys.
   - It prefixes topics only for debezium.
   - Add golden cases. `verify_cutover.py` gets value comparison for Confluent-shaped topics.
3. `ConfluentFormatConnectorIT` (connector tier, no Confluent software): the connector with
   `cdc.output.format=confluent` against Oracle Free, checked against the fixtures.
4. Regenerate the config reference.

### Step 6 (C6): verification against Confluent (before about 4 November 2026)

1. `ConfluentFormatParityQualIT`: both connectors on the same changes. Records are compared per
   key, field by field, excluding `current_ts`. Avro schemas are compared canonically and with
   Schema Registry compatibility checks.
2. `ConfluentInPlaceCutoverQualIT`: this connector continues in Confluent's own topics with Avro.
   No committed row may be lost, the subject must stay compatible, and an unchanged consumer must
   read everything.
3. Run both on the container. Also run them on an RDS for Oracle 19c non-CDB lab when one is up.
   That lab lives in OSO's private infrastructure and is not described here. Evidence goes under
   `docs/qualification/` in general terms only.

### Step 7 (C7): documentation and release

1. ADR-0028 status Accepted, recording the key namespace, the per-format truncate and tombstone
   defaults, LOB topics only in this format, and the documented differences.
2. PRD-01 and PRD-04 edits: configuration rows, MIG-2 and MIG-5.
3. Website:
   - a Confluent section in `website/docs/reference/record-formats.md`;
   - `website/docs/migration/from-confluent.md` rewritten for the same-topics path (today it
     says the format "is not built yet" and always writes new topics);
   - the intro and comparison tables.
4. Open a PR from `feat/confluent-format`, squash-merge it, and let release-please cut the
   release. A `feat` commit gives the next patch version before 1.0. Before release, ask OSO
   for the legal check named in ADR-0028.

### Step 8: housekeeping

- Release PR #29 (0.1.6, docs only) is open and not urgent.
- The Dependabot backlog (Kafka 4.3, JUnit 6, Testcontainers 2 majors) is not in this goal.
- RAC 3d, standby and Autonomous Database stay blocked on Sion's Oracle and OCI accounts.

## 4. How to run the reference suite

Prerequisites:
- Docker with room for Oracle Free, Kafka, a Confluent Platform worker and Schema Registry. Only
  one Docker e2e run may run at a time, because the test network has a fixed subnet.
- JDK 21.
- A local, hand-placed copy of Confluent's connector 2.15.2 (unzipped plugin directory), passed
  with `-De2e.confluent.plugin=<dir>`. It is not in the repository and must never be.

Everything, rewriting all fixtures:

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home
rm -rf kafka-connect-oracle-cdc/src/test/resources/confluent-reference
./mvnw -pl e2e-tests verify -De2e.groups=qual -Dit.test='ConfluentFormatRefQualIT' \
  -De2e.confluent.plugin=<plugin dir> -De2e.confluent.reference.update=true \
  --batch-mode --no-transfer-progress > /tmp/ref-all.log 2>&1
```

A subset: add `-De2e.confluent.reference.profiles=default,renamed` and drop the `rm -rf`. If
modules changed since the last install, run `./mvnw install -DskipTests -DskipE2E
-Djacoco.skip=true` first. Each profile takes about two to three minutes.

To read fixtures quickly, print each line's key payload, headers and value payload (drop the
schemas). The Normaliser numbers SCN, timestamp and ROWID placeholders in first-appearance order
across the whole profile. That keeps equality but **not ordering**, so "less than" relations
cannot be read from fixtures.

## 5. Pitfalls

- Never edit sources in, or start another e2e run from, a checkout whose Maven build is running;
  use the worktree.
- The default JVM on this workstation is too new for the formatter; always export JDK 21.
- `./mvnw install` with tests skipped needs `-Djacoco.skip=true`, or the coverage check trips on
  stale data.
- `ConfigDocsGeneratorTest` fails until the reference is regenerated after any config change.
- Confluent's JSON decimals are base64 by default, so a verification tool must decode them with
  the column scale (`tools/migration/src/oso_cdc_migration/confluent_topics.py` already does).
