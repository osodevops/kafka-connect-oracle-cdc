# Handover: the Confluent-compatible record format (ADR-0028)

**Written:** 13 October 2026, for whoever (person or model) continues this piece of work in a fresh
session. **Read first:** `CLAUDE.md`, then this file, then
`docs/decisions/ADR-0028-a-confluent-compatible-record-format-specified-from-documentation-and-verified-black-box.md`,
then the memory index. The older `docs/HANDOVER.md` covers the Phase 1 build and is background only.

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
  (ADR-0028, licensing research and release decision). On 13 October Sion requested Perplexity
  research and approved proceeding with release; this is a maintainer decision, not a
  solicitor's clearance. Preserve that distinction and the provenance in `NOTICE`.

## 1. Where everything is right now

The authorised implementation plan is complete. [PR #31](https://github.com/osodevops/kafka-connect-oracle-cdc/pull/31)
was squash-merged into `main` at `1cfaaf5`. Main contains the format, truncate extension,
separate LOB topics, same-topic migration tools, accepted ADR-0028 and qualification evidence.
Sion approved the enabling release on 13 October 2026 after requesting Perplexity licensing
research, and confirmed that only the public 30-day trial applied. The research, primary sources
and artefact review are recorded in ADR-0028. No solicitor's opinion or vendor permission was
obtained. Public docs and the release `NOTICE` now state that the implementation is independent,
the official unmodified connector was only an external-output reference, and no reverse-engineered
or decompiled versions were used. No Confluent source was inspected or copied.

The public `website/docs/guides/confluent-compatible-records.md` brings the feature overview,
capture configuration, JSON records, operation/key sequences, metadata layouts, numeric and DATE
mappings, separate LOB topics, truncate, Avro, migration and every documented difference together.
It is linked from the sidebar, README, format reference and migration guide.

Release PR #29 (0.1.6) is being published under Sion's explicit instruction to proceed without
waiting for outstanding tests. The 0.1.6-only exception is recorded in
`docs/qualification/2026-10-13-release-0.1.6/README.md`: prior green CI is reused for unchanged
software inputs; incomplete T2 is waived, never called a pass. The local qualification and its
old release watcher have been stopped. Preserve all regression tests and future release gates.

The [hosted nightly](https://github.com/osodevops/kafka-connect-oracle-cdc/actions/runs/38418299236)
was cancelled after Sion objected to hours-long hosted runs, and the workflow is disabled. Its
schedule is removed and optional manual jobs use a dedicated self-hosted lab runner. The replacement
local T2 run was also stopped following the immediate-publication instruction; it is incomplete.
Both engine tiers, both connector tiers and both container qualification tiers passed in the hosted
run on both database images. The existing green main and release-PR CI checks are complete.
Production code is identical to their tested revision; later changes are docs and release automation.

Preserve the existing 0.1.x preview scope: no completed 72-hour soak or full platform qualification
is claimed; those remain prerequisites for 1.0. The 0.1.6 exception does not authorise restarting
long tests on hosted runners or weakening any regression assertion.

The prior scheduled nightly
[38334075196](https://github.com/osodevops/kafka-connect-oracle-cdc/actions/runs/38334075196)
failed the unchanged worker-kill regression on `23.9-faststart`. The task stopped with a
`ConcurrentModificationException` while Kafka Connect converted record headers, then failed to
catch up. PR #31 already corrected the shared headers in `DebeziumEnvelope`: each emitted record
owns its header list. A focused `RecordQueueSinkTest` regression now verifies stable iteration,
one split flag per record and unchanged acknowledgement positions in both formats. Restoring the
old shared-header behaviour makes that regression fail; the current implementation passes.
No further production change was needed. Preserve all correctness assertions. Full T2 is explicitly
waived for 0.1.6 under the recorded maintainer decision; it remains required for future releases.

Release PR #29's first DCO run failed because release-please generated its commit without a
`Signed-off-by` line. The root release-please configuration now supplies the bot's sign-off for
generated release and snapshot commits at `5b6f8cd`. The regenerated PR head `df35c64` carries the
actual sign-off, and DCO plus every other PR check passed. Never exempt the bot from DCO.

The fifteen-profile reference and step 1 follow-up are committed at `38e5f71`. Fixtures contain
only the suite's own test data and normalised volatile values. The reference-only scale-two
profile is complete: snapshots establish HALF_EVEN rounding, exact streaming 0.12 succeeds and
streaming 0.125 stops Confluent. OSO's intentional no-rounding refusal is protected by fixtures
and typed-failure regressions in both snapshot and streaming paths.

| Completed step | Implementation checkpoints | Final verification |
|---|---|---|
| 2: flat format | `f9eb5ce`, safety review `51a4535` | JSON/Avro fixtures, generated references and clean gate |
| 3: truncate | `5bc0236`, identifier correction `be11ce8` | Reference, ordering/acknowledgement regressions and full tiers |
| 4: LOB topics | `a3c7dc3`, savepoint recovery `3294f56`, null bytes `96bbab4` | Both live SQL variants, LOB parity and full tiers |
| 5: same-topic migration | `8c1cd3a`, keyless/Avro correction `328ad10` | 340 Python tests, lint/format, golden cases and real worker |
| 6: live comparison | `bf1f0ea`, `96bbab4`, `0c859c6` | All fourteen profiles across the recorded full run and affected-profile reruns; same-topic Avro cutover |
| 7: acceptance and documentation | `666d9b0`, PR #31 | Clean gate with generated references, website build and all PR checks |
| 8: housekeeping | After PR #31 merge | Task-only temporary worktrees/stashes removed; unrelated work preserved |

The final production revision `96bbab4` passed all 59 engine and 32 connector tests. All-fields
and renamed passed at `0c859c6`, asserting exact nine-digit OSO SQL literals from the independent
workload and recording Confluent's three-digit SQL as a difference. Same-topic Avro cutover
passed with 24 committed rows and zero missing rows, final database values, both unchanged
reader schemas/subject versions, rollback exclusion and stop/resume internal Avro readback.
Evidence is in `docs/qualification/2026-10-13-confluent-format/`.

All differences and supported-input limits are recorded in ADR-0028 and the core format reference.
Debezium remains the default; its existing output and regressions remain unchanged. LOB-only table
updates are suppressed only for Confluent. Merged savepoint groups reselect final touched LOBs AS
OF commit rather than discard an earlier committed call. Null LOB assignments are required empty
bytes; deletion and old-key cleanup are tombstones. Only the final record acknowledges progress.

The feature, lab and tools worktrees have been removed after reviewing and privately preserving
older task preparations. The three ADR-0028 task stashes are removed. The feature history remains
available in Git. The remaining cloud lab was destroyed through its workflow and all final
qualification containers are removed. No RDS rerun is planned; live non-CDB format parity is not
claimed. Dependabot and unrelated worktrees/development containers are untouched.

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
- `username` is the database user. `row_id` is the ROWID. The live comparison found the all-A
  LogMiner placeholder on Confluent's out-of-row LOB INSERT; OSO resolves the real locator when
  available. This explicit metadata difference is recorded in the reference and ADR.
- Redo and undo fields hold LogMiner's SQL_REDO and SQL_UNDO verbatim, including `ROWID = '...'`.
  A key change's `D` and `I` both carry the update's text. For a LOB insert the redo has
  `EMPTY_CLOB()` and `EMPTY_BLOB()`. Confluent's SQL uses three fractional timestamp digits;
  OSO retains nine from its fixed mining formats, a documented precision-preserving difference.
  Timestamp column values still agree at milliseconds.
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
- A NULL assignment publishes required zero-length bytes, observed for NCLOB in live parity.
  Deletes and old-key removal use tombstones.
- Snapshot rows get one LOB record per LOB column with the current value. Inserts get one per LOB
  column with the final value (not the empty LOB).
- An update that touches only a LOB column gives one LOB record and **no** row record. An update
  of only non-LOB columns gives no LOB record.
- A delete gives `D` on the table topic and a tombstone on every LOB topic of the row, whatever
  the tombstone setting.
- The table topic's value never contains the LOB columns.
- A composite row key change emits no LOB records: the old LOB key remains, and the later delete
  tombstones only the new key. OSO's implemented old-key tombstones and new-key LOB values are a
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

- [x] Completed in `f9eb5ce`: fixture/unit tests, generated docs, full gate, engine and connector tiers.

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

- [x] Reference spike, implementation, regressions, generated config docs and full gate.
- [x] Engine and connector tiers, including unchanged regression reruns; step complete.

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

- [x] Topic mode, byte/key shapes, schema/doctor checks, complete values and delivery regressions.
- [x] Full clean gate and real-worker snapshot, selective update, key-change and delete test.
- [x] Full final engine tier (59 tests) and connector tier (32 tests) at `96bbab4`.

1. `cdc.lob.mode=topic` (Confluent format only, refused with Debezium) and
   `cdc.lob.topic.template` with `${column}` in `envelope/TopicRouter`. Values come from inline
   assembly or reselect as of the commit SCN.
2. One record per LOB column, with the key and value shape from the `lob-topics` fixtures, and
   tombstones on delete and key change. Only the last record of a row carries its offset (as
   snapshot records already do in `RecordQueueSink`).
3. A LOB table without a key stops typed at schema load. Add a doctor rule for the same.

### Step 5 (C5): topics, migration tool, enabling

- [x] Default same-topic routing, migration mappings, same-topic golden cases and flat value verifier.
- [x] Connector fixture test and generated configuration reference; full clean gate.
- [x] Full final engine and connector tiers with step 4's review; both steps complete.

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

- [x] Reference, live comparison and strengthened same-topic Avro cutover suites implemented.
- [x] Eleven profiles at `bf1f0ea`, LOB topics and same-topic cutover at `96bbab4` verified.
- [x] Final SQL profiles with exact precision difference asserted; sanitised evidence published.

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

- [x] ADR accepted, PRDs and website updated; full gate and website build green.
- [x] [PR #31](https://github.com/osodevops/kafka-connect-oracle-cdc/pull/31) opened, all checks passed and squash-merged.
- [x] Perplexity licensing research, primary-source review, artefact audit and Sion's release
  approval recorded; public development provenance added to `NOTICE` and docs.
- [ ] Enabling release: approved, pending technical release qualification and publication.

1. ADR-0028 status Accepted, recording the key namespace, the per-format truncate and tombstone
   defaults, LOB topics only in this format, and the documented differences.
2. PRD-01 and PRD-04 edits: configuration rows, MIG-2 and MIG-5.
3. Website:
   - a Confluent section in `website/docs/reference/record-formats.md`;
   - `website/docs/migration/from-confluent.md` rewritten for the same-topics path (today it
     says the format "is not built yet" and always writes new topics);
   - the intro and comparison tables.
4. Done: PR #31 squash-merges `feat/confluent-format`. Sion subsequently requested the licensing
   research and approved release. A `feat` commit gives the next patch version before 1.0.
   After the technical gates pass, merge release-please PR #29; never tag by hand.

### Step 8: housekeeping

- [x] Cloud lab destroyed through its workflow; no RDS run planned.
- [x] Qualification containers removed after the final suite.
- [x] Task-only temporary worktrees and stashes removed after merge; older preparations privately preserved.

- Release PR #29 (0.1.6, includes the format feature) is open; release is approved, pending gates.
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


## Latest live qualification checkpoint

Steps 1 through 8 are complete within the authorised scope. PR #31 is merged, ADR-0028 is
accepted, all local and PR gates passed, evidence is committed and task housekeeping is complete.
The cloud lab and qualification containers are removed. Sion approved release after requesting
licensing research, which is documented as a maintainer decision rather than solicitor clearance.
The provenance notice is public and included in the plugin's `NOTICE`. Technical release
qualification is in progress; release PR #29 has not been merged. XMLTYPE LOB bytes and live
non-CDB format parity remain documented limits.
