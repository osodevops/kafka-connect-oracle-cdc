# ADR-0028: A Confluent-compatible record format, specified from documentation and verified black-box

**Status:** Accepted, 13 October 2026. Implementation and qualification are complete. Release
requires OSO's legal check and approval.
**Context:** PRD-01 SRC-FMT-2, SRC-FMT-3, SRC-TOP-4 and SRC-LOB-1; PRD-04 MIG-2 and MIG-5.

## Context

The original connector supported only the Debezium envelope. Someone leaving Confluent's Oracle
CDC Source connector therefore had to change topics, record shapes and consumers. A compatible
format allows a stopped-source cutover that keeps the table and LOB topics, consumers and Schema
Registry subjects.

Confluent's public documentation leaves parts of the format unspecified, including timestamp
precision, row and commit SCNs, before-state schema changes and LOB keys and encodings. On
12 October 2026 Sion authorised observing Confluent 2.15.2's external output in a disposable lab
to settle those questions. Its implementation is never read or copied.

## Decision

1. **Specification.** Use Confluent's public documentation first. Where it is silent, compare
   external records, schemas, offsets and REST responses from both connectors on the same
   workload. Record facts and decisions in
   [the format reference](../../oracle-cdc-core/src/main/resources/reference/confluent-record-format.md).
   The committed fixtures contain only our own test data and become the durable specification.
2. **Black-box limits.** Confluent 2.15.2 runs only inside disposable qualification containers,
   never in CI. Its code is never read or decompiled; its files are never committed or downloaded
   by the build. The suites require a hand-placed local plugin and refuse CI execution. Runs
   requiring it finish before about 4 November 2026, ahead of the evaluation licence ending
   11 November 2026. Unsettled representations become documented differences.
3. **Format and namespace.** `cdc.output.format=confluent` selects an unnamed required flat
   struct with table columns followed by configurable string metadata. Column keys retain the
   scalar, struct or concatenated template. Default topics are `${pdb}.${schema}.${table}` in a
   CDB and `${database}.${schema}.${table}` otherwise, with no prefix. Numeric and DATE mappings,
   metadata renames, nesting, headers, operation codes, before-state schema switching and
   requested redo/undo text follow the recorded profiles.
4. **Operations.** Confluent-format tombstones default to off. Enabled deletes emit only a
   tombstone; key changes emit the observed delete/tombstone/insert sequence, including two
   old-key tombstones when that setting is enabled. Truncate defaults to off in both formats.
   Explicit `cdc.truncate.emit=true` produces the OSO extension described below. It participates
   in commit order and advances only acknowledged positions.
5. **Separate LOB topics.** `cdc.lob.mode=topic` is available only with Confluent output.
   `cdc.lob.topic.template` must contain `${column}`. Keys are required structs with `table`,
   `column` and `primary_key`; values are required bytes, UTF-8 for CLOB, UTF-16BE for NCLOB and
   raw bytes for BLOB. Null assignment is zero-length bytes; deletion is a tombstone. Only the
   final row/column record advances completed delivery and snapshot progress. Ambiguous merged
   savepoint groups reselect their final touched values AS OF commit, preserving committed calls.
6. **Same-topic migration.** The migration tool defaults to `--format=confluent`, maps the
   compatible output settings and preserves topic templates and converter settings. The JSON
   verifier compares flat values with exact numeric scales. Historical source records without
   commit metadata need a stopped-source offset and commit fence; otherwise verification is
   INCONCLUSIVE. Real Avro handover is verified separately against unchanged reader schemas.
7. **Debezium unchanged.** Debezium remains the default. Its existing record schemas, bytes,
   tombstone defaults and journal payloads stay unchanged. New SQL retention and LOB tracking
   are opt-in; truncate stays off by default.

## Documented differences and supported-input limits

- **Decimal rounding is refused.** Confluent snapshots round NUMBER values HALF_EVEN to
  `numeric.default.scale`; its tested streaming path fails on an inexact value. OSO stops with
  typed `DecodeException` (CDC-3001) in both paths whenever fitting the Decimal scale would
  alter a value. Integer overflow also stops typed instead of wrapping. No value is silently
  changed or skipped.
- OSO retains additional `cdc.*` diagnostic headers; Confluent has no default headers.
- OSO suppresses redo and undo SQL where column exclusions may apply, protecting excluded data.
- Requested SQL retains nine fractional timestamp digits; Confluent 2.15.2 emits three. OSO's
  fixed FF9 mining formats preserve source precision. The live suite asserts exact higher-
  precision SQL from its own workload and compares all other text. Timestamp column values
  still agree at milliseconds.
- OSO tombstones old LOB keys and publishes complete values under new row keys. Confluent
  2.15.2 leaves the old composite LOB key behind.
- On an out-of-row LOB insert, OSO publishes the resolved real ROWID when available, or null
  otherwise; Confluent publishes LogMiner's all-A placeholder.
- Explicit Confluent-format truncate emission is an OSO extension: a null key and a metadata-
  only `T` struct, with no row columns or before-state. No Confluent truncate record was observed.
  Debezium's opt-in shape is a null-key `op=t` envelope with null images. Default emission is off.
- Flat output requires complete supported row images and every declared key value. Incomplete
  images, missing keys, required-column failures and unsupported key types stop with CDC-3001
  rather than publishing unknown values as null. ALL-column supplemental logging is required.
- Topic mode requires stable column keys and complete supported LOB values. Keyless and
  ROWID-only LOB tables, missing values and malformed text stop typed. Oversize values stop
  with CDC-3003; placeholder handling is refused. XMLTYPE bytes are unsupported unless excluded;
  the recorded black-box comparison did not settle their representation.

## Evidence

[Qualification evidence](../qualification/2026-10-13-confluent-format/README.md) records the
15-profile reference, 14-profile live comparison with the documented differences, same-topic
Avro cutover, and final quality gates. `ConfluentFormatFixtureTest` compares committed JSON
records and Avro schemas in CI without Confluent software. The real-worker tests cover truncate,
LOB snapshots, selective updates, key moves, deletes, SQL output and merged savepoint recovery.
The same-topic Avro suite verifies final database values, every committed row, a transaction
spanning the switch, both cached reader schemas and subject versions, rollback exclusion and
stop/resume journal readback.

Oracle Database Free 23ai is the live format-qualification target. The optional RDS 19c non-CDB
rerun was not performed because no instance was running. Unobserved configuration combinations
are not claimed as verified parity. XMLTYPE's unsettled LOB representation is an explicit limit.

## Consequences

Migrations can retain topics, consumers and subjects for the qualified configurations, subject
to the documented differences and complete-input requirements. Fixtures protect the format after
the evaluation licence ends. Existing Debezium users keep their output.

The evaluation licence permits development and test use but does not expressly settle the
interoperability comparison. OSO must confirm the black-box approach legally before the format
is released. Acceptance and PR merge authorise implementation; they do not authorise the enabling
release. The cloud lab is destroyed; disposable qualification containers are removed on suite exit.

## PRD edits

PRD-01 records the output selector, namespaces, per-format defaults and LOB topic requirements.
PRD-04 records same-topic translation, explicit Debezium migration and flat-value verification.
