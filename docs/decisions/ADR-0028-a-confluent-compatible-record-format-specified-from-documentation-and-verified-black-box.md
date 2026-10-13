# ADR-0028: A Confluent-compatible record format, specified from documentation and verified black-box

**Status:** Proposed. The reference spike (`ConfluentFormatRefQualIT`) comes first; the format,
truncate events and LOB topics follow it.
**Context:** PRD-01 SRC-FMT-2, SRC-FMT-3, SRC-TOP-4 and SRC-LOB-1; PRD-04 MIG-2 and MIG-5; the
Confluent cutover qualification of 12 October 2026 (`docs/qualification/2026-10-12-rds-19c-se2-release-candidate`).

## Context

`cdc.output.format` accepts only `debezium`. Someone leaving Confluent's Oracle CDC Source connector
therefore moves every consumer to new topics and to a different record shape, which is the largest
cost of the migration the tools now verify. PRD-01 promises a format whose records match Confluent's
so that consumers and Schema Registry subjects stay unchanged.

Confluent documents the shape only in part. Its pages list the metadata fields, the `output.*`
properties, the operation codes, the key templates, a data type table and a few example records, but
leave open, among others, the representation of INTERVAL, BINARY_FLOAT and TIMESTAMP precision,
whether `scn` and `op_ts` on streaming rows are the row's or the commit's, the truncate record, the
LOB topic value types and the exact Avro schemas. A format built from the documentation alone would
differ from Confluent's in ways a consumer or a Schema Registry compatibility check would notice.

Until now the project's clean-room rule said the Confluent connector is never installed. On 12
October 2026 Sion decided otherwise for this purpose: Confluent's connector, under its evaluation
licence, was already run in the lab for the cutover qualification, and it may be run there to observe
its output.

## Decision

1. **Specification.** The format is specified from Confluent's public documentation. Where the
   documentation is silent or ambiguous, the answer is settled by running Confluent's connector and
   this connector on the same database changes in a lab and comparing their external output: the
   records (keys, values, headers, timestamps), the schemas registered in Schema Registry, the stored
   offsets and REST responses. The facts and decisions are recorded in
   `oracle-cdc-core/src/main/resources/reference/confluent-record-format.md`.
2. **Black-box only.** Confluent's connector runs only inside a disposable lab container (a Confluent
   Platform worker started by a qualification suite), never in a development environment's build and
   never in CI. It is never decompiled and its code is never read; its files are never committed. The
   recorded fixtures contain only our own test data. Everything that needs Confluent's connector is
   done while its evaluation licence is in force (until 11 November 2026); afterwards the fixtures are
   the specification.
3. **Scope.** Full parity: the flat row with metadata fields, renames and operation codes through
   `cdc.output.confluent.*` keys that mirror Confluent's `output.*` properties; the key templates;
   `numeric.mapping` and the DATE mapping; deletes, tombstones and the key-change sequence; the
   before-state field; the redo and undo fields; `header:` placement and nested field paths; truncate
   records; separate LOB topics; Avro schemas Schema Registry accepts as the same subject's.
4. **Debezium format unchanged.** Every change keeps the Debezium-compatible output byte-identical;
   truncate records stay off by default there, as in Debezium.

## Consequences

- A migration from Confluent can keep its topics and consumers: stop Confluent's connector, start
  this one from its position with `cdc.output.format=confluent`, same topics.
- The evaluation licence allows use in development and test environments but does not say whether
  comparing output to build an interoperable product is within it. The specification therefore rests
  on the documentation first, observation settles only what it leaves open, and OSO should confirm
  the approach legally before the format is released.
- Anything the comparison cannot settle before 11 November 2026 becomes a documented difference.

## Evidence

To come: `ConfluentFormatRefQualIT` (reference), `ConfluentFormatFixtureTest` (CI, against the
recorded fixtures), `ConfluentFormatParityQualIT` and `ConfluentInPlaceCutoverQualIT` (lab).

## PRD edits

PRD-01 clean-room note and the acceptance line for the Confluent format; PRD-04 clean-room note.
Further PRD-01 and PRD-04 edits (configuration rows, MIG-2, MIG-5) land with the implementation.

## Amendments

13 October 2026: the reference follow-up is committed in `38e5f71`. Confluent's snapshot rounds
NUMBER values HALF_EVEN to `numeric.default.scale`; its streaming path fails on a value needing
rounding. OSO instead stops with typed `DecodeException` (CDC-3001) in **both** paths whenever the
configured Decimal scale would alter a value. This difference is required by the no-silent-data-loss
rule. Integer conversions also fail typed on overflow rather than wrapping.

The follow-up establishes that Confluent emits no LOB records for a composite row key change.
The planned OSO old-key tombstones and new-key values are a documented difference required to keep
LOB state consistent. The implementation and qualification evidence follow before acceptance.

Confluent's default output has no headers. OSO retains its `cdc.*` diagnostic headers and suppresses
SQL text wherever column exclusions may apply. Default truncate emission remains off: no Confluent
`T` shape was observed, so any explicit OSO emission must be documented as an extension.

The truncate spike records operation code 5 in its own committed transaction, between the earlier
implicit commit and a later INSERT commit. `cdc.truncate.emit` defaults to false in both formats.
When enabled, Debezium gets a null-key `op=t` envelope with null images; Confluent gets a null-key,
metadata-only `op_type=T` struct, without columns or before-state. That latter shape is an OSO
extension because no Confluent `T` output was observed. The logical operation is appended after
UNKNOWN, preserving existing codec ordinals, and only its acknowledged record advances its offset.

Separate LOB topics require stable column keys and complete values. Topic mode refuses keyless
and ROWID-only LOB tables, missing values, placeholder oversize handling and unsupported XMLTYPE
bytes; the operator must exclude XMLTYPE or use supported LOB types. Malformed text also stops
rather than substituting characters. These scope limits are documented, including XMLTYPE where
black-box output has not settled its representation. Null LOB values tombstone their column key.
