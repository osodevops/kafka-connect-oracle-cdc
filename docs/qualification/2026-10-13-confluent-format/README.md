# Confluent-compatible format qualification, 13 October 2026

ADR-0028's format is implemented for the next release. This evidence verifies the documented
profiles and the same-topic Avro handover; release still requires OSO's legal check and approval.
No proprietary connector runs in CI or forms part of the build. All observations use a
hand-placed Confluent Oracle CDC Source connector 2.15.2 in disposable qualification containers.

## Target and scope

- Oracle Database Free 23ai, image tag `23.26.3-slim-faststart`.
- Confluent Platform worker and Schema Registry 7.9.2, Kafka 3.9.1, JDK 21.
- Fifteen reference profiles, recorded in committed fixtures with volatile values replaced.
- Fourteen live profiles comparing both connectors on the same database changes.
- Same-topic Avro cutover with unchanged consumers and Schema Registry subjects.

The optional RDS 19c non-CDB rerun was not performed because no instance was running. Non-CDB
routing and migration translation have unit/golden coverage; live format parity on that target
is not claimed. Unobserved configuration combinations are not claimed as verified parity.
The remaining cloud lab is destroyed. Qualification containers are removed when each suite exits.

## Reference and live parity

The step 1 follow-up at `38e5f71` completed the reference, including numeric widths, NOT NULL
columns, distinct row SCNs in a transaction, every metadata field, composite LOB keys and Avro
subject versions. The fixtures specify keys, values, headers and schemas independently of
Confluent software in subsequent builds.

The full live comparison at `bf1f0ea` exercised fourteen profiles and collected every profile's
result. Eleven passed. All-fields and renamed found an Oracle LOB SQL prelude split; `1ea1d18`
keeps complete LOB blocks and restores DML/DDL terminators after continuation assembly. LOB topics
found that a null NCLOB assignment must be required empty bytes, corrected in `96bbab4`.
The affected profiles were rerun after those corrections. The final SQL profiles also assert the
exact FF9 literals from the workload, recording Confluent's FF3 SQL as an explicit difference.
This table records successful comparisons from those runs, rather than claiming a single final
fourteen-profile invocation.

| Live profile | Successful revision |
|---|---|
| `default` | `bf1f0ea` |
| `default-avro` | `bf1f0ea` |
| `before-state-avro` | `bf1f0ea` |
| `key-struct` | `bf1f0ea` |
| `key-concat` | `bf1f0ea` |
| `numeric-best-fit-or-decimal` | `bf1f0ea` |
| `numeric-best-fit-or-double` | `bf1f0ea` |
| `numeric-best-fit-or-string` | `bf1f0ea` |
| `numeric-precision-only` | `bf1f0ea` |
| `date-timestamp` | `bf1f0ea` |
| `tombstones` | `bf1f0ea` |
| `lob-topics` | `96bbab4` |
| `all-fields` | `0c859c6` |
| `renamed` | `0c859c6` |

The changes after `bf1f0ea` affect requested SQL output and null LOB assignments; their affected
profiles have the final reruns above. The final production implementation otherwise retains the
successful row, key and schema behaviour from that full run.

The comparator checks supported column values, key/value schemas, non-`cdc.*` headers, row and
commit SCNs, operation timestamps, before-state and other metadata. `current_ts` is volatile;
snapshot SCNs are normalised because the two snapshots run independently. Streaming SCNs remain
compared. Real Avro schemas are compared canonically and checked for compatibility in Schema
Registry. Known differences are asserted explicitly: resolved LOB-insert ROWIDs, complete LOB
key moves and exact higher-precision SQL literals. SQL fields are not ignored or truncated.

`numeric-none-scale2` is reference-only in live parity. The reference proves snapshot HALF_EVEN
rounding, a successful streaming 0.12 and source failure on 0.125. OSO's fixture and unit tests
require typed CDC-3001 in both paths whenever a Decimal scale would alter a value. This deliberate
no-rounding decision is a documented difference, not an unsettled profile.

## Same-topic Avro handover

`ConfluentInPlaceCutoverQualIT` passed against `96bbab4`, after earlier successful runs. Its
machine-readable evidence reports 24 committed rows and zero missing rows. It verifies:

- Every committed row, including a transaction begun before the source stops and committed
  across the switch; rolled-back rows are absent.
- Final database values, including an update, and key/value equality.
- The same topic, both key/value subject IDs and versions, and successful decoding with reader
  schemas cached before cutover.
- Stop/resume readback of internal Avro state and a subsequent committed row.

The source is stopped before takeover, its offset is read and the replacement starts from the
safe SCN in the same topics. There is no consumer or subject migration.

## Regression and quality gates

- `./mvnw clean verify -DskipE2E` passes; configuration/error references regenerated with
  `-Dconfigdocs.update=true` and `-Derrordocs.update=true`.
- Final engine tier: 59 tests, zero failures/errors/skips, production revision `96bbab4`.
- Final connector tier: 32 tests, zero failures/errors/skips, production revision `96bbab4`.
  This includes both live LOB SQL-output variants, merged savepoint recovery, truncate,
  same-topic fixtures, Avro, Debezium cutover and unchanged delivery/restart regressions.
- Python migration: 340 tests pass; ruff check and format check pass.
- Website builds with the final format and migration documentation.
- Gitleaks is clean before the branch pushes. All feature commits have DCO sign-off and the
  requested co-author trailer. No failing regression was removed.

## Differences and limits

The complete list is maintained in
[ADR-0028](../../decisions/ADR-0028-a-confluent-compatible-record-format-specified-from-documentation-and-verified-black-box.md)
and [the format reference](../../../oracle-cdc-core/src/main/resources/reference/confluent-record-format.md):
Decimal rounding refusal and typed integer overflow; extra diagnostic headers; SQL suppression
with column exclusions; nine-digit SQL timestamp literals; consistent LOB key moves; resolved
LOB-insert ROWIDs; optional OSO truncate output; complete supported flat images and declared keys;
and stable-key/complete-value LOB requirements, including unsupported XMLTYPE bytes.
Null LOB empty bytes now match the source and are not a difference.

## Artifacts

- `ConfluentFormatParityQualIT-evidence.json`: final all-fields and renamed comparison.
- `ConfluentInPlaceCutoverQualIT-evidence.json`: final same-topic Avro cutover.
- `profile-results.json`: successful comparison revisions for all fourteen live profiles.

These files contain only general target versions, correctness results and the suite's own data.
Credentials, endpoints, private infrastructure identifiers and proprietary files are excluded.
