# PRD-04: Migration Tooling (Confluent and Debezium to OSO CDC Connector)

**Status:** Draft for implementation
**Module:** `tools/migration` (Python 3.11, `python-oracledb`, `confluent-kafka`)
**Replaces:** Manual migration runbooks
**Depends on:** PRD-01 (`cdc.start.scn`, output formats), Connect offsets REST API from [KIP-875](https://cwiki.apache.org/confluence/display/KAFKA/KIP-875:%20First-class%20offsets%20support%20in%20Kafka%20Connect)
**Clean-room note:** Built from public property documentation of Confluent and Debezium only. Follows the conventions of the Salesforce repo's `tools/confluent-migration` (exit codes, evidence JSON).

---

## 1. Objective

Make it safe and boring to replace an existing Confluent Oracle CDC or Debezium Oracle connector: translate configuration, take over at the exact SCN without a gap, keep topic names and record shape, and produce verifiable evidence that nothing was lost.

## 2. Tools

| Tool | Purpose |
|---|---|
| `migrate_from_confluent.py` | Translate a Confluent Oracle CDC connector config (JSON or properties) into a OSO CDC config; produce a migration report |
| `migrate_from_debezium.py` | Translate a Debezium Oracle connector config; produce a migration report |
| `takeover_scn.py` | Read the existing connector's committed offset through the Connect REST API (`GET /connectors/{name}/offsets`) and compute `cdc.start.scn` |
| `verify_cutover.py` | Compare Oracle state with materialised topic state at a check SCN; write signed evidence |

All tools share: `--input`, `--output`, `--report` (Markdown), `--json`, and exit codes: 0 success, 1 failure, 2 success with manual follow-ups (same as the Salesforce tools).

## 3. Translator requirements

| ID | Requirement |
|---|---|
| MIG-1 | Every source property is classified as `mapped`, `mapped-with-change`, `dropped` (with reason) or `manual` (with instruction). Unknown properties are `manual`. The report lists every property; nothing is silently dropped. |
| MIG-2 | Output preserves topic names: `migrate_from_confluent` converts `table.topic.name.template` variables to `cdc.topic.template` and sets `cdc.output.format=confluent`; `migrate_from_debezium` keeps `topic.prefix` naming and `cdc.output.format=debezium`. |
| MIG-3 | Defaults that differ are pinned to the source behaviour: for example Confluent `emit.tombstone.on.delete` default false becomes `cdc.tombstones.on.delete=false`; Confluent `numeric.mapping=none` becomes `cdc.decimal.mode=precise` with a note on bytes encoding; Debezium `decimal.handling.mode=precise` stays precise. |
| MIG-4 | Sets `exactly.once.support=required` and `transaction.boundary=connector` when the target Connect cluster reports exactly-once enabled (checked with `--connect-url`), otherwise leaves at-least-once and adds a follow-up. |
| MIG-5 | Confluent-specific items reported as follow-ups: redo log topic consumers (the topic will stop receiving data), LOB topics (map to `cdc.lob.mode=topic`), `confluent.license` removal, `connection.pool.*` removal, `tasks.max` reduced to one with explanation. |
| MIG-6 | Debezium-specific items: schema history topic no longer needed (keep until verified, then delete); signal table replaced by signal topic; `heartbeat.action.query` removed; non-LogMiner adapters (`xstream`, `olr`) are `manual` with guidance. |
| MIG-7 | Secrets are never written to reports; config provider references (`${file:...}`) are preserved. |

## 4. Takeover procedure (both sources)

1. Run the translator; resolve follow-ups.
2. Run `oracle-cdc-doctor check` against the new config.
3. Stop the old connector (pause is not enough; stop so its offset is final).
4. `takeover_scn.py` reads the old offset:
   - Confluent: the SCN in the connector's committed offset for the redo log or table partition; the tool takes the lowest SCN across offset partitions.
   - Debezium: `scn` (the resume position, low watermark) and `commit_scn`; the tool uses `scn` as `cdc.start.scn` and records `commit_scn` for the report.
5. Deploy the OSO CDC Connector with `cdc.snapshot.mode=none` and `cdc.start.scn`. Commits at or before the old connector's last committed position are re-emitted (bounded duplicates during the overlap; consumers that upsert by key are unaffected). There is no gap.
6. Run `verify_cutover.py` after the overlap has passed.
7. Archive the evidence artefact; remove old internal topics after the retention agreed with the customer.

Required log retention: archived logs from `cdc.start.scn` must still exist; `takeover_scn.py` checks `V$ARCHIVED_LOG` and fails with exit code 1 if not.

## 5. `verify_cutover.py`

| ID | Requirement |
|---|---|
| VER-1 | Inputs: database connection, bootstrap servers, topics or connector config, tables, `--check-scn` (default: current SCN minus a safety margin, after confirming the connector position has passed it). |
| VER-2 | For each table, compute in Oracle `COUNT(*)` and an order-independent hash of key plus normalised row values `AS OF SCN :check_scn`, in key-range batches to keep each flashback query short. |
| VER-3 | Materialise each topic by key up to records whose `commit_scn` (header `cdc.commit_scn`) is at or below the check SCN, applying tombstones, and compute the same hash with the same normalisation (types normalised per `cdc.decimal.mode` and `cdc.temporal.mode`). |
| VER-4 | Output PASS or FAIL per table, mismatching key samples (up to 100, values masked unless `--show-values`), and an evidence JSON containing tool version, inputs (no secrets), per-table results and a SHA-256 over the canonical JSON. Exit 0 PASS, 1 FAIL. |
| VER-5 | Works against the old connector's topics too, so customers can verify their current pipeline before migrating. |

## 6. CI requirements

- A cutover test in CI (Phase 1 for Debezium, Phase 2 for Confluent fixtures) runs: workload, old-style offsets fixture, takeover, overlap, verify; uploads the evidence JSON as a `migration-evidence` workflow artefact.
- Golden-file tests for every property in both mapping tables.

## 7. Acceptance criteria

- [ ] Every Confluent property listed in `research/confluent_oracle_connectors_detail.md` section 1.4 has a classification and a golden test.
- [ ] Every Debezium property in PRD-01 Appendix A has a classification and a golden test.
- [ ] Debezium takeover in CI (real Debezium 3.x against Oracle Free) shows zero missing keys and `verify_cutover` PASS.
- [ ] `takeover_scn.py` refuses when the required archived log is gone.
- [ ] Reports contain no secrets (tested with canary values).
