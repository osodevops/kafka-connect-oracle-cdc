# OSO CDC Connector for Oracle Database: PRD Package

**Status:** Draft for approval, 4 October 2026
**Owner:** Sion Smith (OSO)
**Target repository:** `osodevops/kafka-connect-oracle-cdc` (Apache-2.0)

## Headline recommendation

Build an Oracle-only, Apache-2.0 Kafka Connect source on LogMiner that competes on correctness and operability: no silent loss, a durable Kafka transaction journal so long transactions stop pinning offsets, bounded memory with spill to disk, resumable SCN-anchored snapshots, multi-PDB capture from one session, exactly-once delivery aligned to Oracle transactions, a preflight `oracle-cdc-doctor`, and drop-in migration from Confluent and Debezium. Oracle's trademark rules keep "Oracle" out of the product name and the domain; the repository name uses it descriptively, as Confluent, Debezium and A2 do. See the feasibility report section 2 and `research/naming_and_seo.md`.

## Reading order

| Order | Document | For |
|---|---|---|
| 1 | `00_feasibility_and_strategy.md` | Decision makers: verdict, legal and IP, naming, architecture, parity matrix, phases, risks |
| 2 | `research/debezium_oracle_pain_points.md` | Everyone: source-linked complaints and how each maps to a requirement |
| 3 | `research/confluent_oracle_connectors_detail.md` | Parity and migration: Confluent behaviour and full property list |
| 4 | `research/logminer_reference.md` | Engineers: LogMiner facts the engine depends on |
| 5 | `prd/PRD-00_oracle-cdc-core.md` | Capture engine (`oracle-cdc-core`) |
| 6 | `prd/PRD-01_source_connector.md` | Kafka Connect source, formats, exactly-once |
| 7 | `prd/PRD-02_snapshots.md` | SCN-anchored chunked snapshots |
| 8 | `prd/PRD-03_schema_and_ddl.md` | Schema registry and DDL |
| 9 | `prd/PRD-04_migration_tooling.md` | Confluent and Debezium migration, cutover verification |
| 10 | `prd/PRD-05_doctor_operations_observability.md` | `oracle-cdc-doctor`, admin CLI, metrics, dashboards, runbooks |
| 11 | `testing_strategy.md` | Correctness oracle, fault injection, regression corpus, release gate |
| 12 | `06_repo_scaffolding_spec.md` | Repository layout, `CLAUDE.md`, CI, release, site, support and security |
| 13 | `07_test_lab_and_budget.md` | Local, AWS dev and OCI lab, licensing conditions, monthly budget |
| 14 | `research/naming_and_seo.md` | Naming and trademark evidence, Semrush data, SEO content plan |

## Decisions needed from Sion

1. Approve the product name "OSO CDC Connector for Oracle Database", the repository `osodevops/kafka-connect-oracle-cdc`, and the website domain `kafkacdcconnector.com` (approved 4 October 2026; register it).
2. Approve Phase 1 scope (single instance and CDB with multiple PDBs; RAC moves to Phase 2 qualification).
3. Approve the lab plan and monthly AWS dev budget in `07_test_lab_and_budget.md` section 4.
4. Get a legal view on continued lab use under Oracle's development terms and price Oracle PartnerNetwork before 1.0 GA.

## Conventions

- Requirement IDs (`CORE-*`, `SRC-*`, `SNAP-*`, `SCH-*`, `MIG-*`, `VER-*`, `DOC-*`) are stable and referenced from tests.
- Pain point IDs (`PP-01` to `PP-16`) trace every requirement to user evidence.
- Performance numbers in the PRDs are targets to validate in Phase 0, not claims.
- "23ai and 26ai" in the CI matrix means the two newest Oracle Database Free images available; no 26ai Free image exists yet, so the second version is `23.9-faststart` for now.
- Configuration keys use the `cdc.*` prefix; design changes are recorded in `decisions/` as ADRs.
