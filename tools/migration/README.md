# Migration tools

Tools for moving an existing pipeline from the Debezium Oracle connector or the Confluent Oracle
CDC Source connector to the OSO CDC Connector for Oracle Database, as specified in
`docs/prd/PRD-04_migration_tooling.md`. This is a standalone Python project managed with uv; the
Maven build does not use it.

The user documentation is on the website under Migration (`website/docs/migration/`).

## Requirements

- Python 3.11 and [uv](https://docs.astral.sh/uv/).
- `python-oracledb` in thin mode, so no Oracle client libraries are needed, and `confluent-kafka`.
  uv installs both.
- The translators need neither; they read and write files only (and optionally call the Connect
  REST API).

## Install and run

```bash
cd tools/migration
uv sync
uv run python migrate_from_debezium.py --help
```

Each script is also installed as a console command: `migrate-from-debezium`,
`migrate-from-confluent`, `takeover-scn` and `verify-cutover`.

## The tools

| Script | What it does |
|---|---|
| `migrate_from_debezium.py` | Translates a Debezium Oracle connector configuration into `cdc.*` properties and writes a report that classifies every source property as mapped, mapped with a change, dropped (with the reason) or manual (with an instruction). |
| `migrate_from_confluent.py` | The same for a Confluent Oracle CDC Source connector configuration. The Confluent-compatible record format is not built yet, so the report's first follow-up explains what that means for existing consumers. |
| `takeover_scn.py` | Reads the stopped old connector's offset through the Connect REST API, checks that the redo from its SCN is still available, and writes `cdc.start.scn` and `cdc.snapshot.mode=none` into the new connector's configuration. The connector honours `cdc.start.scn` while it has no stored offset. |
| `verify_cutover.py` | Compares each table, read with flashback queries AS OF a check SCN, with the state materialised from its topic up to the same SCN, and writes evidence JSON with a SHA-256 over its canonical form. |

All four share `--input`, `--output`, `--report` (Markdown) and `--json` (the machine-readable
result on standard output), and these exit codes:

| Exit code | Meaning |
|---|---|
| 0 | Success |
| 1 | Failure, including usage errors |
| 2 | Success with manual follow-ups (translators and `takeover_scn.py`) |

`verify_cutover.py` exits 0 only when every table passes.

## Secrets

Secrets are never written to an output, a report or a log line. Literal secret values in a
source configuration are replaced with `********` and listed as follow-ups; config provider
references such as `${file:/etc/kafka-connect/secrets.properties:password}` are kept as they are.
Database passwords and Connect credentials are read from an environment variable or a file,
never from the command line. Every text the tools write passes through a scrubber that masks
any known secret value, and a canary test checks every output of every tool.

## Development

```bash
uv sync
uv run ruff check . && uv run ruff format --check . && uv run pytest -q
```

The translators are tested against golden files under `tests/golden/<source>/<case>/`: `input.*`
is the source configuration, `args.json` the options (including the exactly-once answer of a
fake Connect cluster), and `expected/` holds the translated configuration, the Markdown report
and the JSON report. After a deliberate change, regenerate them and review the diff:

```bash
UPDATE_GOLDEN=1 uv run pytest -q tests/test_golden.py
git diff tests/golden
```

`tests/test_mapping_coverage.py` keeps every property of PRD-01 Appendix A and of section 1.4 of
`docs/research/confluent_oracle_connectors_detail.md` classified and covered by a golden case.
The takeover and verification logic is tested with fakes of the Connect REST API, the database
and the topics (`tests/fakes.py`); no database, Kafka or Docker is needed.

| Path | Contents |
|---|---|
| `src/oso_cdc_migration/translate.py` | The translator framework: classifications, rules, reports |
| `src/oso_cdc_migration/debezium.py`, `confluent.py` | The two rule sets |
| `src/oso_cdc_migration/offsets.py`, `redo.py`, `takeover.py` | Offsets, redo log coverage and the takeover |
| `src/oso_cdc_migration/normalise.py` | Canonical values, the counterpart of `bench/.../check/Normaliser.java` |
| `src/oso_cdc_migration/records.py`, `materialise.py`, `rowstore.py`, `verify.py` | Topic records, materialisation, the SQLite row store and the verifier |
| `src/oso_cdc_migration/oracle.py`, `kafka.py`, `connect.py` | python-oracledb, confluent-kafka and Connect REST access |
| `src/oso_cdc_migration/redact.py` | Secret masking |
