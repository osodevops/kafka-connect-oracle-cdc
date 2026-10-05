---
title: Migration tools
description: The four migration tools, how to install them, their common options, exit codes and secret handling.
---

# Migration tools

Four command-line tools in `tools/migration` of the repository move an existing pipeline to the
connector:

| Tool | What it does |
|---|---|
| `migrate_from_debezium.py` | Translates a Debezium Oracle connector configuration into `cdc.*` properties, with a report on every source property. See [Migrating from Debezium Oracle](from-debezium.md). |
| `migrate_from_confluent.py` | Translates a Confluent Oracle CDC Source connector configuration the same way. See [Migrating from Confluent Oracle CDC Source](from-confluent.md). |
| `takeover_scn.py` | Reads the stopped old connector's offset, checks that the redo from its SCN is still available, and writes `cdc.start.scn` (with `cdc.snapshot.mode=none`) into the new connector's configuration, so it begins where the old one stopped. |
| `verify_cutover.py` | Compares the database at a check SCN with the state in the topics and writes evidence you can attach to a change record. See [Cutover verification](cutover-verification.md). |

## Install

The tools need Python 3.11 and [uv](https://docs.astral.sh/uv/). They use python-oracledb in thin
mode, so no Oracle client libraries are needed, and confluent-kafka.

```bash
git clone https://github.com/osodevops/kafka-connect-oracle-cdc.git
cd kafka-connect-oracle-cdc/tools/migration
uv sync
uv run python migrate_from_debezium.py --help
```

The translators only read and write files, and call the Kafka Connect REST API when given
`--connect-url`. `takeover_scn.py` needs the database and, unless the offsets come from a file,
the Connect REST API;
`verify_cutover.py` needs the database, the Kafka brokers and, to confirm the connector's
position, the Connect REST API.

## Common options

| Option | Meaning |
|---|---|
| `--input FILE` | The tool's input: the source configuration (translators), the old connector's offsets instead of reading them over REST (`takeover_scn.py`), or the connector configuration that names the topics and record settings (`verify_cutover.py`) |
| `--output FILE` | The tool's artefact: the translated configuration, the configuration with `cdc.start.scn`, or the evidence JSON |
| `--report FILE` | A Markdown report for the change record |
| `--json` | Print the machine-readable result on standard output instead of a summary |
| `-v`, `--verbose` | Log progress at debug level |

Configurations are read from JSON (a Connect REST create body, the output of
`GET /connectors/{name}`, or a flat map) or from a `.properties` file.

## Exit codes

| Exit code | Meaning |
|---|---|
| 0 | Success |
| 1 | Failure, including usage errors. `takeover_scn.py` exits 1 when the redo from the start SCN is gone; `verify_cutover.py` when any table fails or is inconclusive. |
| 2 | Success with manual follow-ups: the translator's report or the takeover report lists what still needs a decision |

## Secrets

No tool writes a secret to its output, its report or a log line.

- A literal secret in a source configuration (a password, a JAAS configuration, Schema Registry
  credentials, a licence) is replaced with `********` in the translated configuration and the
  report, and a follow-up asks you to set it through a config provider.
- Config provider references such as `${file:/etc/kafka-connect/secrets.properties:password}`
  are kept as they are.
- A password embedded in a JDBC URL is masked, with a follow-up to move it to
  `cdc.database.password`.
- The tools read the database password and the Connect REST credentials (`user:password`) from an
  environment variable (`--db-password-env`, `--connect-auth-env`) or a file
  (`--db-password-file`), never from the command line.
- Every text a tool writes passes through a scrubber that masks the secret values it has seen,
  including in error messages.
