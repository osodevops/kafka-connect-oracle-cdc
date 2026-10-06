---
title: Cutover verification
description: How verify_cutover.py compares the database at a check SCN with the state in the topics, and the evidence it writes.
---

# Cutover verification

`verify_cutover.py` (see [Migration tools](tools.md)) proves a cutover table by table: it reads
each table AS OF a check SCN, materialises the table's topic up to the same SCN, and compares the
two with one normalisation for both sides. It writes PASS or FAIL per table and an evidence JSON
with a SHA-256 over its canonical form, which you can attach to a change record. It reads topics
written by this connector and by the Debezium Oracle connector, so it can also check an existing
Debezium pipeline before a migration.

```bash
export ORACLE_PASSWORD='...'
uv run python verify_cutover.py --input oso-connector.json \
    --table APP.ORDERS --table APP.CUSTOMERS \
    --db-dsn db.example.com:1521/ORCLPDB1 --db-user c##cdc --db-password-env ORACLE_PASSWORD \
    --bootstrap-servers kafka:9092 --connect-url http://connect:8083 \
    --output cutover-evidence.json --report verification.md
```

## Inputs

- **Tables.** `--table OWNER.TABLE`, repeated, or `OWNER.TABLE=TOPIC` to name the topic.
- **Connector configuration.** `--input` takes this connector's configuration or Debezium's. It
  gives the topic names (prefix and template), `cdc.decimal.mode` and `cdc.temporal.mode` (or
  `decimal.handling.mode` and `time.precision.mode`), the LOB mode, the unavailable placeholder,
  key overrides and the connector's name. Without it, give `--topic-prefix` or topics per table,
  and the modes on the command line.
- **Database.** `--db-dsn` names the PDB (or non-CDB) that holds the tables. The user needs
  `SELECT` and `FLASHBACK` on the tables, or `FLASHBACK ANY TABLE`.
- **Kafka.** `--bootstrap-servers`, and `--kafka-config` for a properties file with further client
  settings (security protocol, SASL, TLS). Topics are read from the beginning with
  `isolation.level=read_committed`. Records must have been written with the JSON converter, with
  or without schemas; numbers encoded as base64 decimals need the schemas, or
  `decimal.format=NUMERIC`.
- **Check SCN.** `--check-scn`, or by default the SCN of `--safety-margin-seconds` ago (30).
- **Connector position.** With `--connect-url` the tool reads the connector's offset and waits, up
  to `--wait-seconds` (300), until its resume SCN has passed the check SCN, so every change
  committed at or before it has been delivered. `--no-position-check` skips this, and the
  evidence records that it was skipped.

## What is compared

For each table the tool reads the columns from the data dictionary and the key the connector
uses: the primary key, else the first unique index on NOT NULL columns, unless the connector
configuration overrides it.

**The database side.** The table is read with flashback queries AS OF the check SCN, in batches
of `--batch-rows` rows (50,000) ordered by the key, so each query stays short. A table without a
key is read in ROWID order. An ORA-01555 (or ORA-08181, ORA-01466) marks the table inconclusive,
not failed.

**The topic side.** Records apply in partition order, by key:

- A record counts at its commit SCN, from the `cdc.commit_scn` header or the envelope's
  `source.commit_scn`; a snapshot row (`op` `r`) counts at its read SCN. Records above the check
  SCN are skipped.
- Creates, snapshot rows and updates store the `after` image, deletes remove the key, a tombstone
  removes its key, and a truncate (`op` `t`) empties the table. A tombstone without headers takes
  the SCN of the record before it in the partition.
- A field carrying the unavailable placeholder keeps the row's previous value, as a consumer must.
- A table without a key is compared as a multiset of whole rows.

**Normalisation.** Both sides become the same canonical text, as in the project's correctness
oracle:

| Type | Canonical form |
|---|---|
| NUMBER, FLOAT | Decimal text without trailing zeros; with `cdc.decimal.mode=double`, the database value goes through a double first |
| BINARY_FLOAT, BINARY_DOUBLE | The shortest decimal text that identifies the value, as Java prints it |
| DATE, TIMESTAMP | Local date and time text, with the fraction the value has |
| TIMESTAMP WITH TIME ZONE, WITH LOCAL TIME ZONE | The instant in UTC |
| INTERVAL | Microseconds |
| RAW, BLOB | Lower-case hexadecimal |
| Character types, CLOB | The text itself |

Columns are left out of the comparison, and listed in the evidence, when the connector leaves them
out of the records: LOB columns with `cdc.lob.mode=skip` (or `lob.enabled=false` for Debezium),
XMLTYPE columns, columns of types the connector does not capture, and columns named with
`--ignore-column` (for example ones Debezium excluded).

The comparison runs in a temporary SQLite file (`--work-dir`), so large tables do not need to fit
in memory.

## Results

| Result | Meaning |
|---|---|
| PASS | Same row count and the same order-independent hash of key and normalised values |
| FAIL | Rows only in the database, only in the topics, or with different values. Up to 100 keys are listed with the columns that differ; values appear only with `--show-values` |
| INCONCLUSIVE | The tool could not decide: a flashback query could not reach the check SCN, a record could not be read, a record had no commit SCN, or snapshot rows were read after the check SCN |

The tool exits 0 when every table passes and 1 otherwise.

## Evidence

The evidence JSON holds the tool version, the start and finish times, the inputs (never a
password), the connector position check, and per table the result and reason, the key and
compared columns, the columns left out, the row counts and hashes of both sides, the difference
counts and samples, and counters for the records read. Its `sha256` is the SHA-256 of the
document's canonical JSON (keys sorted, no whitespace) without the `sha256` field. To check that
an evidence file is unchanged:

```bash
uv run python verify_cutover.py --check-evidence cutover-evidence.json
```

## Limits

- Compaction removes history. On a compacted topic, choose a check SCN close to the present, or
  values overwritten after the check SCN are missing from the topic side.
- The tool reads only topics written with the JSON converter. Topics written with Avro or
  Protobuf converters, and Confluent's flat record format, are not read yet.
- Duplicates and ordering are not checked; a repeated change during a takeover overlap leaves the
  same state and passes.
