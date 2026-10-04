# PRD-05: `oracle-cdc-doctor`, Operations and Observability

**Status:** Draft for implementation
**Modules:** `oracle-cdc-doctor` (Java CLI, picocli, container image `ghcr.io/osodevops/oracle-cdc-doctor`), `ops/` (dashboards, alerts, runbooks)
**Depends on:** PRD-00 probes, PRD-01 metrics
**Clean-room note:** Our own design.

---

## 1. Objective

Remove most of the DBA back-and-forth and day-two guesswork: check every prerequisite before go-live, tell the DBA exactly what to run, show why the connector is slow, and give operators safe commands for recovery (PP-05, PP-06, PP-11, PP-14).

## 2. `oracle-cdc-doctor` commands

| Command | Purpose |
|---|---|
| `oracle-cdc-doctor check --config connector.json` | Run all rules; output a report (Markdown, JSON, JUnit XML for CI). Exit 0 no blocking findings, 1 blocking findings, 2 warnings only |
| `oracle-cdc-doctor setup-sql --config connector.json` | Generate a commented SQL script for the DBA: user creation (common user in CDB), grants, supplemental logging per captured table, optional `DBMS_LOGMNR_D.BUILD` grant, recommended redo log changes. Variants for on-premises, RDS (`rdsadmin` procedures) and Autonomous |
| `oracle-cdc-doctor redo-profile --window 2h` | Mine an archived window with no table filter and report redo operations by owner, table and operation, flag truncate-and-reload patterns, and estimate what share of mined rows the connector would discard (the Pepkor diagnosis, automated; see [Debezium blog](https://debezium.io/blog/2026/04/20/oracle-cdc-replication-lag/)) |
| `oracle-cdc-doctor sizing` | Log switch rate per thread over 7 days, archive generation per day, recommended online log size and archive retention for a stated maximum downtime |
| `oracle-cdc-doctor explain-lag --connect-url` | Read connector metrics and break lag into mining query, fetch, decode, buffer wait, emit and Kafka produce time, with the top cause in plain English |

## 3. Rules (`check`)

| ID | Rule | Severity |
|---|---|---|
| DOC-1 | ARCHIVELOG mode enabled | Blocking |
| DOC-2 | Minimal supplemental logging at database level (or per-PDB on 26ai local undo) | Blocking |
| DOC-3 | `ALL` column supplemental logging on each captured table (PK-only is a warning with partial before images) | Blocking or warning |
| DOC-4 | Required privileges and views accessible | Blocking |
| DOC-5 | Captured tables containing BFILE, nested tables, identity columns, temporal validity, PKREF or PKOID (LogMiner ignores the whole table) | Blocking |
| DOC-6 | Captured table or column names over 30 characters | Blocking |
| DOC-7 | Captured tables without primary or usable unique key | Blocking unless `cdc.key.missing` allows |
| DOC-8 | LOB, LONG, XMLTYPE columns and chosen LOB mode consequences | Info or warning |
| DOC-9 | Log switch rate above six per hour on any thread at peak | Warning, with recommended size |
| DOC-10 | Archive retention shorter than `cdc.txjournal.threshold.ms` plus planned maximum downtime | Warning |
| DOC-11 | `UNDO_RETENTION` below the longest expected chunk read | Warning |
| DOC-12 | Archive destination valid and readable for all threads | Blocking |
| DOC-13 | RAC: all threads enabled; FAN availability | Info |
| DOC-14 | Database role and open mode match `cdc.capture.mode` | Blocking |
| DOC-15 | Version and RU supported (matrix in docs); per-PDB mining needs 19c RU10 or later | Blocking |
| DOC-16 | Fixed-object statistics present (affects LogMiner view performance) | Warning |
| DOC-17 | Kafka: internal topics exist with correct cleanup policy, or topic creation allowed | Blocking |
| DOC-18 | Connect exactly-once enabled when config requires it | Blocking |
| DOC-19 | Network idle timeout guard configured below known load balancer limits | Info |
| DOC-20 | Dictionary build privilege present (enables DDL lag recovery) | Info |

The connector's `validate()` runs DOC-1 to DOC-7, DOC-12, DOC-14, DOC-15, DOC-17 and DOC-18 in fast mode.

## 4. `oracle-cdc-admin` (subcommands of the same CLI)

| Command | Purpose |
|---|---|
| `oracle-cdc-admin offsets show` | Read the connector offset through `GET /connectors/{name}/offsets` and decode it |
| `oracle-cdc-admin offsets set --scn` | Stop connector, `PATCH /connectors/{name}/offsets`, with a mandatory `--reason` recorded on the ops topic; refuses if the SCN's logs are missing ([KIP-875](https://cwiki.apache.org/confluence/display/KAFKA/KIP-875:%20First-class%20offsets%20support%20in%20Kafka%20Connect)) |
| `oracle-cdc-admin resnapshot --tables` | Recovery after `OracleCdcPurgedException`: set position past the gap and signal snapshots of affected tables (PRD-02 SNAP-7) |
| `oracle-cdc-admin transactions` | List buffered and journaled transactions with age, size, user and whether present in `GV$TRANSACTION` |
| `oracle-cdc-admin journal inspect` | Show journal state and consistency |

No documentation will ever tell users to write raw offsets with `kafkacat`.

## 5. Metrics

Exposed through JMX (`sh.oso.connect.oracle:type=...,connector=...`) and the Prometheus JMX exporter configuration shipped in `ops/`.

| Group | Metrics |
|---|---|
| Lag | `lag.ms` (commit timestamp to emit), `mined.scn`, `current.scn`, `scn.lag`, `resume.scn.age.ms` |
| Mining | step count, window SCN span, logs per step, rows fetched, rows filtered, query time, fetch time, decode time, timeouts, halvings, catch-up sessions active |
| Buffer | open transactions, heap bytes, spilled bytes, journaled transactions, oldest transaction age, top transactions (CORE-TX-8) |
| Emit | records per second by table, Kafka transaction commit time, batches split |
| Logs | per thread: current sequence, oldest needed sequence, switches per hour |
| Snapshot | per table progress (PRD-02 SNAP-12) |
| Errors | retries by ORA code, DLQ records, orphan releases, discards |
| Source impact | mining session CPU, PGA used |

## 6. Dashboards, alerts, runbooks

- `ops/grafana/oracle-cdc-connector.json`: overview, lag breakdown, buffer, logs, snapshot, errors.
- `ops/alerts/prometheus-rules.yaml`: lag above SLO for 10 minutes; `resume.scn.age.ms` approaching archive retention (80 per cent); oldest transaction age above threshold; spill above 70 per cent of cap; task failed; orphan released; DLQ non-empty.
- `website/docs/runbooks/`: one page per exception class in PRD-00 CORE-ERR with cause, checks, and the exact `oracle-cdc-admin` command. Error messages link to these pages.

## 7. Acceptance criteria

- [ ] `check` detects each rule's failure in a seeded test database (one fixture per rule).
- [ ] `setup-sql` output applied to a fresh Oracle Free container makes `check` pass.
- [ ] `redo-profile` identifies a seeded truncate-and-reload job on a non-captured table as the top redo source.
- [ ] `explain-lag` names the injected bottleneck (slow Kafka, slow mining query, large transaction) in three scripted scenarios.
- [ ] `offsets set` refuses an SCN whose logs are purged.
- [ ] Dashboard JSON imports into Grafana 11 and every panel has data in the e2e environment.
