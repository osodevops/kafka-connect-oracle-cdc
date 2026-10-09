---
title: Local testing
description: The test tiers, how to run the Oracle Database Free suites with Testcontainers, and the Docker Compose and Strimzi labs.
---

# Local testing

## Test tiers

| Tier | Classes | Needs | What it covers |
|---|---|---|---|
| T0 | `*Test` in every module | Nothing | The engine against `FakeLogMiner`, a scripted LogMiner, plus the regression corpus and the documentation generators |
| T1 engine | `*EngineIT` | Docker | The engine and the doctor against Oracle Database Free |
| T1 connector | `*ConnectorIT` | Docker | The connector in a real Kafka Connect worker, with Kafka and Oracle Database Free |
| T2 nightly | `*NightlyIT` | Docker, time | Faults and long runs: worker kills under exactly-once, broker and database restarts, network faults, log switch storms, archive purges, a five million row transaction, a soak trial |
| T4 qualification | `*QualIT` | Docker, or an external database | The checks a platform must pass: the doctor finds nothing blocking, capture in online and archive-only mode, dictionary builds. Runs against the container nightly, and against an external database for platform qualification |

The tier is the class name suffix. Each data-loss bug is fixed together with a regression test
named by the invariant it protects and tagged with its public issue, for example
`@Tag("dbz-2504")`; the regression corpus lists them.

```bash
./mvnw clean verify -DskipE2E                               # T0 and every quality gate
./mvnw install -DskipTests -DskipE2E                        # the e2e module tests the installed jars
./mvnw -pl e2e-tests verify -De2e.groups=engine             # T1 engine
./mvnw -pl e2e-tests verify -De2e.groups=connector          # T1 connector
./mvnw -pl e2e-tests verify -De2e.groups=nightly            # T2
./mvnw -pl e2e-tests verify -De2e.groups=nightly -Dit.test=LargeTransactionNightlyIT
./mvnw -pl e2e-tests verify -De2e.groups=qual               # T4 suites against the container
```

A tier that selects no class fails the build, so a mistyped group never passes by running nothing.

## The test database

The suites build their own Oracle Database Free image from `docker/test-oracle`: ARCHIVELOG mode,
minimal supplemental logging, three pluggable databases (`FREEPDB1` to `FREEPDB3`), small online
redo logs for frequent log switches, and the capture user with the grants that
`oracle-cdc-doctor setup-sql` generates. The image is tagged with a digest of that directory, so a
change to it builds a new image on the next run.

One database container serves every suite in a JVM, and suites keep apart by schema rather than by
restarting the database. Run one tier at a time: two runs at once share the test network and the
build directory.

On Apple Silicon the image runs emulated, and the suites take noticeably longer than on an x86
host. Give Docker enough memory for the database, a Kafka broker and a Connect worker together.

## An external database

The qualification suites also run against a database you point them at, which is how a platform
such as Amazon RDS for Oracle is qualified. Passwords come from the environment so they never
appear in a build log or an evidence file:

```bash
export CDC_E2E_PASSWORD='<capture user password>'
export CDC_E2E_ADMIN_PASSWORD='<master user password>'
./mvnw -pl e2e-tests verify -De2e.groups=qual \
  -De2e.external.url=jdbc:oracle:thin:@//<host>:1521/<service> \
  -De2e.external.user=CDC \
  -De2e.external.admin.user=<master user> \
  -De2e.external.platform=rds \
  -De2e.external.apply.setup=true
```

With `e2e.external.apply.setup=true` the run first applies `setup-sql` for the platform as the
admin user, so it proves the script too. The admin user creates and drops one schema per suite
and switches logs (through `rdsadmin` on RDS). Only non-CDB targets are supported so far. The
evidence files under `e2e-tests/target` record the platform and the JDBC URL of the target.

## Nightly options

| Property | Default | Use |
|---|---|---|
| `-Dnightly.kills` | 25 | Worker kills in the exactly-once kill loop |
| `-Dnightly.large.rows` | 5,000,000 | Rows in the large transaction suite's single transaction |

Every nightly suite writes evidence JSON (the workload, the faults it induced and the correctness
oracle's verdict) under `e2e-tests/target`.

## Labs

| Lab | Path | What it is for |
|---|---|---|
| Docker Compose | `lab/local/compose` | Oracle Database Free, Kafka, a Connect worker, Prometheus and Grafana with the dashboard; the [quick start](../getting-started/quick-start.md) |
| Strimzi on minikube | `lab/local/k8s` | The operator, a KRaft cluster, two Connect workers and an Oracle StatefulSet, with eleven edge cases (`edge-cases.sh`); see [Strimzi on minikube](../getting-started/strimzi.md) |

Both labs use the same test database image. The correctness oracle in `bench` checks a lab run the
same way the suites do: a seeded workload writes a ledger of every committed transaction, and
`bench check` compares the ledger and the table state with what reached Kafka.
