---
title: Building from source
description: Build the connector, the plugin ZIP and the oracle-cdc-doctor CLI from source, run the quality gates and regenerate the generated reference pages.
---

# Building from source

You need JDK 17 or 21. The repository carries the Maven wrapper, which downloads Maven 3.9.16 and
checks its checksum, so `./mvnw` needs no Maven installation. Docker is needed only for the
integration suites ([local testing](local-testing.md)).

```bash
git clone https://github.com/osodevops/kafka-connect-oracle-cdc.git
cd kafka-connect-oracle-cdc
./mvnw clean verify -DskipE2E
```

That runs every quality gate and every unit test without Docker:

- Spotless formatting (google-java-format) for Java and the POMs;
- SpotBugs;
- JaCoCo line coverage of at least 80 per cent on `oracle-cdc-core`;
- the licence allowlist, which fails the build on any GPL or AGPL dependency;
- the check that the generated reference pages on this site match the code.

## Modules

| Module | What it builds |
|---|---|
| `oracle-cdc-core` | The capture engine: mining, transaction buffer and journal, positions, schema versions, doctor rules |
| `kafka-connect-oracle-cdc` | `sh.oso.connect.oracle.OracleCdcSourceConnector` and the plugin ZIP |
| `oracle-cdc-doctor` | The preflight checker, setup script generator, redo profiler and admin CLI, as one runnable jar and a container image |
| `e2e-tests` | Integration suites against Oracle Database Free and a real Kafka Connect worker, and the generators of the reference pages |
| `bench` | The workload generator and correctness oracle the suites and labs use |
| `tools/migration` | The Python migration tools for moving from Confluent or Debezium |

## Artefacts

To build the artefacts without running the tests:

```bash
./mvnw -q package -DskipTests -DskipE2E
```

| File | Path |
|---|---|
| Plugin ZIP | `kafka-connect-oracle-cdc/target/kafka-connect-oracle-cdc-<version>-kafka-connect-plugin.zip` |
| Doctor CLI | `oracle-cdc-doctor/target/oracle-cdc-doctor-<version>-cli.jar` |

The ZIP has the same contents as the one attached to each GitHub release
([installation](../getting-started/installation.md)).

## Formatting

```bash
./mvnw spotless:apply
```

On JDK 17, `.mvn/jvm.config` adds the compiler exports google-java-format needs.

## Generated reference pages

The configuration reference, the metrics reference with the JMX exporter rules, and the database
setup script on this site are generated from the code. Never edit them by hand; regenerate them
after changing a `cdc.*` key, a metric or the setup script:

```bash
./mvnw -pl e2e-tests -am test -DskipE2E -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=ConfigDocsGeneratorTest -Dconfigdocs.update=true
./mvnw -pl oracle-cdc-core test -Dtest=MetricsReferenceTest -Dmetricsdocs.update=true
./mvnw -pl oracle-cdc-core test -Dtest=SetupSqlDocsTest -Dsetupsqldocs.update=true
```

The quality gates fail when a generated page no longer matches the code.

## This site

The documentation lives in `website/` (Docusaurus). Broken links and anchors fail the build:

```bash
cd website
npm ci
npm run build
```
