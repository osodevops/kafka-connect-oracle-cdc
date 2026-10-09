---
title: Installation
description: Install the plugin ZIP on Apache Kafka Connect, Strimzi or Amazon MSK Connect, and the versions it works with.
---

# Installation

The connector ships as a plugin ZIP in the Confluent Hub component layout: one directory,
`osodevops-kafka-connect-oracle-cdc-<version>/`, holding `manifest.json`, `lib/` (the connector,
its core library and their dependencies), `doc/` (licence and notice files) and `etc/` (an example
configuration). The Oracle JDBC driver (`ojdbc11`) is bundled under Oracle's Free Use Terms and
Conditions, reproduced in `doc/licenses/`. Jars the Connect worker provides itself, such as the
Connect API and the Kafka clients, are not bundled.

Releases are on the [GitHub release page](https://github.com/osodevops/kafka-connect-oracle-cdc/releases);
the current one is 0.1.1. Each release attaches:

| File | Contents |
|---|---|
| `osodevops-kafka-connect-oracle-cdc-<version>.zip` | The plugin ZIP |
| `osodevops-kafka-connect-oracle-cdc-<version>.spdx.json` | Software bill of materials for the ZIP (SPDX) |
| `oracle-cdc-doctor-<version>-cli.jar` | The preflight checker as a single jar |
| `SHA256SUMS` | SHA-256 checksums of the files above |

Download the ZIP and check it against the release's checksums:

```bash
VERSION=0.1.1
BASE=https://github.com/osodevops/kafka-connect-oracle-cdc/releases/download/v$VERSION
curl -fsSLO "$BASE/osodevops-kafka-connect-oracle-cdc-$VERSION.zip"
curl -fsSLO "$BASE/SHA256SUMS"
sha256sum --check --ignore-missing SHA256SUMS
```

The jars are also on [Maven Central](https://central.sonatype.com/namespace/sh.oso) under the group
`sh.oso` (`oracle-cdc-core`, `kafka-connect-oracle-cdc` and `oracle-cdc-doctor`), signed with the
OSO release key. A Connect worker needs the ZIP rather than the jars, because the ZIP carries the
dependencies and the Oracle JDBC driver.

To build the ZIP from source instead, use JDK 17 or 21; the repository's Maven wrapper fetches
Maven itself (see [building from source](../development/building.md)):

```bash
git clone https://github.com/osodevops/kafka-connect-oracle-cdc.git
cd kafka-connect-oracle-cdc
./mvnw -q package -DskipTests -DskipE2E
ls kafka-connect-oracle-cdc/target/*-kafka-connect-plugin.zip
```

The build names the file `kafka-connect-oracle-cdc-<version>-kafka-connect-plugin.zip`; its
contents are the same as the release ZIP.

The same build produces `oracle-cdc-doctor/target/oracle-cdc-doctor-<version>-cli.jar`, the
[doctor](../operations/doctor.md) as one runnable jar.

## Install the plugin

| Platform | Steps |
|---|---|
| Apache Kafka Connect | Unzip into a directory listed in the worker's `plugin.path` and restart the worker |
| Confluent Platform | `confluent-hub install` with the path of the ZIP, or unzip it into `plugin.path` |
| Strimzi | Add the ZIP as a plugin artefact in the `build` section of the `KafkaConnect` resource, or build an image with the plugin under `/opt/kafka/plugins`, as the [Strimzi lab](strimzi.md) does |
| Amazon MSK Connect | Upload the ZIP to S3 and create a custom plugin from it |

After the restart, `GET /connector-plugins` on the worker lists
`sh.oso.connect.oracle.OracleCdcSourceConnector`.

## Worker settings

- Distributed mode is recommended; the connector stores its position in Kafka Connect's offsets.
- For [exactly-once delivery](../concepts/exactly-once.md), every worker needs
  `exactly.once.source.support=enabled` (distributed mode only), and the brokers'
  `transaction.max.timeout.ms` must be longer than the connector's Kafka transactions.
- Give the connector broker access with `cdc.kafka.bootstrap.servers` (and `cdc.kafka.*` for
  security settings). It needs it to create its internal topics, to journal long transactions, to
  keep schema versions and to read signals (see the [ops topic](../reference/ops-topic.md#internal-topics)
  reference).
- Size the worker heap for `cdc.buffer.memory.max.bytes` (256 MiB by default) on top of the
  worker's usual needs, and give `cdc.buffer.spill.dir` a volume with room for
  `cdc.buffer.spill.max.bytes` (10 GiB by default).
- The connector runs one task. A `tasks.max` above one is accepted with a warning and ignored.

## Compatibility

| Component | Supported | What the test suites run today |
|---|---|---|
| Oracle Database | 19c and later (the doctor refuses older releases), single instance, CDB or non-CDB | Oracle Database Free 23.26.3, CDB with three PDBs. 19c and 21c are listed in the support policy for 1.0 and are not yet qualified |
| Kafka Connect | 3.6 and later (the offsets REST API used in the runbooks needs 3.6) | Apache Kafka 3.9.1 in the integration tests and the Docker Compose lab; Strimzi with Kafka 4 in the Kubernetes lab |
| Java | 17 and 21 | Builds and unit tests on 17 and 21 |
| Converters | JSON, and Avro with the [name adjustment modes](../reference/record-formats.md#avro-and-other-strict-naming-rules) | The JSON converter with and without schemas, and Apicurio Registry 3.3.3's Avro converter. Protobuf and other converters are not tested |
| Platforms | Apache Kafka Connect, Strimzi, Confluent Platform 7.6 and later, Amazon MSK Connect | Apache Kafka Connect and Strimzi. Exactly-once delivery on MSK Connect has not been verified |

Amazon RDS for Oracle 19c non-CDB is a preview: it passed the qualification suites, and
[its setup](../database-setup/amazon-rds.md) differs. Not supported yet: Oracle RAC (a database
with more than one enabled redo thread is refused at start with CDC-5001), the RDS CDB
architecture, Autonomous Database, and capture from a standby database (accepted in archive-only
mode but not qualified).
