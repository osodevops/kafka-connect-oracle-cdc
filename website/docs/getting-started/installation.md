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

No release has been published yet. Each release will attach to its GitHub release page:

| File | Contents |
|---|---|
| `osodevops-kafka-connect-oracle-cdc-<version>.zip` | The plugin ZIP |
| `osodevops-kafka-connect-oracle-cdc-<version>.spdx.json` | Software bill of materials for the ZIP (SPDX) |
| `oracle-cdc-doctor-<version>-cli.jar` | The preflight checker as a single jar |
| `SHA256SUMS` | SHA-256 checksums of the files above |

The jars are also published to Maven Central under the group `sh.oso`, signed. Every release file
carries a build provenance attestation, so you can check that it was built from this repository
by the release workflow:

```bash
sha256sum --check --ignore-missing SHA256SUMS
gh attestation verify osodevops-kafka-connect-oracle-cdc-<version>.zip --repo osodevops/kafka-connect-oracle-cdc
```

Until the first release, build the ZIP from source with JDK 17 or 21 and Maven 3.9:

```bash
git clone https://github.com/osodevops/kafka-connect-oracle-cdc.git
cd kafka-connect-oracle-cdc
mvn -q package -DskipTests -DskipE2E
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
| Strimzi | Add the ZIP as a `plugins` artifact in the `KafkaConnect` build section, or build an image with the plugin under `/opt/kafka/plugins`, as the [Strimzi lab](strimzi.md) does |
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
| Oracle Database | 19c and later (the doctor refuses older releases), single instance, CDB or non-CDB | Oracle Database Free 23.26.3, CDB with two PDBs. 19c and 21c are listed in the support policy for 1.0 and are not yet qualified |
| Kafka Connect | 3.6 and later (the offsets REST API used in the runbooks needs 3.6) | Apache Kafka 3.9.1 in the integration tests and the Docker Compose lab; Strimzi with Kafka 4 in the Kubernetes lab |
| Java | 17 and 21 | Builds and unit tests on 17 and 21 |
| Platforms | Apache Kafka Connect, Strimzi, Confluent Platform 7.6 and later, Amazon MSK Connect | Apache Kafka Connect and Strimzi. Exactly-once delivery on MSK Connect has not been verified |

Not supported yet: Oracle RAC, Amazon RDS for Oracle, Autonomous Database, and capture from a
standby database.
