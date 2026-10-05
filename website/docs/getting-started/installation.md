---
title: Installation
description: Install the plugin ZIP on Apache Kafka Connect, Strimzi or Amazon MSK Connect.
---

# Installation

Releases publish a plugin ZIP in the Confluent Hub component layout
(`osodevops-kafka-connect-oracle-cdc-<version>/` with `manifest.json`, `lib/`, `doc/` and
`etc/`). The Oracle JDBC driver (`ojdbc11`) is bundled under its Free Use Terms and Conditions,
reproduced in `doc/licenses/`.

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

Until the first release, build it:

```bash
mvn -q package -DskipTests
ls kafka-connect-oracle-cdc/target/*.zip
```

The build names the file `kafka-connect-oracle-cdc-<version>-kafka-connect-plugin.zip`; its
contents are the same as the release ZIP.

| Platform | Steps |
|---|---|
| Apache Kafka Connect | Unzip into a directory on `plugin.path` and restart the worker |
| Confluent Hub client | `confluent-hub install osodevops-kafka-connect-oracle-cdc-<version>.zip` |
| Strimzi | Add the ZIP as a `plugins` artifact in the `KafkaConnect` build section, or bake it into an image as the lab does |
| Amazon MSK Connect | Upload the ZIP to S3 and create a custom plugin; exactly-once is treated as unverified on MSK Connect until the qualification run completes |

Connect 3.6 or later is supported. Exactly-once delivery needs distributed mode with
`exactly.once.source.support=enabled` on every worker.
