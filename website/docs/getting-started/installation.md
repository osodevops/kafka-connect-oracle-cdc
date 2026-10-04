---
title: Installation
description: Install the plugin ZIP on Apache Kafka Connect, Strimzi or Amazon MSK Connect.
---

# Installation

Releases publish a plugin ZIP in the Confluent Hub component layout
(`osodevops-kafka-connect-oracle-cdc-<version>/` with `manifest.json`, `lib/`, `doc/` and
`etc/`). The Oracle JDBC driver (`ojdbc11`) is bundled under its Free Use Terms and Conditions,
reproduced in `doc/licenses/`.

Until the first release, build it:

```bash
mvn -q package -DskipTests
ls kafka-connect-oracle-cdc/target/*.zip
```

| Platform | Steps |
|---|---|
| Apache Kafka Connect | Unzip into a directory on `plugin.path` and restart the worker |
| Confluent Hub client | `confluent-hub install osodevops-kafka-connect-oracle-cdc-<version>.zip` |
| Strimzi | Add the ZIP as a `plugins` artifact in the `KafkaConnect` build section, or bake it into an image as the lab does |
| Amazon MSK Connect | Upload the ZIP to S3 and create a custom plugin; exactly-once is treated as unverified on MSK Connect until the qualification run completes |

Connect 3.6 or later is supported. Exactly-once delivery needs distributed mode with
`exactly.once.source.support=enabled` on every worker.
