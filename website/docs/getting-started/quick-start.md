---
title: Quick start with Docker Compose
description: Build the plugin, start Oracle Database Free, Kafka and Connect, and register the connector.
---

# Quick start with Docker Compose

Requirements: Docker Desktop (or Docker Engine with Compose v2), JDK 17, Maven 3.9 and about
8 GB of free memory. The lab is tested on Apple silicon and x86-64.

```bash
git clone https://github.com/osodevops/kafka-connect-oracle-cdc.git
cd kafka-connect-oracle-cdc
mvn -q package -DskipTests          # builds the plugin directory the Connect worker mounts
make -C lab/local/compose up        # oracle, kafka and connect
make -C lab/local/compose status    # waits until Connect lists the plugin
make -C lab/local/compose register  # registers examples/connector-freepdb1.json
```

What happens on the first start:

1. The Oracle container runs its first-start hooks: ARCHIVELOG, minimal supplemental logging,
   50 MB redo groups, a local archive destination and the `C##CDC` mining user. The first start
   takes a few minutes; later starts are quick.
2. Kafka starts as a single KRaft node with `transaction.max.timeout.ms` raised so exactly-once
   source transactions can span a long Oracle transaction.
3. Connect starts with `exactly.once.source.support=enabled` and the plugin mounted from the
   Maven build output.

Useful targets:

```bash
make -C lab/local/compose topics      # list topics
make -C lab/local/compose consume T=cdc.FREEPDB1.WORKLOAD.WL_T1
make -C lab/local/compose oracle-sql  # sqlplus as the mining user
make -C lab/local/compose down
```

Profiles add Prometheus and Grafana (`observability`), Toxiproxy (`chaos`) and a Debezium
worker (`debezium`) for side-by-side comparison on the same database.

Until the capture engine lands, the registered connector starts, reports RUNNING and idles;
the doctor and the workload generator already work against this database:

```bash
java -jar oracle-cdc-doctor/target/oracle-cdc-doctor-*-cli.jar check \
  --config lab/local/compose/examples/connector-freepdb1.json
java -jar bench/target/bench-*-cli.jar workload \
  --url jdbc:oracle:thin:@//localhost:1521/FREEPDB1 --user workload --password workload \
  --spec bench/src/main/resources/workloads/simple.json --reset
```
