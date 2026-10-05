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

Register the example connector, generate a workload with the bench tool and read the records:

```bash
make -C lab/local/compose register
java -jar bench/target/bench-*-cli.jar workload \
  --url jdbc:oracle:thin:@//localhost:1521/FREEPDB1 --user workload --password workload \
  --spec bench/src/main/resources/workloads/simple.json --transactions 30 --reset
make -C lab/local/compose consume TOPIC=cdc.FREEPDB1.WORKLOAD.WL_T1
curl -s localhost:8083/connectors/oracle-cdc/offsets | python3 -m json.tool
java -jar oracle-cdc-doctor/target/oracle-cdc-doctor-*-cli.jar check \
  --config lab/local/compose/examples/connector-freepdb1.json
```

Tables created after the connector started are picked up at the CREATE TABLE in the redo, so the
bench tool's `--reset` needs no restart. LOB columns are left out of the records unless
`cdc.lob.mode` is `inline` or `reselect` (see [record formats](../reference/record-formats.md)); to
compare a workload with `lobWeight` above 0 using `bench check`, run the connector with
`cdc.lob.mode=reselect`.
