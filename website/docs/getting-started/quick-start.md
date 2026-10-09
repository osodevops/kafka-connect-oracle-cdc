---
title: Quick start with Docker Compose
description: Build the plugin, start Oracle Database Free, Kafka and Connect with Docker Compose, register the connector and watch changes arrive.
---

# Quick start with Docker Compose

The Docker Compose lab in `lab/local/compose` runs Oracle Database Free, a single Kafka broker in
KRaft mode and a Kafka Connect worker with the connector, all on one machine.

Requirements: Docker with Compose v2, JDK 17 or 21, Maven 3.9, `curl`, `python3` and
[kcat](https://github.com/edenhill/kcat) for the topic targets, and about 8 GB of free memory for
the containers. The lab is verified on an Apple silicon workstation.

## Start the stack

```bash
git clone https://github.com/osodevops/kafka-connect-oracle-cdc.git
cd kafka-connect-oracle-cdc
make -C lab/local/compose up          # build the plugin, start Oracle, Kafka and Connect, wait until healthy
make -C lab/local/compose register    # register examples/connector-freepdb1.json
make -C lab/local/compose status      # connector and task state
```

`up` runs `mvn package -DskipTests -DskipE2E`, copies the plugin directory from the build into
`lab/local/compose/plugins`, which the Connect container mounts, and starts the containers. When it
returns, Connect is on `localhost:8083`, Kafka on `localhost:9092` and Oracle on
`localhost:1521/FREE`.

What the containers do on first start:

1. The Oracle container is built from `docker/test-oracle`, Oracle Database Free with first-start
   hooks: ARCHIVELOG mode with a local archive destination, minimal supplemental logging, six
   online redo groups of 50 MB, a second PDB (`FREEPDB2`), the common capture user `C##CDC`
   (password `cdc`) with the grants that `oracle-cdc-doctor setup-sql --profile lab` generates, and a
   `WORKLOAD` schema in each PDB. The first start takes a few minutes; later starts are quick.
   Oracle Database Free is downloaded under Oracle's own licence terms; nothing from Oracle is
   redistributed by this project.
2. Kafka starts as a single KRaft node, with `transaction.max.timeout.ms` at 15 minutes.
3. Connect starts in distributed mode with `exactly.once.source.support=enabled` and the JSON
   converters with schemas. The Prometheus JMX exporter agent (downloaded by `up`) runs in the
   worker with the shipped rules, so the task metrics are on `localhost:9404`.

The example connector captures every table of the `WORKLOAD` schema in `FREEPDB1` except the
workload ledger, with the topic prefix `cdc`:

```json
{
  "name": "oracle-cdc",
  "config": {
    "connector.class": "sh.oso.connect.oracle.OracleCdcSourceConnector",
    "tasks.max": "1",
    "cdc.database.host": "oracle",
    "cdc.database.port": "1521",
    "cdc.database.service": "FREE",
    "cdc.database.user": "C##CDC",
    "cdc.database.password": "cdc",
    "cdc.database.pdbs": "FREEPDB1",
    "cdc.topic.prefix": "cdc",
    "cdc.tables.include": "FREEPDB1\\.WORKLOAD\\..*",
    "cdc.tables.exclude": "FREEPDB1\\.WORKLOAD\\.WL_LEDGER"
  }
}
```

## Generate changes and read them

The `bench` tool from the same build runs a seeded workload of inserts, updates, deletes, LOB
writes, savepoint rollbacks and DDL against the `WORKLOAD` schema:

```bash
export WORKLOAD_PASSWORD=workload   # the lab image's local test user
java -jar bench/target/bench-*-cli.jar workload \
  --url jdbc:oracle:thin:@//localhost:1521/FREEPDB1 --user workload --password-env WORKLOAD_PASSWORD \
  --spec bench/src/main/resources/workloads/simple.json --transactions 30 --reset
make -C lab/local/compose topics
make -C lab/local/compose consume TOPIC=cdc.FREEPDB1.WORKLOAD.WL_T1
curl -s localhost:8083/connectors/oracle-cdc/offsets | python3 -m json.tool
```

Tables created after the connector started are picked up when their CREATE TABLE is mined, so the
workload's `--reset` needs no connector restart. Records follow the
[record formats](../reference/record-formats.md) reference. LOB columns are left out unless
`cdc.lob.mode` is `inline` or `reselect`.

To check that every committed transaction of the workload arrived exactly as the database holds
it, run the correctness oracle. It compares each table's topic with the database as of the
highest commit SCN seen, and the transactions on the topics with the workload's ledger. The
example workload writes LOB values, so register the connector with `"cdc.lob.mode": "reselect"`
added to the example configuration before running the workload:

```bash
make -C lab/local/compose check       # writes its evidence to /tmp/oracle-cdc-evidence.json
```

`make soak` runs the same oracle at check points during a long paced workload; the lab's README
(`lab/local/compose/README.md`) describes it.

## Run the doctor

The doctor checks the database against a connector configuration. From the host, the database is
on `localhost` rather than `oracle`:

```bash
sed 's/"cdc.database.host": "oracle"/"cdc.database.host": "localhost"/' \
  lab/local/compose/examples/connector-freepdb1.json > /tmp/connector-local.json
java -jar oracle-cdc-doctor/target/oracle-cdc-doctor-*-cli.jar check --config /tmp/connector-local.json
```

## More

```bash
make -C lab/local/compose logs        # follow the Connect worker log
make -C lab/local/compose oracle-sql  # sqlplus as the capture user at CDB$ROOT
make -C lab/local/compose down        # stop everything and remove the volumes
```

Optional profiles, passed as `PROFILES="--profile <name>"` to `up`:

| Profile | Adds |
|---|---|
| `observability` | Prometheus with the shipped alert rules and Grafana (on `localhost:3000`) with the shipped dashboard; Prometheus scrapes the worker's JMX exporter on port 9404 (see [dashboards and alerts](../operations/dashboards-and-alerts.md)) |
| `chaos` | Toxiproxy between Connect and Oracle; point the connector at `toxiproxy:11521` |
| `debezium` | A Debezium 3.7 Connect worker on the same Kafka and database, on port 8084, for side-by-side runs |

For production workers, continue with [installation](installation.md) and
[first connector](first-connector.md).
