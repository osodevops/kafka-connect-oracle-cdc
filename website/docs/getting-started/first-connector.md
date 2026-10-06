---
title: First connector
description: A minimal configuration for capturing tables from one PDB, and what the connector does when it starts.
---

# First connector

Prepare the database first ([database setup](../database-setup/index.md)), then register a
connector with the Kafka Connect REST API:

```json
{
  "name": "orders-cdc",
  "config": {
    "connector.class": "sh.oso.connect.oracle.OracleCdcSourceConnector",
    "tasks.max": "1",
    "cdc.database.host": "db.example.internal",
    "cdc.database.port": "1521",
    "cdc.database.service": "ORCLCDB",
    "cdc.database.user": "c##cdc",
    "cdc.database.password": "${file:/opt/kafka/secrets/oracle.properties:password}",
    "cdc.database.pdbs": "ORCLPDB1",
    "cdc.tables.include": "ORCLPDB1\\.APP\\.ORDER.*",
    "cdc.tables.exclude": "ORCLPDB1\\.APP\\.ORDER_AUDIT",
    "cdc.topic.prefix": "app",
    "cdc.kafka.bootstrap.servers": "kafka:9092"
  }
}
```

```bash
curl -s -X POST -H 'Content-Type: application/json' --data @orders-cdc.json http://localhost:8083/connectors
curl -s http://localhost:8083/connectors/orders-cdc/status
```

Points worth knowing before the first run:

- **Where to connect.** In a container database the mining user connects to the root container
  (the `CDB$ROOT` service, `ORCLCDB` here), not to a PDB. The PDBs to capture are listed in
  `cdc.database.pdbs`; table patterns are regular expressions matched against `PDB.SCHEMA.TABLE`.
  For a non-container database leave `cdc.database.pdbs` empty and match `SCHEMA.TABLE`.
- **Check first.** Run `oracle-cdc-doctor check --config orders-cdc.json`. It reports what is
  missing with the SQL to fix it. Kafka Connect runs the same fast rules when the connector is
  created, so a configuration that passes the doctor is accepted.
- **Credentials.** Passwords are Connect `PASSWORD` types and never appear in logs. Use a config
  provider as shown rather than a literal.
- **Broker access.** `cdc.kafka.bootstrap.servers` lets the connector create its internal topics,
  journal long transactions, keep schema versions across restarts and read
  [signals](../operations/signals.md). Without it the connector still captures, but those features
  are off. Security settings for these clients go under `cdc.kafka.*`.
- **One task.** The engine pipelines mining and decoding internally; a `tasks.max` above one is
  accepted with a warning and ignored.

## What happens at the first start

1. The connector records the database's current SCN and identity as its start position and writes
   a heartbeat record to `app.cdc.heartbeat` (the prefix followed by `.cdc.heartbeat`). The
   heartbeat's offset makes the start position durable before the first change. A transaction
   already open at that moment is captured whole when it commits: the connector reads the redo
   from that transaction's start, so the redo since the start of the oldest open transaction must
   still be on disk. The worker log names that transaction.
2. With the default `cdc.snapshot.mode=initial`, it reads the rows the captured tables already hold
   and publishes them as `op=r` records, while it streams the changes made meanwhile (see
   [snapshots](../concepts/snapshots.md)). Set `cdc.snapshot.mode=none` to stream only.
3. Changes then flow to one topic per table, `app.ORCLPDB1.APP.ORDERS` for example, in the
   [Debezium-compatible format](../reference/record-formats.md).
4. On a quiet database a heartbeat every ten seconds (`cdc.heartbeat.interval.ms`) moves the
   committed offset forward without writing to the source database.
5. A `startup` event on `app.cdc.ops` records the start position; if the task ever stops, a `stop`
   event names the error code and its [runbook](../operations/runbooks/index.md).

The full property list is generated from the code into the
[configuration reference](../reference/configuration/index.md).
