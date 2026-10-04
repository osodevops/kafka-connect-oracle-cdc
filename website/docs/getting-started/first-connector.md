---
title: First connector
description: The minimum configuration for capturing one table from one PDB.
---

# First connector

```json
{
  "name": "oracle-cdc",
  "config": {
    "connector.class": "sh.oso.connect.oracle.OracleCdcSourceConnector",
    "tasks.max": "1",
    "cdc.database.host": "oracle",
    "cdc.database.port": "1521",
    "cdc.database.service": "FREE",
    "cdc.database.user": "c##cdc",
    "cdc.database.password": "${file:/opt/kafka/secrets/oracle.properties:password}",
    "cdc.database.pdbs": "FREEPDB1",
    "cdc.tables.include": "FREEPDB1\\.WORKLOAD\\.WL_T.*",
    "cdc.tables.exclude": "FREEPDB1\\.WORKLOAD\\.WL_LEDGER",
    "cdc.topic.prefix": "cdc"
  }
}
```

Points worth knowing before the first run:

- The mining user connects to the container database root (`FREE` here), not to the PDB. The
  PDBs to capture are listed in `cdc.database.pdbs`; table patterns are matched against
  `PDB.SCHEMA.TABLE`.
- Run `oracle-cdc-doctor check --config first-connector.json` first. It reports what is missing
  with the SQL to fix it; the connector's own `validate` runs the same fast rules.
- One task per connector. The engine pipelines mining and decoding internally; `tasks.max`
  above one is accepted with a warning and ignored.
- Passwords are Connect `PASSWORD` types and never appear in logs. Use a config provider as
  shown rather than a literal.

The full property list is generated from the code into the
[configuration reference](../reference/configuration/index.md).
