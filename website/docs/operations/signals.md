---
title: Signals
description: Commands sent to a running connector through its signal topic.
---

# Signals

A running connector reads commands from its signal topic, `cdc.signals.topic` (by default the topic
prefix followed by `.cdc.signals`). Signals need `cdc.kafka.bootstrap.servers`; they do not write to
the source database and work in archive-only mode. They are not read in `snapshot_only` mode.

A signal is a JSON value whose record key is the connector's name (or its topic prefix). Records
with any other key are ignored, so one topic can serve several connectors:

```bash
kafka-console-producer --bootstrap-server kafka:9092 --topic app.cdc.signals \
  --property parse.key=true --property key.separator='|'
orders-cdc|{"id": "s-42", "type": "snapshot", "data": {"tables": ["FREEPDB1.APP.ORDERS"], "predicate": "STATUS <> 'ARCHIVED'"}}
```

| Type | What it does |
|---|---|
| `snapshot` | Snapshots the captured tables in `data.tables` (PDB.OWNER.TABLE), filtered by the optional SQL condition in `data.predicate`, while streaming continues. Its records carry `source.snapshot` `incremental`. Rejected while another snapshot runs. |
| `snapshot-pause` | Stops reading new chunks of the running snapshot; streaming continues. |
| `snapshot-resume` | Resumes a paused snapshot. |
| `snapshot-stop` | Ends the running snapshot; its unread chunks are not read. |
| `refresh-tables` | Resolves the include and exclude patterns again before the next mining step. |
| `log-state` | Writes the mined-to SCN, the transaction buffer's size and its largest open transactions to the ops topic. |

Every signal for the connector ends in a `signal-ack` event on the ops topic with its `id`, `type`
and `outcome`: `ok`, `rejected`, `unknown`, `invalid` or `failed`, and a `message` where there is
one. A signal is handled once: the connector's offset records the last handled signal, so a restart
reads only later ones. A snapshot started by signal is recorded in the offset as well and resumes
after a restart.
