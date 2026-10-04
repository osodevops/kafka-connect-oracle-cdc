---
title: Configuration reference
description: Every cdc.* property, generated from the connector's ConfigDef.
---

# Configuration reference

This section is generated from the connector's `ConfigDef` by the `e2e-tests` module and checked
in CI, so it cannot drift from the code. The generator lands with the first Kafka record
(plan increment P1-10); until then the groups below name what the core configuration already
defines.

| Group | Prefix | Examples |
|---|---|---|
| Database | `cdc.database.*` | host, port, service or SID or URL, user, password, PDB list, wallet and TLS truststore, connection properties |
| Capture | `cdc.capture.mode`, `cdc.archive.destination` | online or archive-only mining; archive destination by name |
| Mining | `cdc.mining.*` | target latency, max logs per step, fetch size, query timeout, session max age, decode threads, in-list limit |
| Transaction buffer | `cdc.buffer.*` | memory budget, spill directory and limit |
| Transaction journal | `cdc.txjournal.*` | topic, age and event thresholds |
| Transactions | `cdc.transaction.*` | max age and action, orphan check interval and action |
| Errors and retries | `cdc.on.decode.error`, `cdc.retry.*`, `cdc.log.sensitive.data` | decode error policy, retry budget, extra transient error codes |
