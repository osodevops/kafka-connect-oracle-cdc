---
title: Dashboards and alerts
description: Export the task metrics to Prometheus, and the Grafana dashboard and alert rules that ship with the connector.
---

# Dashboards and alerts

Each capture task registers one MXBean, `sh.oso.cdc:type=task,server=<prefix>`, with the attributes
listed in the [metrics reference](../reference/metrics.md). The repository ships three files under
`ops/` that turn them into a dashboard and alerts:

| File | What it is |
|---|---|
| `ops/jmx-exporter/oracle-cdc.yml` | Rules for the Prometheus JMX exporter: every numeric attribute becomes `oracle_cdc_<attribute>` (counters end in `_total`) with a `server` label. Generated from the code |
| `ops/grafana/oracle-cdc-connector.json` | A Grafana dashboard |
| `ops/alerts/prometheus-rules.yaml` | Prometheus alert rules |

A build check fails if the dashboard or the alert rules use a metric name the exporter rules do not
produce, so the three stay consistent.

## Export the metrics

Attach the Prometheus JMX exporter to each Connect worker as a Java agent with the shipped rules,
for example:

```bash
KAFKA_OPTS="-javaagent:/opt/jmx_prometheus_javaagent.jar=9404:/opt/oracle-cdc.yml"
```

or merge the rules into the exporter configuration the workers already use. Prometheus then scrapes
port 9404 of every worker. On Strimzi, add the rules to the ConfigMap that the `metricsConfig` of
the `KafkaConnect` resource points to.

## The dashboard

Import `ops/grafana/oracle-cdc-connector.json` and choose the Prometheus data source. It has a
`server` variable (the topic prefix) and these panels:

| Panel | What it shows |
|---|---|
| SCN lag | The database SCN minus the SCN the engine has mined to |
| Time behind source at last commit | How long after its commit the last transaction was queued |
| Rows mined per second, transactions committed per second | Throughput |
| Open transactions, buffer heap use, spill use | The transaction buffer |
| Last step duration | How long mining steps take |
| Step retries, timeouts and reconnects | Database and LogMiner trouble that the engine handled |
| DLQ records and decode failures | Rows that went to the DLQ topic |
| Records waiting for Connect | The queue between the engine and Kafka Connect |
| Journal chunks and LOB rows | Transaction journal and LOB activity |

## The alert rules

| Alert | Fires when | Severity |
|---|---|---|
| `OracleCdcTaskMetricsAbsent` | No task reports metrics for five minutes | critical |
| `OracleCdcNotAdvancing` | A task has neither mined nor polled for ten minutes | critical |
| `OracleCdcBehindSource` | Commits are published more than five minutes after they happened, for ten minutes | warning |
| `OracleCdcDlqRecords` | A task wrote records to the DLQ topic in the last 15 minutes | warning |
| `OracleCdcBufferNearMemoryLimit` | The buffer has held more than 90 per cent of `cdc.buffer.memory.max.bytes` for 15 minutes | info |
| `OracleCdcSpillNearLimit` | Spill use has been above 80 per cent of `cdc.buffer.spill.max.bytes` for five minutes | critical |
| `OracleCdcReconnecting` | A task reconnected to the database more than three times in 15 minutes | warning |

Load the file into Prometheus (`rule_files`) or convert it for your alerting system. A task that
stops is also visible in Kafka Connect's own task status, and on the
[ops topic](../reference/ops-topic.md) as a `stop` event with its error code and runbook.

## What the metrics cannot see yet

There is no alert on how close the resume position is to the oldest archived log, because the
connector does not read the archive retention. Watch `OldestOpenScn`, `MillisBehindSource` and the
queries in [redo sizing and archive retention](../database-setup/redo-sizing.md) instead.
