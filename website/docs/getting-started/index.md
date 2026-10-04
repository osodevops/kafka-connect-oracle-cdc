---
title: Getting started
description: Run Oracle Database Free, Apache Kafka and Kafka Connect with the connector on a laptop.
---

# Getting started

Two local labs ship in the repository under `lab/local`. Both build the connector from source,
start Oracle Database Free with ARCHIVELOG and supplemental logging already configured, and
register a connector.

| Lab | Stack | Use it for |
|---|---|---|
| [Docker Compose](quick-start.md) | Oracle Free, Kafka (KRaft), Kafka Connect, optional Prometheus, Grafana, Toxiproxy and Debezium | First contact, debugging, the fastest edit and rerun loop |
| [Strimzi on minikube](strimzi.md) | Strimzi operator, Kafka 4.x, two Connect workers, Oracle StatefulSet | Kubernetes behaviour: rolling updates, rebalances, pod kills |

Both labs use the derived test image in `docker/test-oracle`, which is Oracle Database Free with
first-start hooks. Oracle Database Free is downloaded from Docker Hub under its own licence
terms when you start the lab; nothing from Oracle is redistributed by this project.

Once a lab is running, continue with [installation](installation.md) for production workers and
[first connector](first-connector.md) for the configuration that gets a table flowing.
