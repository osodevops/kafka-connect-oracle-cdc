# lab/local/k8s: minikube and Strimzi end-to-end lab

Strimzi (pinned in `versions.env`) running Kafka in KRaft mode and a two-worker Kafka Connect
cluster whose image carries the OSO CDC Connector plugin, plus Oracle Database Free from
`docker/test-oracle` as a StatefulSet. Everything is built locally for arm64 and loaded with
`minikube image load`; no registry or public artefact URL is needed.

```bash
make -C lab/local/k8s up        # cluster, operator, Kafka, images, Connect, connector
make -C lab/local/k8s smoke     # plugin listed once, connector RUNNING, Oracle pod restart survives
make -C lab/local/k8s status
make -C lab/local/k8s down
```

Edge cases (`make chaos-*`, and `edge-cases.sh` once the connector emits records) follow the
matrix in the plan: worker SIGKILL mid-transaction, Strimzi rolling update, two-worker rebalance,
broker restart, Oracle pod restart from the PVC, NetworkPolicy partition, Toxiproxy latency, PVC
loss in a disposable namespace, offsets topic compaction, connector config update, operator
restart during a change, plugin upgrade with existing offsets, OOMKill under a large transaction,
and Strimzi connector offsets ConfigMap list, alter and reset. Verification uses the correctness
oracle in `bench` and `read_committed` consumers.

Single-node Kafka cannot show ISR or coordinator failover; use a three-node pool for those cases.
