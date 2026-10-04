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

Verified 4 October 2026 on an Apple Silicon workstation with Strimzi 1.2.0 and Kafka 4.3.1:
operator, Kafka and two Connect workers Ready; the plugin listed once by the workers; the
`KafkaConnector` and its task RUNNING; the Oracle StatefulSet restarted from its volume in about
15 seconds with ARCHIVELOG, supplemental logging and both PDBs intact.

Lessons baked into the manifests: Strimzi 1.x serves only `kafka.strimzi.io/v1` and wants
`groupId` and the storage topics as top-level `spec` fields; kubectl's discovery cache must be
purged after the CRDs are installed; the Oracle base must be the regular (not faststart) image
because faststart keeps the datafiles inside the image where a volume would hide them; the slim
image has neither `hostname` nor `rman`; a pod keeps its IPC namespace across container restarts,
so the wrapper clears `/dev/shm` and stale semaphores before starting Oracle.
