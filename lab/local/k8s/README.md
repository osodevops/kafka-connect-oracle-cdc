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
- A `StatefulSet` pod must never be deleted with `--force --grace-period=0`: the old instance keeps
  running against the volume and the replacement fails with ORA-01081. `edge-cases.sh` and
  `make chaos-oracle-restart` use a graceful delete (120 s) and the container wrapper runs
  `shutdown abort` and clears `/dev/shm` before `container-entrypoint.sh`, so a crashed pod still
  comes back.
- The single-replica Strimzi operator lost its leader lease on a busy laptop and exited with
  "Stopped being a leader => exiting" every few minutes; `make operator` now sets
  `STRIMZI_LEADER_ELECTION_ENABLED=false` (one replica needs no election).
- `minikube image load` of an existing tag never reaches the pods: the node keeps its copy of
  `oracle-cdc-connect:dev`. `make connect-image` now builds a uniquely tagged image, loads it and
  patches the `KafkaConnect` spec so Strimzi rolls both workers onto it (`:dev` is only the first
  apply's value). A worker killed before that fix came back on the old plugin.
- `edge-cases.sh` now runs a seeded bench workload (`WORKLOAD_SECONDS`, default 30) through
  every fault over a port-forward to the Oracle pod and then requires every committed ledger
  transaction to be in the workload topics (read_committed), reporting duplicated transactions.
  It needs `bench/target/bench-*-cli.jar` (`mvn -pl bench package`).
- Observed with ledger verification: SIGKILL of a worker, a manual rolling update and a task
  rebalance (deleting the worker that owns the task) all end with every committed transaction in
  Kafka and no duplicated transaction. After the rebalance the task resumed only after about
  four minutes, Connect's default `scheduled.rebalance.max.delay.ms`; lower it on the
  `KafkaConnect` when faster failover matters more than avoiding needless rebalances.
- A fault that restarts Oracle also kills the generator's own connections; the harness treats a
  stopped generator as expected for those cases and still requires every ledger transaction to be
  in Kafka.
- Archived redo lived on an `emptyDir`, so an Oracle pod restart wiped it; the connector then
  stopped with `CDC-2002 LOG_PURGED` naming the missing archived log, which is the right outcome
  and exactly what the ledger check reported (three transactions in that log). The archive
  directory now lives on the data PVC (`subPath: archive`). Resetting the lab connector after
  such a loss: set the `KafkaConnector` to `spec.state: stopped`, write a tombstone for the offset
  key `["oracle-cdc",{"server":"cdc"}]` to `_connect_offsets` (console producer with
  `parse.key=true` and `null.marker=NULL`), then set it running; the task logs "No stored offset".
  On this lab Strimzi 1.2's `strimzi.io/connector-offsets` annotation rejected both `delete` and
  `DELETE` and the worker's `GET /connectors/oracle-cdc/offsets` returned HTTP 500, so neither
  shortcut was usable.
