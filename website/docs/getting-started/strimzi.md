---
title: Strimzi on minikube
description: The Kubernetes lab with the Strimzi operator, a KRaft Kafka cluster, two Connect workers and an Oracle StatefulSet.
---

# Strimzi on minikube

The Kubernetes lab in `lab/local/k8s` exercises the connector the way most production
deployments run it: a `KafkaConnect` resource with the plugin baked into the image, a
`KafkaConnector` resource managed by the operator, and the failure modes that only Kubernetes
produces (pod kills, rolling updates, rebalances, operator restarts).

```bash
make -C lab/local/k8s cluster     # minikube profile cdc-lab (6 CPUs, 12 GiB)
make -C lab/local/k8s operator    # Strimzi from the pinned versioned manifest
make -C lab/local/k8s images      # test Oracle image and the Connect image with the plugin
make -C lab/local/k8s kafka       # Kafka (KRaft) and the Oracle StatefulSet
make -C lab/local/k8s connect     # two Connect workers with exactly-once support
make -C lab/local/k8s connector   # the KafkaConnector resource
make -C lab/local/k8s status
```

Pinned versions live in `lab/local/k8s/versions.env`. The Connect image is built from the
Strimzi Kafka image with the plugin under `/opt/kafka/plugins`, loaded into minikube directly,
so nothing is pushed to a registry.

## Spill volume

Transactions larger than `cdc.buffer.memory.max.bytes` spill to disk under
`cdc.buffer.spill.dir`, which defaults to the worker's temporary directory. Strimzi mounts `/tmp`
in Connect pods as a 5 MiB in-memory volume, so with the default the first transaction that spills
stops the task with [CDC-4001](../operations/runbooks/buffer-exhausted.md) ("No space left on
device"). Give the spill a disk-backed volume (Strimzi only accepts additional volume mounts under
`/mnt`) and keep `cdc.buffer.spill.max.bytes` below its size, so the connector stops with a typed
error before Kubernetes would evict the pod:

```yaml
# KafkaConnect
spec:
  template:
    pod:
      volumes:
        - name: cdc-spill
          emptyDir:
            sizeLimit: 10Gi
    connectContainer:
      volumeMounts:
        - name: cdc-spill
          mountPath: /mnt/cdc-spill
---
# KafkaConnector
spec:
  config:
    cdc.buffer.spill.dir: /mnt/cdc-spill
    cdc.buffer.spill.max.bytes: "8589934592"
```

An `emptyDir` is enough: the spill only holds transactions not yet committed, and after a restart
the task mines them again from its position.

## Edge cases

`lab/local/k8s/edge-cases.sh` induces one failure per case and checks that it happened: worker
`SIGKILL`, a Strimzi manual rolling update (every worker replaced), every Connect pod deleted at
once, a rebalance away from the worker that owns the task, the only broker killed, the Oracle pod
restarted from its volume, a 90-second network partition between Connect and Oracle (packets
dropped on the node), a connector config update (read back from the worker), an operator restart
while a change is pending, workers OOMKilled by a memory limit below the heap ceiling while a
400,000 row transaction waits to be buffered, and the connector offsets listing. Each case runs a
seeded workload from the bench tool through the fault, requires the platform to recover, and
requires every committed transaction in the workload ledger in Kafka, reporting duplicated
transactions. The OOM case also requires every row of its large transaction in Kafka once.

On 6 October 2026 all eleven cases passed on this lab, with no committed transaction missing and
none duplicated; the OOM case saw a worker OOMKilled twice and still delivered every row of its
transaction once.

## Lessons the manifests encode

- Strimzi 1.x serves only `kafka.strimzi.io/v1`; kubectl's discovery cache must be purged
  after the CRDs are installed before the new kinds resolve.
- A StatefulSet pod is never deleted with `--force`: the old Oracle instance keeps running on
  the volume and the replacement fails to start. The container wrapper runs a shutdown abort
  and clears shared memory before starting Oracle, so even a crashed pod comes back.
- The Oracle image for a volume-backed pod must be the regular (not faststart) variant;
  faststart keeps datafiles inside the image where a mounted volume hides them.
- A single-replica operator on a busy laptop can lose its leader lease and exit; the lab
  disables leader election.
- Loading a new image under an existing tag does not reach running pods; the lab tags every
  Connect image build uniquely and patches the `KafkaConnect` spec so Strimzi rolls onto it.
