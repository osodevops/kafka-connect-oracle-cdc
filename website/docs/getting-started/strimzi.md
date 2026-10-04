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

## Edge cases

`lab/local/k8s/edge-cases.sh` induces one failure per case and verifies that the platform
recovers: worker `SIGKILL`, Strimzi manual rolling update, rollout restart, two-worker
rebalance, broker restart, Oracle pod restart from its volume, a NetworkPolicy partition
between Connect and Oracle, a connector config update, an operator restart during a change,
an OOM kill under a low memory limit, and the connector offsets listing. Each case runs a seeded
workload from the bench tool through the fault and then checks that every committed transaction
in the workload ledger reached Kafka, reporting any duplicated transactions.

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
