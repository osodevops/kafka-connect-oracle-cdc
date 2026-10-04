#!/usr/bin/env bash
# Edge-case matrix for the Strimzi lab (plan increment P1-31). Each case is induce -> wait -> verify.
# Verification calls the correctness oracle (bench) once the connector emits records (P1-10, P1-13);
# until then every case verifies recovery of the platform: Connect Ready, connector RUNNING, Oracle
# READ WRITE in ARCHIVELOG mode. Usage: ./edge-cases.sh [case ...]   (default: all)
set -Eeuo pipefail
cd "$(dirname "$0")"
# shellcheck disable=SC1091
source versions.env
K="kubectl --context ${MINIKUBE_PROFILE} -n ${NAMESPACE}"
CONNECTOR=oracle-cdc

log() { printf '%s %s\n' "$(date +%T)" "$*"; }

wait_connect_ready() { $K wait kafkaconnect/connect --for=condition=Ready --timeout=600s >/dev/null; }
wait_connector_running() {
  for _ in $(seq 1 90); do
    s=$($K get kafkaconnector "$CONNECTOR" -o jsonpath='{.status.connectorStatus.connector.state}' 2>/dev/null || true)
    t=$($K get kafkaconnector "$CONNECTOR" -o jsonpath='{.status.connectorStatus.tasks[0].state}' 2>/dev/null || true)
    [ "$s" = RUNNING ] && [ "$t" = RUNNING ] && return 0
    sleep 5
  done
  log "connector not RUNNING: connector=$s task=$t"; $K get kafkaconnector "$CONNECTOR" -o yaml | tail -40; return 1
}
wait_oracle_ready() {
  $K rollout status statefulset/oracle --timeout=900s >/dev/null
  $K exec oracle-0 -- bash -c "echo \"SELECT log_mode FROM v\\\$database;\" | sqlplus -s c##cdc/cdc@//localhost:1521/FREE" | grep -q ARCHIVELOG
}
verify_platform() { wait_connect_ready && wait_connector_running && wait_oracle_ready && log "verify: platform recovered"; }
# Placeholder until bench exists: compares topics with Oracle at a check SCN (plan P1-13).
verify_data() { log "verify: data check not wired yet (needs bench correctness oracle)"; }

case_kill_worker() {
  log "case: SIGKILL one Connect worker"
  pod=$($K get pod -l strimzi.io/name=connect-connect -o jsonpath='{.items[0].metadata.name}')
  $K delete pod "$pod" --grace-period=0 --force >/dev/null
  verify_platform; verify_data
}
case_rolling_update() {
  log "case: Strimzi manual rolling update of Connect"
  $K annotate kafkaconnect connect strimzi.io/manual-rolling-update=true --overwrite >/dev/null
  sleep 15; verify_platform; verify_data
}
case_rollout_restart() {
  log "case: kubectl rollout restart of the Connect pods"
  $K rollout restart deployment/connect-connect 2>/dev/null || $K rollout restart strimzipodset/connect-connect 2>/dev/null || true
  $K annotate kafkaconnect connect strimzi.io/manual-rolling-update=true --overwrite >/dev/null
  sleep 15; verify_platform; verify_data
}
case_rebalance() {
  log "case: task rebalance by deleting the worker that owns the task"
  owner=$($K get kafkaconnector "$CONNECTOR" -o jsonpath='{.status.connectorStatus.tasks[0].worker_id}' | cut -d: -f1)
  pod=$($K get pod -l strimzi.io/name=connect-connect -o jsonpath="{.items[?(@.status.podIP==\"$owner\")].metadata.name}")
  [ -n "$pod" ] || pod=$($K get pod -l strimzi.io/name=connect-connect -o jsonpath='{.items[0].metadata.name}')
  $K delete pod "$pod" --grace-period=0 --force >/dev/null
  verify_platform; verify_data
}
case_broker_restart() {
  log "case: broker pod restart (single node: full outage and recovery)"
  $K delete pod lab-dual-0 --grace-period=0 --force >/dev/null
  $K wait kafka/lab --for=condition=Ready --timeout=600s >/dev/null
  verify_platform; verify_data
}
case_oracle_restart() {
  log "case: Oracle pod deleted; must return from the PVC"
  # never --force a StatefulSet pod: the old instance may still hold the volume when the new one starts
  $K delete pod oracle-0 --grace-period=120 >/dev/null
  verify_platform; verify_data
}
case_partition() {
  log "case: network partition Connect -> Oracle for 90 s (needs a policy-enforcing CNI)"
  $K apply -f chaos/deny-connect-to-oracle.yaml >/dev/null
  sleep 90
  $K delete -f chaos/deny-connect-to-oracle.yaml --ignore-not-found >/dev/null
  verify_platform; verify_data
}
case_config_update() {
  log "case: connector config update through kubectl apply"
  $K patch kafkaconnector "$CONNECTOR" --type=merge -p '{"spec":{"config":{"cdc.poll.linger.ms":"150"}}}' >/dev/null
  sleep 10; verify_platform; verify_data
}
case_operator_restart_during_change() {
  log "case: operator restarted while a connector change is pending"
  $K patch kafkaconnector "$CONNECTOR" --type=merge -p '{"spec":{"state":"paused"}}' >/dev/null
  $K delete pod -l name=strimzi-cluster-operator --grace-period=0 --force >/dev/null
  $K wait --for=condition=Available deployment/strimzi-cluster-operator --timeout=300s >/dev/null
  $K patch kafkaconnector "$CONNECTOR" --type=merge -p '{"spec":{"state":"running"}}' >/dev/null
  verify_platform; verify_data
}
case_oom() {
  log "case: low memory limit (OOMKill path); restored afterwards"
  $K patch kafkaconnect connect --type=merge -p '{"spec":{"resources":{"limits":{"memory":"512Mi"}},"jvmOptions":{"-Xmx":"400m"}}}' >/dev/null
  sleep 60; $K get pods -l strimzi.io/name=connect-connect -o jsonpath='{range .items[*]}{.metadata.name} restarts={.status.containerStatuses[0].restartCount} last={.status.containerStatuses[0].lastState.terminated.reason}{"\n"}{end}'
  $K patch kafkaconnect connect --type=merge -p '{"spec":{"resources":{"limits":{"memory":"2Gi"}},"jvmOptions":{"-Xmx":"1536m"}}}' >/dev/null
  verify_platform; verify_data
}
case_offsets_list() {
  log "case: list connector offsets through the Strimzi annotation"
  $K annotate kafkaconnector "$CONNECTOR" strimzi.io/connector-offsets=list strimzi.io/connector-offsets-configmap=oracle-cdc-offsets --overwrite >/dev/null
  for _ in $(seq 1 30); do $K get configmap oracle-cdc-offsets >/dev/null 2>&1 && break; sleep 2; done
  $K get configmap oracle-cdc-offsets -o jsonpath='{.data}' ; echo
}

ALL=(kill_worker rolling_update rebalance broker_restart oracle_restart config_update operator_restart_during_change offsets_list)
cases=("${@:-${ALL[@]}}")
verify_platform
for c in "${cases[@]}"; do "case_$c"; done
log "all cases finished: ${cases[*]}"
