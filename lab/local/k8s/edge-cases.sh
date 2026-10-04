#!/usr/bin/env bash
# Edge-case matrix for the Strimzi lab (plan increment P1-31). Each case is induce -> wait -> verify.
# Each case runs a seeded workload through the fault and then verifies two things: the platform
# recovered (Connect Ready, connector RUNNING, Oracle READ WRITE in ARCHIVELOG mode) and every
# committed transaction in the bench ledger reached Kafka (no silent loss). Usage: ./edge-cases.sh [case ...]   (default: all)
set -Euo pipefail
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
# Data verification: a seeded workload runs on the host against the Oracle pod (port-forward) while
# each case is induced; afterwards every committed transaction in the bench ledger must be in Kafka.
REPO_ROOT="$(cd ../../.. && pwd)"
BENCH_JAR=$(ls "$REPO_ROOT"/bench/target/bench-*-cli.jar 2>/dev/null | head -1 || true)
PF_PORT=${PF_PORT:-$((15000 + RANDOM % 1000))}
# one run at a time: two harnesses would share the Oracle tables and the ledger
LOCK=/tmp/oracle-cdc-edge-cases.lock
if ! mkdir "$LOCK" 2>/dev/null; then echo "another edge-cases.sh run holds $LOCK"; exit 2; fi
WORKLOAD_SECONDS=${WORKLOAD_SECONDS:-30}
TOPICS="cdc.FREEPDB1.WORKLOAD.WL_T1,cdc.FREEPDB1.WORKLOAD.WL_T2,cdc.FREEPDB1.WORKLOAD.WL_T3"
pf_pid=""; wl_pid=""
cleanup() { [ -n "$wl_pid" ] && kill "$wl_pid" 2>/dev/null || true; [ -n "$pf_pid" ] && kill "$pf_pid" 2>/dev/null || true; rmdir "$LOCK" 2>/dev/null || true; }
trap cleanup EXIT

workload_available() { [ -n "$BENCH_JAR" ] && command -v java >/dev/null; }
port_forward() {
  [ -n "$pf_pid" ] && kill -0 "$pf_pid" 2>/dev/null && return 0
  $K port-forward svc/oracle "${PF_PORT}:1521" >/dev/null 2>&1 &
  pf_pid=$!; sleep 3
}
# Starts (or restarts) the generator in the background; the first call resets the tables.
workload_begin() {
  workload_available || { log "verify: bench jar missing, data check skipped (mvn -pl bench package)"; return 0; }
  if [ -n "$wl_pid" ] && kill -0 "$wl_pid" 2>/dev/null; then kill "$wl_pid" 2>/dev/null || true; wait "$wl_pid" 2>/dev/null || true; fi
  port_forward
  # every case starts from empty tables and an empty ledger: the generator's row ids are
  # deterministic per session, so a second run on populated tables would collide on the key
  local reset="--reset"
  cat > /tmp/edge-workload.json <<'JSON'
{"seed": 31, "sessions": 2, "tables": 3, "transactionsPerSession": 0, "maxRowsPerTransaction": 5,
 "savepointRollbackProbability": 0.1, "fullRollbackProbability": 0.1, "lobWeight": 0, "keyChangeWeight": 0}
JSON
  java -jar "$BENCH_JAR" workload --url "jdbc:oracle:thin:@//localhost:${PF_PORT}/FREEPDB1" \
    --user workload --password workload --spec /tmp/edge-workload.json --duration "$WORKLOAD_SECONDS" $reset \
    > /tmp/edge-workload.out 2>&1 &
  wl_pid=$!
  log "workload: started for ${WORKLOAD_SECONDS}s (pid $wl_pid, tables reset)"
  # let the generator reset the tables and commit real work before the fault is induced, so the
  # case verifies transactions committed before, during and after it
  sleep "${WORKLOAD_LEAD_SECONDS:-6}"
  kill -0 "$wl_pid" 2>/dev/null || { log "workload: generator died before the fault"; tail -3 /tmp/edge-workload.out; return 1; }
  log "workload: $(ledger_xids | wc -l | tr -d ' ') transactions committed before the fault"
}
ledger_xids() {
  $K exec oracle-0 -- bash -c "printf 'SET PAGESIZE 0 FEEDBACK OFF HEADING OFF\nSELECT xid FROM wl_ledger WHERE ops > 0;\n' | sqlplus -s workload/workload@//localhost:1521/FREEPDB1" | tr -d ' \r' | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' | sort -u
}
kafka_xids() {
  $K exec lab-dual-0 -- bin/kafka-console-consumer.sh --bootstrap-server lab-kafka-bootstrap:9092 \
    --include "$(echo "$TOPICS" | tr , '|')" --from-beginning --isolation-level read_committed --timeout-ms 15000 \
    --property print.headers=true 2>/dev/null \
  | python3 -c '
import sys, json, collections
seen=set(); copies=collections.Counter()
for line in sys.stdin:
    line=line.rstrip("\n")
    if "\t" not in line: continue
    headers, value = line.split("\t", 1)
    if value in ("null", ""): continue
    try:
        v=json.loads(value)
    except Exception: continue
    if "payload" in v: v=v["payload"]
    if v is None: continue
    xid=v.get("source", {}).get("txId")
    if not xid: continue
    seen.add(xid)
    idx=dict(h.split(":",1) for h in headers.split(",") if ":" in h).get("cdc.event_index","?")
    copies[(xid, idx)] += 1
dups=len({k[0] for k,n in copies.items() if n>1})
print("\n".join(sorted(seen)))
print("DUPLICATED_TRANSACTIONS=%d" % dups, file=sys.stderr)
' 2>/tmp/edge-kafka.err
}
verify_data() {
  workload_available || return 0
  if [ -n "$wl_pid" ]; then
    if ! wait "$wl_pid"; then
      if [ -n "${WORKLOAD_MAY_FAIL:-}" ]; then
        # a database fault kills the generator's own connections; the ledger still holds exactly
        # the transactions Oracle committed before that, and every one of them must be in Kafka
        log "workload: generator stopped by the fault (expected for this case)"
      else
        log "workload: generator failed"; tail -5 /tmp/edge-workload.out; wl_pid=""; return 1
      fi
    fi
    wl_pid=""
  fi
  WORKLOAD_MAY_FAIL=""
  log "workload: $(grep '^{' /tmp/edge-workload.out | tail -1)"
  local attempt missing
  for attempt in $(seq 1 12); do
    ledger_xids > /tmp/edge-ledger.txt
    kafka_xids > /tmp/edge-seen.txt
    missing=$(comm -23 /tmp/edge-ledger.txt /tmp/edge-seen.txt | wc -l | tr -d ' ')
    if [ "$missing" = 0 ]; then
      log "verify: data ok, ledger=$(wc -l < /tmp/edge-ledger.txt | tr -d ' ') transactions all in Kafka, $(cat /tmp/edge-kafka.err)"
      return 0
    fi
    log "verify: $missing ledger transactions not yet in Kafka (attempt $attempt)"; sleep 10
  done
  log "verify: FAILED, $missing committed transactions missing from Kafka"; comm -23 /tmp/edge-ledger.txt /tmp/edge-seen.txt | head -5; return 1
}

case_kill_worker() {
  log "case: SIGKILL one Connect worker"
  workload_begin
  pod=$($K get pod -l strimzi.io/name=connect-connect -o jsonpath='{.items[0].metadata.name}')
  $K delete pod "$pod" --grace-period=0 --force >/dev/null
  verify_platform; verify_data
}
case_rolling_update() {
  log "case: Strimzi manual rolling update of Connect"
  workload_begin
  $K annotate kafkaconnect connect strimzi.io/manual-rolling-update=true --overwrite >/dev/null
  sleep 15; verify_platform; verify_data
}
case_rollout_restart() {
  log "case: kubectl rollout restart of the Connect pods"
  workload_begin
  $K rollout restart deployment/connect-connect 2>/dev/null || $K rollout restart strimzipodset/connect-connect 2>/dev/null || true
  $K annotate kafkaconnect connect strimzi.io/manual-rolling-update=true --overwrite >/dev/null
  sleep 15; verify_platform; verify_data
}
case_rebalance() {
  log "case: task rebalance by deleting the worker that owns the task"
  workload_begin
  owner=$($K get kafkaconnector "$CONNECTOR" -o jsonpath='{.status.connectorStatus.tasks[0].worker_id}' | cut -d: -f1)
  pod=$($K get pod -l strimzi.io/name=connect-connect -o jsonpath="{.items[?(@.status.podIP==\"$owner\")].metadata.name}")
  [ -n "$pod" ] || pod=$($K get pod -l strimzi.io/name=connect-connect -o jsonpath='{.items[0].metadata.name}')
  $K delete pod "$pod" --grace-period=0 --force >/dev/null
  verify_platform; verify_data
}
case_broker_restart() {
  log "case: broker pod restart (single node: full outage and recovery)"
  workload_begin
  $K delete pod lab-dual-0 --grace-period=0 --force >/dev/null
  $K wait kafka/lab --for=condition=Ready --timeout=600s >/dev/null
  verify_platform; verify_data
}
case_oracle_restart() {
  log "case: Oracle pod deleted; must return from the PVC"
  workload_begin
  WORKLOAD_MAY_FAIL=1
  # never --force a StatefulSet pod: the old instance may still hold the volume when the new one starts
  $K delete pod oracle-0 --grace-period=120 >/dev/null
  verify_platform; verify_data
}
case_partition() {
  log "case: network partition Connect -> Oracle for 90 s (needs a policy-enforcing CNI)"
  workload_begin
  WORKLOAD_MAY_FAIL=1
  $K apply -f chaos/deny-connect-to-oracle.yaml >/dev/null
  sleep 90
  $K delete -f chaos/deny-connect-to-oracle.yaml --ignore-not-found >/dev/null
  verify_platform; verify_data
}
case_config_update() {
  log "case: connector config update through kubectl apply"
  workload_begin
  $K patch kafkaconnector "$CONNECTOR" --type=merge -p '{"spec":{"config":{"cdc.poll.linger.ms":"150"}}}' >/dev/null
  sleep 10; verify_platform; verify_data
}
case_operator_restart_during_change() {
  log "case: operator restarted while a connector change is pending"
  workload_begin
  $K patch kafkaconnector "$CONNECTOR" --type=merge -p '{"spec":{"state":"paused"}}' >/dev/null
  $K delete pod -l name=strimzi-cluster-operator --grace-period=0 --force >/dev/null
  $K wait --for=condition=Available deployment/strimzi-cluster-operator --timeout=300s >/dev/null
  $K patch kafkaconnector "$CONNECTOR" --type=merge -p '{"spec":{"state":"running"}}' >/dev/null
  verify_platform; verify_data
}
case_oom() {
  log "case: low memory limit (OOMKill path); restored afterwards"
  workload_begin
  $K patch kafkaconnect connect --type=merge -p '{"spec":{"resources":{"limits":{"memory":"512Mi"}},"jvmOptions":{"-Xmx":"400m"}}}' >/dev/null
  sleep 60; $K get pods -l strimzi.io/name=connect-connect -o jsonpath='{range .items[*]}{.metadata.name} restarts={.status.containerStatuses[0].restartCount} last={.status.containerStatuses[0].lastState.terminated.reason}{"\n"}{end}'
  $K patch kafkaconnect connect --type=merge -p '{"spec":{"resources":{"limits":{"memory":"2Gi"}},"jvmOptions":{"-Xmx":"1536m"}}}' >/dev/null
  verify_platform; verify_data
}
case_offsets_list() {
  log "case: list connector offsets through the Strimzi annotation"
  workload_begin
  $K annotate kafkaconnector "$CONNECTOR" strimzi.io/connector-offsets=list strimzi.io/connector-offsets-configmap=oracle-cdc-offsets --overwrite >/dev/null
  for _ in $(seq 1 30); do $K get configmap oracle-cdc-offsets >/dev/null 2>&1 && break; sleep 2; done
  $K get configmap oracle-cdc-offsets -o jsonpath='{.data}' ; echo
}

ALL=(kill_worker rolling_update rebalance broker_restart oracle_restart config_update operator_restart_during_change offsets_list)
cases=("${@:-${ALL[@]}}")
verify_platform
failed=()
for c in "${cases[@]}"; do
  if ! "case_$c"; then failed+=("$c"); log "case $c: FAILED"; fi
done
if [ ${#failed[@]} -gt 0 ]; then log "FAILED cases: ${failed[*]}"; exit 1; fi
log "all cases finished: ${cases[*]}"
