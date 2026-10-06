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
connector_states() {
  s=$($K get kafkaconnector "$CONNECTOR" -o jsonpath='{.status.connectorStatus.connector.state}' 2>/dev/null || true)
  t=$($K get kafkaconnector "$CONNECTOR" -o jsonpath='{.status.connectorStatus.tasks[0].state}' 2>/dev/null || true)
}
wait_connector_running() {
  local restarted=""
  for i in $(seq 1 90); do
    connector_states
    [ "$s" = RUNNING ] && [ "$t" = RUNNING ] && return 0
    # After a full broker outage a worker that is shutting down can write UNASSIGNED for the
    # connector with an older generation after the new owner wrote RUNNING, and Connect keeps the
    # stale value while the connector runs. Restart the connector instance once (never the task)
    # so the status is written again; the data check still decides the case.
    if [ -z "$restarted" ] && [ "$i" -ge 12 ] && [ "$s" = UNASSIGNED ] && [ "$t" = RUNNING ]; then
      log "connector status UNASSIGNED with the task RUNNING: restarting the connector instance once"
      connect_rest "/connectors/$CONNECTOR/restart" -X POST >/dev/null || true
      restarted=1
    fi
    sleep 5
  done
  log "connector not RUNNING: connector=$s task=$t"; $K get kafkaconnector "$CONNECTOR" -o yaml | tail -40; return 1
}
wait_oracle_ready() {
  $K rollout status statefulset/oracle --timeout=900s >/dev/null
  $K exec oracle-0 -- bash -c "echo \"SELECT log_mode FROM v\\\$database;\" | sqlplus -s c##cdc/cdc@//localhost:1521/FREE" | grep -q ARCHIVELOG
}
connect_rest() { local path=$1; shift; $K exec connect-connect-0 -- curl -s "$@" "http://localhost:8083$path"; }
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
pf_pid=""; wl_pid=""; partition_rules=()
cleanup() {
  [ -n "$wl_pid" ] && kill "$wl_pid" 2>/dev/null || true
  [ -n "$pf_pid" ] && kill "$pf_pid" 2>/dev/null || true
  heal_partition
  rmdir "$LOCK" 2>/dev/null || true
}
trap cleanup EXIT

workload_available() { [ -n "$BENCH_JAR" ] && command -v java >/dev/null; }
# A fresh forward for every case: kubectl keeps a forward alive but broken after the Oracle pod
# it was bound to is deleted, and the generator then fails to connect.
port_forward() {
  if [ -n "$pf_pid" ]; then kill "$pf_pid" 2>/dev/null || true; wait "$pf_pid" 2>/dev/null || true; fi
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
  verify_platform && verify_data
}
connect_pod_uids() { $K get pod -l strimzi.io/name=connect-connect -o jsonpath='{.items[*].metadata.uid}'; }
# the case only counts when every Connect pod that was running before it has been replaced
wait_rolled() {
  local before=$1 now uid left
  for _ in $(seq 1 120); do
    now=" $(connect_pod_uids) "; left=0
    for uid in $before; do case "$now" in *" $uid "*) left=$((left + 1)) ;; esac; done
    if [ "$left" = 0 ]; then log "rolled: every Connect pod replaced"; return 0; fi
    sleep 5
  done
  log "roll did not happen: $left Connect pods were never replaced"; return 1
}
case_rolling_update() {
  log "case: Strimzi manual rolling update of Connect"
  workload_begin
  local before
  before=$(connect_pod_uids)
  # Strimzi reads this annotation on the StrimziPodSet or a pod; on the KafkaConnect it is ignored
  $K annotate strimzipodset connect-connect strimzi.io/manual-rolling-update=true --overwrite >/dev/null
  wait_rolled "$before" || return 1
  verify_platform && verify_data
}
case_rollout_restart() {
  # Strimzi manages Connect pods through a StrimziPodSet, which kubectl rollout restart does not
  # support; deleting every pod at once is the restart an operator or a node drain does outside
  # Strimzi, and unlike rolling_update no worker stays up to take the task
  log "case: every Connect pod deleted at once, gracefully (a restart outside the operator)"
  workload_begin
  $K delete pod -l strimzi.io/name=connect-connect --wait=false >/dev/null
  sleep 15; verify_platform && verify_data
}
case_rebalance() {
  log "case: task rebalance by deleting the worker that owns the task"
  workload_begin
  owner=$($K get kafkaconnector "$CONNECTOR" -o jsonpath='{.status.connectorStatus.tasks[0].worker_id}' | cut -d: -f1)
  # the worker id is the pod's DNS name (connect-connect-0.connect-connect.cdc.svc) or its IP
  pod=$($K get pod "${owner%%.*}" -o jsonpath='{.metadata.name}' 2>/dev/null || true)
  [ -n "$pod" ] || pod=$($K get pod -l strimzi.io/name=connect-connect -o jsonpath="{.items[?(@.status.podIP==\"$owner\")].metadata.name}")
  [ -n "$pod" ] || pod=$($K get pod -l strimzi.io/name=connect-connect -o jsonpath='{.items[0].metadata.name}')
  $K delete pod "$pod" --grace-period=0 --force >/dev/null
  verify_platform && verify_data
}
case_broker_restart() {
  log "case: broker pod restart (single node: full outage and recovery)"
  workload_begin
  local before now=""
  before=$($K get pod lab-dual-0 -o jsonpath='{.metadata.uid}')
  $K delete pod lab-dual-0 --grace-period=0 --force >/dev/null
  # the Kafka resource stays Ready for a while after its only broker dies; wait for the new pod
  for _ in $(seq 1 120); do
    now=$($K get pod lab-dual-0 -o jsonpath='{.metadata.uid}' 2>/dev/null || true)
    [ -n "$now" ] && [ "$now" != "$before" ] && break
    sleep 2
  done
  [ -n "$now" ] && [ "$now" != "$before" ] || { log "broker pod was not replaced"; return 1; }
  $K wait pod/lab-dual-0 --for=condition=Ready --timeout=600s >/dev/null
  $K wait kafka/lab --for=condition=Ready --timeout=600s >/dev/null
  log "broker replaced and Ready"
  verify_platform && verify_data
}
case_oracle_restart() {
  log "case: Oracle pod deleted; must return from the PVC"
  workload_begin
  WORKLOAD_MAY_FAIL=1
  # never --force a StatefulSet pod: the old instance may still hold the volume when the new one starts
  $K delete pod oracle-0 --grace-period=120 >/dev/null
  verify_platform && verify_data
}
# The lab's default CNI does not enforce NetworkPolicy, so the partition drops packets between the
# Connect pods and the Oracle pod in the node's FORWARD chain. Packets vanish without a reset, the
# hardest case for the JDBC socket. The host's port-forward to Oracle does not cross that chain, so
# the workload keeps committing through the partition.
partition_rule() { minikube -p "$MINIKUBE_PROFILE" ssh -- sudo iptables "$1" FORWARD -s "$2" -d "$3" -j DROP; }
heal_partition() {
  local r
  for r in "${partition_rules[@]+"${partition_rules[@]}"}"; do partition_rule -D ${r} >/dev/null 2>&1 || true; done
  partition_rules=()
}
connect_reaches_oracle() {
  local pod=$1
  $K exec "$pod" -- timeout 5 bash -c 'exec 3<>/dev/tcp/oracle/1521' >/dev/null 2>&1
}
case_partition() {
  log "case: network partition between Connect and Oracle for ${PARTITION_SECONDS:-90} s (packets dropped)"
  workload_begin
  local oracle_ip ip pod
  oracle_ip=$($K get pod oracle-0 -o jsonpath='{.status.podIP}')
  for ip in $($K get pod -l strimzi.io/name=connect-connect -o jsonpath='{.items[*].status.podIP}'); do
    partition_rule -I "$ip" "$oracle_ip" && partition_rules+=("$ip $oracle_ip")
    partition_rule -I "$oracle_ip" "$ip" && partition_rules+=("$oracle_ip $ip")
  done
  # the case only counts when the partition holds
  for pod in $($K get pod -l strimzi.io/name=connect-connect -o jsonpath='{.items[*].metadata.name}'); do
    if connect_reaches_oracle "$pod"; then log "partition not in force: $pod still reaches Oracle"; heal_partition; return 1; fi
  done
  log "partition in force (${#partition_rules[@]} rules)"
  sleep "${PARTITION_SECONDS:-90}"
  heal_partition
  log "partition healed"
  verify_platform && verify_data
}
case_config_update() {
  log "case: connector config update through the KafkaConnector resource"
  workload_begin
  local was want
  was=$($K get kafkaconnector "$CONNECTOR" -o jsonpath='{.spec.config.cdc\.poll\.linger\.ms}')
  # a value different from the current one, so every run changes the configuration
  if [ "$was" = 150 ]; then want=200; else want=150; fi
  $K patch kafkaconnector "$CONNECTOR" --type=merge -p "{\"spec\":{\"config\":{\"cdc.poll.linger.ms\":\"$want\"}}}" >/dev/null
  local applied=""
  for _ in $(seq 1 30); do
    applied=$(connect_rest "/connectors/$CONNECTOR/config" | python3 -c 'import sys, json; print(json.load(sys.stdin).get("cdc.poll.linger.ms", ""))' 2>/dev/null || true)
    [ "$applied" = "$want" ] && break
    sleep 2
  done
  if [ "$applied" != "$want" ]; then log "config update: Connect still runs cdc.poll.linger.ms=$applied, wanted $want"; return 1; fi
  log "config update: Connect runs cdc.poll.linger.ms=$want (was ${was:-unset})"
  verify_platform && verify_data
}
case_operator_restart_during_change() {
  log "case: operator restarted while a connector change is pending"
  workload_begin
  $K patch kafkaconnector "$CONNECTOR" --type=merge -p '{"spec":{"state":"paused"}}' >/dev/null
  $K delete pod -l name=strimzi-cluster-operator --grace-period=0 --force >/dev/null
  $K wait --for=condition=Available deployment/strimzi-cluster-operator --timeout=300s >/dev/null
  $K patch kafkaconnector "$CONNECTOR" --type=merge -p '{"spec":{"state":"running"}}' >/dev/null
  verify_platform && verify_data
}
workload_sql() {
  $K exec oracle-0 -- bash -c "printf 'SET PAGESIZE 0 FEEDBACK OFF HEADING OFF\nWHENEVER SQLERROR EXIT 1\n%s\n' \"$1\" | sqlplus -s workload/workload@//localhost:1521/FREEPDB1"
}
# Strimzi rolls the workers after a resources change; wait until every one runs with the limit
wait_connect_limit() {
  local want=$1 got
  for _ in $(seq 1 120); do
    got=$($K get pod -l strimzi.io/name=connect-connect -o jsonpath='{range .items[*]}{.spec.containers[0].resources.limits.memory}{"/"}{.status.containerStatuses[0].ready}{" "}{end}')
    if [ -n "$got" ] && [ -z "$(echo "$got" | tr ' ' '\n' | grep -v "^$want/true\$" | grep -v '^$')" ]; then return 0; fi
    sleep 5
  done
  log "Connect workers did not roll onto the memory limit $want: $got"; return 1
}
oom_kills() {
  $K get pods -l strimzi.io/name=connect-connect -o jsonpath='{range .items[*]}{.metadata.name} restarts={.status.containerStatuses[0].restartCount} last={.status.containerStatuses[0].lastState.terminated.reason}{"\n"}{end}'
}
case_oom() {
  # A heap ceiling (-Xmx 1536m in connect.yaml) above the container limit: under load the JVM grows
  # the heap past the limit and the kernel kills the worker (OOMKilled), the misconfiguration that
  # kills Connect workers in practice. An idle worker needs about 800 MiB, so the limit must stay
  # above that or no worker can start. The limits in connect.yaml come back whatever happens, and
  # the whole transaction must reach Kafka once.
  local rows=${OOM_ROWS:-400000} limit=${OOM_LIMIT:-1Gi} first rc=0
  log "case: OOMKill of the worker buffering a ${rows}-row transaction (memory limit ${limit})"
  workload_sql "DECLARE n NUMBER; BEGIN SELECT COUNT(*) INTO n FROM user_tables WHERE table_name = 'EDGE_BIG'; IF n = 0 THEN EXECUTE IMMEDIATE 'CREATE TABLE edge_big (id NUMBER(12) PRIMARY KEY, pad VARCHAR2(400))'; EXECUTE IMMEDIATE 'ALTER TABLE edge_big ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS'; END IF; END;
/" >/dev/null || { log "oom: could not create EDGE_BIG"; return 1; }
  # a captured table without table-level supplemental logging fails validation (DOC-3), so every
  # later config change on the connector would be refused
  # ids grow from run to run, so rows of an earlier run in the topic never count for this one
  first=$(workload_sql "SELECT NVL(MAX(id), 0) + 1 FROM edge_big;" | tr -d ' \t\r' | grep -E '^[0-9]+$' || true)
  [ -n "$first" ] || { log "oom: could not read EDGE_BIG"; return 1; }
  $K patch kafkaconnect connect --type=merge -p "{\"spec\":{\"resources\":{\"requests\":{\"memory\":\"512Mi\"},\"limits\":{\"memory\":\"$limit\"}}}}" >/dev/null
  oom_under_limit "$rows" "$first" "$limit" || rc=1
  # back to the values in connect.yaml, also after a failure above
  $K patch kafkaconnect connect --type=merge -p '{"spec":{"resources":{"requests":{"memory":"1Gi"},"limits":{"memory":"2Gi"}}}}' >/dev/null
  wait_connect_limit 2Gi || return 1
  [ "$rc" = 0 ] || return 1
  verify_platform && verify_data || return 1
  verify_rows cdc.FREEPDB1.WORKLOAD.EDGE_BIG "$first" "$rows"
}
oom_under_limit() {
  local rows=$1 first=$2 limit=$3
  wait_connect_limit "$limit" || { log "oom: a worker cannot start under $limit: $(oom_kills | tr '\n' ' ')"; return 1; }
  wait_connector_running || true
  workload_begin
  log "oom: inserting ids $first to $((first + rows - 1)) in one transaction"
  workload_sql "INSERT INTO edge_big SELECT $first - 1 + ROWNUM, RPAD('x', 400, 'x') FROM (SELECT 1 FROM dual CONNECT BY LEVEL <= 1000), (SELECT 1 FROM dual CONNECT BY LEVEL <= $(( (rows + 999) / 1000 ))) WHERE ROWNUM <= $rows;
COMMIT;" >/dev/null || { log "oom: large transaction failed"; return 1; }
  sleep "${OOM_HOLD_SECONDS:-120}"
  log "oom: workers under the limit: $(oom_kills | tr '\n' ' ')"
}
# every id in [first, first + rows) must be in the topic exactly once (read_committed); the record
# key holds the ID and is far smaller than the value
verify_rows() {
  local topic=$1 first=$2 rows=$3 attempt out
  for attempt in $(seq 1 10); do
    out=$($K exec lab-dual-0 -- bin/kafka-console-consumer.sh --bootstrap-server lab-kafka-bootstrap:9092 \
      --topic "$topic" --from-beginning --isolation-level read_committed --timeout-ms 30000 \
      --property print.key=true --property print.value=false 2>/dev/null \
    | python3 -c '
import sys, json, collections
first, rows = int(sys.argv[1]), int(sys.argv[2])
seen = collections.Counter()
for line in sys.stdin:
    try:
        v = json.loads(line)
    except Exception:
        continue
    if isinstance(v, dict) and "payload" in v:
        v = v["payload"]
    i = v.get("ID") if isinstance(v, dict) else None
    if i is not None and first <= int(i) < first + rows:
        seen[int(i)] += 1
print("%d %d" % (len(seen), sum(1 for n in seen.values() if n > 1)))
' "$first" "$rows")
    if [ "${out%% *}" = "$rows" ]; then log "verify: $topic holds all $rows rows, duplicated ids ${out##* }"; [ "${out##* }" = 0 ]; return; fi
    log "verify: $topic holds ${out%% *} of $rows rows (attempt $attempt)"; sleep 30
  done
  log "verify: FAILED, $topic is missing rows"; return 1
}
case_offsets_list() {
  log "case: list connector offsets through the Strimzi annotation"
  workload_begin
  # a config map left by an earlier run would satisfy the wait below without a new listing
  $K delete configmap oracle-cdc-offsets --ignore-not-found >/dev/null
  # Strimzi takes the target config map from spec.listOffsets (connector.yaml sets it as well)
  $K patch kafkaconnector "$CONNECTOR" --type=merge -p '{"spec":{"listOffsets":{"toConfigMap":{"name":"oracle-cdc-offsets"}}}}' >/dev/null
  $K annotate kafkaconnector "$CONNECTOR" strimzi.io/connector-offsets=list --overwrite >/dev/null
  local data=""
  for _ in $(seq 1 30); do data=$($K get configmap oracle-cdc-offsets -o jsonpath='{.data}' 2>/dev/null || true); [ -n "$data" ] && break; sleep 2; done
  if [ -z "$data" ]; then log "offsets list: no config map written"; return 1; fi
  case "$data" in *'"server'*cdc*) ;; *) log "offsets list: the config map holds no offset for the cdc partition"; return 1 ;; esac
  log "offsets list: $data"
  verify_platform && verify_data
}

ALL=(kill_worker rolling_update rollout_restart rebalance broker_restart oracle_restart partition config_update
     operator_restart_during_change oom offsets_list)
cases=("${@:-${ALL[@]}}")
verify_platform
failed=()
for c in "${cases[@]}"; do
  if ! "case_$c"; then failed+=("$c"); log "case $c: FAILED"; fi
done
if [ ${#failed[@]} -gt 0 ]; then log "FAILED cases: ${failed[*]}"; exit 1; fi
log "all cases finished: ${cases[*]}"
