#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/../../.." && pwd)
CLUSTER_SCRIPT="$SCRIPT_DIR/run_oplog_batch_cluster.sh"
FAULT_CTL="$SCRIPT_DIR/oplog_fault_ctl.sh"

BUILD_DIR="$REPO_ROOT/build"
RUN_DIR=""
THREADS=16
PRE_SECONDS=3
POST_SECONDS=3
SEGMENT_BYTES=$((512 * 1024 * 1024))
VALUE_BYTES=64
BATCH_ENTRIES=64
START_TIMEOUT_SEC=60

usage() {
  cat >&2 <<EOF
Usage: $0 [options]

Options:
  --build-dir DIR       Build directory (default: $BUILD_DIR)
  --run-dir DIR         Artifact directory (default: /tmp with timestamp)
  --threads N           Concurrent Put/Get workers (default: $THREADS)
  --pre-seconds N       Extra load before killing the leader (default: $PRE_SECONDS)
  --post-seconds N      Load after client reconnection (default: $POST_SECONDS)
  --segment-bytes N     Mounted client segment size (default: $SEGMENT_BYTES)
  --value-bytes N       Value size for each Put (default: $VALUE_BYTES)
  --batch-entries N     OpLog max entries per batch (default: $BATCH_ENTRIES)
  --timeout-sec N       Per-stage timeout (default: $START_TIMEOUT_SEC)
EOF
}

die() {
  echo "error: $*" >&2
  exit 2
}

parse_options() {
  while (($#)); do
    case "$1" in
      --build-dir) BUILD_DIR=$2; shift 2 ;;
      --run-dir) RUN_DIR=$2; shift 2 ;;
      --threads) THREADS=$2; shift 2 ;;
      --pre-seconds) PRE_SECONDS=$2; shift 2 ;;
      --post-seconds) POST_SECONDS=$2; shift 2 ;;
      --segment-bytes) SEGMENT_BYTES=$2; shift 2 ;;
      --value-bytes) VALUE_BYTES=$2; shift 2 ;;
      --batch-entries) BATCH_ENTRIES=$2; shift 2 ;;
      --timeout-sec) START_TIMEOUT_SEC=$2; shift 2 ;;
      --help|-h) usage; exit 0 ;;
      *) die "unknown option: $1" ;;
    esac
  done
  local value
  for value in "$THREADS" "$PRE_SECONDS" "$POST_SECONDS" "$SEGMENT_BYTES" \
    "$VALUE_BYTES" "$BATCH_ENTRIES" "$START_TIMEOUT_SEC"; do
    [[ "$value" =~ ^[1-9][0-9]*$ ]] || die "numeric options must be positive"
  done
}

find_free_port() {
  python3 - <<'PY'
import socket
with socket.socket() as sock:
    sock.bind(("127.0.0.1", 0))
    print(sock.getsockname()[1])
PY
}

wait_for_file() {
  local file=$1
  local timeout=$2
  local watched_pid=${3:-}
  local deadline=$((SECONDS + timeout))
  while ((SECONDS < deadline)); do
    [[ -e "$file" ]] && return 0
    if [[ -n "$watched_pid" ]] && ! kill -0 "$watched_pid" 2>/dev/null; then
      return 1
    fi
    sleep 0.1
  done
  return 1
}

leader_index() {
  local found=""
  local index health
  for index in "${!ADMIN_PORT_ARRAY[@]}"; do
    health=$(curl -fsS "http://127.0.0.1:${ADMIN_PORT_ARRAY[$index]}/health" \
      2>/dev/null || true)
    if python3 -c 'import json,sys
try:
    data=json.load(sys.stdin)
    raise SystemExit(0 if data.get("role") == "leader" and data.get("service_ready") is True else 1)
except Exception:
    raise SystemExit(1)' <<<"$health"; then
      [[ -z "$found" ]] || return 1
      found=$index
    fi
  done
  [[ -n "$found" ]] || return 1
  echo "$found"
}

wait_for_replacement_leader() {
  local old_index=$1
  local timeout=$2
  local deadline=$((SECONDS + timeout))
  local current
  while ((SECONDS < deadline)); do
    current=$(leader_index 2>/dev/null || true)
    if [[ -n "$current" && "$current" != "$old_index" ]]; then
      echo "$current"
      return 0
    fi
    sleep 0.2
  done
  return 1
}

count_queue_failures() {
  local log=$1
  [[ -f "$log" ]] || { echo 0; return; }
  grep -Fc "PutEnd: OpLog queue failed" "$log" || true
}

cleanup() {
  local status=$?
  if [[ -n "${CLIENT_PID:-}" ]] && kill -0 "$CLIENT_PID" 2>/dev/null; then
    touch "$CONTROL_DIR/stop" 2>/dev/null || true
    wait "$CLIENT_PID" 2>/dev/null || true
  fi
  if [[ -n "${RUN_DIR:-}" && -d "$RUN_DIR" ]]; then
    "$CLUSTER_SCRIPT" down --run-dir "$RUN_DIR" >/dev/null 2>&1 || true
  fi
  exit "$status"
}

write_result() {
  local killed_leader=$1
  local replacement_leader=$2
  local counts_csv=$3
  local result_file="$RUN_DIR/workload/result-summary.json"
  if python3 - "$CONTROL_DIR/summary.json" "$RUN_DIR/audit/verify.json" \
    "$result_file" "$killed_leader" "$replacement_leader" "$counts_csv" <<'PY'
import json
import sys

workload_path, verify_path, result_path, killed, replacement, counts_csv = sys.argv[1:]
with open(workload_path, encoding="utf-8") as stream:
    workload = json.load(stream)
with open(verify_path, encoding="utf-8") as stream:
    verify = json.load(stream)
counts = [int(value) for value in counts_csv.split(",")]
phases = workload["phases"]
healthy = (
    not workload["timed_out"]
    and workload["reconnected"]
    and phases["pre"]["put_ok"] > 0
    and phases["pre"]["get_ok"] > 0
    and phases["post"]["put_ok"] > 0
    and phases["post"]["get_errors"] == 0
    and phases["post"]["get_ok"] == phases["post"]["put_ok"]
    and all(phase["mismatch"] == 0 for phase in phases.values())
    and workload["probe"] == {
        "put": "OK",
        "get": "OK",
        "batch_smoke": "OK",
    }
    and sum(counts) == 0
    and verify.get("ok") is True
)
result = {
    "healthy": healthy,
    "killed_leader": f"master-{killed}",
    "replacement_leader": f"master-{replacement}",
    "putend_oplog_queue_failures": {
        f"master-{index}": count for index, count in enumerate(counts)
    },
    "putend_oplog_queue_failures_total": sum(counts),
    "workload": workload,
    "oplog_verify": verify,
}
with open(result_path, "w", encoding="utf-8") as stream:
    json.dump(result, stream, indent=2, sort_keys=True)
    stream.write("\n")
print(json.dumps(result, indent=2, sort_keys=True))
raise SystemExit(0 if healthy else 1)
PY
  then
    return 0
  fi
  return 1
}

main() {
  parse_options "$@"
  command -v python3 >/dev/null || die "missing executable: python3"
  command -v curl >/dev/null || die "missing executable: curl"
  [[ -x "$CLUSTER_SCRIPT" ]] || die "missing cluster script: $CLUSTER_SCRIPT"
  [[ -x "$FAULT_CTL" ]] || die "missing fault controller: $FAULT_CTL"
  local client_bin="$BUILD_DIR/mooncake-store/tests/e2e/oplog_concurrent_failover_client"
  [[ -x "$client_bin" ]] || die "missing executable: $client_bin"

  if [[ -z "$RUN_DIR" ]]; then
    RUN_DIR="/tmp/mooncake-oplog-concurrent-failover-$(date +%Y%m%d-%H%M%S)-$$"
  fi
  CONTROL_DIR="$RUN_DIR/workload/control"
  trap cleanup EXIT

  "$CLUSTER_SCRIPT" up --run-dir "$RUN_DIR" --build-dir "$BUILD_DIR" \
    --masters 3 --cluster-id "oplog-concurrent-$$" \
    --batch-entries "$BATCH_ENTRIES" --timeout-sec "$START_TIMEOUT_SEC"
  # shellcheck disable=SC1090
  source "$RUN_DIR/cluster.env"
  read -r -a ADMIN_PORT_ARRAY <<<"$ADMIN_PORTS"
  mkdir -p "$CONTROL_DIR"

  local client_port
  client_port=$(find_free_port)
  MC_STORE_CLUSTER_ID="$CLUSTER_ID" LSAN_OPTIONS=detect_leaks=0 \
    "$client_bin" --master_server_entry="etcd://$ETCD_ENDPOINTS" \
    --engine_meta_url="http://127.0.0.1:$METADATA_PORT/metadata" \
    --protocol="$PROTOCOL" --client_port="$client_port" \
    --control_dir="$CONTROL_DIR" --key_prefix="concurrent-$CLUSTER_ID" \
    --threads="$THREADS" --segment_bytes="$SEGMENT_BYTES" \
    --value_bytes="$VALUE_BYTES" \
    --max_runtime_sec="$((START_TIMEOUT_SEC * 3))" \
    >"$RUN_DIR/workload/client.log" 2>&1 &
  CLIENT_PID=$!

  wait_for_file "$CONTROL_DIR/ready" "$START_TIMEOUT_SEC" "$CLIENT_PID" ||
    die "client workload did not reach the ready state"
  sleep "$PRE_SECONDS"

  local killed_leader replacement_leader
  killed_leader=$(leader_index) || die "failed to identify the current leader"
  printf 'fault\n' >"$CONTROL_DIR/phase"
  "$FAULT_CTL" process kill --run-dir "$RUN_DIR" \
    --name "master-$killed_leader"
  replacement_leader=$(wait_for_replacement_leader "$killed_leader" \
    "$START_TIMEOUT_SEC") || die "cluster did not elect a replacement leader"
  touch "$CONTROL_DIR/leader_ready"

  wait_for_file "$CONTROL_DIR/reconnected" "$START_TIMEOUT_SEC" "$CLIENT_PID" ||
    die "surviving client did not reconnect to the replacement leader"
  printf 'post\n' >"$CONTROL_DIR/phase"
  sleep "$POST_SECONDS"
  touch "$CONTROL_DIR/stop"
  wait "$CLIENT_PID" || die "client workload failed"
  CLIENT_PID=""

  "$CLUSTER_SCRIPT" collect --run-dir "$RUN_DIR"
  local -a queue_failures=()
  local index
  for index in 0 1 2; do
    queue_failures+=("$(count_queue_failures "$RUN_DIR/logs/master-$index.err")")
  done
  local counts_csv
  counts_csv=$(IFS=,; echo "${queue_failures[*]}")

  local status=0
  write_result "$killed_leader" "$replacement_leader" "$counts_csv" || status=$?
  echo "artifacts: $RUN_DIR"
  if ((status == 0)); then
    echo "PASS: concurrent failover preserved acknowledged writes and reads"
  else
    echo "FAIL: concurrent failover exposed an HA correctness issue" >&2
  fi
  return "$status"
}

main "$@"
