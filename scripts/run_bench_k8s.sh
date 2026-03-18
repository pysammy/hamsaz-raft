#!/usr/bin/env bash
set -euo pipefail

# Run true multi-client benchmark against a Kubernetes StatefulSet deployment.
# Defaults assume local kind/minikube and manifests under deploy/k8s/.

NAMESPACE="${NAMESPACE:-default}"
STATEFULSET="${STATEFULSET:-raft-node}"
SERVICE="${SERVICE:-raft-headless}"
NODES="${NODES:-3}"
OPS="${OPS:-1500}"
CONFLICT_RATIO="${CONFLICT_RATIO:-0.25}"
DEPENDENT_RATIO="${DEPENDENT_RATIO:-0.25}"
CONCURRENCY="${CONCURRENCY:-16}"
API_BASE_PORT="${API_BASE_PORT:-37001}"
WAIT_TIMEOUT="${WAIT_TIMEOUT:-600s}"
OUT_DIR="${OUT_DIR:-artifacts/bench_k8s}"

# Optional helpers for local kind flows.
LOAD_IMAGE="${LOAD_IMAGE:-0}"            # 1 -> run `kind load docker-image`
IMAGE="${IMAGE:-hamsaz-raft:latest}"
KIND_CLUSTER="${KIND_CLUSTER:-hamsaz}"

APPLY_MANIFESTS="${APPLY_MANIFESTS:-1}"  # 1 -> apply deploy/k8s/*.yaml
SHUTDOWN_AFTER="${SHUTDOWN_AFTER:-0}"    # 1 -> send SHUTDOWN after benchmark
VERIFY_CORRECTNESS="${VERIFY_CORRECTNESS:-1}"
FAIL_ON_CORRECTNESS="${FAIL_ON_CORRECTNESS:-1}"
FAIL_ON_OP_ERRORS="${FAIL_ON_OP_ERRORS:-1}"
MAX_RETRIES="${MAX_RETRIES:-3}"
RETRY_DELAY_MS="${RETRY_DELAY_MS:-10}"
REQUEST_TIMEOUT_SEC="${REQUEST_TIMEOUT_SEC:-5}"
SETTLE_TIMEOUT_SEC="${SETTLE_TIMEOUT_SEC:-60}"
SETTLE_POLL_MS="${SETTLE_POLL_MS:-200}"
DRAIN_TIMEOUT_SEC="${DRAIN_TIMEOUT_SEC:-3}"
DRAIN_POLL_MS="${DRAIN_POLL_MS:-200}"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing command: $1" >&2
    exit 1
  fi
}

require_cmd kubectl
require_cmd python3

if [[ "${LOAD_IMAGE}" == "1" ]]; then
  require_cmd kind
  echo "[k8s-bench] loading image ${IMAGE} into kind cluster ${KIND_CLUSTER}"
  kind load docker-image "${IMAGE}" --name "${KIND_CLUSTER}"
fi

if [[ "${APPLY_MANIFESTS}" == "1" ]]; then
  echo "[k8s-bench] applying manifests"
  kubectl apply -f deploy/k8s/raft-headless-service.yaml >/dev/null
  kubectl apply -f deploy/k8s/raft-statefulset.yaml >/dev/null
fi

echo "[k8s-bench] scaling ${STATEFULSET} to ${NODES} replicas"
kubectl -n "${NAMESPACE}" scale "statefulset/${STATEFULSET}" --replicas="${NODES}" >/dev/null
kubectl -n "${NAMESPACE}" set env "statefulset/${STATEFULSET}" "NODES=${NODES}" >/dev/null
kubectl -n "${NAMESPACE}" rollout status "statefulset/${STATEFULSET}" --timeout="${WAIT_TIMEOUT}" >/dev/null
kubectl -n "${NAMESPACE}" wait --for=condition=Ready pod -l app=raft-node --timeout=180s >/dev/null

echo "[k8s-bench] current pod placement"
kubectl -n "${NAMESPACE}" get pods -l app=raft-node -o wide

echo "[k8s-bench] starting port-forwards"
declare -a PF_PIDS=()
declare -a LOCAL_PORTS=()
HOSTS=""

cleanup() {
  for pid in "${PF_PIDS[@]:-}"; do
    kill "${pid}" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

for i in $(seq 0 $((NODES - 1))); do
  pod="${STATEFULSET}-${i}"
  local_port=$((API_BASE_PORT + i))
  LOCAL_PORTS+=("${local_port}")
  kubectl -n "${NAMESPACE}" port-forward "pod/${pod}" "${local_port}:25000" >"/tmp/${pod}-pf.log" 2>&1 &
  PF_PIDS+=("$!")
  if [[ -z "${HOSTS}" ]]; then
    HOSTS="127.0.0.1:${local_port}"
  else
    HOSTS="${HOSTS},127.0.0.1:${local_port}"
  fi
done

sleep 5
for pid in "${PF_PIDS[@]}"; do
  if ! kill -0 "${pid}" 2>/dev/null; then
    echo "[k8s-bench] a port-forward process exited unexpectedly" >&2
    exit 2
  fi
done

wait_local_port() {
  local port="$1"
  local deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    if python3 - "$port" <<'PY'
import socket
import sys
p = int(sys.argv[1])
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(0.5)
try:
    s.connect(("127.0.0.1", p))
    s.close()
    sys.exit(0)
except Exception:
    sys.exit(1)
PY
    then
      return 0
    fi
    sleep 0.5
  done
  return 1
}

for p in "${LOCAL_PORTS[@]}"; do
  if ! wait_local_port "${p}"; then
    echo "[k8s-bench] local port ${p} did not become ready in time" >&2
    exit 3
  fi
done

echo "[k8s-bench] running external benchmark"
cmd=(
  python3 bench/bench_multiclient_external.py
  --hosts "${HOSTS}"
  --wait-members "${NODES}"
  --ops "${OPS}"
  --conflict-ratio "${CONFLICT_RATIO}"
  --dependent-ratio "${DEPENDENT_RATIO}"
  --concurrency "${CONCURRENCY}"
  --drain-timeout-sec "${DRAIN_TIMEOUT_SEC}"
  --drain-poll-ms "${DRAIN_POLL_MS}"
  --settle-timeout-sec "${SETTLE_TIMEOUT_SEC}"
  --settle-poll-ms "${SETTLE_POLL_MS}"
  --out "${OUT_DIR}"
)
if [[ "${VERIFY_CORRECTNESS}" == "1" ]]; then
  cmd+=(--verify-correctness)
fi
if [[ "${FAIL_ON_CORRECTNESS}" == "1" ]]; then
  cmd+=(--fail-on-correctness)
fi
if [[ "${FAIL_ON_OP_ERRORS}" == "1" ]]; then
  cmd+=(--fail-on-op-errors)
fi
if [[ "${SHUTDOWN_AFTER}" == "1" ]]; then
  cmd+=(--shutdown-after)
fi
cmd+=(--max-retries "${MAX_RETRIES}" --retry-delay-ms "${RETRY_DELAY_MS}")
cmd+=(--request-timeout-sec "${REQUEST_TIMEOUT_SEC}")
"${cmd[@]}"

echo "[k8s-bench] done"
