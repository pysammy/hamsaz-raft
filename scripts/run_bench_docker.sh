#!/usr/bin/env bash
set -euo pipefail

# Runs true multi-client benchmark against a Docker Compose cluster.
# Requirements: docker + docker compose + python3 (host).

NODES="${NODES:-10}"
OPS="${OPS:-1500}"
CONFLICT_RATIO="${CONFLICT_RATIO:-0.25}"
DEPENDENT_RATIO="${DEPENDENT_RATIO:-0.25}"
CONCURRENCY="${CONCURRENCY:-32}"
RAFT_PORT="${RAFT_PORT:-15000}"
API_PORT_IN_CONTAINER="${API_PORT_IN_CONTAINER:-25000}"
API_HOST_BASE="${API_HOST_BASE:-35000}"
IMAGE="${IMAGE:-hamsaz-raft:latest}"
GOSSIP_DELAY="${GOSSIP_DELAY:-2-8}"
GOSSIP_DROP="${GOSSIP_DROP:-0.0}"
KEEP_UP="${KEEP_UP:-0}"
OUT_DIR="${OUT_DIR:-artifacts/bench_docker}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker command not found" >&2
  exit 1
fi
if ! docker compose version >/dev/null 2>&1; then
  echo "docker compose command not found" >&2
  exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 command not found" >&2
  exit 1
fi

STAMP="$(date +%Y%m%d-%H%M%S)"
WORK_DIR="artifacts/docker/${STAMP}"
mkdir -p "${WORK_DIR}"
COMPOSE_FILE="${WORK_DIR}/docker-compose.yml"
PROJECT_NAME="hamsazbench${STAMP}"

echo "[docker-bench] building image ${IMAGE}"
docker build -t "${IMAGE}" .

echo "[docker-bench] generating compose file at ${COMPOSE_FILE}"
{
  echo "services:"
  for ((i=1; i<=NODES; ++i)); do
    api_host_port=$((API_HOST_BASE + i))
    cmd="/app/raft_node_server --id ${i} --host node${i} --port ${RAFT_PORT} --api-port ${API_PORT_IN_CONTAINER} --inproc --gossip-delay ${GOSSIP_DELAY} --gossip-drop ${GOSSIP_DROP}"
    for ((j=1; j<=NODES; ++j)); do
      if [[ "${j}" -eq "${i}" ]]; then
        continue
      fi
      cmd="${cmd} --peer ${j}:node${j}:${RAFT_PORT}"
    done
    cat <<EOF
  node${i}:
    image: ${IMAGE}
    container_name: ${PROJECT_NAME}_node${i}
    command: >-
      ${cmd}
    ports:
      - "${api_host_port}:${API_PORT_IN_CONTAINER}"
EOF
  done
} > "${COMPOSE_FILE}"

cleanup() {
  if [[ "${KEEP_UP}" == "1" ]]; then
    echo "[docker-bench] KEEP_UP=1; leaving cluster running"
    return
  fi
  echo "[docker-bench] tearing down compose stack"
  docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "[docker-bench] starting cluster"
docker compose -p "${PROJECT_NAME}" -f "${COMPOSE_FILE}" up -d --remove-orphans

hosts=""
for ((i=1; i<=NODES; ++i)); do
  h="127.0.0.1:$((API_HOST_BASE + i))"
  if [[ -z "${hosts}" ]]; then
    hosts="${h}"
  else
    hosts="${hosts},${h}"
  fi
done

echo "[docker-bench] running external benchmark against: ${hosts}"
python3 bench/bench_multiclient_external.py \
  --hosts "${hosts}" \
  --ops "${OPS}" \
  --conflict-ratio "${CONFLICT_RATIO}" \
  --dependent-ratio "${DEPENDENT_RATIO}" \
  --concurrency "${CONCURRENCY}" \
  --wait-members "${NODES}" \
  --out "${OUT_DIR}"

echo "[docker-bench] done"
